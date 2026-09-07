"""
Tests for automation/snapshot_cache.py

Covers:
- Snapshot structure validation
- Cache validation (previous + current)
- Previous/current detection
- Timestamp read/write
- Metadata read/write
- Rotation logic
- Tarball creation/extraction
- Edge cases (missing dirs, empty cache, etc.)
"""

import json
import os
import shutil
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "automation"))
from snapshot_cache import (
    validate_snapshot_structure,
    validate_cache,
    get_previous_local_path,
    get_current_local_path,
    get_previous_timestamp,
    get_current_timestamp,
    rotate_snapshots,
    write_metadata,
    read_metadata,
    write_new_timestamp,
    read_new_timestamp,
    create_tarball,
    extract_tarball,
    ensure_cache_dirs,
    _find_snapshot_ts,
    METADATA_FILE,
    PREVIOUS_DIR,
    CURRENT_DIR,
    TIMESTAMP_FILE,
)


def _make_snapshot(base: Path, ts: str, complete=True, parts=4):
    """Create a minimal valid snapshot directory."""
    snap = base / ts
    snap.mkdir(parents=True, exist_ok=True)

    # _COMPLETE.json
    status = "complete" if complete else "incomplete"
    (snap / "_COMPLETE.json").write_text(json.dumps({"status": status}))

    # manifests
    (snap / "manifests").mkdir(exist_ok=True)

    # dataset folders with part files
    datasets = ["allocated_limit", "works_recommended"]
    for i, ds in enumerate(datasets):
        ds_dir = snap / ds
        ds_dir.mkdir(exist_ok=True)
        for j in range(max(1, parts // len(datasets))):
            (ds_dir / f"part_{j+1:04d}.ndjson").write_text("{}\n")

    return snap


class TestValidateSnapshotStructure(unittest.TestCase):
    def test_valid_snapshot(self):
        with tempfile.TemporaryDirectory() as td:
            snap = _make_snapshot(Path(td), "2026-09-01T00-00-00Z")
            ok, err = validate_snapshot_structure(snap)
            self.assertTrue(ok)
            self.assertEqual(err, "")

    def test_missing_dir(self):
        ok, err = validate_snapshot_structure(Path("/nonexistent"))
        self.assertFalse(ok)
        self.assertIn("does not exist", err)

    def test_not_a_dir(self):
        with tempfile.NamedTemporaryFile() as f:
            ok, err = validate_snapshot_structure(Path(f.name))
            self.assertFalse(ok)
            self.assertIn("not a directory", err)

    def test_missing_complete_json(self):
        with tempfile.TemporaryDirectory() as td:
            snap = Path(td) / "2026-09-01T00-00-00Z"
            snap.mkdir()
            (snap / "allocated_limit").mkdir()
            (snap / "allocated_limit" / "part_0001.ndjson").write_text("{}\n")
            ok, err = validate_snapshot_structure(snap)
            self.assertFalse(ok)
            self.assertIn("_COMPLETE.json", err)

    def test_incomplete_status(self):
        with tempfile.TemporaryDirectory() as td:
            snap = _make_snapshot(Path(td), "2026-09-01T00-00-00Z", complete=False)
            ok, err = validate_snapshot_structure(snap)
            self.assertFalse(ok)
            self.assertIn("not 'complete'", err)

    def test_no_part_files(self):
        with tempfile.TemporaryDirectory() as td:
            snap = Path(td) / "2026-09-01T00-00-00Z"
            snap.mkdir()
            (snap / "_COMPLETE.json").write_text(json.dumps({"status": "complete"}))
            ok, err = validate_snapshot_structure(snap)
            self.assertFalse(ok)
            self.assertIn("part_*.ndjson", err)


class TestFindSnapshotTs(unittest.TestCase):
    def test_finds_ts(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            (base / "2026-09-01T00-00-00Z").mkdir()
            (base / "2026-09-02T00-00-00Z").mkdir()
            ts = _find_snapshot_ts(base)
            self.assertEqual(ts, "2026-09-01T00-00-00Z")

    def test_empty_dir(self):
        with tempfile.TemporaryDirectory() as td:
            ts = _find_snapshot_ts(Path(td))
            self.assertIsNone(ts)

    def test_nonexistent_dir(self):
        ts = _find_snapshot_ts(Path("/nonexistent"))
        self.assertIsNone(ts)

    def test_skips_hidden(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            (base / ".hidden").mkdir()
            (base / "2026-09-01T00-00-00Z").mkdir()
            ts = _find_snapshot_ts(base)
            self.assertEqual(ts, "2026-09-01T00-00-00Z")


class TestValidateCache(unittest.TestCase):
    def test_valid_cache(self):
        with tempfile.TemporaryDirectory() as td:
            work = Path(td)
            prev = work / PREVIOUS_DIR / "2026-09-01T00-00-00Z"
            curr = work / CURRENT_DIR / "2026-09-02T00-00-00Z"
            _make_snapshot(work / PREVIOUS_DIR, "2026-09-01T00-00-00Z")
            _make_snapshot(work / CURRENT_DIR, "2026-09-02T00-00-00Z")
            ok, err = validate_cache(work)
            self.assertTrue(ok)

    def test_missing_previous(self):
        with tempfile.TemporaryDirectory() as td:
            work = Path(td)
            _make_snapshot(work / CURRENT_DIR, "2026-09-02T00-00-00Z")
            ok, err = validate_cache(work)
            self.assertFalse(ok)
            self.assertIn("previous", err.lower())

    def test_missing_current(self):
        with tempfile.TemporaryDirectory() as td:
            work = Path(td)
            _make_snapshot(work / PREVIOUS_DIR, "2026-09-01T00-00-00Z")
            ok, err = validate_cache(work)
            self.assertFalse(ok)
            self.assertIn("current", err.lower())

    def test_no_work_dir(self):
        ok, err = validate_cache(Path("/nonexistent"))
        self.assertFalse(ok)
        self.assertIn("does not exist", err)


class TestGetPreviousLocalPath(unittest.TestCase):
    def test_returns_path(self):
        with tempfile.TemporaryDirectory() as td:
            work = Path(td)
            _make_snapshot(work / PREVIOUS_DIR, "2026-09-01T00-00-00Z")
            _make_snapshot(work / CURRENT_DIR, "2026-09-02T00-00-00Z")
            path = get_previous_local_path(work)
            self.assertIsNotNone(path)
            self.assertEqual(path.name, "2026-09-01T00-00-00Z")

    def test_returns_none_when_missing(self):
        with tempfile.TemporaryDirectory() as td:
            path = get_previous_local_path(Path(td))
            self.assertIsNone(path)


class TestGetCurrentLocalPath(unittest.TestCase):
    def test_returns_path(self):
        with tempfile.TemporaryDirectory() as td:
            work = Path(td)
            _make_snapshot(work / PREVIOUS_DIR, "2026-09-01T00-00-00Z")
            _make_snapshot(work / CURRENT_DIR, "2026-09-02T00-00-00Z")
            path = get_current_local_path(work)
            self.assertIsNotNone(path)
            self.assertEqual(path.name, "2026-09-02T00-00-00Z")

    def test_returns_none_when_missing(self):
        with tempfile.TemporaryDirectory() as td:
            path = get_current_local_path(Path(td))
            self.assertIsNone(path)


class TestGetTimestamps(unittest.TestCase):
    def test_previous_timestamp(self):
        with tempfile.TemporaryDirectory() as td:
            work = Path(td)
            _make_snapshot(work / PREVIOUS_DIR, "2026-09-01T00-00-00Z")
            ts = get_previous_timestamp(work)
            self.assertEqual(ts, "2026-09-01T00-00-00Z")

    def test_current_timestamp(self):
        with tempfile.TemporaryDirectory() as td:
            work = Path(td)
            _make_snapshot(work / CURRENT_DIR, "2026-09-02T00-00-00Z")
            ts = get_current_timestamp(work)
            self.assertEqual(ts, "2026-09-02T00-00-00Z")


class TestRotateSnapshots(unittest.TestCase):
    def test_basic_rotation(self):
        with tempfile.TemporaryDirectory() as td:
            work = Path(td)
            _make_snapshot(work / PREVIOUS_DIR, "2026-09-01T00-00-00Z")
            _make_snapshot(work / CURRENT_DIR, "2026-09-02T00-00-00Z")

            new_snap = _make_snapshot(Path(td) / "new", "2026-09-03T00-00-00Z")
            rotate_snapshots("2026-09-03T00-00-00Z", new_snap, work)

            # Previous should now be 09-02
            prev_ts = get_previous_timestamp(work)
            self.assertEqual(prev_ts, "2026-09-02T00-00-00Z")

            # Current should be 09-03
            curr_ts = get_current_timestamp(work)
            self.assertEqual(curr_ts, "2026-09-03T00-00-00Z")

    def test_rotation_preserves_data(self):
        with tempfile.TemporaryDirectory() as td:
            work = Path(td)
            prev = _make_snapshot(work / PREVIOUS_DIR, "2026-09-01T00-00-00Z")
            curr = _make_snapshot(work / CURRENT_DIR, "2026-09-02T00-00-00Z")

            # Write unique data to current
            (curr / "allocated_limit" / "part_0001.ndjson").write_text('{"id": 1}\n')

            new_snap = _make_snapshot(Path(td) / "new", "2026-09-03T00-00-00Z")
            rotate_snapshots("2026-09-03T00-00-00Z", new_snap, work)

            # Previous should have the unique data from old current
            prev_path = get_previous_local_path(work)
            data = (prev_path / "allocated_limit" / "part_0001.ndjson").read_text()
            self.assertIn('"id": 1', data)


class TestMetadata(unittest.TestCase):
    def test_write_read_metadata(self):
        with tempfile.TemporaryDirectory() as td:
            work = Path(td)
            write_metadata("2026-09-01T00-00-00Z", "2026-09-02T00-00-00Z", work)
            meta = read_metadata(work)
            self.assertIsNotNone(meta)
            self.assertEqual(meta["previous_ts"], "2026-09-01T00-00-00Z")
            self.assertEqual(meta["current_ts"], "2026-09-02T00-00-00Z")

    def test_read_missing_metadata(self):
        with tempfile.TemporaryDirectory() as td:
            meta = read_metadata(Path(td))
            self.assertIsNone(meta)


class TestTimestampFile(unittest.TestCase):
    def test_write_read_timestamp(self):
        with tempfile.TemporaryDirectory() as td:
            work = Path(td)
            write_new_timestamp("2026-09-02T00-00-00Z", work)
            ts = read_new_timestamp(work)
            self.assertEqual(ts, "2026-09-02T00-00-00Z")

    def test_read_missing_timestamp(self):
        with tempfile.TemporaryDirectory() as td:
            ts = read_new_timestamp(Path(td))
            self.assertIsNone(ts)


class TestTarball(unittest.TestCase):
    def test_create_extract_tarball(self):
        with tempfile.TemporaryDirectory() as td:
            src = _make_snapshot(Path(td) / "src", "2026-09-01T00-00-00Z")
            tarball = Path(td) / "test.tar.gz"
            create_tarball(src, tarball)
            self.assertTrue(tarball.exists())

            extract_dir = Path(td) / "extracted"
            extract_tarball(tarball, extract_dir)
            self.assertTrue((extract_dir / "2026-09-01T00-00-00Z").exists())
            self.assertTrue((extract_dir / "2026-09-01T00-00-00Z" / "_COMPLETE.json").exists())


class TestEnsureCacheDirs(unittest.TestCase):
    def test_creates_dirs(self):
        with tempfile.TemporaryDirectory() as td:
            work = Path(td) / "cache"
            ensure_cache_dirs(work)
            self.assertTrue((work / PREVIOUS_DIR).exists())
            self.assertTrue((work / CURRENT_DIR).exists())


if __name__ == "__main__":
    unittest.main()
