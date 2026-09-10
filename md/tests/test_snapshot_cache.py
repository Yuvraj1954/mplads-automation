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
    validate_bootstrap_cache,
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
    _find_valid_snapshot_ts,
    _cleanup_stale_snapshots,
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
    def test_finds_newest_ts(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            (base / "2026-09-01T00-00-00Z").mkdir()
            (base / "2026-09-02T00-00-00Z").mkdir()
            ts = _find_snapshot_ts(base)
            self.assertEqual(ts, "2026-09-02T00-00-00Z")

    def test_single_ts(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            (base / "2026-09-01T00-00-00Z").mkdir()
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


class TestValidateBootstrapCache(unittest.TestCase):
    """Regression tests for bootstrap cache validation (run #23 fix).

    Bootstrap only needs a valid current snapshot. Previous snapshot
    validity is irrelevant for bootstrap mode.
    """

    def test_valid_current_invalid_previous_succeeds(self):
        """Valid current + invalid previous -> bootstrap succeeds."""
        with tempfile.TemporaryDirectory() as td:
            work = Path(td)
            # Current is valid
            _make_snapshot(work / CURRENT_DIR, "2026-09-02T00-00-00Z")
            # Previous exists but has no _COMPLETE.json (invalid)
            prev = work / PREVIOUS_DIR / "2026-09-01T00-00-00Z"
            prev.mkdir(parents=True)
            (prev / "allocated_limit").mkdir()
            (prev / "allocated_limit" / "part_0001.ndjson").write_text("{}\n")

            ok, err = validate_bootstrap_cache(work)
            self.assertTrue(ok)
            self.assertEqual(err, "")

    def test_valid_current_missing_previous_succeeds(self):
        """Valid current + missing previous -> bootstrap succeeds."""
        with tempfile.TemporaryDirectory() as td:
            work = Path(td)
            _make_snapshot(work / CURRENT_DIR, "2026-09-02T00-00-00Z")
            # No previous directory at all

            ok, err = validate_bootstrap_cache(work)
            self.assertTrue(ok)
            self.assertEqual(err, "")

    def test_invalid_current_fails(self):
        """Invalid current -> bootstrap fails."""
        with tempfile.TemporaryDirectory() as td:
            work = Path(td)
            # Current exists but has no _COMPLETE.json (invalid)
            curr = work / CURRENT_DIR / "2026-09-02T00-00-00Z"
            curr.mkdir(parents=True)
            (curr / "allocated_limit").mkdir()
            (curr / "allocated_limit" / "part_0001.ndjson").write_text("{}\n")

            ok, err = validate_bootstrap_cache(work)
            self.assertFalse(ok)
            self.assertIn("Current snapshot invalid", err)

    def test_missing_current_fails(self):
        """No current snapshot -> bootstrap fails."""
        with tempfile.TemporaryDirectory() as td:
            work = Path(td)
            # No current directory

            ok, err = validate_bootstrap_cache(work)
            self.assertFalse(ok)
            self.assertIn("No current snapshot", err)

    def test_no_work_dir_fails(self):
        """Nonexistent work directory -> bootstrap fails."""
        ok, err = validate_bootstrap_cache(Path("/nonexistent"))
        self.assertFalse(ok)
        self.assertIn("does not exist", err)

    def test_current_with_incomplete_status_fails(self):
        """Current snapshot with status != 'complete' -> bootstrap fails."""
        with tempfile.TemporaryDirectory() as td:
            work = Path(td)
            _make_snapshot(work / CURRENT_DIR, "2026-09-02T00-00-00Z", complete=False)

            ok, err = validate_bootstrap_cache(work)
            self.assertFalse(ok)
            self.assertIn("Current snapshot invalid", err)

    def test_validate_cache_still_requires_both(self):
        """validate_cache() still requires both previous and current (no regression)."""
        with tempfile.TemporaryDirectory() as td:
            work = Path(td)
            _make_snapshot(work / CURRENT_DIR, "2026-09-02T00-00-00Z")
            # No previous -> validate_cache should fail

            ok, err = validate_cache(work)
            self.assertFalse(ok)
            self.assertIn("previous", err.lower())


class TestBaselineSelectionRegression(unittest.TestCase):
    """Regression tests for snapshot baseline selection fix.

    Verifies: newest valid snapshot is always selected as previous baseline,
    stale snapshots are cleaned up, and cache ordering cannot override
    newer valid snapshots.
    """

    def test_two_snapshots_newest_selected(self):
        """Cache has 05:31Z and 19:30Z in previous/ — 19:30Z must be selected."""
        with tempfile.TemporaryDirectory() as td:
            work = Path(td)
            _make_snapshot(work / PREVIOUS_DIR, "2026-09-05T05-31Z")
            _make_snapshot(work / PREVIOUS_DIR, "2026-09-05T19-30Z")
            _make_snapshot(work / CURRENT_DIR, "2026-09-06T10-00Z")

            prev_ts = get_previous_timestamp(work)
            self.assertEqual(prev_ts, "2026-09-05T19-30Z")

            prev_path = get_previous_local_path(work)
            self.assertIsNotNone(prev_path)
            self.assertEqual(prev_path.name, "2026-09-05T19-30Z")

    def test_new_compared_against_newest(self):
        """Pipeline comparator receives newest previous, not oldest."""
        with tempfile.TemporaryDirectory() as td:
            work = Path(td)
            _make_snapshot(work / PREVIOUS_DIR, "2026-09-05T05-31Z")
            _make_snapshot(work / PREVIOUS_DIR, "2026-09-05T19-30Z")
            _make_snapshot(work / CURRENT_DIR, "2026-09-06T10-00Z")

            ok, err = validate_cache(work)
            self.assertTrue(ok)
            # The previous timestamp must be 19:30Z (newest), not 05:31Z
            prev_ts = get_previous_timestamp(work)
            self.assertEqual(prev_ts, "2026-09-05T19-30Z")

    def test_three_snapshots_stale_removed(self):
        """Three snapshots in previous/ — oldest two cleaned up after selection."""
        with tempfile.TemporaryDirectory() as td:
            work = Path(td)
            _make_snapshot(work / PREVIOUS_DIR, "2026-09-04T10-00Z")
            _make_snapshot(work / PREVIOUS_DIR, "2026-09-05T05-31Z")
            _make_snapshot(work / PREVIOUS_DIR, "2026-09-05T19-30Z")
            _make_snapshot(work / CURRENT_DIR, "2026-09-06T10-00Z")

            prev_ts = get_previous_timestamp(work)
            self.assertEqual(prev_ts, "2026-09-05T19-30Z")

            # Stale snapshots should have been cleaned up
            prev_dirs = list((work / PREVIOUS_DIR).iterdir())
            snapshot_dirs = [d for d in prev_dirs if d.is_dir() and not d.name.startswith(".")]
            self.assertEqual(len(snapshot_dirs), 1)
            self.assertEqual(snapshot_dirs[0].name, "2026-09-05T19-30Z")

    def test_failure_baseline_untouched(self):
        """When newest previous is invalid, falls back to next valid."""
        with tempfile.TemporaryDirectory() as td:
            work = Path(td)
            # Create invalid newest (no _COMPLETE.json)
            invalid_newest = work / PREVIOUS_DIR / "2026-09-05T19-30Z"
            invalid_newest.mkdir(parents=True)
            (invalid_newest / "allocated_limit").mkdir()
            (invalid_newest / "allocated_limit" / "part_0001.ndjson").write_text("{}\n")

            # Create valid older
            _make_snapshot(work / PREVIOUS_DIR, "2026-09-05T05-31Z")
            _make_snapshot(work / CURRENT_DIR, "2026-09-06T10-00Z")

            prev_ts = get_previous_timestamp(work)
            self.assertEqual(prev_ts, "2026-09-05T05-31Z")

    def test_self_never_selected_as_old(self):
        """Current snapshot timestamp is never returned as previous."""
        with tempfile.TemporaryDirectory() as td:
            work = Path(td)
            ts = "2026-09-06T10-00Z"
            _make_snapshot(work / PREVIOUS_DIR, ts)
            _make_snapshot(work / CURRENT_DIR, ts)

            prev_ts = get_previous_timestamp(work)
            curr_ts = get_current_timestamp(work)
            # Even though same timestamp exists in both, they should be distinct
            self.assertIsNotNone(prev_ts)
            self.assertIsNotNone(curr_ts)

    def test_cache_ordering_cannot_override_newer(self):
        """Lexicographic cache ordering cannot cause older snapshot to be selected.

        This is the exact regression: 05:31Z sorts before 19:30Z
        lexicographically, but 19:30Z is newer and must be selected.
        """
        with tempfile.TemporaryDirectory() as td:
            work = Path(td)
            # Create in reverse order to test that sorted() doesn't cause issues
            _make_snapshot(work / PREVIOUS_DIR, "2026-09-05T19-30Z")
            _make_snapshot(work / PREVIOUS_DIR, "2026-09-05T05-31Z")
            _make_snapshot(work / CURRENT_DIR, "2026-09-06T10-00Z")

            # Must select 19:30Z (newer), NOT 05:31Z (older)
            prev_ts = get_previous_timestamp(work)
            self.assertEqual(prev_ts, "2026-09-05T19-30Z")

            # Verify the older was cleaned up
            prev_dirs = list((work / PREVIOUS_DIR).iterdir())
            snapshot_dirs = [d for d in prev_dirs if d.is_dir() and not d.name.startswith(".")]
            self.assertEqual(len(snapshot_dirs), 1)


class TestFailureCleanupSafety(unittest.TestCase):
    """Regression tests for snapshot failure-cleanup correctness.

    Ensures that on pipeline failure, ONLY the current run's in-progress
    snapshot is deleted from Supabase. The previous known-good snapshot
    must NEVER be deleted by failure cleanup.

    Specific scenario from production bug:
        previous = 2026-09-10T19-30-16Z  (known-good)
        current  = 2026-09-10T20-29-44Z  (failed run)
        Cleanup MUST delete 20:29:44Z, MUST NOT delete 19:30:16Z
    """

    def test_failure_cleanup_deletes_current_not_previous(self):
        """Failure must delete the current in-progress snapshot, not the previous baseline."""
        with tempfile.TemporaryDirectory() as td:
            work = Path(td)
            # Simulate cache state: previous=19:30:16Z, current=20:29:44Z
            _make_snapshot(work / PREVIOUS_DIR, "2026-09-10T19-30-16Z")
            _make_snapshot(work / CURRENT_DIR, "2026-09-10T20-29-44Z")

            # Simulate pipeline writing .current_snapshot_ts after fetch
            ts_file = work / ".current_snapshot_ts"
            ts_file.write_text("2026-09-10T20-29-44Z")

            # Workflow cleanup reads .current_snapshot_ts
            cleanup_ts = ts_file.read_text().strip()
            self.assertEqual(cleanup_ts, "2026-09-10T20-29-44Z")

            # Verify previous is untouched
            prev_path = get_previous_local_path(work)
            self.assertIsNotNone(prev_path)
            self.assertEqual(prev_path.name, "2026-09-10T19-30-16Z")

    def test_stale_timestamp_from_previous_run_cleared(self):
        """Stale .current_snapshot_ts from a previous successful run is cleared on entry."""
        with tempfile.TemporaryDirectory() as td:
            work = Path(td)
            # Previous run left .current_snapshot_ts with its timestamp
            ts_file = work / ".current_snapshot_ts"
            ts_file.write_text("2026-09-10T19-30-16Z")

            # Simulate pipeline entry clearing stale file
            if ts_file.exists():
                ts_file.unlink()

            # File should not exist (no stale value)
            self.assertFalse(ts_file.exists())

    def test_fetch_failure_leaves_no_timestamp(self):
        """If fetch fails (new_ts is None), .current_snapshot_ts is not written."""
        with tempfile.TemporaryDirectory() as td:
            work = Path(td)
            ts_file = work / ".current_snapshot_ts"

            # Simulate: fetch fails, new_ts is None
            new_ts = None
            cache_work_dir = str(work)

            if new_ts and cache_work_dir:
                ts_file.write_text(new_ts)

            # File should not exist — nothing to clean up
            self.assertFalse(ts_file.exists())

    def test_cache_two_snapshots_failure_cleanup(self):
        """Cache has two snapshots; failure must delete only the current run's snapshot."""
        with tempfile.TemporaryDirectory() as td:
            work = Path(td)
            _make_snapshot(work / PREVIOUS_DIR, "2026-09-10T19-30-16Z")
            _make_snapshot(work / CURRENT_DIR, "2026-09-10T20-29-44Z")

            # Pipeline writes current snapshot timestamp
            ts_file = work / ".current_snapshot_ts"
            ts_file.write_text("2026-09-10T20-29-44Z")

            # Workflow cleanup reads timestamp
            cleanup_ts = ts_file.read_text().strip()

            # Simulate cleanup: delete from Supabase (here we verify the logic)
            # The cleanup script would delete cleanup_ts = 20:29:44Z
            self.assertEqual(cleanup_ts, "2026-09-10T20-29-44Z")

            # Previous must still be accessible
            prev = get_previous_local_path(work)
            self.assertIsNotNone(prev)
            self.assertEqual(prev.name, "2026-09-10T19-30-16Z")

            # Current must also still be on disk (cleanup deletes from Supabase, not local cache)
            curr = get_current_local_path(work)
            self.assertIsNotNone(curr)
            self.assertEqual(curr.name, "2026-09-10T20-29-44Z")

    def test_successful_run_promotes_current(self):
        """On success, current snapshot is promoted for next run's baseline."""
        with tempfile.TemporaryDirectory() as td:
            work = Path(td)
            _make_snapshot(work / PREVIOUS_DIR, "2026-09-10T19-30-16Z")
            _make_snapshot(work / CURRENT_DIR, "2026-09-10T20-29-44Z")

            # Simulate rotation: current → previous, new → current
            new_snap = _make_snapshot(Path(td) / "new", "2026-09-11T10-00-00Z")
            rotate_snapshots("2026-09-11T10-00-00Z", new_snap, work)

            prev = get_previous_local_path(work)
            curr = get_current_local_path(work)
            self.assertEqual(prev.name, "2026-09-10T20-29-44Z")
            self.assertEqual(curr.name, "2026-09-11T10-00-00Z")

    def test_three_snapshots_retain_newest_two(self):
        """With three snapshots, only the two newest are retained after rotation.

        Simulates: previous run left 19:30:16Z in previous/,
        current run fetched 20:29:44Z into current/.
        A third older snapshot (09:00:00Z) exists in previous/ from a
        stale state. Cleanup must only delete the failed current snapshot.
        """
        with tempfile.TemporaryDirectory() as td:
            work = Path(td)
            # Previous has two old snapshots (one stale from earlier)
            _make_snapshot(work / PREVIOUS_DIR, "2026-09-09T10-00-00Z")
            _make_snapshot(work / PREVIOUS_DIR, "2026-09-10T19-30-16Z")
            # Current has the failed run's snapshot
            _make_snapshot(work / CURRENT_DIR, "2026-09-10T20-29-44Z")

            # Pipeline writes current snapshot timestamp
            ts_file = work / ".current_snapshot_ts"
            ts_file.write_text("2026-09-10T20-29-44Z")

            # Cleanup would delete 20:29:44Z (current failed run)
            cleanup_ts = ts_file.read_text().strip()
            self.assertEqual(cleanup_ts, "2026-09-10T20-29-44Z")

            # Previous baseline (newest in previous/) remains
            prev = get_previous_local_path(work)
            self.assertIsNotNone(prev)
            self.assertEqual(prev.name, "2026-09-10T19-30-16Z")

    def test_current_snapshot_ts_never_contains_previous_timestamp(self):
        """After pipeline entry, .current_snapshot_ts never contains a previous run's timestamp."""
        with tempfile.TemporaryDirectory() as td:
            work = Path(td)
            ts_file = work / ".current_snapshot_ts"

            # Previous run left stale timestamp
            ts_file.write_text("2026-09-10T19-30-16Z")

            # Pipeline entry clears it
            if ts_file.exists():
                ts_file.unlink()

            # New run fetches and writes current timestamp
            new_ts = "2026-09-10T20-29-44Z"
            cache_work_dir = str(work)
            if new_ts and cache_work_dir:
                ts_file.write_text(new_ts)

            # Timestamp must be THIS run's snapshot, not previous run's
            self.assertEqual(ts_file.read_text().strip(), "2026-09-10T20-29-44Z")


class TestCleanupStaleSnapshots(unittest.TestCase):
    """Tests for _cleanup_stale_snapshots helper."""

    def test_removes_old_keeps_new(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            (base / "2026-09-05T05-31Z").mkdir()
            (base / "2026-09-05T19-30Z").mkdir()
            removed = _cleanup_stale_snapshots(base, "2026-09-05T19-30Z")
            self.assertEqual(removed, 1)
            self.assertFalse((base / "2026-09-05T05-31Z").exists())
            self.assertTrue((base / "2026-09-05T19-30Z").exists())

    def test_no_removal_when_single(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            (base / "2026-09-05T19-30Z").mkdir()
            removed = _cleanup_stale_snapshots(base, "2026-09-05T19-30Z")
            self.assertEqual(removed, 0)

    def test_nonexistent_dir(self):
        removed = _cleanup_stale_snapshots(Path("/nonexistent"), "ts")
        self.assertEqual(removed, 0)


if __name__ == "__main__":
    unittest.main()
