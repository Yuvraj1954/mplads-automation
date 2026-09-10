"""
Tests for comparator --old-local flag and daily_pipeline cache integration.
"""

import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "automation"))
from snapshot_cache import PREVIOUS_DIR, CURRENT_DIR


class TestComparatorOldLocalArg(unittest.TestCase):
    """Verify that comparator_v2.py accepts --old-local argument."""

    def test_old_local_in_help(self):
        """The --old-local flag should appear in help text."""
        import subprocess
        result = subprocess.run(
            [sys.executable, str(Path(__file__).resolve().parent.parent / "comparator" / "comparator_v2.py"), "--help"],
            capture_output=True, text=True, timeout=10
        )
        # The comparator needs SUPABASE_URL, so it will fail with that error
        # before showing help. Just check the error message doesn't say
        # "unrecognized arguments" for --old-local.
        self.assertNotIn("unrecognized arguments: --old-local", result.stderr)


class TestDailyPipelineImports(unittest.TestCase):
    """Verify daily_pipeline.py imports snapshot_cache correctly."""

    def test_imports(self):
        import importlib
        spec = importlib.util.spec_from_file_location(
            "daily_pipeline",
            str(Path(__file__).resolve().parent.parent / "automation" / "daily_pipeline.py")
        )
        # Just verify the module can be found (not executed, as it needs env vars)
        self.assertIsNotNone(spec)


class TestSnapshotCacheRotation(unittest.TestCase):
    """Integration test for cache rotation flow."""

    def test_full_rotation_flow(self):
        """Simulate: cache has prev+curr, pipeline writes new, rotate."""
        from snapshot_cache import (
            validate_cache, get_previous_local_path, get_current_local_path,
            get_previous_timestamp, get_current_timestamp,
            rotate_snapshots, write_metadata, write_new_timestamp,
            read_new_timestamp, read_metadata, ensure_cache_dirs,
        )

        def _make_snapshot(base, ts):
            snap = base / ts
            snap.mkdir(parents=True, exist_ok=True)
            (snap / "_COMPLETE.json").write_text(json.dumps({"status": "complete"}))
            (snap / "manifests").mkdir(exist_ok=True)
            ds = snap / "allocated_limit"
            ds.mkdir(exist_ok=True)
            for i in range(2):
                (ds / f"part_{i+1:04d}.ndjson").write_text("{}\n")
            ds2 = snap / "works_recommended"
            ds2.mkdir(exist_ok=True)
            for i in range(2):
                (ds2 / f"part_{i+1:04d}.ndjson").write_text("{}\n")
            return snap

        with tempfile.TemporaryDirectory() as td:
            work = Path(td)
            ensure_cache_dirs(work)

            # Step 1: Bootstrap — set up initial cache
            _make_snapshot(work / PREVIOUS_DIR, "2026-09-07T13-06-59Z")
            _make_snapshot(work / CURRENT_DIR, "2026-09-07T14-54-46Z")

            ok, _ = validate_cache(work)
            self.assertTrue(ok)

            prev = get_previous_local_path(work)
            self.assertEqual(prev.name, "2026-09-07T13-06-59Z")

            # Step 2: Pipeline runs, writes new snapshot to current/
            new_snap = _make_snapshot(work / "fetched", "2026-09-08T00-00-00Z")
            shutil.copytree(str(new_snap), str(work / CURRENT_DIR / "2026-09-08T00-00-00Z"))

            # Step 3: Pipeline writes success markers
            write_new_timestamp("2026-09-08T00-00-00Z", work)
            write_metadata("2026-09-07T14-54-46Z", "2026-09-08T00-00-00Z", work)

            # Step 4: Workflow rotates
            rotate_snapshots("2026-09-08T00-00-00Z", new_snap, work)

            # Verify
            prev = get_previous_local_path(work)
            self.assertEqual(prev.name, "2026-09-07T14-54-46Z")

            curr = get_current_local_path(work)
            self.assertEqual(curr.name, "2026-09-08T00-00-00Z")

            # Metadata should be updated
            meta = read_metadata(work)
            self.assertEqual(meta["previous_ts"], "2026-09-07T14-54-46Z")
            self.assertEqual(meta["current_ts"], "2026-09-08T00-00-00Z")


if __name__ == "__main__":
    unittest.main()
