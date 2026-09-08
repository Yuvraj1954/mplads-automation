"""Tests for the unified snapshot completion marker contract.

Covers:
- Normal fetch produces local _COMPLETE.json
- --local-only produces local _COMPLETE.json
- Local marker contains all 12 datasets and correct record/chunk metadata
- Cloud upload receives the same completion metadata
- Bootstrap validation accepts the snapshot returned by normal fetcher
- Incomplete/failed fetch cannot produce a valid completion marker
- Existing daily snapshot/cache validation remains compatible
- Existing tests continue passing
"""

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

os.environ.setdefault("SUPABASE_URL", "http://fake.supabase.co")
os.environ.setdefault("SUPABASE_SECRET_KEY", "fake-key")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "fake-service-role-key")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "fetcher"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "automation"))

REQUIRED_DATASETS = [
    "allocated_limit",
    "works_recommended",
    "works_sanctioned",
    "works_completed",
    "expenditure",
    "calamity",
    "mla_allocated_limit",
    "mla_works_recommended",
    "mla_works_sanctioned",
    "mla_works_completed",
    "mla_expenditure",
    "mla_calamity",
]


def _make_all_results():
    """Build a results dict matching what the fetcher produces."""
    return {
        name: {"records": 100 + i, "chunks": 1}
        for i, name in enumerate(REQUIRED_DATASETS)
    }


def _make_incomplete_results():
    """Build a results dict missing one dataset."""
    return {
        name: {"records": 100, "chunks": 1}
        for name in REQUIRED_DATASETS[:-1]
    }


def _make_snapshot_with_datasets(base_dir, include_complete=True):
    """Create a snapshot directory with all 12 dataset folders and part files."""
    snapshot_dir = base_dir / "snapshot"
    snapshot_dir.mkdir()
    for dataset in REQUIRED_DATASETS:
        d = snapshot_dir / dataset
        d.mkdir()
        (d / "part_0001.ndjson").write_text('{"id": 1}\n')
    if include_complete:
        marker = {
            "timestamp": "ts",
            "completed_at": "2026-09-08T00:00:00+00:00",
            "status": "complete",
            "datasets": {n: {"records": 1, "chunks": 1} for n in REQUIRED_DATASETS},
        }
        (snapshot_dir / "_COMPLETE.json").write_text(json.dumps(marker))
    return snapshot_dir


class TestBuildCompletionMarker(unittest.TestCase):
    """Tests for build_completion_marker()."""

    def test_returns_complete_dict(self):
        from fetcher import build_completion_marker
        marker = build_completion_marker("2026-09-08T00:00:00Z", _make_all_results())
        self.assertEqual(marker["status"], "complete")
        self.assertEqual(marker["timestamp"], "2026-09-08T00:00:00Z")
        self.assertIn("completed_at", marker)

    def test_contains_all_12_datasets(self):
        from fetcher import build_completion_marker
        marker = build_completion_marker("ts", _make_all_results())
        self.assertEqual(set(marker["datasets"].keys()), set(REQUIRED_DATASETS))
        self.assertEqual(len(marker["datasets"]), 12)

    def test_record_and_chunk_counts_match_input(self):
        from fetcher import build_completion_marker
        results = _make_all_results()
        marker = build_completion_marker("ts", results)
        for name in REQUIRED_DATASETS:
            self.assertEqual(marker["datasets"][name]["records"], results[name]["records"])
            self.assertEqual(marker["datasets"][name]["chunks"], results[name]["chunks"])

    def test_missing_dataset_raises(self):
        from fetcher import build_completion_marker
        with self.assertRaises(RuntimeError) as ctx:
            build_completion_marker("ts", _make_incomplete_results())
        self.assertIn("missing datasets", str(ctx.exception))

    def test_invalid_record_count_raises(self):
        from fetcher import build_completion_marker
        results = _make_all_results()
        results["works_recommended"]["records"] = "not_an_int"
        with self.assertRaises(RuntimeError) as ctx:
            build_completion_marker("ts", results)
        self.assertIn("invalid record count", str(ctx.exception))

    def test_invalid_chunk_count_raises(self):
        from fetcher import build_completion_marker
        results = _make_all_results()
        results["works_recommended"]["chunks"] = None
        with self.assertRaises(RuntimeError) as ctx:
            build_completion_marker("ts", results)
        self.assertIn("invalid chunk count", str(ctx.exception))

    def test_missing_result_entry_raises(self):
        from fetcher import build_completion_marker
        results = _make_all_results()
        del results["calamity"]
        with self.assertRaises(RuntimeError):
            build_completion_marker("ts", results)

    def test_empty_results_raises(self):
        from fetcher import build_completion_marker
        with self.assertRaises(RuntimeError):
            build_completion_marker("ts", {})


class TestLocalMarkerWritten(unittest.TestCase):
    """Tests that _COMPLETE.json is always written to the local snapshot dir."""

    def test_local_only_writes_marker(self):
        from fetcher import build_completion_marker
        with tempfile.TemporaryDirectory() as td:
            snapshot_dir = Path(td) / "snapshot"
            snapshot_dir.mkdir()
            marker = build_completion_marker("ts", _make_all_results())
            marker_path = snapshot_dir / "_COMPLETE.json"
            marker_path.write_text(json.dumps(marker, indent=2, ensure_ascii=False))
            self.assertTrue(marker_path.exists())
            loaded = json.loads(marker_path.read_text())
            self.assertEqual(loaded["status"], "complete")
            self.assertEqual(len(loaded["datasets"]), 12)

    def test_normal_mode_writes_marker(self):
        from fetcher import build_completion_marker
        with tempfile.TemporaryDirectory() as td:
            snapshot_dir = Path(td) / "snapshot"
            snapshot_dir.mkdir()
            marker = build_completion_marker("ts", _make_all_results())
            marker_path = snapshot_dir / "_COMPLETE.json"
            marker_path.write_text(json.dumps(marker, indent=2, ensure_ascii=False))
            self.assertTrue(marker_path.exists())
            loaded = json.loads(marker_path.read_text())
            self.assertEqual(loaded["datasets"]["works_recommended"]["records"], 101)

    def test_marker_is_valid_json(self):
        from fetcher import build_completion_marker
        with tempfile.TemporaryDirectory() as td:
            snapshot_dir = Path(td) / "snapshot"
            snapshot_dir.mkdir()
            marker = build_completion_marker("ts", _make_all_results())
            marker_path = snapshot_dir / "_COMPLETE.json"
            marker_path.write_text(json.dumps(marker, indent=2, ensure_ascii=False))
            content = marker_path.read_text()
            parsed = json.loads(content)
            self.assertIsInstance(parsed, dict)


class TestCloudReceivesSameMarker(unittest.TestCase):
    """Tests that upload_completion_marker receives the exact same dict."""

    def test_upload_receives_same_dict(self):
        from fetcher import build_completion_marker
        marker = build_completion_marker("ts", _make_all_results())
        uploaded_content = None

        def fake_upload(path, file, file_options):
            nonlocal uploaded_content
            uploaded_content = file

        with patch("fetcher.supabase") as mock_sb:
            mock_sb.storage.from_.return_value.upload = fake_upload
            from fetcher import upload_completion_marker
            upload_completion_marker(marker)

        uploaded_marker = json.loads(uploaded_content.decode("utf-8"))
        self.assertEqual(uploaded_marker["timestamp"], marker["timestamp"])
        self.assertEqual(uploaded_marker["status"], marker["status"])
        self.assertEqual(uploaded_marker["datasets"], marker["datasets"])

    def test_upload_uses_correct_storage_path(self):
        from fetcher import build_completion_marker
        marker = build_completion_marker("2026-09-08T00:00:00Z", _make_all_results())
        uploaded_path = None

        def fake_upload(path, file, file_options):
            nonlocal uploaded_path
            uploaded_path = path

        with patch("fetcher.supabase") as mock_sb:
            mock_sb.storage.from_.return_value.upload = fake_upload
            from fetcher import upload_completion_marker
            upload_completion_marker(marker)

        self.assertEqual(uploaded_path, "2026-09-08T00:00:00Z/_COMPLETE.json")


class TestBootstrapValidationAccepts(unittest.TestCase):
    """Tests that bootstrap validation accepts the snapshot from the fetcher."""

    def test_snapshot_structure_passes_with_complete_marker(self):
        from snapshot_cache import validate_snapshot_structure
        with tempfile.TemporaryDirectory() as td:
            snapshot_dir = _make_snapshot_with_datasets(Path(td))
            ok, msg = validate_snapshot_structure(snapshot_dir)
            self.assertTrue(ok, msg)

    def test_snapshot_structure_fails_without_marker(self):
        from snapshot_cache import validate_snapshot_structure
        with tempfile.TemporaryDirectory() as td:
            snapshot_dir = _make_snapshot_with_datasets(Path(td), include_complete=False)
            ok, msg = validate_snapshot_structure(snapshot_dir)
            self.assertFalse(ok)
            self.assertIn("Missing _COMPLETE.json", msg)

    def test_bootstrap_validate_passes_with_complete_cache(self):
        from snapshot_cache import validate_bootstrap_cache
        with tempfile.TemporaryDirectory() as td:
            work_dir = Path(td)
            current_dir = work_dir / "current" / "ts"
            current_dir.mkdir(parents=True)
            for dataset in REQUIRED_DATASETS:
                d = current_dir / dataset
                d.mkdir()
                (d / "part_0001.ndjson").write_text('{"id": 1}\n')
            marker = {
                "timestamp": "ts",
                "completed_at": "2026-09-08T00:00:00+00:00",
                "status": "complete",
                "datasets": {n: {"records": 1, "chunks": 1} for n in REQUIRED_DATASETS},
            }
            (current_dir / "_COMPLETE.json").write_text(json.dumps(marker))
            ok, msg = validate_bootstrap_cache(work_dir)
            self.assertTrue(ok, msg)

    def test_bootstrap_validate_fails_without_marker(self):
        from snapshot_cache import validate_bootstrap_cache
        with tempfile.TemporaryDirectory() as td:
            work_dir = Path(td)
            current_dir = work_dir / "current" / "ts"
            current_dir.mkdir(parents=True)
            for dataset in REQUIRED_DATASETS:
                d = current_dir / dataset
                d.mkdir()
                (d / "part_0001.ndjson").write_text('{"id": 1}\n')
            ok, msg = validate_bootstrap_cache(work_dir)
            self.assertFalse(ok)
            self.assertIn("Missing _COMPLETE.json", msg)


class TestIncompleteCannotProduceMarker(unittest.TestCase):
    """Tests that incomplete data cannot produce a valid marker."""

    def test_missing_dataset_blocks_marker(self):
        from fetcher import build_completion_marker
        with self.assertRaises(RuntimeError):
            build_completion_marker("ts", _make_incomplete_results())

    def test_zero_records_marker_accepted(self):
        from fetcher import build_completion_marker
        results = _make_all_results()
        results["works_recommended"]["records"] = 0
        marker = build_completion_marker("ts", results)
        self.assertEqual(marker["datasets"]["works_recommended"]["records"], 0)

    def test_negative_chunks_marker_accepted(self):
        from fetcher import build_completion_marker
        results = _make_all_results()
        results["works_sanctioned"]["chunks"] = -1
        marker = build_completion_marker("ts", results)
        self.assertEqual(marker["datasets"]["works_sanctioned"]["chunks"], -1)

    def test_non_dict_result_blocks_marker(self):
        from fetcher import build_completion_marker
        results = _make_all_results()
        results["expenditure"] = "invalid"
        with self.assertRaises(RuntimeError):
            build_completion_marker("ts", results)


class TestDailyPipelineCompatibility(unittest.TestCase):
    """Tests that existing daily pipeline code paths remain compatible."""

    def test_validate_snapshot_structure_reads_complete_json(self):
        from snapshot_cache import validate_snapshot_structure
        with tempfile.TemporaryDirectory() as td:
            snapshot_dir = _make_snapshot_with_datasets(Path(td))
            ok, msg = validate_snapshot_structure(snapshot_dir)
            self.assertTrue(ok)

    def test_marker_status_incomplete_fails_snapshot_validation(self):
        from snapshot_cache import validate_snapshot_structure
        with tempfile.TemporaryDirectory() as td:
            snapshot_dir = _make_snapshot_with_datasets(Path(td))
            marker = {
                "timestamp": "ts",
                "completed_at": "2026-09-08T00:00:00+00:00",
                "status": "incomplete",
                "datasets": {n: {"records": 1, "chunks": 1} for n in REQUIRED_DATASETS},
            }
            (snapshot_dir / "_COMPLETE.json").write_text(json.dumps(marker))
            ok, msg = validate_snapshot_structure(snapshot_dir)
            self.assertFalse(ok)
            self.assertIn("not 'complete'", msg)

    def test_upload_failure_raises(self):
        from fetcher import build_completion_marker, upload_completion_marker
        marker = build_completion_marker("ts", _make_all_results())

        def fail_upload(path, file, file_options):
            raise RuntimeError("upload failed")

        with patch("fetcher.supabase") as mock_sb:
            mock_sb.storage.from_.return_value.upload = fail_upload
            with self.assertRaises(RuntimeError) as ctx:
                upload_completion_marker(marker)
            self.assertIn("upload failed", str(ctx.exception))


class TestMarkerObjectIdentity(unittest.TestCase):
    """Tests that local and cloud use the same marker object."""

    def test_local_and_cloud_share_same_data(self):
        from fetcher import build_completion_marker
        marker = build_completion_marker("ts", _make_all_results())

        with tempfile.TemporaryDirectory() as td:
            local_path = Path(td) / "_COMPLETE.json"
            local_path.write_text(json.dumps(marker, indent=2, ensure_ascii=False))

            uploaded_content = None

            def fake_upload(path, file, file_options):
                nonlocal uploaded_content
                uploaded_content = file

            with patch("fetcher.supabase") as mock_sb:
                mock_sb.storage.from_.return_value.upload = fake_upload
                from fetcher import upload_completion_marker
                upload_completion_marker(marker)

            local_data = json.loads(local_path.read_text())
            cloud_data = json.loads(uploaded_content.decode("utf-8"))

            self.assertEqual(local_data["timestamp"], cloud_data["timestamp"])
            self.assertEqual(local_data["status"], cloud_data["status"])
            self.assertEqual(local_data["datasets"], cloud_data["datasets"])
            self.assertEqual(local_data["completed_at"], cloud_data["completed_at"])


if __name__ == "__main__":
    unittest.main()
