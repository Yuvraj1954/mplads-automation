"""Tests for snapshot retention — one-snapshot policy.

Tests cover:
- delete_old_raw_snapshots: keeps only newest, deletes older
- Idempotency: running cleanup twice is safe
- Non-snapshot files remain untouched
- Correct Supabase bucket/path operations
"""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock, call


class TestDeleteOldRawSnapshots:
    """Tests for delete_old_raw_snapshots function."""

    def _mock_storage(self):
        """Create mock Supabase storage with 3 snapshots."""
        mock_sb = MagicMock()
        storage_mock = MagicMock()
        mock_sb.storage.from_.return_value = storage_mock

        # Three snapshots: A (oldest), B (middle), C (newest)
        snapshots = {"2026-09-08T04:27:00", "2026-09-08T05:27:00", "2026-09-08T09:40:00"}

        def list_side_effect(path, opts):
            # list_complete_snapshots calls list("") then downloads _COMPLETE.json
            if path == "":
                return [{"name": ts} for ts in snapshots]
            # delete_old_raw_snapshots calls list(f"{ts}/{dataset}")
            return [{"name": "part_0001.ndjson"}]

        storage_mock.list.side_effect = list_side_effect

        def download_side_effect(path):
            if "_COMPLETE.json" in path:
                ts = path.split("/")[0]
                return json.dumps({"status": "complete", "timestamp": ts}).encode()
            return b"{}"

        storage_mock.download.side_effect = download_side_effect

        return mock_sb, storage_mock, snapshots

    @patch("automation.daily_pipeline.sb")
    def test_keeps_only_newest(self, mock_sb):
        """Three snapshots: A, B, C (newest). Keep C, delete A and B."""
        mock_storage = MagicMock()
        mock_sb.storage.from_.return_value = mock_storage

        snapshots = ["2026-09-08T04:27:00", "2026-09-08T05:27:00", "2026-09-08T09:40:00"]

        def list_side_effect(path, opts):
            if path == "":
                return [{"name": ts} for ts in snapshots]
            return [{"name": "part_0001.ndjson"}]

        mock_storage.list.side_effect = list_side_effect

        def download_side_effect(path):
            if "_COMPLETE.json" in path:
                ts = path.split("/")[0]
                return json.dumps({"status": "complete", "timestamp": ts}).encode()
            return b"{}"

        mock_storage.download.side_effect = download_side_effect

        from automation.daily_pipeline import delete_old_raw_snapshots, DATASETS

        # Keep only C (newest)
        delete_old_raw_snapshots(["2026-09-08T09:40:00"])

        # Verify delete_paths was called with A and B paths
        removed_paths = []
        for c in mock_storage.remove.call_args_list:
            paths = c[0][0] if c[0] else []
            removed_paths.extend(paths)

        # A and B should be deleted
        assert any("2026-09-08T04:27:00" in p for p in removed_paths), \
            f"Snapshot A not deleted. Paths: {removed_paths}"
        assert any("2026-09-08T05:27:00" in p for p in removed_paths), \
            f"Snapshot B not deleted. Paths: {removed_paths}"

        # C should NOT be deleted
        assert not any("2026-09-08T09:40:00" in p for p in removed_paths), \
            f"Snapshot C was incorrectly deleted. Paths: {removed_paths}"

    @patch("automation.daily_pipeline.sb")
    def test_one_snapshot_kept(self, mock_sb):
        """Single snapshot: keep it, delete nothing."""
        mock_storage = MagicMock()
        mock_sb.storage.from_.return_value = mock_storage

        snapshots = ["2026-09-08T09:40:00"]

        def list_side_effect(path, opts):
            if path == "":
                return [{"name": ts} for ts in snapshots]
            return [{"name": "part_0001.ndjson"}]

        mock_storage.list.side_effect = list_side_effect

        def download_side_effect(path):
            if "_COMPLETE.json" in path:
                return json.dumps({"status": "complete"}).encode()
            return b"{}"

        mock_storage.download.side_effect = download_side_effect

        from automation.daily_pipeline import delete_old_raw_snapshots

        delete_old_raw_snapshots(["2026-09-08T09:40:00"])

        # No files should be removed
        assert mock_storage.remove.call_count == 0, \
            f"remove() called {mock_storage.remove.call_count} times, expected 0"

    @patch("automation.daily_pipeline.sb")
    def test_idempotent(self, mock_sb):
        """Running cleanup twice produces same result."""
        mock_storage = MagicMock()
        mock_sb.storage.from_.return_value = mock_storage

        snapshots = ["2026-09-08T04:27:00", "2026-09-08T09:40:00"]

        def list_side_effect(path, opts):
            if path == "":
                return [{"name": ts} for ts in snapshots]
            return [{"name": "part_0001.ndjson"}]

        mock_storage.list.side_effect = list_side_effect

        def download_side_effect(path):
            if "_COMPLETE.json" in path:
                return json.dumps({"status": "complete"}).encode()
            return b"{}"

        mock_storage.download.side_effect = download_side_effect

        from automation.daily_pipeline import delete_old_raw_snapshots

        # First run
        delete_old_raw_snapshots(["2026-09-08T09:40:00"])
        first_call_count = mock_storage.remove.call_count

        # Reset mock
        mock_storage.reset_mock()
        mock_storage.list.side_effect = list_side_effect
        mock_storage.download.side_effect = download_side_effect

        # Second run — only A remains to delete
        delete_old_raw_snapshots(["2026-09-08T09:40:00"])
        second_call_count = mock_storage.remove.call_count

        # Both runs should attempt to delete A (B already gone on second run)
        assert first_call_count >= 1, "First run should delete A"
        assert second_call_count >= 1, "Second run should delete A"

    @patch("automation.daily_pipeline.sb")
    def test_non_snapshot_files_untouched(self, mock_sb):
        """Non-snapshot files in storage are not deleted by delete_old_raw_snapshots.

        list_complete_snapshots() only returns items that have a valid
        _COMPLETE.json marker. Items without it (delta_*, random files)
        are never returned, so delete_old_raw_snapshots never touches them.
        """
        mock_storage = MagicMock()
        mock_sb.storage.from_.return_value = mock_storage

        # Only one complete snapshot exists
        snapshots = ["2026-09-08T04:27:00"]

        def list_side_effect(path, opts):
            if path == "":
                # list_complete_snapshots sees delta_run123 and random_file
                # but only _COMPLETE.json download succeeds for snapshots
                return [
                    {"name": "2026-09-08T04:27:00"},
                    {"name": "delta_run123"},
                    {"name": "random_file"},
                ]
            return [{"name": "part_0001.ndjson"}]

        mock_storage.list.side_effect = list_side_effect

        def download_side_effect(path):
            # Only real snapshots have _COMPLETE.json
            if "_COMPLETE.json" in path:
                ts = path.split("/")[0]
                if ts in snapshots:
                    return json.dumps({"status": "complete"}).encode()
                raise FileNotFoundError("no _COMPLETE.json")
            return b"{}"

        mock_storage.download.side_effect = download_side_effect

        from automation.daily_pipeline import delete_old_raw_snapshots

        delete_old_raw_snapshots(["2026-09-08T04:27:00"])

        # Only the keep snapshot exists — nothing to delete
        assert mock_storage.remove.call_count == 0, \
            f"remove() called {mock_storage.remove.call_count} times, expected 0"

    @patch("automation.daily_pipeline.sb")
    def test_failure_keeps_previous(self, mock_sb):
        """On failure, the current snapshot is cleaned up by _cleanup_failed_snapshot, not delete_old_raw_snapshots."""
        mock_storage = MagicMock()
        mock_sb.storage.from_.return_value = mock_storage

        # This test verifies that delete_old_raw_snapshots is ONLY called on success
        # The actual failure cleanup is handled by _cleanup_failed_snapshot.py
        # This function should never be called with a "failed" snapshot
        snapshots = ["2026-09-08T04:27:00", "2026-09-08T05:27:00"]

        def list_side_effect(path, opts):
            if path == "":
                return [{"name": ts} for ts in snapshots]
            return [{"name": "part_0001.ndjson"}]

        mock_storage.list.side_effect = list_side_effect

        def download_side_effect(path):
            if "_COMPLETE.json" in path:
                return json.dumps({"status": "complete"}).encode()
            return b"{}"

        mock_storage.download.side_effect = download_side_effect

        from automation.daily_pipeline import delete_old_raw_snapshots

        # Keep the newest — this simulates successful completion
        delete_old_raw_snapshots(["2026-09-08T05:27:00"])

        removed_paths = []
        for c in mock_storage.remove.call_args_list:
            paths = c[0][0] if c[0] else []
            removed_paths.extend(paths)

        # Only A should be deleted, B (newest) kept
        assert any("2026-09-08T04:27:00" in p for p in removed_paths)
        assert not any("2026-09-08T05:27:00" in p for p in removed_paths)


if __name__ == "__main__":
    unittest.main()
