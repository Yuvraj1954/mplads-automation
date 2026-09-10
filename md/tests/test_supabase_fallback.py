"""Tests for Supabase fallback snapshot selection.

Covers the bug where the fallback selected the latest snapshot
(including the just-fetched current snapshot) as its own previous,
producing false 0-change results.

Tests:
- Latest Supabase snapshot == current → select second-latest
- Several snapshots → select newest strictly older than current
- Only current snapshot exists → fail safely
- Empty snapshot list → fail safely
- Normal GitHub cache path remains unchanged
"""

import os
import sys
from pathlib import Path

# Set env vars required by daily_pipeline module at import time
os.environ.setdefault("SUPABASE_URL", "http://fake.supabase.co")
os.environ.setdefault("SUPABASE_SECRET_KEY", "fake-key")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "fake-service-role-key")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "automation"))
from daily_pipeline import select_previous_snapshot


class TestSelectPreviousSnapshot:
    """Tests for the Supabase fallback previous-snapshot selector."""

    def test_latest_equals_current_selects_second_latest(self):
        """When the newest snapshot IS the current one, select second-newest."""
        snapshots = [
            "mplads_local_2026-09-01T00-00-00Z_aaa",
            "mplads_local_2026-09-05T00-00-00Z_bbb",
            "mplads_local_2026-09-08T15-46-22Z_ccc",  # current
        ]
        current = "mplads_local_2026-09-08T15-46-22Z_ccc"

        result, err = select_previous_snapshot(snapshots, current)
        assert err is None
        assert result == "mplads_local_2026-09-05T00-00-00Z_bbb"

    def test_several_snapshots_selects_newest_strictly_older(self):
        """With multiple snapshots, select the newest one strictly before current."""
        snapshots = [
            "mplads_local_2026-08-01T00-00-00Z_1",
            "mplads_local_2026-08-15T00-00-00Z_2",
            "mplads_local_2026-09-01T00-00-00Z_3",
            "mplads_local_2026-09-07T00-00-00Z_4",
            "mplads_local_2026-09-08T12-00-00Z_5",
            "mplads_local_2026-09-08T15-46-22Z_6",  # current
        ]
        current = "mplads_local_2026-09-08T15-46-22Z_6"

        result, err = select_previous_snapshot(snapshots, current)
        assert err is None
        assert result == "mplads_local_2026-09-08T12-00-00Z_5"

    def test_only_current_exists_fails_safely(self):
        """If only the current snapshot exists, return an error (no self-comparison)."""
        snapshots = ["mplads_local_2026-09-08T15-46-22Z_1"]
        current = "mplads_local_2026-09-08T15-46-22Z_1"

        result, err = select_previous_snapshot(snapshots, current)
        assert result is None
        assert err is not None
        assert "No valid snapshot older than current" in err

    def test_empty_snapshot_list_fails_safely(self):
        """Empty snapshot list returns an error."""
        snapshots = []
        current = "mplads_local_2026-09-08T15-46-22Z_1"

        result, err = select_previous_snapshot(snapshots, current)
        assert result is None
        assert err is not None

    def test_all_snapshots_newer_than_current(self):
        """All snapshots have timestamps > current (edge case with string comparison)."""
        snapshots = [
            "mplads_local_2026-09-09T00-00-00Z_1",
            "mplads_local_2026-09-10T00-00-00Z_2",
        ]
        current = "mplads_local_2026-09-08T00-00-00Z_current"

        result, err = select_previous_snapshot(snapshots, current)
        assert result is None
        assert err is not None
        assert "No valid snapshot older than current" in err

    def test_current_not_in_list(self):
        """Current snapshot not in the list (e.g., upload failed) — still selects correctly."""
        snapshots = [
            "mplads_local_2026-09-01T00-00-00Z_1",
            "mplads_local_2026-09-05T00-00-00Z_2",
        ]
        current = "mplads_local_2026-09-08T15-46-22Z_new"

        result, err = select_previous_snapshot(snapshots, current)
        assert err is None
        assert result == "mplads_local_2026-09-05T00-00-00Z_2"

    def test_deterministic_selection(self):
        """Same inputs always produce the same output (deterministic)."""
        snapshots = [
            "mplads_local_2026-08-01T00-00-00Z_1",
            "mplads_local_2026-09-01T00-00-00Z_2",
            "mplads_local_2026-09-08T15-46-22Z_3",
        ]
        current = "mplads_local_2026-09-08T15-46-22Z_3"

        r1, _ = select_previous_snapshot(snapshots, current)
        r2, _ = select_previous_snapshot(snapshots, current)
        assert r1 == r2

    def test_identical_timestamps_different_names(self):
        """Two snapshots with identical timestamps but different names."""
        snapshots = [
            "mplads_local_2026-09-08T15-46-22Z_aaa",
            "mplads_local_2026-09-08T15-46-22Z_bbb",
        ]
        current = "mplads_local_2026-09-08T15-46-22Z_bbb"

        result, err = select_previous_snapshot(snapshots, current)
        assert err is None
        assert result == "mplads_local_2026-09-08T15-46-22Z_aaa"

    def test_single_older_snapshot(self):
        """Only one snapshot older than current — selects it."""
        snapshots = [
            "mplads_local_2026-09-07T00-00-00Z_1",
            "mplads_local_2026-09-08T15-46-22Z_2",
        ]
        current = "mplads_local_2026-09-08T15-46-22Z_2"

        result, err = select_previous_snapshot(snapshots, current)
        assert err is None
        assert result == "mplads_local_2026-09-07T00-00-00Z_1"

    def test_unsorted_input_handled(self):
        """Input list is not pre-sorted — function handles it via string comparison."""
        snapshots = [
            "mplads_local_2026-09-08T15-46-22Z_3",
            "mplads_local_2026-09-01T00-00-00Z_1",
            "mplads_local_2026-09-05T00-00-00Z_2",
        ]
        current = "mplads_local_2026-09-08T15-46-22Z_3"

        result, err = select_previous_snapshot(snapshots, current)
        assert err is None
        assert result == "mplads_local_2026-09-05T00-00-00Z_2"
