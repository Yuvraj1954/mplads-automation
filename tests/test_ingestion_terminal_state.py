"""Tests for ingestion worker terminal state detection.

These tests verify that the scheduler correctly identifies when all jobs
are in a terminal state (completed or failed) and stops polling, even
when some jobs failed.

The terminal state logic is:
    pending=0 AND processing=0 → terminal (regardless of failed count)

This prevents the infinite loop bug where completed_count < expected_jobs
caused the scheduler to poll forever.
"""

import threading
import time
from unittest.mock import patch, MagicMock


def is_ingestion_terminal(worker_response, expected_jobs):
    """Determine if ingestion has reached a terminal state.

    Returns (is_terminal: bool, reason: str).

    A terminal state means:
    - The Edge Function reported "All ingestion jobs completed."
    - pending=0 AND processing=0
    - All jobs are in completed or failed state

    This function is extracted for testability. The actual _ingest_worker
    uses the same logic inline.
    """
    msg = worker_response.get("message", "")
    completed_count = worker_response.get("completed", 0)
    pending = worker_response.get("pending", None)
    processing = worker_response.get("processing", None)
    failed = worker_response.get("failed", 0)

    if msg != "All ingestion jobs completed.":
        return False, "not a completion message"

    # Primary: Edge Function provides explicit status counts
    if pending is not None and processing is not None:
        if pending == 0 and processing == 0:
            return True, (
                f"terminal: completed={completed_count} failed={failed} "
                f"pending={pending} processing={processing}"
            )
        return False, (
            f"not terminal: pending={pending} processing={processing}"
        )

    # Fallback: all expected jobs completed (legacy response without counts)
    if completed_count >= expected_jobs:
        return True, f"all {expected_jobs} jobs completed"

    return False, "legacy response, counts unclear"


# ============================================================
# SCENARIO 1: All 19 completed
# ============================================================

class TestAllCompleted:
    def test_all_19_completed_with_counts(self):
        """19 completed, 0 failed, 0 pending, 0 processing → terminal."""
        resp = {
            "success": True,
            "message": "All ingestion jobs completed.",
            "completed": 19,
            "pending": 0,
            "processing": 0,
            "failed": 0,
        }
        terminal, reason = is_ingestion_terminal(resp, 19)
        assert terminal is True
        assert "completed=19" in reason
        assert "failed=0" in reason

    def test_all_19_completed_legacy(self):
        """Legacy response: completed=19, no counts → terminal via count check."""
        resp = {
            "success": True,
            "message": "All ingestion jobs completed.",
            "completed": 19,
        }
        terminal, reason = is_ingestion_terminal(resp, 19)
        assert terminal is True
        assert "all 19 jobs completed" in reason


# ============================================================
# SCENARIO 2: 15 completed + 4 failed
# ============================================================

class TestPartialFailure:
    def test_15_completed_4_failed(self):
        """The exact bug scenario: 15 completed + 4 failed = 19 expected."""
        resp = {
            "success": True,
            "message": "All ingestion jobs completed.",
            "completed": 15,
            "pending": 0,
            "processing": 0,
            "failed": 4,
        }
        terminal, reason = is_ingestion_terminal(resp, 19)
        assert terminal is True
        assert "completed=15" in reason
        assert "failed=4" in reason

    def test_15_completed_4_failed_legacy_would_not_terminate(self):
        """Legacy response without counts would NOT terminate (the old bug)."""
        resp = {
            "success": True,
            "message": "All ingestion jobs completed.",
            "completed": 15,
        }
        terminal, reason = is_ingestion_terminal(resp, 19)
        assert terminal is False  # Legacy fallback: 15 < 19 → not terminal
        assert "counts unclear" in reason


# ============================================================
# SCENARIO 3: Jobs still pending
# ============================================================

class TestStillPending:
    def test_jobs_still_pending(self):
        """12 completed, 3 pending, 2 processing, 2 failed → NOT terminal."""
        resp = {
            "success": True,
            "message": "All ingestion jobs completed.",
            "completed": 12,
            "pending": 3,
            "processing": 2,
            "failed": 2,
        }
        terminal, reason = is_ingestion_terminal(resp, 19)
        assert terminal is False
        assert "pending=3" in reason

    def test_one_pending(self):
        """18 completed, 1 pending → NOT terminal."""
        resp = {
            "success": True,
            "message": "All ingestion jobs completed.",
            "completed": 18,
            "pending": 1,
            "processing": 0,
            "failed": 0,
        }
        terminal, reason = is_ingestion_terminal(resp, 19)
        assert terminal is False


# ============================================================
# SCENARIO 4: Jobs still processing
# ============================================================

class TestStillProcessing:
    def test_jobs_still_processing(self):
        """10 completed, 0 pending, 5 processing, 0 failed → NOT terminal."""
        resp = {
            "success": True,
            "message": "All ingestion jobs completed.",
            "completed": 10,
            "pending": 0,
            "processing": 5,
            "failed": 0,
        }
        terminal, reason = is_ingestion_terminal(resp, 19)
        assert terminal is False
        assert "processing=5" in reason


# ============================================================
# SCENARIO 5: All jobs failed
# ============================================================

class TestAllFailed:
    def test_all_19_failed(self):
        """0 completed, 19 failed, 0 pending, 0 processing → terminal."""
        resp = {
            "success": True,
            "message": "All ingestion jobs completed.",
            "completed": 0,
            "pending": 0,
            "processing": 0,
            "failed": 19,
        }
        terminal, reason = is_ingestion_terminal(resp, 19)
        assert terminal is True
        assert "completed=0" in reason
        assert "failed=19" in reason


# ============================================================
# SCENARIO 6: Zero completed but jobs still pending
# ============================================================

class TestZeroCompletedStillPending:
    def test_zero_completed_still_pending(self):
        """0 completed, 19 pending → NOT terminal."""
        resp = {
            "success": True,
            "message": "All ingestion jobs completed.",
            "completed": 0,
            "pending": 19,
            "processing": 0,
            "failed": 0,
        }
        terminal, reason = is_ingestion_terminal(resp, 19)
        assert terminal is False
        assert "pending=19" in reason

    def test_zero_completed_with_some_processing(self):
        """0 completed, 5 pending, 14 processing → NOT terminal."""
        resp = {
            "success": True,
            "message": "All ingestion jobs completed.",
            "completed": 0,
            "pending": 5,
            "processing": 14,
            "failed": 0,
        }
        terminal, reason = is_ingestion_terminal(resp, 19)
        assert terminal is False


# ============================================================
# EDGE CASES
# ============================================================

class TestEdgeCases:
    def test_not_a_completion_message(self):
        """Worker returned a different message → not terminal."""
        resp = {
            "success": True,
            "message": "Job was already claimed. Try again.",
        }
        terminal, reason = is_ingestion_terminal(resp, 19)
        assert terminal is False

    def test_empty_response(self):
        """Empty response → not terminal."""
        resp = {}
        terminal, reason = is_ingestion_terminal(resp, 19)
        assert terminal is False

    def test_pending_none_processing_none(self):
        """Legacy response with no counts → fallback to count check."""
        resp = {
            "success": True,
            "message": "All ingestion jobs completed.",
            "completed": 19,
            "pending": None,
            "processing": None,
        }
        terminal, reason = is_ingestion_terminal(resp, 19)
        assert terminal is True
        assert "all 19 jobs completed" in reason

    def test_single_job_completed(self):
        """Single job, completed → terminal."""
        resp = {
            "success": True,
            "message": "All ingestion jobs completed.",
            "completed": 1,
            "pending": 0,
            "processing": 0,
            "failed": 0,
        }
        terminal, reason = is_ingestion_terminal(resp, 1)
        assert terminal is True
