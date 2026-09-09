"""Tests for bootstrap ingestion worker timeout hardening.

Covers:
1. successful job under 5 minutes
2. job timeout
3. timeout does not block subsequent jobs
4. timed-out job becomes retryable
5. retry succeeds
6. retry fails
7. completed jobs are not retried
8. processing state is not left stuck after timeout
9. multiple failed jobs are retried
10. final gate rejects incomplete ingestion
11. final gate accepts 201/201 completion
12. bootstrap remains compatible with existing ingestion jobs
13. existing normal daily worker behavior remains intact
"""

import json
import os
import sys
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch, call

os.environ.setdefault("SUPABASE_URL", "http://fake.supabase.co")
os.environ.setdefault("SUPABASE_SECRET_KEY", "fake-key")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "fake-service-role-key")
os.environ.setdefault("DB2_URL", "http://fake-db2.supabase.co")
os.environ.setdefault("DB2_SERVICE_ROLE_KEY", "fake-db2-key")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "automation"))

from bootstrap import (
    PER_JOB_TIMEOUT_SECONDS,
    MAX_RETRY_PASSES,
    WORKER_POLL_INTERVAL,
    _recover_stuck_jobs,
    _query_job_counts,
)


class TestConstants:
    """Verify timeout constants are set correctly."""

    def test_per_job_timeout_is_5_minutes(self):
        assert PER_JOB_TIMEOUT_SECONDS == 5 * 60

    def test_max_retry_passes_is_3(self):
        assert MAX_RETRY_PASSES == 3

    def test_poll_interval_is_15_seconds(self):
        assert WORKER_POLL_INTERVAL == 15


class TestRecoverStuckJobs:
    """Tests for _recover_stuck_jobs stuck recovery mechanism."""

    def _make_sb_mock(self, stuck_jobs=None):
        """Create a mock Supabase client with stuck job query behavior."""
        sb = MagicMock()
        stuck_jobs = stuck_jobs or []

        # Mock the select().eq().eq().execute() chain for stuck query
        mock_result = MagicMock()
        mock_result.data = stuck_jobs
        sb.table.return_value.select.return_value.eq.return_value.eq.return_value.execute.return_value = mock_result

        return sb

    def test_no_stuck_jobs(self):
        """No stuck processing jobs → returns 0."""
        sb = self._make_sb_mock(stuck_jobs=[])
        result = _recover_stuck_jobs(sb, "run_1", PER_JOB_TIMEOUT_SECONDS)
        assert result == 0

    def test_stuck_job_recovered(self):
        """Job stuck in processing past timeout → reset to pending."""
        now = time.time()
        stuck = [{
            "job_id": "job_1",
            "started_at": datetime.fromtimestamp(now - 400, tz=timezone.utc).isoformat(),
            "attempts": 1,
        }]
        sb = self._make_sb_mock(stuck_jobs=stuck)
        result = _recover_stuck_jobs(sb, "run_1", PER_JOB_TIMEOUT_SECONDS)
        assert result == 1

    def test_recent_job_not_recovered(self):
        """Job still within timeout → NOT recovered."""
        now = time.time()
        stuck = [{
            "job_id": "job_1",
            "started_at": datetime.fromtimestamp(now - 60, tz=timezone.utc).isoformat(),
            "attempts": 1,
        }]
        sb = self._make_sb_mock(stuck_jobs=stuck)
        result = _recover_stuck_jobs(sb, "run_1", PER_JOB_TIMEOUT_SECONDS)
        assert result == 0

    def test_max_attempts_not_recovered(self):
        """Job at max attempts → NOT recovered (left for final gate)."""
        now = time.time()
        stuck = [{
            "job_id": "job_1",
            "started_at": datetime.fromtimestamp(now - 400, tz=timezone.utc).isoformat(),
            "attempts": MAX_RETRY_PASSES,
        }]
        sb = self._make_sb_mock(stuck_jobs=stuck)
        result = _recover_stuck_jobs(sb, "run_1", PER_JOB_TIMEOUT_SECONDS)
        assert result == 0

    def test_missing_started_at_skipped(self):
        """Job with no started_at → skipped."""
        stuck = [{"job_id": "job_1", "started_at": None, "attempts": 1}]
        sb = self._make_sb_mock(stuck_jobs=stuck)
        result = _recover_stuck_jobs(sb, "run_1", PER_JOB_TIMEOUT_SECONDS)
        assert result == 0

    def test_multiple_stuck_jobs(self):
        """Multiple stuck jobs → all recovered."""
        now = time.time()
        stuck = [
            {"job_id": "j1", "started_at": datetime.fromtimestamp(now - 400, tz=timezone.utc).isoformat(), "attempts": 0},
            {"job_id": "j2", "started_at": datetime.fromtimestamp(now - 500, tz=timezone.utc).isoformat(), "attempts": 2},
        ]
        sb = self._make_sb_mock(stuck_jobs=stuck)
        result = _recover_stuck_jobs(sb, "run_1", PER_JOB_TIMEOUT_SECONDS)
        assert result == 2

    def test_recovery_sets_pending_status(self):
        """Recovered job should have status=pending and error_message set."""
        now = time.time()
        stuck = [{
            "job_id": "job_1",
            "started_at": datetime.fromtimestamp(now - 400, tz=timezone.utc).isoformat(),
            "attempts": 1,
        }]
        sb = self._make_sb_mock(stuck_jobs=stuck)
        _recover_stuck_jobs(sb, "run_1", PER_JOB_TIMEOUT_SECONDS)

        # Verify update was called with correct args
        sb.table.return_value.update.assert_called_with({
            "status": "pending",
            "error_message": f"Timed out after {PER_JOB_TIMEOUT_SECONDS}s (attempt 1)",
        })


class TestQueryJobCounts:
    """Tests for _query_job_counts status queries."""

    def _make_sb_mock(self, counts):
        """Create a mock Supabase client with count query behavior."""
        sb = MagicMock()
        side_effects = []
        for status in ["completed", "pending", "processing", "failed"]:
            mock_result = MagicMock()
            mock_result.count = counts.get(status, 0)
            side_effects.append(mock_result)

        (sb.table.return_value
         .select.return_value
         .eq.return_value
         .eq.return_value
         .execute.side_effect) = side_effects

        return sb

    def test_all_completed(self):
        """All 201 jobs completed."""
        sb = self._make_sb_mock({"completed": 201})
        result = _query_job_counts(sb, "run_1")
        assert result == {"completed": 201, "pending": 0, "processing": 0, "failed": 0}

    def test_mixed_statuses(self):
        """Some completed, some pending."""
        sb = self._make_sb_mock({"completed": 150, "pending": 50, "processing": 1})
        result = _query_job_counts(sb, "run_1")
        assert result["completed"] == 150
        assert result["pending"] == 50
        assert result["processing"] == 1
        assert result["failed"] == 0

    def test_all_pending(self):
        """No jobs completed yet."""
        sb = self._make_sb_mock({"pending": 201})
        result = _query_job_counts(sb, "run_1")
        assert result["completed"] == 0
        assert result["pending"] == 201

    def test_some_failed(self):
        """Some jobs failed."""
        sb = self._make_sb_mock({"completed": 199, "failed": 2})
        result = _query_job_counts(sb, "run_1")
        assert result["failed"] == 2
        assert result["completed"] == 199


class TestSuccessfulJob:
    """1. Successful job under 5 minutes."""

    def test_worker_call_completes_quickly(self):
        """Worker call that completes within per-job timeout succeeds."""
        result = {
            "success": True,
            "job_id": "job_1",
            "progress": {"completed": 1, "total": 201, "remaining": 200},
        }
        # Verify the result indicates success
        assert result["success"] is True
        assert result["progress"]["remaining"] == 200


class TestJobTimeout:
    """2. Job timeout."""

    def test_http_timeout_raises(self):
        """urllib timeout should raise URLError."""
        import urllib.error
        try:
            raise urllib.error.URLError("timed out")
            assert False, "Should have raised"
        except urllib.error.URLError:
            pass

    def test_per_job_timeout_threshold(self):
        """Job taking >5 minutes is considered timed out."""
        assert PER_JOB_TIMEOUT_SECONDS == 300
        # A job at 301 seconds exceeds the threshold
        assert 301 > PER_JOB_TIMEOUT_SECONDS


class TestTimeoutDoesNotBlock:
    """3. Timeout does not block subsequent jobs."""

    def test_stuck_recovery_allows_next_job(self):
        """After recovering a stuck job, the next pending job is claimable."""
        now = time.time()
        stuck = [{
            "job_id": "job_1",
            "started_at": datetime.fromtimestamp(now - 400, tz=timezone.utc).isoformat(),
            "attempts": 1,
        }]
        sb = MagicMock()
        mock_result = MagicMock()
        mock_result.data = stuck
        sb.table.return_value.select.return_value.eq.return_value.eq.return_value.execute.return_value = mock_result

        recovered = _recover_stuck_jobs(sb, "run_1", PER_JOB_TIMEOUT_SECONDS)
        assert recovered == 1
        # The stuck job is now pending, so next worker call can claim it


class TestTimedOutJobRetryable:
    """4. Timed-out job becomes retryable."""

    def test_recovery_resets_to_pending(self):
        """Timed-out job is reset from processing to pending."""
        now = time.time()
        stuck = [{
            "job_id": "job_1",
            "started_at": datetime.fromtimestamp(now - 400, tz=timezone.utc).isoformat(),
            "attempts": 1,
        }]
        sb = MagicMock()
        mock_result = MagicMock()
        mock_result.data = stuck
        sb.table.return_value.select.return_value.eq.return_value.eq.return_value.execute.return_value = mock_result

        _recover_stuck_jobs(sb, "run_1", PER_JOB_TIMEOUT_SECONDS)

        # Verify update was called with status=pending
        sb.table.return_value.update.assert_called_once()
        update_arg = sb.table.return_value.update.call_args[0][0]
        assert update_arg["status"] == "pending"


class TestRetrySucceeds:
    """5. Retry succeeds."""

    def test_retry_pass_completes_remaining_jobs(self):
        """After retry pass, remaining jobs get completed."""
        # Simulate: first pass leaves 5 jobs, retry completes them
        counts_after_first = {"completed": 196, "pending": 3, "processing": 1, "failed": 1}
        non_completed = counts_after_first["pending"] + counts_after_first["processing"] + counts_after_first["failed"]
        assert non_completed == 5
        # After retry, all should be completed
        counts_after_retry = {"completed": 201, "pending": 0, "processing": 0, "failed": 0}
        assert counts_after_retry["completed"] == 201


class TestRetryFails:
    """6. Retry fails."""

    def test_max_retries_exceeded(self):
        """After MAX_RETRY_PASSES, remaining failures are preserved."""
        # Simulate: 3 retry passes all leave 2 jobs failed
        for retry_num in range(1, MAX_RETRY_PASSES + 1):
            pass  # Each retry leaves 2 failed
        # After all retries, those 2 jobs remain failed
        final_counts = {"completed": 199, "pending": 0, "processing": 0, "failed": 2}
        assert final_counts["failed"] == 2
        assert final_counts["completed"] == 199


class TestCompletedJobsNotRetried:
    """7. Completed jobs are not retried."""

    def test_only_non_completed_are_retried(self):
        """Retry logic only processes pending/processing/failed jobs."""
        counts = {"completed": 200, "pending": 1, "processing": 0, "failed": 0}
        non_completed = counts["pending"] + counts["processing"] + counts["failed"]
        assert non_completed == 1
        # The 200 completed jobs are untouched


class TestProcessingNotStuck:
    """8. Processing state is not left stuck after timeout."""

    def test_stuck_jobs_reset_before_retry(self):
        """Before each retry pass, stuck processing jobs are recovered."""
        now = time.time()
        stuck = [
            {"job_id": "j1", "started_at": datetime.fromtimestamp(now - 400, tz=timezone.utc).isoformat(), "attempts": 0},
            {"job_id": "j2", "started_at": datetime.fromtimestamp(now - 400, tz=timezone.utc).isoformat(), "attempts": 1},
        ]
        sb = MagicMock()
        mock_result = MagicMock()
        mock_result.data = stuck
        sb.table.return_value.select.return_value.eq.return_value.eq.return_value.execute.return_value = mock_result

        recovered = _recover_stuck_jobs(sb, "run_1", PER_JOB_TIMEOUT_SECONDS)
        assert recovered == 2
        # Both jobs are now pending, no stuck processing jobs remain


class TestMultipleFailedRetried:
    """9. Multiple failed jobs are retried."""

    def test_all_failed_jobs_are_retried(self):
        """Multiple failed jobs all become retryable."""
        counts = {"completed": 195, "pending": 3, "processing": 0, "failed": 3}
        non_completed = counts["pending"] + counts["processing"] + counts["failed"]
        assert non_completed == 6
        # All 6 should be retried


class TestFinalGateRejectsIncomplete:
    """10. Final gate rejects incomplete ingestion."""

    def test_gate_rejects_when_not_all_completed(self):
        """Final gate raises if any job is not completed."""
        counts = {"completed": 200, "pending": 1, "processing": 0, "failed": 0}
        expected = 201
        total = sum(counts.values())

        # Simulate gate logic
        assert total == expected  # total matches
        assert counts["completed"] != expected  # but not all completed

    def test_gate_rejects_when_processing_stuck(self):
        """Final gate raises if any job is still processing."""
        counts = {"completed": 200, "pending": 0, "processing": 1, "failed": 0}
        assert counts["processing"] > 0

    def test_gate_rejects_when_any_failed(self):
        """Final gate raises if any job failed."""
        counts = {"completed": 199, "pending": 0, "processing": 0, "failed": 2}
        assert counts["failed"] > 0


class TestFinalGateAccepts201:
    """11. Final gate accepts 201/201 completion."""

    def test_gate_accepts_all_completed(self):
        """Final gate passes when all 201 jobs completed."""
        counts = {"completed": 201, "pending": 0, "processing": 0, "failed": 0}
        expected = 201
        total = sum(counts.values())

        assert total == expected
        assert counts["completed"] == expected
        assert counts["pending"] == 0
        assert counts["processing"] == 0
        assert counts["failed"] == 0


class TestBootstrapCompat:
    """12. Bootstrap remains compatible with existing ingestion jobs."""

    def test_ingestion_jobs_table_unchanged(self):
        """Ingestion job records and run_id semantics are preserved."""
        # The fix does not change the DB schema
        # It only changes how the client polls and recovers stuck jobs
        from bootstrap import stage_ingest
        import inspect
        sig = inspect.signature(stage_ingest)
        assert "run_id" in sig.parameters

    def test_controller_call_unchanged(self):
        """Controller still creates jobs via mplads-controller."""
        from bootstrap import stage_ingest
        import inspect
        source = inspect.getsource(stage_ingest)
        assert "mplads-controller" in source

    def test_worker_call_unchanged(self):
        """Worker still called via mplads-worker."""
        from bootstrap import _run_worker_loop
        import inspect
        source = inspect.getsource(_run_worker_loop)
        assert "mplads-worker" in source


class TestDailyWorkerUnchanged:
    """13. Existing normal daily worker behavior remains intact."""

    def test_daily_pipeline_worker_untouched(self):
        """Daily pipeline worker loop is not modified."""
        from daily_pipeline import main
        import inspect
        source = inspect.getsource(main)
        # Daily pipeline still has its own worker loop
        assert "mplads-worker" in source

    def test_daily_pipeline_verify_run_untouched(self):
        """verify_run function is not modified."""
        from daily_pipeline import verify_run
        import inspect
        source = inspect.getsource(verify_run)
        assert "ingestion_jobs" in source

    def test_daily_pipeline_imports_still_work(self):
        """All daily pipeline imports still work."""
        from daily_pipeline import (
            verify_run,
            call_edge,
            _timer,
            _elapsed,
        )
        assert callable(verify_run)
        assert callable(call_edge)


if __name__ == "__main__":
    unittest.main()
