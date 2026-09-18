"""Tests for the persistent Gemini retry backlog system.

Tests use mocks for Supabase client and Gemini API calls.
No real API calls are made.
"""

import json
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone, timedelta


# ---------------------------------------------------------------------------
# Test merge_candidates (pure logic, no DB)
# ---------------------------------------------------------------------------
class TestMergeCandidates:
    def test_empty_backlog(self):
        from automation.gemini_backlog import merge_candidates
        evidence = [
            {"entity_type": "MP", "entity_id": 1},
            {"entity_type": "STATE", "entity_id": 10},
        ]
        merged, backlog_only = merge_candidates(evidence, [])
        assert len(merged) == 2
        assert len(backlog_only) == 0

    def test_empty_evidence(self):
        from automation.gemini_backlog import merge_candidates
        backlog = [
            {"entity_type": "MP", "entity_id": 5, "attempt_count": 1},
        ]
        merged, backlog_only = merge_candidates([], backlog)
        assert len(merged) == 0
        assert len(backlog_only) == 1
        assert backlog_only[0]["entity_id"] == 5

    def test_deduplication(self):
        from automation.gemini_backlog import merge_candidates
        evidence = [
            {"entity_type": "MP", "entity_id": 1},
            {"entity_type": "MP", "entity_id": 2},
        ]
        backlog = [
            {"entity_type": "MP", "entity_id": 2, "attempt_count": 1},
            {"entity_type": "MP", "entity_id": 3, "attempt_count": 2},
        ]
        merged, backlog_only = merge_candidates(evidence, backlog)
        assert len(merged) == 2
        assert len(backlog_only) == 1
        assert backlog_only[0]["entity_id"] == 3

    def test_full_overlap(self):
        from automation.gemini_backlog import merge_candidates
        evidence = [{"entity_type": "MP", "entity_id": 1}]
        backlog = [{"entity_type": "MP", "entity_id": 1, "attempt_count": 1}]
        merged, backlog_only = merge_candidates(evidence, backlog)
        assert len(merged) == 1
        assert len(backlog_only) == 0

    def test_state_entity_type(self):
        from automation.gemini_backlog import merge_candidates
        evidence = [{"entity_type": "STATE", "entity_id": 36}]
        backlog = [{"entity_type": "MP", "entity_id": 100}]
        merged, backlog_only = merge_candidates(evidence, backlog)
        assert len(merged) == 1
        assert len(backlog_only) == 1


# ---------------------------------------------------------------------------
# Test backlog_summary (pure logic)
# ---------------------------------------------------------------------------
class TestBacklogSummary:
    def test_empty(self):
        from automation.gemini_backlog import backlog_summary
        result = backlog_summary([])
        assert result["total"] == 0
        assert result["by_error"] == {}
        assert result["max_attempts"] == 0

    def test_populated(self):
        from automation.gemini_backlog import backlog_summary
        pending = [
            {"attempt_count": 1, "last_error_class": "RPD_EXHAUSTED"},
            {"attempt_count": 3, "last_error_class": "RPD_EXHAUSTED"},
            {"attempt_count": 2, "last_error_class": "TIMEOUT"},
        ]
        result = backlog_summary(pending)
        assert result["total"] == 3
        assert result["by_error"] == {"RPD_EXHAUSTED": 2, "TIMEOUT": 1}
        assert result["max_attempts"] == 3


# ---------------------------------------------------------------------------
# Test _classify_error (pure logic)
# ---------------------------------------------------------------------------
class TestClassifyError:
    def test_timeout(self):
        from automation.gemini_backlog import _classify_error
        assert _classify_error(TimeoutError("timed out")) == "TIMEOUT"

    def test_server_error(self):
        from automation.gemini_backlog import _classify_error
        assert _classify_error(Exception("502 Bad Gateway")) == "SERVER_ERROR"

    def test_rate_limit(self):
        from automation.gemini_backlog import _classify_error
        assert _classify_error(Exception("429 rate limit")) == "RATE_LIMITED"

    def test_unknown(self):
        from automation.gemini_backlog import _classify_error
        assert _classify_error(Exception("something weird")) == "UNKNOWN_ERROR"

    def test_gemini_error_rpd(self):
        from automation.gemini_backlog import _classify_error
        from analysis.gemini_client import GeminiError, ErrorClass
        exc = GeminiError(ErrorClass.RPD_EXHAUSTED, 429, "daily quota")
        assert _classify_error(exc) == "RPD_EXHAUSTED"

    def test_gemini_error_rpm(self):
        from automation.gemini_backlog import _classify_error
        from analysis.gemini_client import GeminiError, ErrorClass
        exc = GeminiError(ErrorClass.RPM_RATE_LIMITED, 429, "rate limit")
        assert _classify_error(exc) == "RPM_RATE_LIMITED"

    def test_gemini_error_auth(self):
        from automation.gemini_backlog import _classify_error
        from analysis.gemini_client import GeminiError, ErrorClass
        exc = GeminiError(ErrorClass.AUTH_ERROR, 401, "unauthorized")
        assert _classify_error(exc) == "AUTH_ERROR"


# ---------------------------------------------------------------------------
# Test record_failure / record_success (mocked DB)
# ---------------------------------------------------------------------------
class TestRecordFailure:
    @patch("supabase.create_client")
    def test_creates_new_entry(self, mock_create):
        from automation.gemini_backlog import record_failure
        mock_client = MagicMock()
        mock_create.return_value = mock_client
        # Chain: .table().select().eq().eq().execute() for reading existing
        mock_client.table.return_value.select.return_value.eq.return_value.eq.return_value.execute.return_value = MagicMock(data=[])

        record_failure("url", "key", "MP", 42, "TIMEOUT", "connection timed out")

        mock_client.table.assert_called_with("gemini_retry_backlog")
        mock_client.table.return_value.upsert.assert_called_once()
        call_args = mock_client.table.return_value.upsert.call_args
        row = call_args[0][0]
        assert row["entity_type"] == "MP"
        assert row["entity_id"] == 42
        assert row["attempt_count"] == 1
        assert row["last_error_class"] == "TIMEOUT"
        assert row["last_error_message"] == "connection timed out"
        assert row["status"] == "PENDING"

    @patch("supabase.create_client")
    def test_increments_existing_entry(self, mock_create):
        from automation.gemini_backlog import record_failure
        mock_client = MagicMock()
        mock_create.return_value = mock_client
        mock_client.table.return_value.select.return_value.eq.return_value.eq.return_value.execute.return_value = MagicMock(
            data=[{"attempt_count": 2}]
        )

        record_failure("url", "key", "MP", 42, "SERVER_ERROR", "500 error")

        call_args = mock_client.table.return_value.upsert.call_args
        row = call_args[0][0]
        assert row["attempt_count"] == 3
        assert row["status"] == "PENDING"

    @patch("supabase.create_client")
    def test_marks_exhausted_at_max(self, mock_create):
        from automation.gemini_backlog import record_failure, MAX_ATTEMPTS
        mock_client = MagicMock()
        mock_create.return_value = mock_client
        mock_client.table.return_value.select.return_value.eq.return_value.eq.return_value.execute.return_value = MagicMock(
            data=[{"attempt_count": MAX_ATTEMPTS - 1}]
        )

        record_failure("url", "key", "MP", 42, "UNKNOWN_ERROR", "weird")

        call_args = mock_client.table.return_value.upsert.call_args
        row = call_args[0][0]
        assert row["attempt_count"] == MAX_ATTEMPTS
        assert row["status"] == "EXHAUSTED"

    @patch("supabase.create_client")
    def test_error_message_truncated(self, mock_create):
        from automation.gemini_backlog import record_failure
        mock_client = MagicMock()
        mock_create.return_value = mock_client
        mock_client.table.return_value.select.return_value.eq.return_value.eq.return_value.execute.return_value = MagicMock(data=[])

        long_msg = "x" * 1000
        record_failure("url", "key", "MP", 42, "UNKNOWN_ERROR", long_msg)

        call_args = mock_client.table.return_value.upsert.call_args
        row = call_args[0][0]
        assert len(row["last_error_message"]) == 500


class TestRecordSuccess:
    @patch("supabase.create_client")
    def test_marks_resolved(self, mock_create):
        from automation.gemini_backlog import record_success
        mock_client = MagicMock()
        mock_create.return_value = mock_client

        record_success("url", "key", "MP", 42)

        call_args = mock_client.table.return_value.upsert.call_args
        row = call_args[0][0]
        assert row["status"] == "RESOLVED"
        assert row["entity_type"] == "MP"
        assert row["entity_id"] == 42


# ---------------------------------------------------------------------------
# Test resolve_failures (mocked DB)
# ---------------------------------------------------------------------------
class TestResolveFailures:
    @patch("automation.gemini_backlog.record_failure")
    def test_records_all_failures(self, mock_record):
        from automation.gemini_backlog import resolve_failures
        failures = [
            {"entity_type": "MP", "entity_id": 1, "reason": "TIMEOUT"},
            {"entity_type": "STATE", "entity_id": 36, "reason": "RPD_EXHAUSTED"},
        ]
        resolve_failures("url", "key", failures)
        assert mock_record.call_count == 2

    def test_empty_failures(self):
        from automation.gemini_backlog import resolve_failures
        resolve_failures("url", "key", [])


# ---------------------------------------------------------------------------
# Test load_pending_backlog (mocked DB)
# ---------------------------------------------------------------------------
class TestLoadPendingBacklog:
    @patch("supabase.create_client")
    def test_loads_pending(self, mock_create):
        from automation.gemini_backlog import load_pending_backlog
        mock_client = MagicMock()
        mock_create.return_value = mock_client
        mock_result = MagicMock()
        mock_result.data = [
            {"entity_type": "MP", "entity_id": 1, "attempt_count": 2, "last_error_class": "TIMEOUT"},
        ]
        mock_client.table.return_value.select.return_value.eq.return_value.order.return_value.range.return_value.execute.return_value = mock_result

        result = load_pending_backlog("url", "key")
        assert len(result) == 1
        assert result[0]["entity_id"] == 1


# ---------------------------------------------------------------------------
# Test fetch_evidence_for_backlog (mocked DB)
# ---------------------------------------------------------------------------
class TestFetchEvidenceForBacklog:
    @patch("supabase.create_client")
    def test_fetches_evidence(self, mock_create):
        from automation.gemini_backlog import fetch_evidence_for_backlog
        mock_client = MagicMock()
        mock_create.return_value = mock_client
        mock_result = MagicMock()
        mock_result.data = [{
            "entity_type": "MP",
            "entity_id": 42,
            "entity_name": "Test MP",
            "evidence": {"portfolio": {"total_works": 10}},
            "evidence_hash": "abc123",
        }]
        mock_client.table.return_value.select.return_value.eq.return_value.eq.return_value.limit.return_value.execute.return_value = mock_result

        backlog = [{"entity_type": "MP", "entity_id": 42}]
        result = fetch_evidence_for_backlog("url", "key", backlog)
        assert len(result) == 1
        assert result[0]["entity_type"] == "MP"
        assert result[0]["entity_id"] == 42
        assert "evidence" in result[0]

    def test_empty_backlog(self):
        from automation.gemini_backlog import fetch_evidence_for_backlog
        result = fetch_evidence_for_backlog("url", "key", [])
        assert result == []


# ---------------------------------------------------------------------------
# Test backup table protection
# ---------------------------------------------------------------------------
class TestNoBackupTables:
    def test_no_backup_in_code(self):
        """Verify the gemini_backlog module does not create backup tables."""
        import inspect
        from automation import gemini_backlog
        source = inspect.getsource(gemini_backlog)
        assert "backup" not in source.lower()
        assert "_backup_" not in source.lower()
        # The DDL for the backlog table itself is OK — only backup_* tables are forbidden
        # Check there's no CREATE TABLE with "backup" in the name
        import re
        backup_creates = re.findall(r'CREATE\s+TABLE.*backup', source, re.IGNORECASE)
        assert len(backup_creates) == 0

    def test_backlog_table_name(self):
        """Verify the backlog table is named gemini_retry_backlog, not backup."""
        from automation.gemini_backlog import BACKLOG_TABLE
        assert BACKLOG_TABLE == "gemini_retry_backlog"
        assert "backup" not in BACKLOG_TABLE


# ---------------------------------------------------------------------------
# Test deterministic intelligence untouched
# ---------------------------------------------------------------------------
class TestDeterministicUntouched:
    def test_no_score_imports(self):
        """Gemini backlog must not import scoring/ranking modules."""
        import inspect
        from automation import gemini_backlog
        source = inspect.getsource(gemini_backlog)
        for forbidden in ["score_mod", "prof_mod", "anom_mod", "risk_mod", "alloc_mod"]:
            assert forbidden not in source

    def test_no_db1_writes(self):
        """Gemini backlog must not write to DB1."""
        import inspect
        from automation import gemini_backlog
        source = inspect.getsource(gemini_backlog)
        assert "get_db1" not in source
        assert "db1_url" not in source
