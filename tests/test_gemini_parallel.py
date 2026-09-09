"""Tests for parallel Gemini architecture: client, scheduler, and pipeline integration.

Covers:
- Error classification (retryable vs permanent)
- Retry-After extraction
- Per-lane RPM limiting (15 RPM)
- Concurrent lanes
- 429 rotation
- 5xx/timeout retry
- Permanent 404/auth failure
- Partial batch failure
- All keys unavailable → 60s cooldown
- Daily quota
- Successful-result persistence despite other failures
- Idempotency/resume
- Already-up-to-date skipping
- No secret leakage in logs
"""

import json
import threading
import time
import urllib.error
from unittest.mock import patch, MagicMock, PropertyMock


# ---------------------------------------------------------------------------
# GeminiClient error classification tests
# ---------------------------------------------------------------------------

class TestErrorClassification:
    """Tests for classify_error and ErrorClass."""

    def test_429_is_retryable(self):
        from analysis.gemini_client import classify_error, ErrorClass
        exc = urllib.error.HTTPError(
            url="http://test", code=429, msg="Too Many Requests",
            hdrs={}, fp=MagicMock(read=lambda: b'{}')
        )
        err = classify_error(exc)
        assert err.error_class == ErrorClass.RETRYABLE
        assert err.status_code == 429

    def test_500_is_retryable(self):
        from analysis.gemini_client import classify_error, ErrorClass
        exc = urllib.error.HTTPError(
            url="http://test", code=500, msg="Internal Server Error",
            hdrs={}, fp=MagicMock(read=lambda: b'{}')
        )
        err = classify_error(exc)
        assert err.error_class == ErrorClass.RETRYABLE
        assert err.status_code == 500

    def test_404_is_permanent(self):
        from analysis.gemini_client import classify_error, ErrorClass
        exc = urllib.error.HTTPError(
            url="http://test", code=404, msg="Not Found",
            hdrs={}, fp=MagicMock(read=lambda: b'{}')
        )
        err = classify_error(exc)
        assert err.error_class == ErrorClass.PERMANENT
        assert err.status_code == 404

    def test_401_is_permanent(self):
        from analysis.gemini_client import classify_error, ErrorClass
        exc = urllib.error.HTTPError(
            url="http://test", code=401, msg="Unauthorized",
            hdrs={}, fp=MagicMock(read=lambda: b'{}')
        )
        err = classify_error(exc)
        assert err.error_class == ErrorClass.PERMANENT

    def test_403_is_permanent(self):
        from analysis.gemini_client import classify_error, ErrorClass
        exc = urllib.error.HTTPError(
            url="http://test", code=403, msg="Forbidden",
            hdrs={}, fp=MagicMock(read=lambda: b'{}')
        )
        err = classify_error(exc)
        assert err.error_class == ErrorClass.PERMANENT

    def test_timeout_is_retryable(self):
        from analysis.gemini_client import classify_error, ErrorClass
        err = classify_error(TimeoutError("timed out"))
        assert err.error_class == ErrorClass.RETRYABLE

    def test_connection_error_is_retryable(self):
        from analysis.gemini_client import classify_error, ErrorClass
        err = classify_error(ConnectionError("connection refused"))
        assert err.error_class == ErrorClass.RETRYABLE

    def test_rate_limit_in_message_is_retryable(self):
        from analysis.gemini_client import classify_error, ErrorClass
        err = classify_error(Exception("rate limit exceeded"))
        assert err.error_class == ErrorClass.RETRYABLE

    def test_quota_in_message_is_retryable(self):
        from analysis.gemini_client import classify_error, ErrorClass
        err = classify_error(Exception("quota exceeded"))
        assert err.error_class == ErrorClass.RETRYABLE

    def test_retry_after_extracted_from_429(self):
        from analysis.gemini_client import classify_error, ErrorClass
        exc = urllib.error.HTTPError(
            url="http://test", code=429, msg="Rate Limited",
            hdrs={"Retry-After": "5"}, fp=MagicMock(read=lambda: b'{}')
        )
        err = classify_error(exc)
        assert err.retry_after == 5.0

    def test_retry_after_float(self):
        from analysis.gemini_client import classify_error
        exc = urllib.error.HTTPError(
            url="http://test", code=429, msg="Rate Limited",
            hdrs={"Retry-After": "2.5"}, fp=MagicMock(read=lambda: b'{}')
        )
        err = classify_error(exc)
        assert err.retry_after == 2.5

    def test_retryable_property(self):
        from analysis.gemini_client import GeminiError, ErrorClass
        err = GeminiError(ErrorClass.RETRYABLE, status_code=429)
        assert err.retryable is True
        assert err.permanent is False

    def test_permanent_property(self):
        from analysis.gemini_client import GeminiError, ErrorClass
        err = GeminiError(ErrorClass.PERMANENT, status_code=404)
        assert err.permanent is True
        assert err.retryable is False


# ---------------------------------------------------------------------------
# WorkerLane RPM limiting tests
# ---------------------------------------------------------------------------

class TestWorkerLaneRPM:
    """Tests for per-lane 15 RPM rate limiting."""

    def test_lane_allows_15_requests_within_minute(self):
        from analysis.gemini_scheduler import WorkerLane
        lane = WorkerLane(model="m1", api_key="k1", lane_id="l1", rpm_limit=15)
        now = time.monotonic()
        for _ in range(15):
            lane.record_request(now)
        assert lane.available(now) is False

    def test_lane_allows_after_window_expires(self):
        from analysis.gemini_scheduler import WorkerLane
        lane = WorkerLane(model="m1", api_key="k1", lane_id="l1", rpm_limit=15)
        now = time.monotonic()
        # Record 15 requests 61 seconds ago
        for _ in range(15):
            lane.record_request(now - 61)
        assert lane.available(now) is True

    def test_lane_allows_partial_window(self):
        from analysis.gemini_scheduler import WorkerLane
        lane = WorkerLane(model="m1", api_key="k1", lane_id="l1", rpm_limit=15)
        now = time.monotonic()
        # 14 requests (should be available)
        for _ in range(14):
            lane.record_request(now)
        assert lane.available(now) is True

    def test_lane_cooldown_prevents_requests(self):
        from analysis.gemini_scheduler import WorkerLane
        lane = WorkerLane(model="m1", api_key="k1", lane_id="l1", rpm_limit=15)
        now = time.monotonic()
        lane.apply_cooldown(60, now)
        assert lane.available(now) is False
        assert lane.available(now + 59) is False
        assert lane.available(now + 61) is True


# ---------------------------------------------------------------------------
# Concurrent lanes tests
# ---------------------------------------------------------------------------

class TestConcurrentLanes:
    """Tests for multiple lanes processing concurrently."""

    def test_lanes_built_from_keys_and_models(self):
        from analysis.gemini_scheduler import GeminiScheduler
        sched = GeminiScheduler(
            api_keys=["k1", "k2"],
            models=["m1", "m2"],
            rpm_per_lane=15,
            max_workers=4,
        )
        assert len(sched.lanes) == 4  # 2 models × 2 keys

    def test_empty_keys_no_lanes(self):
        from analysis.gemini_scheduler import GeminiScheduler
        sched = GeminiScheduler(api_keys=[], models=["m1"])
        assert len(sched.lanes) == 0

    def test_paste_keys_excluded(self):
        from analysis.gemini_scheduler import GeminiScheduler
        sched = GeminiScheduler(
            api_keys=["real_key", "PASTE_YOUR_KEY_HERE", ""],
            models=["m1"],
        )
        assert len(sched.lanes) == 1

    def test_max_workers_capped_at_lane_count(self):
        from analysis.gemini_scheduler import GeminiScheduler
        sched = GeminiScheduler(
            api_keys=["k1"],
            models=["m1"],
            max_workers=8,
        )
        assert sched.max_workers == 1  # Only 1 lane, so 1 worker


# ---------------------------------------------------------------------------
# 429 rotation tests
# ---------------------------------------------------------------------------

class TestRateLimitRotation:
    """Tests for rotating away from rate-limited lanes."""

    def test_lane_excluded_after_tried(self):
        from analysis.gemini_scheduler import GeminiScheduler
        sched = GeminiScheduler(
            api_keys=["k1", "k2"],
            models=["m1"],
            rpm_per_lane=15,
            max_workers=2,
        )
        # Pick a lane, then exclude it
        lane1 = sched._pick_lane()
        assert lane1 is not None
        lane2 = sched._pick_lane(exclude_lanes={lane1.lane_id})
        assert lane2 is not None
        assert lane2.lane_id != lane1.lane_id

    def test_cooldown_makes_lane_unavailable(self):
        from analysis.gemini_scheduler import GeminiScheduler, WorkerLane
        sched = GeminiScheduler(
            api_keys=["k1", "k2"],
            models=["m1"],
            rpm_per_lane=15,
            max_workers=2,
        )
        # Cooldown first lane
        sched.lanes[0].apply_cooldown(60)
        lane = sched._pick_lane()
        assert lane.lane_id == sched.lanes[1].lane_id


# ---------------------------------------------------------------------------
# 5xx/timeout retry tests
# ---------------------------------------------------------------------------

class TestRetryOnTransientError:
    """Tests for retrying on 5xx and timeout errors."""

    def test_retryable_error_does_not_stop_processing(self):
        from analysis.gemini_scheduler import GeminiScheduler, QueueItem
        from analysis.gemini_client import GeminiError, ErrorClass

        call_count = [0]

        def mock_call(prompt, api_key, model=None):
            call_count[0] += 1
            if call_count[0] == 1:
                raise GeminiError(ErrorClass.RETRYABLE, status_code=500)
            return {"summary": "ok", "highlights": [], "cautions": []}

        sched = GeminiScheduler(
            api_keys=["k1"],
            models=["m1"],
            rpm_per_lane=15,
            max_workers=1,
            max_attempts=3,
        )

        with patch.object(sched._clients["m1"], "call", mock_call):
            with patch("analysis.gemini_scheduler.build_prompt", return_value="prompt"):
                with patch("analysis.gemini_scheduler.validate_output", return_value=[]):
                    with patch("analysis.gemini_scheduler.GeminiResult") as mock_result:
                        mock_result.return_value = MagicMock()
                        result = sched.process([{"entity_type": "MP", "entity_id": 1}])

        assert result["success_count"] == 1
        assert call_count[0] == 2  # Retried once


# ---------------------------------------------------------------------------
# Permanent 404/auth failure tests
# ---------------------------------------------------------------------------

class TestPermanentFailure:
    """Tests for non-retryable permanent failures."""

    def test_404_failure_no_retry(self):
        from analysis.gemini_scheduler import GeminiScheduler
        from analysis.gemini_client import GeminiError, ErrorClass

        call_count = [0]

        def mock_call(prompt, api_key, model=None):
            call_count[0] += 1
            raise GeminiError(ErrorClass.PERMANENT, status_code=404)

        sched = GeminiScheduler(
            api_keys=["k1"],
            models=["m1"],
            rpm_per_lane=15,
            max_workers=1,
            max_attempts=3,
        )

        with patch.object(sched._clients["m1"], "call", mock_call):
            with patch("analysis.gemini_scheduler.build_prompt", return_value="prompt"):
                result = sched.process([{"entity_type": "MP", "entity_id": 1}])

        assert result["failure_count"] == 1
        assert result["success_count"] == 0
        assert call_count[0] == 1  # No retry

    def test_auth_failure_no_retry(self):
        from analysis.gemini_scheduler import GeminiScheduler
        from analysis.gemini_client import GeminiError, ErrorClass

        call_count = [0]

        def mock_call(prompt, api_key, model=None):
            call_count[0] += 1
            raise GeminiError(ErrorClass.PERMANENT, status_code=401)

        sched = GeminiScheduler(
            api_keys=["k1"],
            models=["m1"],
            rpm_per_lane=15,
            max_workers=1,
            max_attempts=3,
        )

        with patch.object(sched._clients["m1"], "call", mock_call):
            with patch("analysis.gemini_scheduler.build_prompt", return_value="prompt"):
                result = sched.process([{"entity_type": "MP", "entity_id": 1}])

        assert call_count[0] == 1


# ---------------------------------------------------------------------------
# Partial batch failure tests
# ---------------------------------------------------------------------------

class TestPartialBatchFailure:
    """Tests for successful results persisted despite other failures."""

    def test_success_persisted_when_others_fail(self):
        from analysis.gemini_scheduler import GeminiScheduler
        from analysis.gemini_client import GeminiError, ErrorClass

        def mock_call(prompt, api_key, model=None):
            if "prompt_2" in prompt:
                raise GeminiError(ErrorClass.PERMANENT, status_code=404)
            return {"summary": "ok", "highlights": [], "cautions": []}

        sched = GeminiScheduler(
            api_keys=["k1"],
            models=["m1"],
            rpm_per_lane=15,
            max_workers=1,
            max_attempts=3,
        )

        successes = []

        def on_success(result):
            successes.append(result)

        with patch.object(sched._clients["m1"], "call", mock_call):
            with patch("analysis.gemini_scheduler.build_prompt", side_effect=lambda r: f"prompt_{r['entity_id']}"):
                with patch("analysis.gemini_scheduler.validate_output", return_value=[]):
                    with patch("analysis.gemini_scheduler.GeminiResult") as mock_result:
                        mock_result.side_effect = lambda **kw: MagicMock(**kw)
                        result = sched.process(
                            [
                                {"entity_type": "MP", "entity_id": 1},
                                {"entity_type": "MP", "entity_id": 2},
                            ],
                            on_success=on_success,
                        )

        assert result["success_count"] == 1
        assert result["failure_count"] == 1
        assert len(successes) == 1


# ---------------------------------------------------------------------------
# All keys unavailable → cooldown tests
# ---------------------------------------------------------------------------

class TestAllKeysUnavailable:
    """Tests for cooldown when all lanes are exhausted."""

    def test_cooldown_applied_when_no_lanes_available(self):
        from analysis.gemini_scheduler import GeminiScheduler
        sched = GeminiScheduler(
            api_keys=["k1"],
            models=["m1"],
            rpm_per_lane=15,
            max_workers=1,
        )
        # Cooldown the only lane
        sched.lanes[0].apply_cooldown(60)
        assert sched.lanes[0].available() is False
        assert sched._pick_lane() is None


# ---------------------------------------------------------------------------
# Daily quota tests
# ---------------------------------------------------------------------------

class TestDailyQuota:
    """Tests for global daily quota enforcement."""

    def test_quota_stops_processing(self):
        from analysis.gemini_scheduler import GeminiScheduler
        sched = GeminiScheduler(
            api_keys=["k1"],
            models=["m1"],
            daily_quota=2,
        )
        sched._daily_count = 2

        result = sched.process([
            {"entity_type": "MP", "entity_id": 1},
        ])

        assert result["failure_count"] == 1
        assert result["failures"][0]["reason"] == "daily_quota_exceeded"

    def test_quota_allows_processing_below_limit(self):
        from analysis.gemini_scheduler import GeminiScheduler

        def mock_call(prompt, api_key, model=None):
            return {"summary": "ok", "highlights": [], "cautions": []}

        sched = GeminiScheduler(
            api_keys=["k1"],
            models=["m1"],
            daily_quota=10,
            max_workers=1,
        )

        with patch.object(sched._clients["m1"], "call", mock_call):
            with patch("analysis.gemini_scheduler.build_prompt", return_value="prompt"):
                with patch("analysis.gemini_scheduler.validate_output", return_value=[]):
                    with patch("analysis.gemini_scheduler.GeminiResult") as mock_result:
                        mock_result.return_value = MagicMock()
                        result = sched.process([{"entity_type": "MP", "entity_id": 1}])

        assert result["success_count"] == 1


# ---------------------------------------------------------------------------
# Idempotency / already-up-to-date skipping tests
# ---------------------------------------------------------------------------

class TestIdempotency:
    """Tests for idempotent processing behavior."""

    def test_filter_affected_skips_unchanged(self):
        from analysis.gemini_processor import filter_affected, PROMPT_VERSION

        evidence = [
            {"entity_type": "MP", "entity_id": 1,
             "evidence_hash": "abc", "prompt_version": PROMPT_VERSION},
            {"entity_type": "MP", "entity_id": 2,
             "evidence_hash": "def", "prompt_version": PROMPT_VERSION},
        ]
        existing = [
            {"entity_type": "MP", "entity_id": 1,
             "evidence_hash": "abc", "prompt_version": PROMPT_VERSION},
        ]

        affected = filter_affected(evidence, existing)
        assert len(affected) == 1
        assert affected[0]["entity_id"] == 2

    def test_filter_affected_empty_existing(self):
        from analysis.gemini_processor import filter_affected

        evidence = [
            {"entity_type": "MP", "entity_id": 1,
             "evidence_hash": "abc", "prompt_version": "v5"},
        ]
        affected = filter_affected(evidence, [])
        assert len(affected) == 1

    def test_filter_affected_changed_hash(self):
        from analysis.gemini_processor import filter_affected

        evidence = [
            {"entity_type": "MP", "entity_id": 1,
             "evidence_hash": "new_hash", "prompt_version": "v5"},
        ]
        existing = [
            {"entity_type": "MP", "entity_id": 1,
             "evidence_hash": "old_hash", "prompt_version": "v5"},
        ]
        affected = filter_affected(evidence, existing)
        assert len(affected) == 1


# ---------------------------------------------------------------------------
# No secret leakage in logs
# ---------------------------------------------------------------------------

class TestNoSecretLeakage:
    """Tests for API key redaction in logs/errors."""

    def test_scheduler_does_not_log_api_keys(self, capsys):
        from analysis.gemini_scheduler import GeminiScheduler

        sched = GeminiScheduler(
            api_keys=["super_secret_key_123"],
            models=["m1"],
        )
        # Lane repr should not contain the key
        for lane in sched.lanes:
            assert "super_secret_key_123" not in repr(lane)

    def test_scheduler_log_output_no_keys(self, capsys):
        from analysis.gemini_scheduler import GeminiScheduler

        sched = GeminiScheduler(
            api_keys=["secret_api_key_xyz"],
            models=["m1"],
        )
        result = sched.process([])
        # Check that no output contains the key
        captured = capsys.readouterr()
        assert "secret_api_key_xyz" not in captured.out


# ---------------------------------------------------------------------------
# Pipeline integration test
# ---------------------------------------------------------------------------

class TestPipelineIntegration:
    """Tests for stage_gemini with new scheduler."""

    def test_stage_gemini_uses_scheduler(self):
        from automation.pipeline_controller import stage_gemini
        import inspect

        src = inspect.getsource(stage_gemini)
        assert "GeminiScheduler" in src
        assert "scheduler.process" in src

    def test_stage_gemini_skips_without_keys(self):
        from automation.pipeline_controller import stage_gemini

        with patch("automation.pipeline_controller.get_config") as mock_cfg:
            mock_cfg.return_value = MagicMock(gemini_keys=[])
            result = stage_gemini([{"entity_type": "MP", "entity_id": 1}])

        assert result["skipped"] is True
        assert result["reason"] == "no_api_keys"

    def test_stage_gemini_filters_affected(self):
        from automation.pipeline_controller import stage_gemini
        from analysis.gemini_processor import PROMPT_VERSION

        evidence = [
            {"entity_type": "MP", "entity_id": 1,
             "evidence_hash": "abc", "prompt_version": PROMPT_VERSION},
            {"entity_type": "MP", "entity_id": 2,
             "evidence_hash": "def", "prompt_version": PROMPT_VERSION},
        ]

        with patch("automation.pipeline_controller.get_config") as mock_cfg:
            mock_cfg.return_value = MagicMock(gemini_keys=["k1"])
            with patch("automation.pipeline_controller.get_db2", return_value=("url", "key")):
                with patch("automation.pipeline_controller.load_table", return_value=[
                    {"entity_type": "MP", "entity_id": 1,
                     "evidence_hash": "abc", "prompt_version": PROMPT_VERSION},
                ]):
                    with patch("analysis.gemini_scheduler.GeminiScheduler") as mock_sched:
                        mock_sched.return_value.process.return_value = {
                            "success_count": 1,
                            "failure_count": 0,
                            "failures": [],
                            "retries": 0,
                        }
                        result = stage_gemini(evidence)

        assert result["processed"] == 1  # Only entity_id 2 is affected
        assert result["skipped"] == 1    # entity_id 1 was up-to-date
