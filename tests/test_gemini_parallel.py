"""Tests for parallel Gemini architecture: client, scheduler, packing, multi-model, RPD/RPM, and pipeline integration.

Covers all 32 requirements from the specification.
"""

import json
import threading
import time
import urllib.error
from unittest.mock import patch, MagicMock


# ---------------------------------------------------------------------------
# 1. Error Classification & RPD/RPM Distinction Tests
# ---------------------------------------------------------------------------

class TestErrorClassification:
    """Tests for classify_error and ErrorClass distinguishing RPD vs RPM."""

    def test_429_rpm_classified(self):
        from analysis.gemini_client import classify_error, ErrorClass
        exc = urllib.error.HTTPError(
            url="http://test", code=429, msg="Too Many Requests",
            hdrs={}, fp=MagicMock(read=lambda: b'{"error": {"message": "Rate limit exceeded"}}')
        )
        err = classify_error(exc)
        assert err.error_class == ErrorClass.RPM_RATE_LIMITED
        assert err.rpm_limited is True
        assert err.rpd_exhausted is False

    def test_429_rpd_daily_quota_classified(self):
        from analysis.gemini_client import classify_error, ErrorClass
        exc = urllib.error.HTTPError(
            url="http://test", code=429, msg="Too Many Requests",
            hdrs={}, fp=MagicMock(read=lambda: b'{"error": {"message": "Resource exhausted: daily limit reached"}}')
        )
        err = classify_error(exc)
        assert err.error_class == ErrorClass.RPD_EXHAUSTED
        assert err.rpd_exhausted is True

    def test_401_auth_error_is_permanent(self):
        from analysis.gemini_client import classify_error, ErrorClass
        exc = urllib.error.HTTPError(
            url="http://test", code=401, msg="Unauthorized",
            hdrs={}, fp=MagicMock(read=lambda: b'{}')
        )
        err = classify_error(exc)
        assert err.error_class == ErrorClass.AUTH_ERROR
        assert err.permanent is True

    def test_404_not_found_is_permanent(self):
        from analysis.gemini_client import classify_error, ErrorClass
        exc = urllib.error.HTTPError(
            url="http://test", code=404, msg="Not Found",
            hdrs={}, fp=MagicMock(read=lambda: b'{}')
        )
        err = classify_error(exc)
        assert err.error_class == ErrorClass.NOT_FOUND
        assert err.permanent is True

    def test_retry_after_extracted_from_429(self):
        from analysis.gemini_client import classify_error
        exc = urllib.error.HTTPError(
            url="http://test", code=429, msg="Rate Limited",
            hdrs={"Retry-After": "15"}, fp=MagicMock(read=lambda: b'{}')
        )
        err = classify_error(exc)
        assert err.retry_after == 15.0


# ---------------------------------------------------------------------------
# 2. Multi-Model & Lane Setup Tests
# ---------------------------------------------------------------------------

class TestMultiModelLanes:
    """Tests for 8-lane setup (2 models × 4 keys)."""

    def test_8_lanes_created_for_2_models_and_4_keys(self):
        from analysis.gemini_scheduler import GeminiScheduler
        sched = GeminiScheduler(
            api_keys=["k1", "k2", "k3", "k4"],
            models=["gemini-3.1-flash-lite", "gemini-3.5-flash-lite"],
            rpm_per_lane=15,
        )
        assert len(sched.lanes) == 8
        models_in_lanes = set(l.model for l in sched.lanes)
        assert models_in_lanes == {"gemini-3.1-flash-lite", "gemini-3.5-flash-lite"}

    def test_paste_keys_excluded(self):
        from analysis.gemini_scheduler import GeminiScheduler
        sched = GeminiScheduler(
            api_keys=["real_key", "PASTE_YOUR_KEY_HERE", ""],
            models=["gemini-3.1-flash-lite"],
        )
        assert len(sched.lanes) == 1


# ---------------------------------------------------------------------------
# 3. Packing & Prompt Generation Tests
# ---------------------------------------------------------------------------

class TestPacking:
    """Tests for multi-record request packing."""

    def test_5_records_packed_into_one_request(self):
        from analysis.gemini_scheduler import GeminiScheduler

        call_count = [0]
        submitted_prompts = []

        def mock_call(prompt, api_key, model=None):
            call_count[0] += 1
            submitted_prompts.append(prompt)
            return [
                {"entity_id": str(i), "summary": f"Summary {i}", "highlights": ["h1", "h2", "h3"], "cautions": []}
                for i in range(1, 6)
            ]

        sched = GeminiScheduler(
            api_keys=["k1"],
            models=["gemini-3.1-flash-lite"],
            items_per_request=5,
            max_workers=1,
        )

        records = [{"entity_type": "MP", "entity_id": i} for i in range(1, 6)]
        with patch.object(sched._clients["gemini-3.1-flash-lite"], "call", mock_call):
            res = sched.process(records)

        assert res["success_count"] == 5
        assert call_count[0] == 1  # Exactly 1 Gemini API request for 5 records!

    def test_configurable_items_per_request(self):
        from analysis.gemini_scheduler import GeminiScheduler

        call_count = [0]

        def mock_call(prompt, api_key, model=None):
            call_count[0] += 1
            return [
                {"entity_id": "1", "summary": "s", "highlights": ["h1", "h2", "h3"], "cautions": []}
            ]

        sched = GeminiScheduler(
            api_keys=["k1"],
            models=["gemini-3.1-flash-lite"],
            items_per_request=2,
            max_workers=1,
        )

        records = [{"entity_type": "MP", "entity_id": i} for i in range(1, 5)]
        with patch.object(sched._clients["gemini-3.1-flash-lite"], "call", mock_call):
            with patch("analysis.gemini_scheduler.validate_packed_output") as mock_val:
                from analysis.gemini_processor import GeminiResult
                mock_val.side_effect = lambda raw, rows: (
                    [GeminiResult("MP", r["entity_id"], "s", ["h1", "h2", "h3"], [], "hash") for r in rows],
                    []
                )
                sched.process(records)

        assert call_count[0] == 2  # 4 records / 2 per request = 2 requests


# ---------------------------------------------------------------------------
# 4. Token Safety Tests
# ---------------------------------------------------------------------------

class TestTokenSafety:
    """Tests for splitting oversized evidence batches."""

    def test_token_safety_splits_oversized_batch(self):
        from analysis.gemini_processor import check_token_safety

        large_evidence = {"data": "x" * 25000}
        rows = [{"entity_type": "MP", "entity_id": i, "evidence": large_evidence} for i in range(1, 6)]

        # Request splitting when prompt > 20000 chars
        batches = check_token_safety(rows, max_chars=20000)
        assert len(batches) > 1
        total_items = sum(len(b) for b in batches)
        assert total_items == 5


# ---------------------------------------------------------------------------
# 5. Output Validation & Entity ID Matching Tests
# ---------------------------------------------------------------------------

class TestPackedValidation:
    """Tests for validating response arrays and matching by stable entity_id."""

    def test_json_array_parsing(self):
        from analysis.gemini_processor import validate_packed_output

        rows = [
            {"entity_type": "MP", "entity_id": "101"},
            {"entity_type": "MP", "entity_id": "102"},
        ]
        raw = [
            {"entity_id": "101", "summary": "Summary 101", "highlights": ["h1", "h2", "h3"], "cautions": []},
            {"entity_id": "102", "summary": "Summary 102", "highlights": ["h1", "h2", "h3"], "cautions": []},
        ]
        valid, failed = validate_packed_output(raw, rows)
        assert len(valid) == 2
        assert len(failed) == 0

    def test_no_array_position_matching(self):
        from analysis.gemini_processor import validate_packed_output

        rows = [
            {"entity_type": "MP", "entity_id": "101"},
            {"entity_type": "MP", "entity_id": "102"},
        ]
        # Reversed order from Gemini
        raw = [
            {"entity_id": "102", "summary": "Summary 102", "highlights": ["h1", "h2", "h3"], "cautions": []},
            {"entity_id": "101", "summary": "Summary 101", "highlights": ["h1", "h2", "h3"], "cautions": []},
        ]
        valid, failed = validate_packed_output(raw, rows)
        assert len(valid) == 2
        res_map = {r.entity_id: r.summary for r in valid}
        assert res_map["101"] == "Summary 101"
        assert res_map["102"] == "Summary 102"

    def test_missing_entity_detection(self):
        from analysis.gemini_processor import validate_packed_output

        rows = [
            {"entity_type": "MP", "entity_id": "101"},
            {"entity_type": "MP", "entity_id": "102"},
        ]
        # Only returns 101
        raw = [
            {"entity_id": "101", "summary": "Summary 101", "highlights": ["h1", "h2", "h3"], "cautions": []},
        ]
        valid, failed = validate_packed_output(raw, rows)
        assert len(valid) == 1
        assert len(failed) == 1
        assert failed[0]["entity_id"] == "102"
        assert failed[0]["reason"] == "missing_entity_id_in_response"

    def test_duplicate_entity_detection(self):
        from analysis.gemini_processor import validate_packed_output

        rows = [{"entity_type": "MP", "entity_id": "101"}]
        raw = [
            {"entity_id": "101", "summary": "Summary 1", "highlights": ["h1", "h2", "h3"], "cautions": []},
            {"entity_id": "101", "summary": "Summary 2", "highlights": ["h1", "h2", "h3"], "cautions": []},
        ]
        valid, failed = validate_packed_output(raw, rows)
        assert len(valid) == 1
        assert len(failed) == 1
        assert "duplicate_entity_id" in failed[0]["reason"]

    def test_unexpected_entity_detection(self):
        from analysis.gemini_processor import validate_packed_output

        rows = [{"entity_type": "MP", "entity_id": "101"}]
        raw = [
            {"entity_id": "101", "summary": "Summary 1", "highlights": ["h1", "h2", "h3"], "cautions": []},
            {"entity_id": "999", "summary": "Unexpected", "highlights": ["h1", "h2", "h3"], "cautions": []},
        ]
        valid, failed = validate_packed_output(raw, rows)
        assert len(valid) == 1
        # Unexpected 999 is ignored cleanly


# ---------------------------------------------------------------------------
# 6. Concurrency & Partial Failure Tests
# ---------------------------------------------------------------------------

class TestConcurrency:
    """Tests for parallel execution and partial response handling."""

    def test_10_concurrent_workers(self):
        from analysis.gemini_scheduler import GeminiScheduler
        sched = GeminiScheduler(
            api_keys=["k1", "k2"],
            models=["gemini-3.1-flash-lite", "gemini-3.5-flash-lite"],
            max_workers=10,
        )
        assert sched.max_workers == 10

    def test_genuine_overlapping_execution(self):
        from analysis.gemini_scheduler import GeminiScheduler
        import re

        def mock_call(prompt, api_key, model=None):
            time.sleep(0.1)  # Simulate 100ms API call
            # Extract requested entity_ids from prompt
            ids = re.findall(r'"entity_id":\s*"(\d+)"', prompt)
            return [
                {"entity_id": str(i), "summary": "s", "highlights": ["h1", "h2", "h3"], "cautions": []}
                for i in ids
            ]

        sched = GeminiScheduler(
            api_keys=["k1", "k2", "k3", "k4"],
            models=["gemini-3.1-flash-lite", "gemini-3.5-flash-lite"],
            max_workers=10,
            items_per_request=5,
        )

        records = [{"entity_type": "MP", "entity_id": i} for i in range(1, 51)]  # 50 items -> 10 requests

        start = time.monotonic()
        with patch.object(sched._clients["gemini-3.1-flash-lite"], "call", mock_call):
            with patch.object(sched._clients["gemini-3.5-flash-lite"], "call", mock_call):
                res = sched.process(records)
        elapsed = time.monotonic() - start

        # 10 requests sequentially = 1.0s. 10 requests concurrently = ~0.1-0.3s.
        assert elapsed < 0.6
        assert res["success_count"] == 50

    def test_one_failure_does_not_stop_others(self):
        from analysis.gemini_scheduler import GeminiScheduler
        from analysis.gemini_client import GeminiError, ErrorClass

        def mock_call(prompt, api_key, model=None):
            if "fail" in prompt:
                raise GeminiError(ErrorClass.NOT_FOUND, status_code=404)
            return [{"entity_id": "ok", "summary": "s", "highlights": ["h1", "h2", "h3"], "cautions": []}]

        sched = GeminiScheduler(
            api_keys=["k1", "k2"],
            models=["gemini-3.1-flash-lite"],
            max_workers=2,
            items_per_request=1,
        )

        records = [
            {"entity_type": "MP", "entity_id": "ok"},
            {"entity_type": "MP", "entity_id": "fail"},
        ]

        with patch.object(sched._clients["gemini-3.1-flash-lite"], "call", mock_call):
            with patch("analysis.gemini_scheduler.build_packed_prompt", side_effect=lambda rows: f"prompt_{rows[0]['entity_id']}"):
                res = sched.process(records)

        assert res["success_count"] == 1
        assert res["failure_count"] == 1



# ---------------------------------------------------------------------------
# 7. Partial Failure & Retry Tests
# ---------------------------------------------------------------------------

class TestPartialResponse:
    """Tests for immediate persistence of valid records and requeuing of failed items."""

    def test_partial_response_immediate_persistence_and_requeue(self):
        from analysis.gemini_scheduler import GeminiScheduler

        successes = []

        def on_success(res):
            successes.append(res.entity_id)

        call_count = [0]

        def mock_call(prompt, api_key, model=None):
            call_count[0] += 1
            if call_count[0] == 1:
                # First call returns A, B, D, E (missing C)
                return [
                    {"entity_id": "A", "summary": "s", "highlights": ["h1", "h2", "h3"], "cautions": []},
                    {"entity_id": "B", "summary": "s", "highlights": ["h1", "h2", "h3"], "cautions": []},
                    {"entity_id": "D", "summary": "s", "highlights": ["h1", "h2", "h3"], "cautions": []},
                    {"entity_id": "E", "summary": "s", "highlights": ["h1", "h2", "h3"], "cautions": []},
                ]
            else:
                # Retry call for C returns C
                return [
                    {"entity_id": "C", "summary": "s", "highlights": ["h1", "h2", "h3"], "cautions": []},
                ]

        sched = GeminiScheduler(
            api_keys=["k1"],
            models=["gemini-3.1-flash-lite"],
            items_per_request=5,
            max_workers=1,
        )

        records = [
            {"entity_type": "MP", "entity_id": "A"},
            {"entity_type": "MP", "entity_id": "B"},
            {"entity_type": "MP", "entity_id": "C"},
            {"entity_type": "MP", "entity_id": "D"},
            {"entity_type": "MP", "entity_id": "E"},
        ]

        with patch.object(sched._clients["gemini-3.1-flash-lite"], "call", mock_call):
            res = sched.process(records, on_success=on_success)

        assert res["success_count"] == 5
        assert set(successes) == {"A", "B", "C", "D", "E"}
        assert call_count[0] == 2  # Request 1 for 5, Request 2 retried ONLY C!


# ---------------------------------------------------------------------------
# 8. RPM & RPD Rate Limit / Quota Handling Tests
# ---------------------------------------------------------------------------

class TestRateLimits:
    """Tests for RPM temporary pause vs RPD in-memory blacklisting."""

    def test_rpm_causes_temporary_lane_pause_and_rotation(self):
        from analysis.gemini_scheduler import GeminiScheduler
        from analysis.gemini_client import GeminiError, ErrorClass

        call_keys = []

        def mock_call(prompt, api_key, model=None):
            call_keys.append(api_key)
            if api_key == "k1" and len(call_keys) == 1:
                raise GeminiError(ErrorClass.RPM_RATE_LIMITED, status_code=429, retry_after=5)
            return [{"entity_id": "1", "summary": "s", "highlights": ["h1", "h2", "h3"], "cautions": []}]

        sched = GeminiScheduler(
            api_keys=["k1", "k2"],
            models=["gemini-3.1-flash-lite"],
            max_workers=1,
            items_per_request=1,
        )

        with patch.object(sched._clients["gemini-3.1-flash-lite"], "call", mock_call):
            res = sched.process([{"entity_type": "MP", "entity_id": "1"}])

        assert res["success_count"] == 1
        assert "k2" in call_keys  # Rotated to k2 after k1 RPM limited

    def test_rpd_exhaustion_blacklists_lane_in_memory(self):
        from analysis.gemini_scheduler import GeminiScheduler
        from analysis.gemini_client import GeminiError, ErrorClass

        def mock_call(prompt, api_key, model=None):
            if api_key == "k1":
                raise GeminiError(ErrorClass.RPD_EXHAUSTED, status_code=429, message="Daily quota reached")
            return [{"entity_id": "1", "summary": "s", "highlights": ["h1", "h2", "h3"], "cautions": []}]

        sched = GeminiScheduler(
            api_keys=["k1", "k2"],
            models=["gemini-3.1-flash-lite"],
            max_workers=1,
            items_per_request=1,
        )

        with patch.object(sched._clients["gemini-3.1-flash-lite"], "call", mock_call):
            res = sched.process([{"entity_type": "MP", "entity_id": "1"}])

        # Lane k1 should be blacklisted in memory
        lane_k1 = next(l for l in sched.lanes if l.api_key == "k1")
        assert lane_k1.rpd_exhausted is True
        assert res["success_count"] == 1

    def test_rpd_blacklist_disappears_between_runs(self):
        from analysis.gemini_scheduler import GeminiScheduler

        # New scheduler instance for next run
        sched = GeminiScheduler(
            api_keys=["k1", "k2"],
            models=["gemini-3.1-flash-lite"],
        )
        for lane in sched.lanes:
            assert lane.rpd_exhausted is False

    def test_all_lanes_rpd_exhausted_clean_termination(self):
        from analysis.gemini_scheduler import GeminiScheduler
        from analysis.gemini_client import GeminiError, ErrorClass

        def mock_call(prompt, api_key, model=None):
            raise GeminiError(ErrorClass.RPD_EXHAUSTED, status_code=429, message="Daily quota reached")

        sched = GeminiScheduler(
            api_keys=["k1", "k2"],
            models=["gemini-3.1-flash-lite"],
            max_workers=1,
            items_per_request=1,
        )

        with patch.object(sched._clients["gemini-3.1-flash-lite"], "call", mock_call):
            res = sched.process([{"entity_type": "MP", "entity_id": "1"}])

        assert res["success_count"] == 0
        assert res["failure_count"] == 1
        assert res["failures"][0]["reason"] == "all_lanes_rpd_exhausted"


# ---------------------------------------------------------------------------
# 9. Security & Secret Redaction Tests
# ---------------------------------------------------------------------------

class TestSecurity:
    """Tests for ensuring secrets are not printed in logs."""

    def test_secret_redaction(self, capsys):
        from analysis.gemini_scheduler import GeminiScheduler

        sched = GeminiScheduler(
            api_keys=["SECRET_KEY_12345678"],
            models=["gemini-3.1-flash-lite"],
        )

        for lane in sched.lanes:
            assert "SECRET_KEY_12345678" not in repr(lane)

        with patch.object(sched._clients["gemini-3.1-flash-lite"], "call", return_value=[{"entity_id": "1", "summary": "s", "highlights": ["h1", "h2", "h3"], "cautions": []}]):
            sched.process([{"entity_type": "MP", "entity_id": "1"}])

        captured = capsys.readouterr()
        assert "SECRET_KEY_12345678" not in captured.out


# ---------------------------------------------------------------------------
# 10. Pipeline Integration & Idempotency Tests
# ---------------------------------------------------------------------------

class TestPipelineIntegration:
    """Tests for stage_gemini and idempotency."""

    def test_stage_gemini_uses_new_scheduler(self):
        from automation.pipeline_controller import stage_gemini
        import inspect

        src = inspect.getsource(stage_gemini)
        assert "GeminiScheduler" in src
        assert "gemini-3.1-flash-lite" in src
        assert "gemini-3.5-flash-lite" in src

    def test_already_up_to_date_skipping(self):
        from analysis.gemini_processor import filter_affected, PROMPT_VERSION

        evidence = [
            {"entity_type": "MP", "entity_id": 1, "evidence_hash": "abc", "prompt_version": PROMPT_VERSION},
            {"entity_type": "MP", "entity_id": 2, "evidence_hash": "def", "prompt_version": PROMPT_VERSION},
        ]
        existing = [
            {"entity_type": "MP", "entity_id": 1, "evidence_hash": "abc", "prompt_version": PROMPT_VERSION},
        ]

        affected = filter_affected(evidence, existing)
        assert len(affected) == 1
        assert affected[0]["entity_id"] == 2


# ---------------------------------------------------------------------------
# 11. 12-Lane & Dual-Model Concurrency Tests
# ---------------------------------------------------------------------------

class Test12LanesAndDualModel:
    """Tests for 12 lanes (6 keys × 2 models) and true dual-model distribution."""

    def test_12_lanes_created_for_6_keys_and_2_models(self):
        from analysis.gemini_scheduler import GeminiScheduler
        keys = [f"key_{i}" for i in range(1, 7)]
        sched = GeminiScheduler(
            api_keys=keys,
            models=["gemini-3.1-flash-lite", "gemini-3.5-flash-lite"],
            max_workers=12,
        )
        assert len(sched.lanes) == 12
        assert sched.max_workers == 12
        m1_lanes = [l for l in sched.lanes if l.model == "gemini-3.1-flash-lite"]
        m2_lanes = [l for l in sched.lanes if l.model == "gemini-3.5-flash-lite"]
        assert len(m1_lanes) == 6
        assert len(m2_lanes) == 6

    def test_default_models_and_concurrency(self):
        from analysis.gemini_scheduler import GeminiScheduler
        keys = [f"key_{i}" for i in range(1, 7)]
        sched = GeminiScheduler(api_keys=keys)
        assert sched.models == ["gemini-3.1-flash-lite", "gemini-3.5-flash-lite"]
        assert sched.max_workers == 12
        assert len(sched.lanes) == 12

    def test_both_models_are_called_in_scheduler(self):
        from analysis.gemini_scheduler import GeminiScheduler

        keys = [f"key_{i}" for i in range(1, 7)]
        sched = GeminiScheduler(
            api_keys=keys,
            models=["gemini-3.1-flash-lite", "gemini-3.5-flash-lite"],
            max_workers=12,
            items_per_request=1,
        )

        called_models = []
        lock = threading.Lock()

        def make_mock(model_name):
            def mock_call(prompt, api_key):
                with lock:
                    called_models.append(model_name)
                import re
                eids = re.findall(r'"entity_id":\s*"([^"]+)"', prompt)
                if not eids:
                    eids = ["1"]
                return [{"entity_id": eid, "summary": "s", "highlights": ["h1", "h2", "h3"], "cautions": []} for eid in eids]
            return mock_call

        for m in sched.models:
            sched._clients[m].call = make_mock(m)

        # Process 12 items (1 item per request = 12 requests)
        items = [{"entity_type": "MP", "entity_id": str(i)} for i in range(12)]
        res = sched.process(items)

        assert res["success_count"] == 12
        assert "gemini-3.1-flash-lite" in called_models
        assert "gemini-3.5-flash-lite" in called_models
        # Both models should be evenly utilized
        assert called_models.count("gemini-3.1-flash-lite") == 6
        assert called_models.count("gemini-3.5-flash-lite") == 6
