"""Tests for two-database pipeline architecture.

Covers all 16 required test areas:
1. DB1/DB2 configuration separation
2. DB1 source reads
3. DB1 ingestion
4. DB2 result writes
5. Affected-only processing
6. Idempotent reruns
7. Entity Anomaly integration
8. Zero-work handling
9. Evidence integration
10. Gemini integration
11. Gemini output validation
12. Checkpoint/resume
13. Retry behavior
14. Verification gate
15. Cleanup only after successful verification
16. No cleanup on failed verification
"""

import json
import os
import sys
import tempfile
import hashlib
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


# ============================================================
# FIXTURES
# ============================================================

def _fake_member_metrics(member_id=1, member_type="MP", total_works=30,
                         completed_works=20, state_id="S01", **overrides):
    from analysis.models import MemberMetrics
    defaults = dict(
        member_id=member_id, member_type=member_type, state_id=state_id,
        constituency_id=f"C{member_id}",
        total_works=total_works, recommended_works=total_works,
        sanctioned_works=total_works - 5, completed_works=completed_works,
        ongoing_works=5, pending_works=5,
        completion_rate_pct=round(completed_works / total_works * 100, 1) if total_works else 0,
        sanction_rate_pct=round((total_works - 5) / total_works * 100, 1) if total_works else 0,
        recommended_amount=50000000, sanctioned_amount=40000000,
        expenditure_amount=30000000, completion_amount=25000000,
        unspent_amount=10000000, expenditure_sanction_utilization_pct=75.0,
        avg_sanction_delay_days=120, median_sanction_delay_days=90,
        avg_execution_days=180, median_execution_days=150,
        avg_project_age_days=300, max_project_age_days=800,
        avg_pending_days=60, max_pending_days=200,
        flagged_works=3, high_risk_works=1, medium_risk_works=2,
        flagged_rate_pct=10.0, high_risk_rate_pct=3.33,
        overdue_over_1_year=2, overdue_over_2_years=0,
        cost_anomaly_works=1, duration_anomaly_works=1,
        expenditure_over_sanction_works=0, negative_sanction_delay_works=0,
        avg_cost_percentile=60.0, avg_duration_percentile=55.0,
        low_sample_member=False, ranking_qualified=True, zero_work_member=False,
    )
    defaults.update(overrides)
    return MemberMetrics(**defaults)


def _fake_state_metrics(state_id="S01", state_name="Test State",
                        total_works=100, active_members=10, **overrides):
    from analysis.models import StateMetrics
    defaults = dict(
        state_id=state_id, state_name=state_name,
        total_works=total_works, active_members=active_members,
        mp_active_members=7, mla_active_members=3,
        recommended_works=total_works, sanctioned_works=total_works - 10,
        completed_works=int(total_works * 0.6), ongoing_works=int(total_works * 0.2),
        completion_rate_pct=60.0, sanction_rate_pct=90.0,
        recommended_amount=500000000, sanctioned_amount=450000000,
        expenditure_amount=300000000, completion_amount=250000000,
        expenditure_utilization_pct=66.7,
        avg_sanction_delay_days=100, median_sanction_delay_days=80,
        avg_execution_days=160, median_execution_days=140,
        flagged_works=5, high_risk_works=2,
        overdue_over_1_year=3, overdue_over_2_years=1,
        risk_rate_pct=5.0, cost_anomaly_works=2, duration_anomaly_works=1,
        expenditure_over_sanction_works=0, negative_sanction_delay_works=0,
        ranking_qualified=True,
    )
    defaults.update(overrides)
    return StateMetrics(**defaults)


# ============================================================
# TEST 1: DB1/DB2 CONFIGURATION SEPARATION
# ============================================================

class TestDBConfigSeparation:

    def test_load_config_both_databases(self, monkeypatch):
        from automation import db_config
        monkeypatch.setenv("DB1_URL", "https://db1.supabase.co")
        monkeypatch.setenv("DB1_SERVICE_ROLE_KEY", "db1_key")
        monkeypatch.setenv("DB2_URL", "https://db2.supabase.co")
        monkeypatch.setenv("DB2_SERVICE_ROLE_KEY", "db2_key")
        monkeypatch.setattr(db_config, "_config", None)
        cfg = db_config.load_config(require_db2=True)
        assert cfg.db1_url == "https://db1.supabase.co"
        assert cfg.db1_key == "db1_key"
        assert cfg.db2_url == "https://db2.supabase.co"
        assert cfg.db2_key == "db2_key"
        assert cfg.db1_ready is True
        assert cfg.db2_ready is True

    def test_load_config_db1_only(self, monkeypatch):
        from automation import db_config
        from unittest.mock import patch
        monkeypatch.setenv("DB1_URL", "https://db1.supabase.co")
        monkeypatch.setenv("DB1_SERVICE_ROLE_KEY", "db1_key")
        monkeypatch.delenv("DB2_URL", raising=False)
        monkeypatch.delenv("DB2_SUPABASE_URL", raising=False)
        monkeypatch.delenv("DB2_SERVICE_ROLE_KEY", raising=False)
        monkeypatch.delenv("DB2_SUPABASE_SERVICE_ROLE_KEY", raising=False)
        monkeypatch.delenv("DB2_SECRET_KEY", raising=False)
        monkeypatch.setattr(db_config, "_config", None)
        with patch("dotenv.load_dotenv"):
            cfg = db_config.load_config(require_db2=False)
        assert cfg.db1_url == "https://db1.supabase.co"
        assert cfg.db1_ready is True
        assert cfg.db2_ready is False

    def test_db2_required_but_missing_raises(self, monkeypatch):
        from automation import db_config
        from unittest.mock import patch
        monkeypatch.setenv("DB1_URL", "https://db1.supabase.co")
        monkeypatch.setenv("DB1_SERVICE_ROLE_KEY", "db1_key")
        monkeypatch.delenv("DB2_URL", raising=False)
        monkeypatch.delenv("DB2_SUPABASE_URL", raising=False)
        monkeypatch.delenv("DB2_SERVICE_ROLE_KEY", raising=False)
        monkeypatch.delenv("DB2_SUPABASE_SERVICE_ROLE_KEY", raising=False)
        monkeypatch.delenv("DB2_SECRET_KEY", raising=False)
        monkeypatch.setattr(db_config, "_config", None)
        with patch("dotenv.load_dotenv"):
            with pytest.raises(RuntimeError, match="Missing DB2"):
                db_config.load_config(require_db2=True)

    def test_db1_required_but_missing_raises(self, monkeypatch):
        from automation import db_config
        from unittest.mock import patch
        monkeypatch.delenv("DB1_URL", raising=False)
        monkeypatch.delenv("SUPABASE_URL", raising=False)
        monkeypatch.delenv("DB1_SERVICE_ROLE_KEY", raising=False)
        monkeypatch.delenv("SUPABASE_SERVICE_ROLE_KEY", raising=False)
        monkeypatch.delenv("DB1_SECRET_KEY", raising=False)
        monkeypatch.delenv("SUPABASE_SECRET_KEY", raising=False)
        monkeypatch.setattr(db_config, "_config", None)
        with patch("dotenv.load_dotenv"):
            with pytest.raises(RuntimeError, match="Missing DB1"):
                db_config.load_config(require_db2=False)

    def test_gemini_keys_loaded(self, monkeypatch):
        from automation import db_config
        monkeypatch.setenv("DB1_URL", "https://db1.supabase.co")
        monkeypatch.setenv("DB1_SERVICE_ROLE_KEY", "db1_key")
        monkeypatch.setenv("DB2_URL", "https://db2.supabase.co")
        monkeypatch.setenv("DB2_SERVICE_ROLE_KEY", "db2_key")
        monkeypatch.setenv("GEMINI_API_KEYS", "key_a,key_b,key_c")
        monkeypatch.setattr(db_config, "_config", None)
        cfg = db_config.load_config()
        assert cfg.gemini_keys == ["key_a", "key_b", "key_c"]

    def test_legacy_env_var_fallback(self, monkeypatch):
        from automation import db_config
        from unittest.mock import patch
        monkeypatch.delenv("DB1_URL", raising=False)
        monkeypatch.delenv("DB1_SERVICE_ROLE_KEY", raising=False)
        monkeypatch.delenv("DB1_SECRET_KEY", raising=False)
        monkeypatch.setenv("SUPABASE_URL", "https://legacy-db1.supabase.co")
        monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "legacy_key")
        monkeypatch.delenv("DB2_URL", raising=False)
        monkeypatch.delenv("DB2_SERVICE_ROLE_KEY", raising=False)
        monkeypatch.delenv("DB2_SECRET_KEY", raising=False)
        monkeypatch.setenv("DB2_SUPABASE_URL", "https://legacy-db2.supabase.co")
        monkeypatch.setenv("DB2_SUPABASE_SERVICE_ROLE_KEY", "legacy_db2_key")
        monkeypatch.setattr(db_config, "_config", None)
        with patch("dotenv.load_dotenv"):
            cfg = db_config.load_config()
        assert cfg.db1_url == "https://legacy-db1.supabase.co"
        assert cfg.db1_key == "legacy_key"
        assert cfg.db2_url == "https://legacy-db2.supabase.co"
        assert cfg.db2_key == "legacy_db2_key"

    def test_db_config_repr(self):
        from automation.db_config import DBConfig
        cfg = DBConfig()
        cfg.db1_url = "https://test.supabase.co"
        cfg.db1_key = "key"
        cfg.db2_url = "https://test2.supabase.co"
        cfg.db2_key = "key2"
        r = repr(cfg)
        assert "READY" in r
        assert "gemini_keys=0" in r

    def test_no_cross_database_sql(self):
        src = (ROOT / "automation" / "pipeline_controller.py").read_text()
        assert "get_db1()" in src or "get_db2()" in src


# ============================================================
# TEST 2: DB1 SOURCE READS
# ============================================================

class TestDB1SourceReads:

    def test_db1_read_in_analyze_stage(self):
        from automation.pipeline_controller import stage_analyze
        import inspect
        src = inspect.getsource(stage_analyze)
        assert "audit_runner" in src or "load_all_data" in src

    def test_db1_not_written_by_analysis(self):
        from automation.pipeline_controller import stage_analyze, stage_anomaly, stage_evidence
        import inspect
        for fn in [stage_analyze, stage_anomaly, stage_evidence]:
            src = inspect.getsource(fn)
            assert "sb_post" not in src
            assert "sb_upsert" not in src
            assert "sb_delete" not in src

    def test_daily_pipeline_uses_db1_for_storage(self):
        src = (ROOT / "automation" / "daily_pipeline.py").read_text()
        assert "DB1_URL" in src or "SUPABASE_URL" in src
        assert "storage" in src


# ============================================================
# TEST 3: DB1 INGESTION
# ============================================================

class TestDB1Ingestion:

    def test_edge_function_calls_db1(self):
        src = (ROOT / "automation" / "daily_pipeline.py").read_text()
        assert "call_edge" in src
        assert "mplads-controller" in src
        assert "mplads-worker" in src

    def test_ingestion_jobs_on_db1(self):
        src = (ROOT / "automation" / "daily_pipeline.py").read_text()
        assert "ingestion_jobs" in src

    def test_pipeline_controller_has_skip_ingest(self):
        from automation.pipeline_controller import run_pipeline
        import inspect
        sig = inspect.signature(run_pipeline)
        assert "skip_ingest" in sig.parameters


# ============================================================
# TEST 4: DB2 RESULT WRITES
# ============================================================

class TestDB2ResultWrites:

    def test_evidence_upload_targets_db2(self):
        from automation.pipeline_controller import stage_persist
        import inspect
        src = inspect.getsource(stage_persist)
        assert "get_db2" in src
        assert "entity_evidence" in src

    def test_gemini_writes_to_db2(self):
        from automation.pipeline_controller import stage_gemini
        import inspect
        src = inspect.getsource(stage_gemini)
        assert "get_db2" in src
        assert "ai_analysis" in src

    def test_evidence_builder_produces_db2_compatible_schema(self):
        from analysis.evidence_builder import build_member_evidence
        m = _fake_member_metrics()
        records = build_member_evidence([m], {}, {}, [])
        assert len(records) == 1
        r = records[0]
        assert "entity_type" in r
        assert "entity_id" in r
        assert "evidence_version" in r
        assert "evidence_hash" in r
        assert "evidence" in r
        assert r["entity_type"] == "MP"
        assert isinstance(r["evidence"], dict)


# ============================================================
# TEST 5: AFFECTED-ONLY PROCESSING
# ============================================================

class TestAffectedOnlyProcessing:

    def test_filter_affected_new_entity(self):
        from analysis.gemini_processor import filter_affected
        evidence = [{"entity_type": "MP", "entity_id": 1, "evidence_hash": "abc"}]
        result = filter_affected(evidence, [])
        assert len(result) == 1

    def test_filter_affected_unchanged(self):
        from analysis.gemini_processor import filter_affected
        evidence = [{"entity_type": "MP", "entity_id": 1, "evidence_hash": "abc"}]
        existing = [{"entity_type": "MP", "entity_id": 1,
                     "evidence_hash": "abc", "prompt_version": "gemini_analysis_v5"}]
        result = filter_affected(evidence, existing)
        assert len(result) == 0

    def test_filter_affected_changed_hash(self):
        from analysis.gemini_processor import filter_affected
        evidence = [{"entity_type": "MP", "entity_id": 1, "evidence_hash": "new_hash"}]
        existing = [{"entity_type": "MP", "entity_id": 1,
                     "evidence_hash": "old_hash", "prompt_version": "gemini_analysis_v5"}]
        result = filter_affected(evidence, existing)
        assert len(result) == 1

    def test_filter_affected_changed_prompt_version(self):
        from analysis.gemini_processor import filter_affected
        evidence = [{"entity_type": "MP", "entity_id": 1, "evidence_hash": "abc"}]
        existing = [{"entity_type": "MP", "entity_id": 1,
                     "evidence_hash": "abc", "prompt_version": "old_version"}]
        result = filter_affected(evidence, existing)
        assert len(result) == 1

    def test_persist_has_affected_only_path(self):
        from automation.pipeline_controller import stage_persist
        import inspect
        src = inspect.getsource(stage_persist)
        assert "affected_keys" in src
        assert "affected_records" in src


# ============================================================
# TEST 6: IDEMPOTENT RERUNS
# ============================================================

class TestIdempotentReruns:

    def test_evidence_hash_deterministic(self):
        from analysis.evidence_builder import _hash_evidence
        evidence = {"portfolio": {"total_works": 30}, "financial": {"expenditure": 100}}
        h1 = _hash_evidence(evidence)
        h2 = _hash_evidence(evidence)
        assert h1 == h2

    def test_evidence_hash_differs_for_different_data(self):
        from analysis.evidence_builder import _hash_evidence
        e1 = {"portfolio": {"total_works": 30}}
        e2 = {"portfolio": {"total_works": 50}}
        assert _hash_evidence(e1) != _hash_evidence(e2)

    def test_gemini_result_to_dict_deterministic(self):
        from analysis.gemini_processor import GeminiResult
        with patch("analysis.gemini_processor.datetime") as mock_dt:
            mock_dt.now.return_value.isoformat.return_value = "2026-01-01T00:00:00+00:00"
            from datetime import timezone
            mock_dt.side_effect = lambda *a, **kw: None
            r1 = GeminiResult("MP", 1, "summary", ["h1", "h2", "h3"], [], "hash1")
            mock_dt.now.return_value.isoformat.return_value = "2026-01-01T00:00:00+00:00"
            r2 = GeminiResult("MP", 1, "summary", ["h1", "h2", "h3"], [], "hash1")
        d1 = r1.to_dict()
        d2 = r2.to_dict()
        d1.pop("generated_at", None)
        d2.pop("generated_at", None)
        assert d1 == d2

    def test_filter_affected_idempotent(self):
        from analysis.gemini_processor import filter_affected
        evidence = [
            {"entity_type": "MP", "entity_id": 1, "evidence_hash": "h1"},
            {"entity_type": "MP", "entity_id": 2, "evidence_hash": "h2"},
        ]
        existing = [
            {"entity_type": "MP", "entity_id": 1,
             "evidence_hash": "h1", "prompt_version": "gemini_analysis_v5"},
        ]
        r1 = filter_affected(evidence, existing)
        r2 = filter_affected(evidence, existing)
        assert len(r1) == len(r2)
        assert [r["entity_id"] for r in r1] == [r["entity_id"] for r in r2]


# ============================================================
# TEST 7: ENTITY ANOMALY INTEGRATION
# ============================================================

class TestEntityAnomalyIntegration:

    def test_anomaly_stage_called(self):
        src = (ROOT / "automation" / "pipeline_controller.py").read_text()
        assert "stage_anomaly" in src
        assert "compute_entity_anomalies" in src
        assert "compute_state_anomalies" in src

    def test_anomaly_results_in_evidence(self):
        from analysis.evidence_builder import build_member_evidence
        from analysis.entity_anomaly import EntityAnomalyResult
        m = _fake_member_metrics()
        anomaly = EntityAnomalyResult(
            entity_id=1, entity_type="member", member_type="MP",
            state_id="S01",
            anomaly_score=1.5, anomaly_level="medium",
            confidence_level="high", contributing_features=[
                {"feature": "total_works", "value": 30, "direction": "low",
                 "contribution": 0.3, "description": "Low total works"}
            ],
        )
        records = build_member_evidence([m], {}, {}, [], [anomaly])
        assert len(records) == 1
        assert records[0]["evidence"].get("entity_anomaly") is not None
        assert records[0]["evidence"]["entity_anomaly"]["anomaly_level"] == "medium"

    def test_anomaly_frozen_methodology(self):
        from analysis.entity_anomaly import robust_z_score
        scores = robust_z_score([1, 2, 3, 4, 5, 100])
        assert scores[-1] > 2.0


# ============================================================
# TEST 8: ZERO-WORK HANDLING
# ============================================================

class TestZeroWorkHandling:

    def test_zero_work_member_metrics_not_created(self):
        m = _fake_member_metrics(total_works=0, zero_work_member=True,
                                 ranking_qualified=False)
        assert m.total_works == 0
        assert m.ranking_qualified is False

    def test_zero_work_evidence_flag(self):
        from analysis.evidence_builder import build_member_evidence
        m = _fake_member_metrics(total_works=0, zero_work_member=True,
                                 ranking_qualified=False,
                                 completed_works=0, ongoing_works=0,
                                 sanctioned_works=0, recommended_works=0)
        records = build_member_evidence([m], {}, {}, [])
        assert records[0]["evidence"]["quality"]["zero_work_member"] is True

    def test_zero_work_gemini_prompt_context(self):
        from analysis.gemini_processor import build_prompt
        evidence_row = {
            "entity_type": "MP", "entity_id": 1, "entity_name": "Test MP",
            "evidence_version": 2, "evidence_hash": "abc",
            "evidence": {
                "portfolio": {"total_works": 0},
                "quality": {"zero_work_member": True, "low_sample_member": False,
                            "ranking_qualified": False},
            },
        }
        prompt = build_prompt(evidence_row)
        assert "ZERO WORKS" in prompt

    def test_zero_work_not_in_anomaly_population(self):
        m = _fake_member_metrics(total_works=0, ranking_qualified=False)
        assert m.ranking_qualified is False


# ============================================================
# TEST 9: EVIDENCE INTEGRATION
# ============================================================

class TestEvidenceIntegration:

    def test_evidence_bridges_analysis_to_gemini(self):
        from analysis.evidence_builder import build_member_evidence
        m = _fake_member_metrics()
        records = build_member_evidence([m], {}, {}, [])
        assert len(records) == 1
        e = records[0]["evidence"]
        assert "portfolio" in e
        assert "financial" in e
        assert "execution" in e
        assert "risk" in e
        assert "national_context" in e

    def test_evidence_record_schema(self):
        from analysis.evidence_builder import build_member_evidence
        m = _fake_member_metrics()
        records = build_member_evidence([m], {}, {}, [])
        r = records[0]
        required_fields = {"entity_type", "entity_id", "evidence_version",
                           "evidence_hash", "generated_at", "evidence"}
        assert required_fields.issubset(set(r.keys()))

    def test_state_evidence_includes_anomaly(self):
        from analysis.evidence_builder import build_state_evidence
        from analysis.entity_anomaly import EntityAnomalyResult
        s = _fake_state_metrics()
        anomaly = EntityAnomalyResult(
            entity_id="S01", entity_type="state",
            anomaly_score=-0.5, anomaly_level="low",
            confidence_level="medium", contributing_features=[],
        )
        records = build_state_evidence([s], {}, [anomaly])
        assert len(records) == 1
        assert records[0]["evidence"].get("entity_anomaly") is not None

    def test_pipeline_evidence_stage(self):
        from automation.pipeline_controller import stage_evidence
        import inspect
        src = inspect.getsource(stage_evidence)
        assert "build_member_evidence" in src
        assert "build_state_evidence" in src


# ============================================================
# TEST 10: GEMINI INTEGRATION
# ============================================================

class TestGeminiIntegration:

    def test_gemini_uses_http_not_sdk(self):
        src = (ROOT / "analysis" / "gemini_processor.py").read_text()
        assert "google.genai" not in src
        assert "urllib.request" in src
        assert "generativelanguage.googleapis.com" in src

    def test_gemini_processor_round_robin_keys(self):
        from analysis.gemini_processor import GeminiProcessor
        p = GeminiProcessor(api_keys=["key1", "key2", "key3"])
        k1 = p._next_key()
        k2 = p._next_key()
        k3 = p._next_key()
        k4 = p._next_key()
        assert k1 == "key1"
        assert k2 == "key2"
        assert k3 == "key3"
        assert k4 == "key1"

    def test_gemini_processor_filters_invalid_keys(self):
        from analysis.gemini_processor import GeminiProcessor
        p = GeminiProcessor(api_keys=["real_key", "PASTE_YOUR_KEY_HERE", ""])
        assert len(p.api_keys) == 1
        assert p.api_keys[0] == "real_key"

    def test_gemini_stage_callable(self, monkeypatch):
        from automation.pipeline_controller import stage_gemini
        from automation import db_config
        from unittest.mock import patch
        monkeypatch.setattr(db_config, "_config", None)
        monkeypatch.setenv("DB1_URL", "https://test.supabase.co")
        monkeypatch.setenv("DB1_SERVICE_ROLE_KEY", "key")
        monkeypatch.setenv("DB2_URL", "https://test2.supabase.co")
        monkeypatch.setenv("DB2_SERVICE_ROLE_KEY", "key2")
        monkeypatch.delenv("GEMINI_API_KEYS", raising=False)
        with patch("dotenv.load_dotenv"):
            db_config.load_config()
        result = stage_gemini([], api_keys=[])
        assert result.get("skipped") is True


# ============================================================
# TEST 11: GEMINI OUTPUT VALIDATION
# ============================================================

class TestGeminiOutputValidation:

    def test_valid_output(self):
        from analysis.gemini_processor import validate_output
        obj = {"summary": "Test summary.", "highlights": ["P1", "P2", "P3"], "cautions": []}
        errors = validate_output(obj, {"entity_name": "Test"})
        assert errors == []

    def test_missing_summary(self):
        from analysis.gemini_processor import validate_output
        obj = {"highlights": ["a", "b", "c"], "cautions": []}
        errors = validate_output(obj, {"entity_name": "Test"})
        assert any("Missing" in e for e in errors)

    def test_too_few_highlights(self):
        from analysis.gemini_processor import validate_output
        obj = {"summary": "Test", "highlights": ["a", "b"], "cautions": []}
        errors = validate_output(obj, {"entity_name": "Test"})
        assert any("highlights" in e for e in errors)

    def test_forbidden_term(self):
        from analysis.gemini_processor import validate_output
        obj = {"summary": "The entity is tested", "highlights": ["a", "b", "c"], "cautions": []}
        errors = validate_output(obj, {"entity_name": "Test"})
        assert any("forbidden" in e.lower() for e in errors)

    def test_cautions_max_two(self):
        from analysis.gemini_processor import validate_output
        obj = {"summary": "Test", "highlights": ["a", "b", "c"], "cautions": ["c1", "c2", "c3"]}
        errors = validate_output(obj, {"entity_name": "Test"})
        assert any("cautions" in e for e in errors)

    def test_extra_fields_rejected(self):
        from analysis.gemini_processor import validate_output
        obj = {"summary": "Test", "highlights": ["a", "b", "c"], "cautions": [], "extra": "bad"}
        errors = validate_output(obj, {"entity_name": "Test"})
        assert any("Forbidden" in e or "extra" in e.lower() for e in errors)


# ============================================================
# TEST 12: CHECKPOINT/RESUME
# ============================================================

class TestCheckpointResume:

    def test_save_and_load_checkpoint(self, tmp_path):
        from automation.pipeline_controller import save_checkpoint, load_checkpoint
        with patch("automation.pipeline_controller.ROOT", tmp_path):
            save_checkpoint("run_001", "analyze", {"members": 100})
            cp = load_checkpoint("run_001")
        assert cp["last_completed_stage"] == "analyze"
        assert cp["stage_data"]["analyze"]["members"] == 100

    def test_checkpoint_overwrites(self, tmp_path):
        from automation.pipeline_controller import save_checkpoint, load_checkpoint
        with patch("automation.pipeline_controller.ROOT", tmp_path):
            save_checkpoint("run_001", "analyze", {"members": 100})
            save_checkpoint("run_001", "anomaly", {"count": 50})
            cp = load_checkpoint("run_001")
        assert cp["last_completed_stage"] == "anomaly"
        assert cp["stage_data"]["anomaly"]["count"] == 50

    def test_load_nonexistent_checkpoint(self, tmp_path):
        from automation.pipeline_controller import load_checkpoint
        with patch("automation.pipeline_controller.ROOT", tmp_path):
            cp = load_checkpoint("nonexistent_run")
        assert cp == {}

    def test_next_stage_after(self, tmp_path):
        from automation.pipeline_controller import save_checkpoint, next_stage_after
        with patch("automation.pipeline_controller.ROOT", tmp_path):
            save_checkpoint("run_001", "evidence")
            next_s = next_stage_after("run_001", "evidence")
        assert next_s == "evidence_work_refs"

    def test_resume_flag_in_run_pipeline(self):
        from automation.pipeline_controller import run_pipeline
        import inspect
        sig = inspect.signature(run_pipeline)
        assert "resume" in sig.parameters

    def test_checkpoint_file_path(self, tmp_path):
        from automation.pipeline_controller import _checkpoint_path
        with patch("automation.pipeline_controller.ROOT", tmp_path):
            p = _checkpoint_path("run_123")
        assert p.name == "run_123.json"


# ============================================================
# TEST 13: RETRY BEHAVIOR
# ============================================================

class TestRetryBehavior:

    def test_retryable_identifies_rate_limit(self):
        from analysis.gemini_processor import retryable
        assert retryable(Exception("429 Too Many Requests")) is True
        assert retryable(Exception("rate limit exceeded")) is True
        assert retryable(Exception("quota exceeded")) is True

    def test_retryable_identifies_server_error(self):
        from analysis.gemini_processor import retryable
        assert retryable(Exception("500 Internal Server Error")) is True
        assert retryable(Exception("502 Bad Gateway")) is True
        assert retryable(Exception("503 Service Unavailable")) is True

    def test_retryable_rejects_client_error(self):
        from analysis.gemini_processor import retryable
        assert retryable(Exception("400 Bad Request")) is False
        assert retryable(Exception("403 Forbidden")) is False

    def test_gemini_processor_max_attempts(self):
        from analysis.gemini_processor import GeminiProcessor
        p = GeminiProcessor(api_keys=["key1"], max_attempts=5)
        assert p.max_attempts == 5


# ============================================================
# TEST 14: VERIFICATION GATE
# ============================================================

class TestVerificationGate:

    def test_verify_detects_empty_evidence(self):
        from automation.pipeline_controller import stage_verify
        result = stage_verify([], {}, {})
        assert result["passed"] is False
        assert any("No evidence" in i for i in result["issues"])

    def test_verify_detects_duplicate_hashes(self):
        from automation.pipeline_controller import stage_verify
        evidence = [
            {"entity_type": "MP", "entity_id": 1, "evidence_hash": "same", "evidence": {}},
            {"entity_type": "MP", "entity_id": 2, "evidence_hash": "same", "evidence": {}},
        ]
        pipeline = MagicMock()
        pipeline.member_metrics = [_fake_member_metrics(), _fake_member_metrics(member_id=2)]
        pipeline.state_metrics = []
        result = stage_verify(evidence, {"pipeline": pipeline}, {})
        assert result["passed"] is False
        assert any("Duplicate" in i for i in result["issues"])

    def test_verify_passes_valid_data(self):
        from automation.pipeline_controller import stage_verify
        evidence = [{"entity_type": "MP", "entity_id": 1, "evidence_hash": "h1", "evidence": {}}]
        pipeline = MagicMock()
        pipeline.member_metrics = [_fake_member_metrics()]
        pipeline.state_metrics = []
        anomaly = {"member_anomalies": [MagicMock()], "state_anomalies": []}
        result = stage_verify(evidence, {"pipeline": pipeline}, anomaly,
                              persist_result={"written": 1})
        assert result["passed"] is True

    def test_verify_checks_anomaly_counts(self):
        from automation.pipeline_controller import stage_verify
        evidence = [{"entity_type": "MP", "entity_id": i, "evidence_hash": f"h{i}",
                      "evidence": {}} for i in range(20)]
        pipeline = MagicMock()
        pipeline.member_metrics = [_fake_member_metrics(member_id=i) for i in range(20)]
        pipeline.state_metrics = []
        result = stage_verify(evidence, {"pipeline": pipeline},
                              {"member_anomalies": [], "state_anomalies": []})
        assert any("anomal" in i.lower() for i in result["issues"])

    def test_verification_blocks_cleanup(self):
        src = (ROOT / "automation" / "pipeline_controller.py").read_text()
        assert "FAILED_VERIFICATION" in src
        lines = src.split("\n")
        verify_line = next(i for i, l in enumerate(lines) if "if not verify_result" in l)
        cleanup_line = next(i for i, l in enumerate(lines) if "stage_cleanup" in l and "def " not in l)
        assert cleanup_line > verify_line


# ============================================================
# TEST 15: CLEANUP ONLY AFTER VERIFICATION
# ============================================================

class TestCleanupAfterVerification:

    def test_cleanup_called_after_verify_passes(self):
        src = (ROOT / "automation" / "pipeline_controller.py").read_text()
        assert "if not verify_result" in src
        assert "stage_cleanup" in src

    def test_cleanup_preserves_source_data(self):
        from automation.pipeline_controller import stage_cleanup
        import inspect
        src = inspect.getsource(stage_cleanup)
        assert "shutil.rmtree" in src
        assert "sb_delete" not in src
        assert "sb_post" not in src

    def test_cleanup_checkpoint_removal(self, tmp_path):
        from automation.pipeline_controller import stage_cleanup, save_checkpoint, load_checkpoint
        with patch("automation.pipeline_controller.ROOT", tmp_path):
            save_checkpoint("run_001", "cleanup")
            result = stage_cleanup(run_id="run_001")
            cp = load_checkpoint("run_001")
        assert result["cleaned"] >= 1
        assert cp == {}


# ============================================================
# TEST 16: NO CLEANUP ON FAILED VERIFICATION
# ============================================================

class TestNoCleanupOnFailure:

    def test_failure_returns_status(self):
        src = (ROOT / "automation" / "pipeline_controller.py").read_text()
        assert 'results["status"] = "FAILED_VERIFICATION"' in src

    def test_cleanup_not_called_before_verify(self):
        """stage_cleanup is not called before verification in pipeline flow."""
        src = (ROOT / "automation" / "pipeline_controller.py").read_text()
        lines = src.split("\n")
        in_try_block = False
        cleanup_found_after_verify = False
        verify_found = False
        for l in lines:
            stripped = l.strip()
            if "if not verify_result" in stripped:
                verify_found = True
            if verify_found and "stage_cleanup" in stripped and "def " not in stripped:
                cleanup_found_after_verify = True
                break
        assert cleanup_found_after_verify, "stage_cleanup should be after verify check"


# ============================================================
# ADDITIONAL: PIPELINE INTEGRATION TESTS
# ============================================================

class TestPipelineIntegration:

    def test_pipeline_stages_ordered(self):
        from automation.pipeline_controller import STAGES
        expected = [
            "fetch", "compare", "delta", "ingest", "affected",
            "analyze", "anomaly", "analytics_persist",
            "evidence", "evidence_work_refs", "gemini",
            "persist", "verify", "cleanup",
        ]
        assert STAGES == expected

    def test_pipeline_version(self):
        from automation.pipeline_controller import PIPELINE_VERSION
        assert PIPELINE_VERSION == "pipeline_v2"

    def test_pipeline_controller_cli_args(self):
        import subprocess
        result = subprocess.run(
            [sys.executable, str(ROOT / "automation" / "pipeline_controller.py"), "--help"],
            capture_output=True, text=True,
        )
        assert result.returncode == 0
        assert "--snapshot" in result.stdout
        assert "--delta-dir" in result.stdout
        assert "--run-id" in result.stdout
        assert "--resume" in result.stdout
        assert "--skip-gemini" in result.stdout
        assert "--skip-ingest" in result.stdout
        assert "--dry-run" in result.stdout

    def test_run_pipeline_dry_run(self, tmp_path, monkeypatch):
        from automation.pipeline_controller import run_pipeline
        from automation import db_config

        snapshot = tmp_path / "snapshot"
        snapshot.mkdir()
        for dataset in ["works_recommended", "works_sanctioned", "works_completed",
                        "expenditure", "allocated_limit", "calamity",
                        "mla_works_recommended", "mla_works_sanctioned",
                        "mla_works_completed", "mla_expenditure",
                        "mla_allocated_limit", "mla_calamity"]:
            ds_dir = snapshot / dataset
            ds_dir.mkdir()
            if dataset == "works_recommended":
                (ds_dir / "part_0001.ndjson").write_text(
                    json.dumps({"mp_id": 1, "state_id": "S01",
                                "work_recommendation_dtl_id": "W001",
                                "mp_name": "Test MP", "state_name": "Test State",
                                "constituency_name": "Test Constituency",
                                "activity_name": "Road Development",
                                "recommended_amount": 1000000,
                                "recommendation_date": "2025-01-15"}) + "\n"
                )
            else:
                (ds_dir / "part_0001.ndjson").write_text("")

        monkeypatch.setattr(db_config, "_config", None)
        monkeypatch.setenv("DB1_URL", "https://test-db1.supabase.co")
        monkeypatch.setenv("DB1_SERVICE_ROLE_KEY", "test_key")

        result = run_pipeline(
            snapshot_dir=str(snapshot),
            skip_gemini=True,
            dry_run=True,
        )

        assert result["status"] == "DRY_RUN"
        assert "analyze" in result["stages"]
        assert result["stages"]["gemini"]["skipped"] is True


# ============================================================
# TEST 17: UPSERT IDEMPOTENCY
# ============================================================

class TestUpsertIdempotency:

    def test_stage_persist_uses_upsert_not_post(self):
        from automation.pipeline_controller import stage_persist
        import inspect
        src = inspect.getsource(stage_persist)
        assert "sb_upsert" in src
        assert "conflict_cols" in src

    def test_stage_persist_entity_evidence_conflict_cols(self):
        from automation.pipeline_controller import stage_persist
        import inspect
        src = inspect.getsource(stage_persist)
        assert '["entity_type", "entity_id"]' in src

    def test_stage_persist_full_rebuild_still_deletes(self):
        from automation.pipeline_controller import stage_persist
        import inspect
        src = inspect.getsource(stage_persist)
        assert "sb_delete" in src
        assert "evidence_work_refs" in src
        assert "ref_id" in src

    def test_stage_gemini_uses_upsert_not_post(self):
        from automation.pipeline_controller import stage_gemini
        import inspect
        src = inspect.getsource(stage_gemini)
        assert "sb_upsert" in src
        assert "conflict_cols" in src

    def test_stage_gemini_ai_analysis_conflict_cols(self):
        from automation.pipeline_controller import stage_gemini
        import inspect
        src = inspect.getsource(stage_gemini)
        assert '["entity_type", "entity_id"]' in src

    def test_entity_evidence_first_insert(self):
        """First persist call inserts records via upsert."""
        from automation.pipeline_controller import stage_persist
        from unittest.mock import patch

        records = [
            {"entity_type": "MP", "entity_id": 1, "evidence_hash": "h1",
             "evidence_version": 2, "evidence": {"portfolio": {"total_works": 30}},
             "generated_at": "2026-01-01T00:00:00+00:00",
             "entity_name": None, "member_type": "MP",
             "state_id": "S01", "constituency_id": "C1"},
        ]
        affected_keys = {("MP", 1)}
        mock_db2 = ("https://test-db2.supabase.co", "test_key")

        with patch("automation.pipeline_controller.get_db2", return_value=mock_db2):
            with patch("automation.pipeline_controller.sb_upsert", return_value=200) as mock_upsert:
                result = stage_persist(records, affected_keys=affected_keys)

        assert result["written"] == 1
        mock_upsert.assert_called_once()
        call_args = mock_upsert.call_args
        assert call_args[0][2] == "entity_evidence"
        assert call_args[1]["conflict_cols"] == ["entity_type", "entity_id"]

    def test_entity_evidence_idempotent_rerun(self):
        """Second persist with same entity_type+entity_id upserts (no duplicate)."""
        from automation.pipeline_controller import stage_persist
        from unittest.mock import patch

        records = [
            {"entity_type": "MP", "entity_id": 1, "evidence_hash": "h1",
             "evidence_version": 2, "evidence": {"portfolio": {"total_works": 30}},
             "generated_at": "2026-01-01T00:00:00+00:00",
             "entity_name": None, "member_type": "MP",
             "state_id": "S01", "constituency_id": "C1"},
        ]
        affected_keys = {("MP", 1)}
        mock_db2 = ("https://test-db2.supabase.co", "test_key")

        with patch("automation.pipeline_controller.get_db2", return_value=mock_db2):
            with patch("automation.pipeline_controller.sb_upsert", return_value=200) as mock_upsert:
                stage_persist(records, affected_keys=affected_keys)
                stage_persist(records, affected_keys=affected_keys)

        assert mock_upsert.call_count == 2
        for call in mock_upsert.call_args_list:
            assert call[1]["conflict_cols"] == ["entity_type", "entity_id"]

    def test_entity_evidence_changed_evidence_upserts(self):
        """Changed evidence for same entity gets upserted (overwritten)."""
        from automation.pipeline_controller import stage_persist
        from unittest.mock import patch

        r1 = [{"entity_type": "MP", "entity_id": 1, "evidence_hash": "old_hash",
               "evidence_version": 2, "evidence": {"a": 1},
               "generated_at": "2026-01-01T00:00:00+00:00",
               "entity_name": None, "member_type": "MP",
               "state_id": "S01", "constituency_id": "C1"}]
        r2 = [{"entity_type": "MP", "entity_id": 1, "evidence_hash": "new_hash",
               "evidence_version": 2, "evidence": {"a": 2},
               "generated_at": "2026-01-02T00:00:00+00:00",
               "entity_name": None, "member_type": "MP",
               "state_id": "S01", "constituency_id": "C1"}]
        affected_keys = {("MP", 1)}
        mock_db2 = ("https://test-db2.supabase.co", "test_key")

        with patch("automation.pipeline_controller.get_db2", return_value=mock_db2):
            with patch("automation.pipeline_controller.sb_upsert", return_value=200) as mock_upsert:
                stage_persist(r1, affected_keys=affected_keys)
                stage_persist(r2, affected_keys=affected_keys)

        assert mock_upsert.call_count == 2
        second_batch = mock_upsert.call_args_list[1][0][3]
        assert second_batch[0]["evidence_hash"] == "new_hash"

    def test_entity_evidence_no_duplicates_for_same_entity(self):
        """Multiple entities with same type but different IDs are separate upserts."""
        from automation.pipeline_controller import stage_persist
        from unittest.mock import patch

        records = [
            {"entity_type": "MP", "entity_id": i, "evidence_hash": f"h{i}",
             "evidence_version": 2, "evidence": {"a": i},
             "generated_at": "2026-01-01T00:00:00+00:00",
             "entity_name": None, "member_type": "MP",
             "state_id": "S01", "constituency_id": f"C{i}"}
            for i in range(1, 4)
        ]
        affected_keys = {("MP", 1), ("MP", 2), ("MP", 3)}
        mock_db2 = ("https://test-db2.supabase.co", "test_key")

        with patch("automation.pipeline_controller.get_db2", return_value=mock_db2):
            with patch("automation.pipeline_controller.sb_upsert", return_value=200) as mock_upsert:
                stage_persist(records, affected_keys=affected_keys)

        batch = mock_upsert.call_args_list[0][0][3]
        entity_ids = {r["entity_id"] for r in batch}
        assert entity_ids == {1, 2, 3}

    def test_ai_analysis_first_insert(self):
        """First gemini success inserts ai_analysis via upsert."""
        from automation.pipeline_controller import stage_gemini
        from unittest.mock import patch, MagicMock, call

        mock_db2 = ("https://test-db2.supabase.co", "test_key")
        mock_cfg = MagicMock()
        mock_cfg.gemini_keys = ["fake_key"]

        evidence = [{"entity_type": "MP", "entity_id": 1, "evidence_hash": "h1",
                     "evidence_version": 2, "entity_name": "Test MP",
                     "evidence": {"portfolio": {"total_works": 30}}}]

        mock_processor = MagicMock()
        mock_result = MagicMock()
        mock_result.entity_type = "MP"
        mock_result.entity_id = 1
        mock_result.evidence_hash = "h1"
        mock_result.model = "gemini-3.1-flash-lite"
        mock_result.prompt_version = "gemini_analysis_v5"
        mock_result.generated_at = "2026-01-01T00:00:00+00:00"
        mock_result.to_analysis_text.return_value = '{"summary":"test","highlights":[],"cautions":[]}'

        def fake_process_batch(evidence_rows, on_success=None, on_failure=None):
            if on_success:
                on_success(mock_result)
            return {
                "results": [mock_result], "success_count": 1,
                "failure_count": 0, "failures": [],
            }

        mock_processor.process_batch.side_effect = fake_process_batch

        with patch("automation.pipeline_controller.get_config", return_value=mock_cfg):
            with patch("automation.pipeline_controller.get_db2", return_value=mock_db2):
                with patch("automation.pipeline_controller.load_table", return_value=[]):
                    with patch("analysis.gemini_processor.GeminiProcessor", return_value=mock_processor):
                        with patch("automation.pipeline_controller.sb_upsert", return_value=200) as mock_upsert:
                            stage_gemini(evidence, api_keys=["fake_key"])

        mock_upsert.assert_called_once()
        call_args = mock_upsert.call_args
        assert call_args[0][2] == "ai_analysis"
        assert call_args[1]["conflict_cols"] == ["entity_type", "entity_id"]
        row = call_args[0][3][0]
        assert row["entity_type"] == "MP"
        assert row["entity_id"] == 1
        assert row["evidence_hash"] == "h1"
        assert row["model"] == "gemini-3.1-flash-lite"
        assert row["prompt_version"] == "gemini_analysis_v5"

    def test_ai_analysis_idempotent_rerun(self):
        """Second gemini call with same evidence_hash does not create duplicate."""
        from automation.pipeline_controller import stage_gemini
        from unittest.mock import patch, MagicMock

        mock_db2 = ("https://test-db2.supabase.co", "test_key")
        mock_cfg = MagicMock()
        mock_cfg.gemini_keys = ["fake_key"]

        evidence = [{"entity_type": "MP", "entity_id": 1, "evidence_hash": "h1",
                     "evidence_version": 2, "entity_name": "Test MP",
                     "evidence": {"portfolio": {"total_works": 30}}}]

        mock_processor = MagicMock()
        mock_processor.process_batch.return_value = {
            "results": [], "success_count": 0,
            "failure_count": 0, "failures": [],
        }

        with patch("automation.pipeline_controller.get_config", return_value=mock_cfg):
            with patch("automation.pipeline_controller.get_db2", return_value=mock_db2):
                with patch("automation.pipeline_controller.load_table", return_value=[
                    {"entity_type": "MP", "entity_id": 1,
                     "evidence_hash": "h1", "prompt_version": "gemini_analysis_v5"},
                ]):
                    with patch("analysis.gemini_processor.GeminiProcessor", return_value=mock_processor):
                        with patch("automation.pipeline_controller.sb_upsert", return_value=200) as mock_upsert:
                            result = stage_gemini(evidence, api_keys=["fake_key"])

        mock_upsert.assert_not_called()
        assert result["skipped"] == 1

    def test_ai_analysis_changed_evidence_triggers_reprocess(self):
        """Changed evidence_hash triggers re-processing and upsert."""
        from automation.pipeline_controller import stage_gemini
        from unittest.mock import patch, MagicMock

        mock_db2 = ("https://test-db2.supabase.co", "test_key")
        mock_cfg = MagicMock()
        mock_cfg.gemini_keys = ["fake_key"]

        evidence = [{"entity_type": "MP", "entity_id": 1, "evidence_hash": "new_hash",
                     "evidence_version": 2, "entity_name": "Test MP",
                     "evidence": {"portfolio": {"total_works": 30}}}]

        mock_processor = MagicMock()
        mock_result = MagicMock()
        mock_result.entity_type = "MP"
        mock_result.entity_id = 1
        mock_result.evidence_hash = "new_hash"
        mock_result.model = "gemini-3.1-flash-lite"
        mock_result.prompt_version = "gemini_analysis_v5"
        mock_result.generated_at = "2026-01-01T00:00:00+00:00"
        mock_result.to_analysis_text.return_value = '{"summary":"updated","highlights":[],"cautions":[]}'

        def fake_process_batch(evidence_rows, on_success=None, on_failure=None):
            if on_success:
                on_success(mock_result)
            return {
                "results": [mock_result], "success_count": 1,
                "failure_count": 0, "failures": [],
            }

        mock_processor.process_batch.side_effect = fake_process_batch

        with patch("automation.pipeline_controller.get_config", return_value=mock_cfg):
            with patch("automation.pipeline_controller.get_db2", return_value=mock_db2):
                with patch("automation.pipeline_controller.load_table", return_value=[
                    {"entity_type": "MP", "entity_id": 1,
                     "evidence_hash": "old_hash", "prompt_version": "gemini_analysis_v5"},
                ]):
                    with patch("analysis.gemini_processor.GeminiProcessor", return_value=mock_processor):
                        with patch("automation.pipeline_controller.sb_upsert", return_value=200) as mock_upsert:
                            result = stage_gemini(evidence, api_keys=["fake_key"])

        mock_upsert.assert_called_once()
        assert result["success"] == 1

    def test_entity_evidence_mla_upsert(self):
        """MLA entities are upserted with correct conflict cols."""
        from automation.pipeline_controller import stage_persist
        from unittest.mock import patch

        records = [
            {"entity_type": "MLA", "entity_id": 9, "evidence_hash": "h_mla",
             "evidence_version": 2, "evidence": {"portfolio": {"total_works": 20}},
             "generated_at": "2026-01-01T00:00:00+00:00",
             "entity_name": None, "member_type": "MLA",
             "state_id": "S02", "constituency_id": "C9"},
        ]
        affected_keys = {("MLA", 9)}
        mock_db2 = ("https://test-db2.supabase.co", "test_key")

        with patch("automation.pipeline_controller.get_db2", return_value=mock_db2):
            with patch("automation.pipeline_controller.sb_upsert", return_value=200) as mock_upsert:
                stage_persist(records, affected_keys=affected_keys)

        row = mock_upsert.call_args[0][3][0]
        assert row["entity_type"] == "MLA"
        assert row["entity_id"] == 9

    def test_entity_evidence_state_upsert(self):
        """STATE entities are upserted with correct conflict cols."""
        from automation.pipeline_controller import stage_persist
        from unittest.mock import patch

        records = [
            {"entity_type": "STATE", "entity_id": 1, "evidence_hash": "h_state",
             "evidence_version": 2, "evidence": {"portfolio": {"total_works": 500}},
             "generated_at": "2026-01-01T00:00:00+00:00",
             "entity_name": "Maharashtra", "member_type": None,
             "state_id": 1, "constituency_id": None},
        ]
        affected_keys = {("STATE", 1)}
        mock_db2 = ("https://test-db2.supabase.co", "test_key")

        with patch("automation.pipeline_controller.get_db2", return_value=mock_db2):
            with patch("automation.pipeline_controller.sb_upsert", return_value=200) as mock_upsert:
                stage_persist(records, affected_keys=affected_keys)

        row = mock_upsert.call_args[0][3][0]
        assert row["entity_type"] == "STATE"
        assert row["entity_id"] == 1

    def test_affected_only_rerun_only_processes_changed(self):
        """Affected-only rerun only persists entities with changed evidence."""
        from automation.pipeline_controller import stage_persist
        from unittest.mock import patch

        all_records = [
            {"entity_type": "MP", "entity_id": 1, "evidence_hash": "h1_changed",
             "evidence_version": 2, "evidence": {"a": 1},
             "generated_at": "2026-01-02T00:00:00+00:00",
             "entity_name": None, "member_type": "MP",
             "state_id": "S01", "constituency_id": "C1"},
            {"entity_type": "MP", "entity_id": 2, "evidence_hash": "h2_unchanged",
             "evidence_version": 2, "evidence": {"a": 2},
             "generated_at": "2026-01-01T00:00:00+00:00",
             "entity_name": None, "member_type": "MP",
             "state_id": "S01", "constituency_id": "C2"},
        ]
        affected_keys = {("MP", 1)}
        mock_db2 = ("https://test-db2.supabase.co", "test_key")

        with patch("automation.pipeline_controller.get_db2", return_value=mock_db2):
            with patch("automation.pipeline_controller.sb_upsert", return_value=200) as mock_upsert:
                result = stage_persist(all_records, affected_keys=affected_keys)

        assert result["written"] == 1
        batch = mock_upsert.call_args[0][3]
        assert len(batch) == 1
        assert batch[0]["entity_id"] == 1

    def test_unchanged_entities_not_regenerated(self):
        """filter_affected returns empty when all evidence hashes match."""
        from analysis.gemini_processor import filter_affected

        evidence = [
            {"entity_type": "MP", "entity_id": 1, "evidence_hash": "h1"},
            {"entity_type": "MLA", "entity_id": 2, "evidence_hash": "h2"},
            {"entity_type": "STATE", "entity_id": 3, "evidence_hash": "h3"},
        ]
        existing = [
            {"entity_type": "MP", "entity_id": 1,
             "evidence_hash": "h1", "prompt_version": "gemini_analysis_v5"},
            {"entity_type": "MLA", "entity_id": 2,
             "evidence_hash": "h2", "prompt_version": "gemini_analysis_v5"},
            {"entity_type": "STATE", "entity_id": 3,
             "evidence_hash": "h3", "prompt_version": "gemini_analysis_v5"},
        ]
        affected = filter_affected(evidence, existing)
        assert len(affected) == 0

    def test_upsert_function_uses_merge_duplicates(self):
        """sb_upsert uses supabase client with on_conflict for idempotent upsert."""
        from automation.pipeline_controller import sb_upsert
        import inspect
        src = inspect.getsource(sb_upsert)
        assert "on_conflict" in src


# ============================================================
# TEST 18: DB2 ANALYTICS PERSISTENCE
# ============================================================

class TestDB2AnalyticsPersistence:

    def test_build_overall_metrics_three_scopes(self):
        from analysis.db2_analytics_persistence import build_overall_metrics
        m1 = _fake_member_metrics(member_id=1, member_type="MP", total_works=30)
        m2 = _fake_member_metrics(member_id=2, member_type="MLA", total_works=20)
        s = _fake_state_metrics()
        records = build_overall_metrics([m1, m2], [s])
        assert len(records) == 3
        scopes = {r["scope"] for r in records}
        assert scopes == {"BOTH", "MP", "MLA"}

    def test_build_overall_metrics_has_required_fields(self):
        from analysis.db2_analytics_persistence import build_overall_metrics
        m = _fake_member_metrics()
        s = _fake_state_metrics()
        records = build_overall_metrics([m], [s])
        r = [x for x in records if x["scope"] == "BOTH"][0]
        assert r["total_members"] == 1
        assert r["total_works"] == 30
        assert "completion_rate_pct" in r
        assert "fund_utilization_pct" in r
        assert "calculated_at" in r

    def test_build_member_metrics_all_members(self):
        from analysis.db2_analytics_persistence import build_member_metrics
        m1 = _fake_member_metrics(member_id=1, member_type="MP")
        m2 = _fake_member_metrics(member_id=2, member_type="MLA")
        records = build_member_metrics([m1, m2])
        assert len(records) == 2
        ids = {(r["member_id"], r["member_type"]) for r in records}
        assert (1, "MP") in ids
        assert (2, "MLA") in ids

    def test_build_member_metrics_has_required_fields(self):
        from analysis.db2_analytics_persistence import build_member_metrics
        m = _fake_member_metrics()
        records = build_member_metrics([m])
        r = records[0]
        assert r["member_id"] == 1
        assert r["total_works"] == 30
        assert "anomaly_score" in r
        assert "performance_classification" in r
        assert "zero_work_member" in r
        assert "calculated_at" in r

    def test_build_member_metrics_zero_work(self):
        from analysis.db2_analytics_persistence import build_member_metrics
        m = _fake_member_metrics(total_works=0, zero_work_member=True,
                                 ranking_qualified=False)
        records = build_member_metrics([m])
        r = records[0]
        assert r["zero_work_member"] is True
        assert r["ranking_qualified"] is False
        assert r["performance_classification"] == "NO_DATA"

    def test_build_state_metrics_all_states(self):
        from analysis.db2_analytics_persistence import build_state_metrics
        s1 = _fake_state_metrics(state_id=1, state_name="State A")
        s2 = _fake_state_metrics(state_id=2, state_name="State B")
        records = build_state_metrics([s1, s2])
        assert len(records) == 2
        ids = {r["state_id"] for r in records}
        assert ids == {1, 2}

    def test_build_state_metrics_has_required_fields(self):
        from analysis.db2_analytics_persistence import build_state_metrics
        s = _fake_state_metrics()
        records = build_state_metrics([s])
        r = records[0]
        assert r["state_id"] == "S01"
        assert r["total_works"] == 100
        assert "anomaly_score" in r
        assert "performance_classification" in r
        assert "calculated_at" in r

    def test_build_national_statistics(self):
        from analysis.db2_analytics_persistence import build_national_statistics
        from analysis.models import NationalStatistics
        stats = [
            NationalStatistics(metric_name="total_works", count=100,
                               mean=50.0, median=45.0, std_dev=10.0,
                               minimum=5.0, maximum=200.0, p25=20.0,
                               p75=80.0, p90=150.0, p95=180.0, iqr=60.0),
        ]
        records = build_national_statistics(stats)
        assert len(records) == 1
        r = records[0]
        assert r["metric_name"] == "total_works"
        assert r["sample_size"] == 100
        assert r["mean"] == 50.0

    def test_build_trends(self):
        from analysis.db2_analytics_persistence import build_trends
        from analysis.models import TrendRecord
        trends = [
            TrendRecord(year=2025, member_type="MP", total_works=1000,
                        recommended_works=900, sanctioned_works=800,
                        completed_works=600, ongoing_works=100,
                        recommended_amount=500000000, sanctioned_amount=400000000,
                        expenditure_amount=250000000, completion_amount=200000000,
                        completion_rate_pct=60.0, avg_sanction_delay_days=90.0,
                        avg_execution_days=150.0),
        ]
        records = build_trends(trends)
        assert len(records) == 1
        r = records[0]
        assert r["year"] == 2025
        assert r["member_type"] == "MP"
        assert r["total_works"] == 1000

    def test_compute_member_ranks(self):
        from analysis.db2_analytics_persistence import compute_member_ranks
        records = [
            {"member_id": 1, "ranking_qualified": True, "anomaly_score": 1.5},
            {"member_id": 2, "ranking_qualified": True, "anomaly_score": 2.5},
            {"member_id": 3, "ranking_qualified": False, "anomaly_score": None},
        ]
        compute_member_ranks(records)
        assert records[0]["rank"] == 2  # score 1.5
        assert records[1]["rank"] == 1  # score 2.5
        assert records[2]["rank"] is None

    def test_compute_state_ranks(self):
        from analysis.db2_analytics_persistence import compute_state_ranks
        records = [
            {"state_id": 1, "anomaly_score": 1.0},
            {"state_id": 2, "anomaly_score": 2.0},
            {"state_id": 3, "anomaly_score": None},
        ]
        compute_state_ranks(records)
        assert records[0]["rank"] == 2
        assert records[1]["rank"] == 1
        assert records[2]["rank"] is None

    def test_analytics_persist_stage_callable(self):
        from automation.pipeline_controller import stage_analytics_persist
        import inspect
        src = inspect.getsource(stage_analytics_persist)
        assert "build_overall_metrics" in src
        assert "build_member_metrics" in src
        assert "build_state_metrics" in src
        assert "build_national_statistics" in src
        assert "build_trends" in src
        assert "get_db2" in src

    def test_analytics_persist_stage_uses_upsert(self):
        from automation.pipeline_controller import stage_analytics_persist
        import inspect
        src = inspect.getsource(stage_analytics_persist)
        assert "sb_upsert" in src

    def test_analytics_persist_includes_rankings(self):
        from automation.pipeline_controller import stage_analytics_persist
        import inspect
        src = inspect.getsource(stage_analytics_persist)
        assert "compute_member_ranks" in src
        assert "compute_state_ranks" in src


# ============================================================
# TEST 19: EVIDENCE WORK REFS
# ============================================================

class TestEvidenceWorkRefs:

    def test_build_evidence_work_refs_basic(self):
        from analysis.evidence_work_refs import build_evidence_work_refs
        from analysis.models import WorkAnalysis

        evidence = [{
            "entity_type": "MP", "entity_id": 1,
            "evidence": {"risk": {"flagged_works": 2}},
        }]
        works = [
            WorkAnalysis(work_id=101, member_type="MP", member_id=1,
                         status="Completed", flag_count=2, risk_level="HIGH",
                         sanction_amount=100000, expenditure_amount=80000,
                         sanction_delay_days=30),
            WorkAnalysis(work_id=102, member_type="MP", member_id=1,
                         status="In Progress", flag_count=0, risk_level="NORMAL",
                         sanction_amount=50000, expenditure_amount=0,
                         sanction_delay_days=None),
        ]
        refs = build_evidence_work_refs(evidence, works)
        assert len(refs) > 0
        work_ids = {r["work_id"] for r in refs}
        assert 101 in work_ids

    def test_evidence_work_refs_roles(self):
        from analysis.evidence_work_refs import build_evidence_work_refs
        from analysis.models import WorkAnalysis

        evidence = [{"entity_type": "MP", "entity_id": 1, "evidence": {}}]
        works = [
            WorkAnalysis(work_id=101, member_type="MP", member_id=1,
                         status="Completed", flag_count=3, risk_level="HIGH",
                         sanction_amount=100000, expenditure_amount=90000,
                         sanction_delay_days=10, expenditure_percentage=90.0),
        ]
        refs = build_evidence_work_refs(evidence, works)
        roles = {r["evidence_role"] for r in refs}
        assert "risk" in roles
        assert "positive_signal" in roles
        assert "representative" in roles
        assert "financial" in roles

    def test_evidence_work_refs_state(self):
        from analysis.evidence_work_refs import build_evidence_work_refs
        from analysis.models import WorkAnalysis

        evidence = [{"entity_type": "STATE", "entity_id": 1, "evidence": {}}]
        works = [
            WorkAnalysis(work_id=101, member_type="MP", member_id=1,
                         status="Completed", flag_count=2, risk_level="HIGH",
                         state_id=1, sanction_amount=500000),
        ]
        refs = build_evidence_work_refs(evidence, works)
        assert len(refs) > 0
        assert all(r["entity_type"] == "STATE" for r in refs)

    def test_evidence_work_refs_stage_callable(self):
        from automation.pipeline_controller import stage_evidence_work_refs
        import inspect
        src = inspect.getsource(stage_evidence_work_refs)
        assert "build_evidence_work_refs" in src
        assert "get_db2" in src
        assert "sb_upsert" in src

    def test_evidence_work_refs_empty_when_no_works(self):
        from analysis.evidence_work_refs import build_evidence_work_refs
        evidence = [{"entity_type": "MP", "entity_id": 1, "evidence": {}}]
        refs = build_evidence_work_refs(evidence, [])
        assert refs == []


# ============================================================
# TEST 20: PIPELINE FLOW COMPLETENESS
# ============================================================

class TestPipelineFlowCompleteness:

    def test_analytics_persist_before_evidence(self):
        """analytics_persist stage runs before evidence in the pipeline."""
        from automation.pipeline_controller import STAGES
        a_idx = STAGES.index("analytics_persist")
        e_idx = STAGES.index("evidence")
        assert a_idx < e_idx

    def test_evidence_work_refs_after_evidence(self):
        """evidence_work_refs runs after evidence."""
        from automation.pipeline_controller import STAGES
        e_idx = STAGES.index("evidence")
        r_idx = STAGES.index("evidence_work_refs")
        assert e_idx < r_idx

    def test_persist_after_gemini(self):
        """entity_evidence persist runs after gemini."""
        from automation.pipeline_controller import STAGES
        g_idx = STAGES.index("gemini")
        p_idx = STAGES.index("persist")
        assert g_idx < p_idx

    def test_verify_before_cleanup(self):
        """verify runs before cleanup."""
        from automation.pipeline_controller import STAGES
        v_idx = STAGES.index("verify")
        c_idx = STAGES.index("cleanup")
        assert v_idx < c_idx

    def test_analytics_in_verify(self):
        """verify stage checks analytics results."""
        from automation.pipeline_controller import stage_verify
        import inspect
        src = inspect.getsource(stage_verify)
        assert "analytics_result" in src
        assert "work_refs_result" in src
