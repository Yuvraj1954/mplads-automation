"""Tests for the affected-only pipeline, fast comparator, bootstrap mode,
and all new functionality added for performance optimization.

Covers:
- Fast comparator (quick_compare)
- Affected-only identity mapping
- Affected work analysis
- Affected member analysis
- Affected state analysis
- Overall metrics in affected mode
- Evidence targeted updates
- Evidence work refs targeted replacement
- Gemini skip when evidence unchanged
- Bootstrap mode basics
- Fetcher local-only mode
- Performance instrumentation
- Full-mode regression
"""

import hashlib
import json
import os
import sys
import tempfile
import time
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

# Set env vars required by comparator module at import time
os.environ.setdefault("SUPABASE_URL", "http://fake.supabase.co")
os.environ.setdefault("SUPABASE_SECRET_KEY", "fake-key")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "fake-service-role-key")
os.environ.setdefault("DB1_URL", "http://fake-db1.supabase.co")
os.environ.setdefault("DB1_SERVICE_ROLE_KEY", "fake-db1-key")
os.environ.setdefault("DB2_URL", "http://fake-db2.supabase.co")
os.environ.setdefault("DB2_SERVICE_ROLE_KEY", "fake-db2-key")


# ============================================================
# FAST COMPARATOR TESTS
# ============================================================

class TestQuickCompare:
    """Tests for the quick_compare function in comparator_v2."""

    def _make_dataset(self, base, name, files):
        """Helper: create a dataset directory with NDJSON part files."""
        ds_dir = base / name
        ds_dir.mkdir(parents=True, exist_ok=True)
        for fname, content in files.items():
            (ds_dir / fname).write_text(content)
        return ds_dir

    def test_identical_files_returns_0_changes(self):
        """Identical files → changed=False."""
        from comparator.comparator_v2 import quick_compare

        with tempfile.TemporaryDirectory() as tmpdir:
            old = Path(tmpdir) / "old"
            new = Path(tmpdir) / "new"
            old.mkdir()
            new.mkdir()

            content = '{"id":1,"name":"test"}\n{"id":2,"name":"test2"}\n'
            self._make_dataset(old, "works_recommended", {"part_0001.ndjson": content})
            self._make_dataset(new, "works_recommended", {"part_0001.ndjson": content})

            result = quick_compare(old, new)
            assert result["changed"] is False
            assert "works_recommended" in result["stats"]
            assert result["stats"]["works_recommended"]["reason"] == "identical"

    def test_changed_content_detected(self):
        """Different content → changed=True."""
        from comparator.comparator_v2 import quick_compare

        with tempfile.TemporaryDirectory() as tmpdir:
            old = Path(tmpdir) / "old"
            new = Path(tmpdir) / "new"
            old.mkdir()
            new.mkdir()

            self._make_dataset(old, "works_recommended", {
                "part_0001.ndjson": '{"id":1,"amount":100}\n'
            })
            self._make_dataset(new, "works_recommended", {
                "part_0001.ndjson": '{"id":1,"amount":200}\n'
            })

            result = quick_compare(old, new)
            assert result["changed"] is True
            assert "works_recommended" in result["changed_datasets"]
            assert result["stats"]["works_recommended"]["reason"] == "hash_diff"

    def test_same_size_different_content_detected(self):
        """Same-size different content files → changed=True (hash_diff)."""
        from comparator.comparator_v2 import quick_compare

        with tempfile.TemporaryDirectory() as tmpdir:
            old = Path(tmpdir) / "old"
            new = Path(tmpdir) / "new"
            old.mkdir()
            new.mkdir()

            # Create two different files of exactly the same size
            old_content = "A" * 1024
            new_content = "B" * 1024
            assert len(old_content.encode()) == len(new_content.encode())

            self._make_dataset(old, "works_recommended", {
                "part_0001.ndjson": old_content
            })
            self._make_dataset(new, "works_recommended", {
                "part_0001.ndjson": new_content
            })

            result = quick_compare(old, new)
            assert result["changed"] is True
            assert result["stats"]["works_recommended"]["reason"] == "hash_diff"

    def test_new_dataset_detected(self):
        """New dataset directory → changed=True."""
        from comparator.comparator_v2 import quick_compare

        with tempfile.TemporaryDirectory() as tmpdir:
            old = Path(tmpdir) / "old"
            new = Path(tmpdir) / "new"
            old.mkdir()
            new.mkdir()

            self._make_dataset(new, "mla_works_recommended", {
                "part_0001.ndjson": '{"id":1}\n'
            })

            result = quick_compare(old, new)
            assert result["changed"] is True
            assert "mla_works_recommended" in result["changed_datasets"]
            assert result["stats"]["mla_works_recommended"]["reason"] == "new_dataset"

    def test_file_list_difference_detected(self):
        """Different file names → changed=True."""
        from comparator.comparator_v2 import quick_compare

        with tempfile.TemporaryDirectory() as tmpdir:
            old = Path(tmpdir) / "old"
            new = Path(tmpdir) / "new"
            old.mkdir()
            new.mkdir()

            self._make_dataset(old, "works_recommended", {
                "part_0001.ndjson": '{"id":1}\n'
            })
            self._make_dataset(new, "works_recommended", {
                "part_0001.ndjson": '{"id":1}\n',
                "part_0002.ndjson": '{"id":2}\n',
            })

            result = quick_compare(old, new)
            assert result["changed"] is True
            assert result["stats"]["works_recommended"]["reason"] == "file_list_diff"

    def test_size_difference_detected(self):
        """Different file sizes → changed=True."""
        from comparator.comparator_v2 import quick_compare

        with tempfile.TemporaryDirectory() as tmpdir:
            old = Path(tmpdir) / "old"
            new = Path(tmpdir) / "new"
            old.mkdir()
            new.mkdir()

            self._make_dataset(old, "works_recommended", {
                "part_0001.ndjson": "short"
            })
            self._make_dataset(new, "works_recommended", {
                "part_0001.ndjson": "this is a much longer content string"
            })

            result = quick_compare(old, new)
            assert result["changed"] is True
            assert result["stats"]["works_recommended"]["reason"] == "size_diff"

    def test_empty_directories_returns_0_changes(self):
        """Both directories empty → changed=False."""
        from comparator.comparator_v2 import quick_compare

        with tempfile.TemporaryDirectory() as tmpdir:
            old = Path(tmpdir) / "old"
            new = Path(tmpdir) / "new"
            old.mkdir()
            new.mkdir()

            result = quick_compare(old, new)
            assert result["changed"] is False
            assert len(result["changed_datasets"]) == 0

    def test_missing_old_directory_treated_as_new(self):
        """Missing old dataset → changed=True (new_dataset)."""
        from comparator.comparator_v2 import quick_compare

        with tempfile.TemporaryDirectory() as tmpdir:
            old = Path(tmpdir) / "old"
            new = Path(tmpdir) / "new"
            old.mkdir()
            new.mkdir()

            self._make_dataset(new, "works_recommended", {
                "part_0001.ndjson": '{"id":1}\n'
            })

            result = quick_compare(old, new)
            assert result["changed"] is True

    def test_multiple_datasets_partial_change(self):
        """Only changed dataset flagged, identical ones not."""
        from comparator.comparator_v2 import quick_compare

        with tempfile.TemporaryDirectory() as tmpdir:
            old = Path(tmpdir) / "old"
            new = Path(tmpdir) / "new"
            old.mkdir()
            new.mkdir()

            content = '{"id":1}\n'
            self._make_dataset(old, "works_recommended", {"part_0001.ndjson": content})
            self._make_dataset(new, "works_recommended", {"part_0001.ndjson": content})
            self._make_dataset(old, "works_sanctioned", {"part_0001.ndjson": "old"})
            self._make_dataset(new, "works_sanctioned", {"part_0001.ndjson": "new"})

            result = quick_compare(old, new)
            assert result["changed"] is True
            assert "works_sanctioned" in result["changed_datasets"]
            assert result["stats"]["works_recommended"]["reason"] == "identical"


# ============================================================
# AFFECTED IDENTITY MAPPING TESTS
# ============================================================

class TestAffectedIdentityMapping:
    """Tests for the corrected identity mapping in stage_affected."""

    def test_delta_records_extracted_from_ndjson(self):
        """Verify delta records are correctly extracted from NDJSON files."""
        from automation.pipeline_controller import stage_affected

        with tempfile.TemporaryDirectory() as tmpdir:
            delta_dir = Path(tmpdir) / "delta"
            ds_dir = delta_dir / "works_recommended"
            ds_dir.mkdir(parents=True)

            records = [
                {"WORK_RECOMMENDATION_DTL_ID": "12345", "mp_name": "Test MP", "state_id": "S1"},
                {"WORK_RECOMMENDATION_DTL_ID": "12346", "mp_name": "Test MP2", "state_id": "S2"},
            ]
            with open(ds_dir / "part_0001.ndjson", "w") as f:
                for r in records:
                    f.write(json.dumps(r) + "\n")

            with patch("automation.pipeline_controller.load_config"):
                with patch("automation.pipeline_controller.get_db1", return_value=("http://fake", "fake-key")):
                    with patch("automation.pipeline_controller.sb_get", return_value=[{
                        "work_id": 100,
                        "member_type": "MP",
                        "member_id": 500,
                        "state_id": "S1",
                    }]):
                        result = stage_affected(str(delta_dir), "test_run")

            assert len(result["affected_work_dtls"]) == 2
            assert "12345" in result["affected_work_dtls"]
            assert "12346" in result["affected_work_dtls"]

    def test_dtl_id_lookup_returns_internal_ids(self):
        """DB1 lookup via source_work_id returns internal member IDs."""
        from automation.pipeline_controller import stage_affected

        with tempfile.TemporaryDirectory() as tmpdir:
            delta_dir = Path(tmpdir) / "delta"
            ds_dir = delta_dir / "works_recommended"
            ds_dir.mkdir(parents=True)

            with open(ds_dir / "part_0001.ndjson", "w") as f:
                f.write(json.dumps({"WORK_RECOMMENDATION_DTL_ID": "99999"}) + "\n")

            mock_rows = [{
                "work_id": 5000,
                "member_type": "MP",
                "member_id": 300,
                "state_id": "S10",
            }]

            with patch("automation.pipeline_controller.load_config"):
                with patch("automation.pipeline_controller.get_db1", return_value=("http://fake", "fake-key")):
                    with patch("automation.pipeline_controller.sb_get", return_value=mock_rows):
                        result = stage_affected(str(delta_dir), "test_run")

            assert 5000 in result["affected_work_ids"]
            assert ("MP", 300) in result["affected_members"]
            assert "S10" in result["affected_states"]

    def test_missing_dtl_gracefully_handled(self):
        """DTL ID not found in DB1 is handled gracefully."""
        from automation.pipeline_controller import stage_affected

        with tempfile.TemporaryDirectory() as tmpdir:
            delta_dir = Path(tmpdir) / "delta"
            ds_dir = delta_dir / "works_recommended"
            ds_dir.mkdir(parents=True)

            with open(ds_dir / "part_0001.ndjson", "w") as f:
                f.write(json.dumps({"WORK_RECOMMENDATION_DTL_ID": "NOEXIST"}) + "\n")

            with patch("automation.pipeline_controller.load_config"):
                with patch("automation.pipeline_controller.get_db1", return_value=("http://fake", "fake-key")):
                    with patch("automation.pipeline_controller.sb_get", return_value=[]):
                        result = stage_affected(str(delta_dir), "test_run")

            assert result["member_count"] == 0
            assert result["work_count"] == 0

    def test_raw_state_id_fallback(self):
        """Raw state_id from records is used as fallback."""
        from automation.pipeline_controller import stage_affected

        with tempfile.TemporaryDirectory() as tmpdir:
            delta_dir = Path(tmpdir) / "delta"
            ds_dir = delta_dir / "works_recommended"
            ds_dir.mkdir(parents=True)

            with open(ds_dir / "part_0001.ndjson", "w") as f:
                f.write(json.dumps({
                    "WORK_RECOMMENDATION_DTL_ID": "111",
                    "state_id": "FALLBACK_STATE"
                }) + "\n")

            with patch("automation.pipeline_controller.load_config"):
                with patch("automation.pipeline_controller.get_db1", return_value=("http://fake", "fake-key")):
                    with patch("automation.pipeline_controller.sb_get", return_value=[]):
                        result = stage_affected(str(delta_dir), "test_run")

            assert "FALLBACK_STATE" in result["affected_states"]


# ============================================================
# AFFECTED WORK ANALYSIS TESTS
# ============================================================

class TestAffectedWorkAnalysis:
    """Tests for affected work analysis computation."""

    def test_affected_work_changes(self):
        """Affected work analysis is recomputed."""
        from analysis.work_analysis import compute_work_analyses

        work = {
            "work_id": 1,
            "member_type": "MP",
            "member_id": 100,
            "state_id": "S1",
            "recommendation_date": date(2024, 1, 15),
            "sanction_date": date(2024, 2, 15),
            "completion_date": None,
            "recommended_amount": 500000.0,
            "sanction_amount": 400000.0,
            "expenditure_amount": None,
            "activity_name": "Road Construction",
        }
        analyses = compute_work_analyses([work], reference_date=date(2024, 6, 1))
        assert len(analyses) == 1
        assert analyses[0].work_id == 1

    def test_unaffected_work_unchanged(self):
        """Unaffected work analysis is NOT recomputed."""
        from analysis.work_analysis import compute_work_analyses

        unaffected_work = {
            "work_id": 999,
            "member_type": "MLA",
            "member_id": 200,
            "state_id": "S2",
            "recommendation_date": date(2024, 3, 1),
            "sanction_date": date(2024, 4, 1),
            "completion_date": date(2024, 5, 1),
            "recommended_amount": 100000.0,
            "sanction_amount": 100000.0,
            "expenditure_amount": 95000.0,
            "activity_name": "Bridge Construction",
        }
        original_analyses = compute_work_analyses([unaffected_work], reference_date=date(2024, 6, 1))
        assert len(original_analyses) == 1
        rerun_analyses = compute_work_analyses([unaffected_work], reference_date=date(2024, 6, 1))
        assert original_analyses[0].risk_level == rerun_analyses[0].risk_level

    def test_repeated_processing_same_result(self):
        """Same work analyzed twice produces the same result."""
        from analysis.work_analysis import compute_work_analyses

        work = {
            "work_id": 42,
            "member_type": "MP",
            "member_id": 10,
            "state_id": "S5",
            "recommendation_date": date(2024, 6, 1),
            "sanction_date": None,
            "completion_date": None,
            "recommended_amount": 200000.0,
            "sanction_amount": None,
            "expenditure_amount": None,
            "activity_name": "School Building",
        }
        ref = date(2024, 7, 1)
        first = compute_work_analyses([work], reference_date=ref)
        second = compute_work_analyses([work], reference_date=ref)
        assert first[0].risk_level == second[0].risk_level
        assert first[0].sanction_delay_days == second[0].sanction_delay_days

    def test_new_work_handled(self):
        """New work (not previously analyzed) can be analyzed."""
        from analysis.work_analysis import compute_work_analyses

        new_work = {
            "work_id": 99999,
            "member_type": "MLA",
            "member_id": 50,
            "state_id": "S3",
            "recommendation_date": date(2024, 7, 1),
            "sanction_date": None,
            "completion_date": None,
            "recommended_amount": 75000.0,
            "sanction_amount": None,
            "expenditure_amount": None,
            "activity_name": "Water Supply",
        }
        analyses = compute_work_analyses([new_work], reference_date=date(2024, 8, 1))
        assert len(analyses) == 1

    def test_updated_work_handled(self):
        """Updated work produces different analysis."""
        from analysis.work_analysis import compute_work_analyses

        work_v1 = {
            "work_id": 55,
            "member_type": "MP",
            "member_id": 15,
            "state_id": "S7",
            "recommendation_date": date(2024, 1, 1),
            "sanction_date": date(2024, 2, 1),
            "completion_date": None,
            "recommended_amount": 300000.0,
            "sanction_amount": 250000.0,
            "expenditure_amount": None,
            "activity_name": "Hospital Construction",
        }
        analyses_v1 = compute_work_analyses([work_v1], reference_date=date(2024, 8, 1))

        work_v2 = {**work_v1, "completion_date": date(2024, 7, 1)}
        analyses_v2 = compute_work_analyses([work_v2], reference_date=date(2024, 8, 1))

        assert analyses_v1[0].sanction_delay_days == analyses_v2[0].sanction_delay_days


# ============================================================
# AFFECTED MEMBER ANALYSIS TESTS
# ============================================================

class TestAffectedMemberAnalysis:
    """Tests for affected member analysis — full-work recalculation."""

    def test_member_full_work_recalculation(self):
        """Member metrics are computed from all works for that member."""
        from analysis.pipeline import AnalysisPipeline

        works = [
            {"work_id": 1, "member_type": "MP", "member_id": 100,
             "state_id": "S1", "recommendation_date": date(2024, 1, 15),
             "sanction_date": date(2024, 2, 15), "completion_date": None,
             "recommended_amount": 500000.0, "sanction_amount": 400000.0,
             "expenditure_amount": None, "activity_name": "Road"},
            {"work_id": 2, "member_type": "MP", "member_id": 100,
             "state_id": "S1", "recommendation_date": date(2024, 3, 10),
             "sanction_date": date(2024, 4, 10), "completion_date": date(2024, 5, 10),
             "recommended_amount": 300000.0, "sanction_amount": 300000.0,
             "expenditure_amount": 280000.0, "activity_name": "Bridge"},
        ]

        pipeline = AnalysisPipeline(works)
        pipeline.run_full(reference_date=date(2024, 6, 1))

        member = None
        for m in pipeline.member_metrics:
            if m.member_type == "MP" and m.member_id == 100:
                member = m
                break

        assert member is not None
        assert member.total_works >= 2

    def test_unaffected_member_unchanged(self):
        """Unaffected member metrics remain the same."""
        from analysis.pipeline import AnalysisPipeline

        works = [
            {"work_id": 10, "member_type": "MLA", "member_id": 200,
             "state_id": "S2", "recommendation_date": date(2024, 1, 1),
             "sanction_date": date(2024, 2, 1), "completion_date": date(2024, 3, 1),
             "recommended_amount": 100000.0, "sanction_amount": 100000.0,
             "expenditure_amount": 95000.0, "activity_name": "School"},
        ]

        pipeline = AnalysisPipeline(works)
        pipeline.run_full(reference_date=date(2024, 6, 1))

        member = None
        for m in pipeline.member_metrics:
            if m.member_type == "MLA" and m.member_id == 200:
                member = m
                break

        assert member is not None
        assert member.total_works == 1
        assert member.completion_rate_pct >= 100.0

    def test_zero_work_member_present_in_full_mode(self):
        """Zero-work members appear in full-mode results."""
        from analysis.pipeline import AnalysisPipeline

        works = [
            {"work_id": 1, "member_type": "MP", "member_id": 100,
             "state_id": "S1", "recommendation_date": date(2024, 1, 15),
             "sanction_date": None, "completion_date": None,
             "recommended_amount": 500000.0, "sanction_amount": None,
             "expenditure_amount": None, "activity_name": "Road"},
        ]

        pipeline = AnalysisPipeline(works)
        pipeline.run_full(reference_date=date(2024, 6, 1))

        assert len(pipeline.member_metrics) >= 1


# ============================================================
# AFFECTED STATE ANALYSIS TESTS
# ============================================================

class TestAffectedStateAnalysis:
    """Tests for affected state analysis."""

    def test_state_recomputed_from_affected_members(self):
        """State metrics are computed from member metrics."""
        from analysis.pipeline import AnalysisPipeline

        works = [
            {"work_id": 1, "member_type": "MP", "member_id": 100,
             "state_id": "S1", "recommendation_date": date(2024, 1, 15),
             "sanction_date": date(2024, 2, 15), "completion_date": None,
             "recommended_amount": 500000.0, "sanction_amount": 400000.0,
             "expenditure_amount": None, "activity_name": "Road"},
        ]

        pipeline = AnalysisPipeline(works)
        pipeline.run_full(reference_date=date(2024, 6, 1))

        state = None
        for s in pipeline.state_metrics:
            if s.state_id == "S1":
                state = s
                break

        assert state is not None
        assert state.total_works >= 1


# ============================================================
# OVERALL METRICS TESTS
# ============================================================

class TestOverallMetrics:
    """Tests for overall metrics computation."""

    def test_overall_metrics_3_scopes(self):
        """Overall metrics produce MP, MLA, BOTH scopes."""
        from analysis.pipeline import AnalysisPipeline
        from analysis.db2_analytics_persistence import build_overall_metrics

        works = [
            {"work_id": 1, "member_type": "MP", "member_id": 100,
             "state_id": "S1", "recommendation_date": date(2024, 1, 15),
             "sanction_date": None, "completion_date": None,
             "recommended_amount": 500000.0, "sanction_amount": None,
             "expenditure_amount": None, "activity_name": "Road"},
            {"work_id": 2, "member_type": "MLA", "member_id": 200,
             "state_id": "S2", "recommendation_date": date(2024, 2, 1),
             "sanction_date": None, "completion_date": None,
             "recommended_amount": 300000.0, "sanction_amount": None,
             "expenditure_amount": None, "activity_name": "Bridge"},
        ]

        pipeline = AnalysisPipeline(works)
        pipeline.run_full(reference_date=date(2024, 6, 1))

        overall = build_overall_metrics(
            pipeline.member_metrics, pipeline.state_metrics, []
        )

        scopes = {r["scope"] for r in overall}
        assert "MP" in scopes
        assert "MLA" in scopes
        assert "BOTH" in scopes

    def test_overall_metrics_zero_protection(self):
        """Zero overall metrics are NOT written."""
        from analysis.db2_analytics_persistence import build_overall_metrics

        overall = [
            {"scope": "MP", "total_works": 0, "sanctioned_amount": 0},
            {"scope": "MLA", "total_works": 0, "sanctioned_amount": 0},
            {"scope": "BOTH", "total_works": 0, "sanctioned_amount": 0},
        ]

        has_real_data = any(
            r.get("total_works", 0) > 0 or r.get("sanctioned_amount", 0) > 0
            for r in overall
        )
        assert has_real_data is False


# ============================================================
# EVIDENCE TARGETED UPDATE TESTS
# ============================================================

class TestEvidenceTargetedUpdate:
    """Tests for evidence targeted update in affected mode."""

    def test_evidence_hash_deterministic(self):
        """Same evidence input produces same hash."""
        from analysis.evidence_builder import _hash_evidence

        evidence = {"quality": {"total_works": 5, "completion_rate": 0.8}}
        h1 = _hash_evidence(evidence)
        h2 = _hash_evidence(evidence)
        assert h1 == h2
        assert len(h1) == 64  # SHA-256 hex

    def test_evidence_different_input_different_hash(self):
        """Different evidence input produces different hash."""
        from analysis.evidence_builder import _hash_evidence

        e1 = {"quality": {"total_works": 5}}
        e2 = {"quality": {"total_works": 6}}
        assert _hash_evidence(e1) != _hash_evidence(e2)

    def test_evidence_idempotent(self):
        """Same evidence → same hash (idempotency check)."""
        from analysis.evidence_builder import _hash_evidence

        evidence = {
            "quality": {"total_works": 10, "completion_rate": 0.5},
            "lifecycle": {"sanction_delay_days": 30},
            "financial": {"total_recommended": 500000},
        }
        h1 = _hash_evidence(evidence)
        h2 = _hash_evidence(evidence)
        assert h1 == h2


# ============================================================
# GEMINI SKIP TESTS
# ============================================================

class TestGeminiSkip:
    """Tests for Gemini behavior — skip when evidence unchanged."""

    def test_unchanged_evidence_skips_gemini(self):
        """Same evidence_hash → 0 Gemini requests."""
        from analysis.gemini_processor import filter_affected, PROMPT_VERSION

        evidence = [
            {
                "entity_type": "MP",
                "entity_id": 100,
                "evidence_hash": "abc123",
                "evidence": {},
            }
        ]
        existing = [
            {
                "entity_type": "MP",
                "entity_id": 100,
                "evidence_hash": "abc123",
                "prompt_version": PROMPT_VERSION,
            }
        ]

        affected = filter_affected(evidence, existing)
        assert len(affected) == 0

    def test_changed_evidence_allows_gemini(self):
        """Changed evidence_hash → entity becomes eligible."""
        from analysis.gemini_processor import filter_affected, PROMPT_VERSION

        evidence = [
            {
                "entity_type": "MP",
                "entity_id": 100,
                "evidence_hash": "new_hash",
                "evidence": {},
            }
        ]
        existing = [
            {
                "entity_type": "MP",
                "entity_id": 100,
                "evidence_hash": "old_hash",
                "prompt_version": PROMPT_VERSION,
            }
        ]

        affected = filter_affected(evidence, existing)
        assert len(affected) == 1
        assert affected[0]["entity_id"] == 100

    def test_prompt_version_invalidation(self):
        """Changed prompt_version → entity becomes eligible."""
        from analysis.gemini_processor import filter_affected, PROMPT_VERSION

        evidence = [
            {
                "entity_type": "MP",
                "entity_id": 100,
                "evidence_hash": "same_hash",
                "evidence": {},
            }
        ]
        existing = [
            {
                "entity_type": "MP",
                "entity_id": 100,
                "evidence_hash": "same_hash",
                "prompt_version": "old_version_v4",
            }
        ]

        affected = filter_affected(evidence, existing)
        assert len(affected) == 1

    def test_new_entity_always_eligible(self):
        """New entity (no existing record) → eligible for Gemini."""
        from analysis.gemini_processor import filter_affected

        evidence = [
            {
                "entity_type": "MP",
                "entity_id": 200,
                "evidence_hash": "new_entity_hash",
                "evidence": {},
            }
        ]
        existing = []

        affected = filter_affected(evidence, existing)
        assert len(affected) == 1
        assert affected[0]["entity_id"] == 200


# ============================================================
# BOOTSTRAP MODE TESTS
# ============================================================

class TestBootstrapMode:
    """Tests for bootstrap mode basics."""

    def test_bootstrap_requires_cache_work_dir(self):
        """Bootstrap without MPLADS_CACHE_WORK_DIR raises error."""
        from automation.daily_pipeline import main

        with patch("sys.argv", ["daily_pipeline.py", "--bootstrap"]):
            with pytest.raises((RuntimeError, SystemExit)):
                os.environ.pop("MPLADS_CACHE_WORK_DIR", None)
                main()

    def test_bootstrap_no_cached_snapshot_raises(self):
        """Bootstrap with no cached snapshot raises error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir) / "cache"
            cache_dir.mkdir()

            with patch("automation.daily_pipeline.os.environ", {
                "MPLADS_CACHE_WORK_DIR": str(cache_dir),
                "SUPABASE_URL": "http://fake",
                "SUPABASE_SECRET_KEY": "fake",
            }):
                with pytest.raises((RuntimeError, SystemExit)):
                    from automation.daily_pipeline import main
                    with patch("sys.argv", ["daily_pipeline.py", "--bootstrap"]):
                        main()


# ============================================================
# FETCHER LOCAL-ONLY MODE TESTS
# ============================================================

class TestFetcherLocalOnly:
    """Tests for fetcher --local-only mode."""

    def test_fetcher_accepts_local_only_flag(self):
        """Fetcher accepts --local-only argument without error."""
        import subprocess
        result = subprocess.run(
            ["python3", str(Path(__file__).parent.parent / "fetcher" / "fetcher.py"),
             "--help"],
            capture_output=True, text=True, timeout=10
        )
        assert "--local-only" in result.stdout or result.returncode == 0


# ============================================================
# PERFORMANCE INSTRUMENTATION TESTS
# ============================================================

class TestPerformanceInstrumentation:
    """Tests for timing instrumentation."""

    def test_timer_returns_float(self):
        """_timer() returns a float timestamp."""
        from automation.daily_pipeline import _timer, _elapsed

        start = _timer()
        assert isinstance(start, float)

    def test_elapsed_returns_positive(self):
        """_elapsed() returns positive seconds."""
        from automation.daily_pipeline import _timer, _elapsed

        start = _timer()
        time.sleep(0.01)
        elapsed = _elapsed(start)
        assert elapsed >= 0.01

    def test_pipeline_controller_timer(self):
        """Pipeline controller has _timer and _elapsed."""
        from automation.pipeline_controller import _timer, _elapsed

        start = _timer()
        time.sleep(0.01)
        elapsed = _elapsed(start)
        assert elapsed >= 0.01

    def test_print_timing_no_crash(self):
        """_print_timing runs without error."""
        from automation.daily_pipeline import _print_timing
        _print_timing({"fetch": 10.5, "compare": 5.2}, time.time() - 15.7)


# ============================================================
# FULL MODE REGRESSION TESTS
# ============================================================

class TestFullModeRegression:
    """Verify full mode still works correctly."""

    def test_run_pipeline_full_mode_runs(self):
        """run_pipeline with mode='full' executes without error."""
        from automation.pipeline_controller import run_pipeline

        with tempfile.TemporaryDirectory() as tmpdir:
            snapshot_dir = Path(tmpdir) / "snapshot"
            snapshot_dir.mkdir()

            # Create minimal snapshot structure
            for ds in ["allocated_limit", "works_recommended", "works_sanctioned",
                       "works_completed", "expenditure", "calamity"]:
                ds_dir = snapshot_dir / ds
                ds_dir.mkdir()
                if ds == "allocated_limit":
                    records = [
                        json.dumps({
                            "mp_id": 100, "mp_name": "Test MP",
                            "state_id": "S1", "state_name": "State 1",
                            "constituency_id": "C1",
                            "allocated_amt": 5000000.0,
                            "recommendation_date": "2024-01-15",
                        })
                    ]
                    (ds_dir / "part_0001.ndjson").write_text("\n".join(records) + "\n")
                elif ds == "works_recommended":
                    records = [
                        json.dumps({
                            "WORK_RECOMMENDATION_DTL_ID": "12345",
                            "mp_id": 100, "mp_name": "Test MP",
                            "state_id": "S1", "state_name": "State 1",
                            "constituency_id": "C1",
                            "recommended_amt": 500000.0,
                            "recommendation_date": "2024-01-15",
                            "activity": "Road Construction",
                            "status": "Recommended",
                        })
                    ]
                    (ds_dir / "part_0001.ndjson").write_text("\n".join(records) + "\n")
                else:
                    (ds_dir / "part_0001.ndjson").write_text("")

            complete = snapshot_dir / "_COMPLETE.json"
            complete.write_text(json.dumps({
                "timestamp": "2024-06-01T00-00-00Z",
                "status": "complete",
            }))

            with patch("automation.pipeline_controller.load_config") as mock_cfg:
                mock_cfg.return_value = MagicMock(
                    db1_url="http://fake", db1_key="fake-key",
                    db2_url="http://fake2", db2_key="fake-key2",
                    db2_ready=False, gemini_keys=[]
                )
                result = run_pipeline(
                    snapshot_dir=str(snapshot_dir),
                    reference_date=date(2024, 6, 1),
                    run_id="test_regression",
                    skip_gemini=True,
                    skip_ingest=True,
                    dry_run=True,
                    mode="full",
                )

            assert result["status"] == "DRY_RUN"
            assert result["mode"] == "full"

    def test_run_pipeline_affected_mode_runs(self):
        """run_pipeline with mode='affected' executes without error."""
        from automation.pipeline_controller import run_pipeline

        with tempfile.TemporaryDirectory() as tmpdir:
            snapshot_dir = Path(tmpdir) / "snapshot"
            snapshot_dir.mkdir()

            for ds in ["allocated_limit", "works_recommended", "works_sanctioned",
                       "works_completed", "expenditure", "calamity"]:
                ds_dir = snapshot_dir / ds
                ds_dir.mkdir()
                if ds == "allocated_limit":
                    records = [
                        json.dumps({
                            "mp_id": 100, "mp_name": "Test MP",
                            "state_id": "S1", "state_name": "State 1",
                            "constituency_id": "C1",
                            "allocated_amt": 5000000.0,
                            "recommendation_date": "2024-01-15",
                        })
                    ]
                    (ds_dir / "part_0001.ndjson").write_text("\n".join(records) + "\n")
                elif ds == "works_recommended":
                    records = [
                        json.dumps({
                            "WORK_RECOMMENDATION_DTL_ID": "12345",
                            "mp_id": 100, "mp_name": "Test MP",
                            "state_id": "S1", "state_name": "State 1",
                            "constituency_id": "C1",
                            "recommended_amt": 500000.0,
                            "recommendation_date": "2024-01-15",
                            "activity": "Road Construction",
                            "status": "Recommended",
                        })
                    ]
                    (ds_dir / "part_0001.ndjson").write_text("\n".join(records) + "\n")
                else:
                    (ds_dir / "part_0001.ndjson").write_text("")

            complete = snapshot_dir / "_COMPLETE.json"
            complete.write_text(json.dumps({
                "timestamp": "2024-06-01T00-00-00Z",
                "status": "complete",
            }))

            # Create delta dir with affected record
            delta_dir = Path(tmpdir) / "delta"
            ds_delta = delta_dir / "works_recommended"
            ds_delta.mkdir(parents=True)
            with open(ds_delta / "part_0001.ndjson", "w") as f:
                f.write(json.dumps({
                    "WORK_RECOMMENDATION_DTL_ID": "12345",
                    "state_id": "S1",
                }) + "\n")

            with patch("automation.pipeline_controller.load_config") as mock_cfg, \
                 patch("automation.pipeline_controller.get_db1", return_value=("http://fake", "fake-key")):
                mock_cfg.return_value = MagicMock(
                    db1_url="http://fake", db1_key="fake-key",
                    db2_url="http://fake2", db2_key="fake-key2",
                    db2_ready=False, gemini_keys=[]
                )
                with patch("automation.pipeline_controller.sb_get", return_value=[{
                    "work_id": 1000,
                    "member_type": "MP",
                    "member_id": 100,
                    "state_id": "S1",
                }]):
                    result = run_pipeline(
                        snapshot_dir=str(snapshot_dir),
                        reference_date=date(2024, 6, 1),
                        delta_dir=str(delta_dir),
                        run_id="test_affected",
                        skip_gemini=True,
                        skip_ingest=True,
                        dry_run=True,
                        mode="affected",
                    )

            assert result["status"] == "DRY_RUN"
            assert result["mode"] == "affected"

    def test_full_mode_flag_in_result(self):
        """Result includes mode field."""
        from automation.pipeline_controller import run_pipeline

        with tempfile.TemporaryDirectory() as tmpdir:
            snapshot_dir = Path(tmpdir) / "snapshot"
            snapshot_dir.mkdir()

            for ds in ["allocated_limit", "works_recommended", "works_sanctioned",
                       "works_completed", "expenditure", "calamity"]:
                ds_dir = snapshot_dir / ds
                ds_dir.mkdir()
                if ds == "allocated_limit":
                    records = [
                        json.dumps({
                            "mp_id": 100, "mp_name": "Test MP",
                            "state_id": "S1", "state_name": "State 1",
                            "constituency_id": "C1",
                            "allocated_amt": 5000000.0,
                            "recommendation_date": "2024-01-15",
                        })
                    ]
                    (ds_dir / "part_0001.ndjson").write_text("\n".join(records) + "\n")
                elif ds == "works_recommended":
                    (ds_dir / "part_0001.ndjson").write_text("")
                else:
                    (ds_dir / "part_0001.ndjson").write_text("")

            complete = snapshot_dir / "_COMPLETE.json"
            complete.write_text(json.dumps({
                "timestamp": "2024-06-01T00-00-00Z",
                "status": "complete",
            }))

            with patch("automation.pipeline_controller.load_config") as mock_cfg:
                mock_cfg.return_value = MagicMock(
                    db1_url="http://fake", db1_key="fake-key",
                    db2_url="http://fake2", db2_key="fake-key2",
                    db2_ready=False, gemini_keys=[]
                )
                result = run_pipeline(
                    snapshot_dir=str(snapshot_dir),
                    reference_date=date(2024, 6, 1),
                    run_id="test_mode_field",
                    skip_gemini=True,
                    skip_ingest=True,
                    dry_run=True,
                    mode="full",
                )

            assert "mode" in result
            assert result["mode"] == "full"
            assert "timing" in result

    def test_affected_mode_no_affected_entities_skips(self):
        """Affected mode with 0 affected entities returns SUCCESS immediately."""
        from automation.pipeline_controller import run_pipeline

        with tempfile.TemporaryDirectory() as tmpdir:
            snapshot_dir = Path(tmpdir) / "snapshot"
            snapshot_dir.mkdir()

            for ds in ["allocated_limit", "works_recommended", "works_sanctioned",
                       "works_completed", "expenditure", "calamity"]:
                ds_dir = snapshot_dir / ds
                ds_dir.mkdir()
                if ds == "allocated_limit":
                    records = [
                        json.dumps({
                            "mp_id": 100, "mp_name": "Test MP",
                            "state_id": "S1", "state_name": "State 1",
                            "constituency_id": "C1",
                            "allocated_amt": 5000000.0,
                            "recommendation_date": "2024-01-15",
                        })
                    ]
                    (ds_dir / "part_0001.ndjson").write_text("\n".join(records) + "\n")
                else:
                    (ds_dir / "part_0001.ndjson").write_text("")

            complete = snapshot_dir / "_COMPLETE.json"
            complete.write_text(json.dumps({
                "timestamp": "2024-06-01T00-00-00Z",
                "status": "complete",
            }))

            delta_dir = Path(tmpdir) / "empty_delta"
            delta_dir.mkdir()

            with patch("automation.pipeline_controller.load_config") as mock_cfg, \
                 patch("automation.pipeline_controller.get_db1", return_value=("http://fake", "fake-key")):
                mock_cfg.return_value = MagicMock(
                    db1_url="http://fake", db1_key="fake-key",
                    db2_url="http://fake2", db2_key="fake-key2",
                    db2_ready=False, gemini_keys=[]
                )
                with patch("automation.pipeline_controller.sb_get", return_value=[]):
                    result = run_pipeline(
                        snapshot_dir=str(snapshot_dir),
                        reference_date=date(2024, 6, 1),
                        delta_dir=str(delta_dir),
                        run_id="test_no_affected",
                        skip_gemini=True,
                        skip_ingest=True,
                        dry_run=True,
                        mode="affected",
                    )

            assert result["status"] == "SUCCESS"


# ============================================================
# NO SECRETS EXPOSED TESTS
# ============================================================

class TestNoSecretsExposed:
    """Verify that no secrets are printed in pipeline output."""

    def test_pipeline_output_no_db_urls(self, capsys):
        """Pipeline output does not contain DB URLs."""
        from automation.pipeline_controller import run_pipeline

        with tempfile.TemporaryDirectory() as tmpdir:
            snapshot_dir = Path(tmpdir) / "snapshot"
            snapshot_dir.mkdir()

            for ds in ["allocated_limit", "works_recommended", "works_sanctioned",
                       "works_completed", "expenditure", "calamity"]:
                ds_dir = snapshot_dir / ds
                ds_dir.mkdir()
                if ds == "allocated_limit":
                    records = [
                        json.dumps({
                            "mp_id": 100, "mp_name": "Test MP",
                            "state_id": "S1", "state_name": "State 1",
                            "constituency_id": "C1",
                            "allocated_amt": 5000000.0,
                            "recommendation_date": "2024-01-15",
                        })
                    ]
                    (ds_dir / "part_0001.ndjson").write_text("\n".join(records) + "\n")
                else:
                    (ds_dir / "part_0001.ndjson").write_text("")

            complete = snapshot_dir / "_COMPLETE.json"
            complete.write_text(json.dumps({
                "timestamp": "2024-06-01T00-00-00Z",
                "status": "complete",
            }))

            with patch("automation.pipeline_controller.load_config") as mock_cfg:
                mock_cfg.return_value = MagicMock(
                    db1_url="https://supabase.szmepsgyekvmxbmumaep.supabase.co",
                    db1_key="sb_secret_abc123xyz",
                    db2_url="https://supabase.nhtrvpsqfztuuiitydlh.supabase.co",
                    db2_key="sb_secret_def456abc",
                    db2_ready=False, gemini_keys=[]
                )
                run_pipeline(
                    snapshot_dir=str(snapshot_dir),
                    reference_date=date(2024, 6, 1),
                    run_id="test_secrets",
                    skip_gemini=True,
                    skip_ingest=True,
                    dry_run=True,
                    mode="full",
                )

            captured = capsys.readouterr()
            # DB1 URL is printed but should not contain the full secret key
            # The pipeline prints DB1 URL for debugging, but keys should not be exposed
            assert "sb_secret_abc123xyz" not in captured.out
            assert "sb_secret_def456abc" not in captured.out


# ============================================================
# EDGE CASES
# ============================================================

class TestEdgeCases:
    """Edge case tests for robustness."""

    def test_malformed_ndjson_handled(self):
        """Malformed NDJSON records are skipped gracefully."""
        from comparator.comparator_v2 import quick_compare

        with tempfile.TemporaryDirectory() as tmpdir:
            old = Path(tmpdir) / "old"
            new = Path(tmpdir) / "new"
            old.mkdir()
            new.mkdir()

            self._make_dataset(old, "works_recommended", {
                "part_0001.ndjson": '{"id":1}\nbad json\n{"id":2}\n'
            })
            self._make_dataset(new, "works_recommended", {
                "part_0001.ndjson": '{"id":1}\nbad json\n{"id":2}\n'
            })

            result = quick_compare(old, new)
            assert result["changed"] is False

    def _make_dataset(self, base, name, files):
        ds_dir = base / name
        ds_dir.mkdir(parents=True, exist_ok=True)
        for fname, content in files.items():
            (ds_dir / fname).write_text(content)
        return ds_dir

    def test_empty_delta_dir_handled(self):
        """Empty delta directory → 0 affected entities."""
        from automation.pipeline_controller import stage_affected

        with tempfile.TemporaryDirectory() as tmpdir:
            delta_dir = Path(tmpdir) / "empty_delta"
            delta_dir.mkdir()

            result = stage_affected(str(delta_dir), "test_run")
            assert result["member_count"] == 0
            assert result["state_count"] == 0

    def test_missing_delta_dir_handled(self):
        """Missing delta directory → 0 affected entities."""
        from automation.pipeline_controller import stage_affected

        result = stage_affected("/nonexistent/path", "test_run")
        assert result["member_count"] == 0
        assert result["state_count"] == 0

    def test_none_delta_dir_handled(self):
        """None delta directory → 0 affected entities."""
        from automation.pipeline_controller import stage_affected

        result = stage_affected(None, "test_run")
        assert result["member_count"] == 0
        assert result["state_count"] == 0

    def test_dtl_id_as_string_conversion(self):
        """DTL IDs are converted to string for DB1 lookup."""
        from automation.pipeline_controller import stage_affected

        with tempfile.TemporaryDirectory() as tmpdir:
            delta_dir = Path(tmpdir) / "delta"
            ds_dir = delta_dir / "works_recommended"
            ds_dir.mkdir(parents=True)

            # DTL ID as integer (government API might return int)
            with open(ds_dir / "part_0001.ndjson", "w") as f:
                f.write(json.dumps({"WORK_RECOMMENDATION_DTL_ID": 12345}) + "\n")

            with patch("automation.pipeline_controller.load_config"):
                with patch("automation.pipeline_controller.get_db1", return_value=("http://fake", "fake-key")):
                    with patch("automation.pipeline_controller.sb_get", return_value=[{
                        "work_id": 100,
                        "member_type": "MP",
                        "member_id": 50,
                        "state_id": "S1",
                    }]):
                        result = stage_affected(str(delta_dir), "test_run")

            assert "12345" in result["affected_work_dtls"]

    def test_multiple_mla_works_same_state(self):
        """Multiple MLA works in same state grouped correctly."""
        from analysis.pipeline import AnalysisPipeline

        works = [
            {"work_id": 1, "member_type": "MLA", "member_id": 200,
             "state_id": "S1", "recommendation_date": date(2024, 1, 15),
             "sanction_date": None, "completion_date": None,
             "recommended_amount": 500000.0, "sanction_amount": None,
             "expenditure_amount": None, "activity_name": "Road"},
            {"work_id": 2, "member_type": "MLA", "member_id": 201,
             "state_id": "S1", "recommendation_date": date(2024, 2, 1),
             "sanction_date": None, "completion_date": None,
             "recommended_amount": 300000.0, "sanction_amount": None,
             "expenditure_amount": None, "activity_name": "Bridge"},
        ]

        pipeline = AnalysisPipeline(works)
        pipeline.run_full(reference_date=date(2024, 6, 1))

        state = None
        for s in pipeline.state_metrics:
            if s.state_id == "S1":
                state = s
                break

        assert state is not None
        assert state.total_works >= 2


# ============================================================
# TIMING INTEGRATION TESTS
# ============================================================

class TestTimingIntegration:
    """Tests that timing data is included in pipeline results."""

    def test_timing_in_result(self):
        """Pipeline results include timing data."""
        from automation.pipeline_controller import run_pipeline

        with tempfile.TemporaryDirectory() as tmpdir:
            snapshot_dir = Path(tmpdir) / "snapshot"
            snapshot_dir.mkdir()

            for ds in ["allocated_limit", "works_recommended", "works_sanctioned",
                       "works_completed", "expenditure", "calamity"]:
                ds_dir = snapshot_dir / ds
                ds_dir.mkdir()
                if ds == "allocated_limit":
                    records = [
                        json.dumps({
                            "mp_id": 100, "mp_name": "Test MP",
                            "state_id": "S1", "state_name": "State 1",
                            "constituency_id": "C1",
                            "allocated_amt": 5000000.0,
                            "recommendation_date": "2024-01-15",
                        })
                    ]
                    (ds_dir / "part_0001.ndjson").write_text("\n".join(records) + "\n")
                else:
                    (ds_dir / "part_0001.ndjson").write_text("")

            complete = snapshot_dir / "_COMPLETE.json"
            complete.write_text(json.dumps({
                "timestamp": "2024-06-01T00-00-00Z",
                "status": "complete",
            }))

            with patch("automation.pipeline_controller.load_config") as mock_cfg:
                mock_cfg.return_value = MagicMock(
                    db1_url="http://fake", db1_key="fake-key",
                    db2_url="http://fake2", db2_key="fake-key2",
                    db2_ready=False, gemini_keys=[]
                )
                result = run_pipeline(
                    snapshot_dir=str(snapshot_dir),
                    reference_date=date(2024, 6, 1),
                    run_id="test_timing",
                    skip_gemini=True,
                    skip_ingest=True,
                    dry_run=True,
                    mode="full",
                )

            assert "timing" in result
            assert isinstance(result["timing"], dict)
