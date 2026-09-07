"""Tests for evidence building, Gemini processing, and pipeline flow."""

import json
import hashlib
from datetime import date, datetime, timezone
from dataclasses import dataclass, field
from typing import Optional

import pytest


# ============================================================
# FIXTURES
# ============================================================

REF = date(2026, 9, 6)


@dataclass
class FakeMemberMetrics:
    member_id: int
    member_type: str
    state_id: Optional[int] = None
    constituency_id: Optional[int] = None
    total_works: int = 0
    recommended_works: int = 0
    sanctioned_works: int = 0
    completed_works: int = 0
    ongoing_works: int = 0
    pending_works: int = 0
    recommended_amount: float = 0
    sanctioned_amount: float = 0
    expenditure_amount: float = 0
    completion_amount: float = 0
    unspent_amount: float = 0
    avg_sanction_delay_days: Optional[float] = None
    median_sanction_delay_days: Optional[float] = None
    avg_execution_days: Optional[float] = None
    median_execution_days: Optional[float] = None
    avg_project_age_days: Optional[float] = None
    max_project_age_days: Optional[int] = None
    avg_pending_days: Optional[float] = None
    max_pending_days: Optional[int] = None
    flagged_works: int = 0
    medium_risk_works: int = 0
    high_risk_works: int = 0
    flagged_rate_pct: float = 0
    high_risk_rate_pct: float = 0
    cost_anomaly_works: int = 0
    duration_anomaly_works: int = 0
    expenditure_over_sanction_works: int = 0
    expenditure_over_recommendation_works: int = 0
    negative_sanction_delay_works: int = 0
    negative_execution_works: int = 0
    completion_before_sanction_works: int = 0
    expenditure_before_sanction_works: int = 0
    overdue_over_1_year: int = 0
    overdue_over_2_years: int = 0
    avg_cost_percentile: Optional[float] = None
    avg_duration_percentile: Optional[float] = None
    avg_cost_deviation_pct: Optional[float] = None
    avg_duration_deviation_pct: Optional[float] = None
    completion_rate_pct: float = 0
    sanction_rate_pct: float = 0
    sanction_conversion_pct: float = 0
    expenditure_sanction_utilization_pct: float = 0
    expenditure_recommendation_pct: float = 0
    zero_work_member: bool = False
    low_sample_member: bool = False
    ranking_qualified: bool = False


@dataclass
class FakeStateMetrics:
    state_id: int
    state_name: Optional[str] = None
    member_type: Optional[str] = None
    total_works: int = 0
    active_members: int = 0
    mp_active_members: int = 0
    mla_active_members: int = 0
    recommended_works: int = 0
    sanctioned_works: int = 0
    completed_works: int = 0
    ongoing_works: int = 0
    recommended_amount: float = 0
    sanctioned_amount: float = 0
    expenditure_amount: float = 0
    completion_amount: float = 0
    avg_sanction_delay_days: Optional[float] = None
    median_sanction_delay_days: Optional[float] = None
    avg_execution_days: Optional[float] = None
    median_execution_days: Optional[float] = None
    flagged_works: int = 0
    high_risk_works: int = 0
    overdue_over_1_year: int = 0
    overdue_over_2_years: int = 0
    completion_rate_pct: float = 0
    sanction_rate_pct: float = 0
    expenditure_utilization_pct: float = 0
    sanction_conversion_pct: float = 0
    risk_rate_pct: float = 0
    cost_anomaly_works: int = 0
    duration_anomaly_works: int = 0
    expenditure_over_sanction_works: int = 0
    expenditure_over_recommendation_works: int = 0
    negative_execution_works: int = 0
    negative_sanction_delay_works: int = 0
    ranking_qualified: bool = False


@dataclass
class FakeStatistics:
    metric_name: str
    member_type: Optional[str] = None
    count: int = 0
    mean: Optional[float] = None
    std_dev: Optional[float] = None
    minimum: Optional[float] = None
    p25: Optional[float] = None
    median: Optional[float] = None
    p75: Optional[float] = None
    p90: Optional[float] = None
    p95: Optional[float] = None
    maximum: Optional[float] = None
    iqr: Optional[float] = None


def _make_member(mid, **overrides):
    defaults = dict(
        member_id=mid, member_type="MP", state_id=1,
        total_works=100, completed_works=30, ongoing_works=40,
        sanctioned_works=80, recommended_works=30,
        recommended_amount=5000000, sanctioned_amount=4000000,
        expenditure_amount=2000000, completion_amount=1500000,
        flagged_works=25, high_risk_works=2, medium_risk_works=8,
        flagged_rate_pct=25.0, high_risk_rate_pct=2.0,
        cost_anomaly_works=3, duration_anomaly_works=4,
        overdue_over_1_year=10, overdue_over_2_years=2,
        expenditure_over_sanction_works=1,
        negative_sanction_delay_works=0,
        avg_sanction_delay_days=100, avg_execution_days=200,
        avg_project_age_days=400, avg_pending_days=150,
        avg_cost_percentile=55.0,
        completion_rate_pct=30.0, sanction_rate_pct=80.0,
        expenditure_sanction_utilization_pct=50.0,
        ranking_qualified=True,
    )
    defaults.update(overrides)
    return FakeMemberMetrics(**defaults)


def _make_state(sid, **overrides):
    defaults = dict(
        state_id=sid, state_name=f"State {sid}",
        total_works=2000, active_members=20,
        mp_active_members=15, mla_active_members=5,
        completed_works=600, ongoing_works=800,
        sanctioned_works=1600, recommended_works=400,
        recommended_amount=100000000, sanctioned_amount=80000000,
        expenditure_amount=40000000, completion_amount=30000000,
        flagged_works=500, high_risk_works=20,
        overdue_over_1_year=200, overdue_over_2_years=50,
        expenditure_over_sanction_works=10,
        negative_sanction_delay_works=2,
        avg_sanction_delay_days=100, avg_execution_days=200,
        completion_rate_pct=30.0, sanction_rate_pct=80.0,
        expenditure_utilization_pct=50.0,
        risk_rate_pct=25.0, cost_anomaly_works=30,
        duration_anomaly_works=40,
        ranking_qualified=True,
    )
    defaults.update(overrides)
    return FakeStateMetrics(**defaults)


# ============================================================
# EVIDENCE BUILDER TESTS
# ============================================================

class TestEvidenceBuilder:

    def test_member_evidence_structure(self):
        from analysis.evidence_builder import build_member_evidence
        members = [_make_member(i) for i in range(5)]
        evidence = build_member_evidence(members, {}, {}, [])
        assert len(evidence) == 5
        for r in evidence:
            assert r["entity_type"] in ("MP", "MLA")
            assert r["evidence_version"] == 2
            assert r["evidence_hash"]
            assert "portfolio" in r["evidence"]
            assert "financial" in r["evidence"]
            assert "execution" in r["evidence"]
            assert "risk" in r["evidence"]

    def test_state_evidence_structure(self):
        from analysis.evidence_builder import build_state_evidence
        states = [_make_state(i) for i in range(5)]
        evidence = build_state_evidence(states, {})
        assert len(evidence) == 5
        for r in evidence:
            assert r["entity_type"] == "STATE"
            assert r["evidence_version"] == 2
            assert r["evidence_hash"]

    def test_evidence_hash_deterministic(self):
        from analysis.evidence_builder import build_member_evidence
        members = [_make_member(1)]
        e1 = build_member_evidence(members, {}, {}, [])
        e2 = build_member_evidence(members, {}, {}, [])
        assert e1[0]["evidence_hash"] == e2[0]["evidence_hash"]

    def test_evidence_hash_differs_for_different_data(self):
        from analysis.evidence_builder import build_member_evidence
        e1 = build_member_evidence([_make_member(1, total_works=10)], {}, {}, [])
        e2 = build_member_evidence([_make_member(1, total_works=20)], {}, {}, [])
        assert e1[0]["evidence_hash"] != e2[0]["evidence_hash"]

    def test_zero_work_member_evidence(self):
        from analysis.evidence_builder import build_member_evidence
        m = _make_member(1, total_works=0, zero_work_member=True,
                         ranking_qualified=False)
        evidence = build_member_evidence([m], {}, {}, [])
        assert len(evidence) == 1
        assert evidence[0]["evidence"]["quality"]["zero_work_member"] is True
        assert evidence[0]["evidence"]["portfolio"]["total_works"] == 0

    def test_group_works_by_member(self):
        from analysis.evidence_builder import group_works_by_member
        from analysis.models import WorkAnalysis

        wa1 = WorkAnalysis(work_id=1, member_type="MP", member_id=1,
                           status="Completed")
        wa2 = WorkAnalysis(work_id=2, member_type="MP", member_id=1,
                           status="In Progress")
        wa3 = WorkAnalysis(work_id=3, member_type="MLA", member_id=2,
                           status="Completed")

        grouped = group_works_by_member([wa1, wa2, wa3])
        assert len(grouped[("MP", 1)]) == 2
        assert len(grouped[("MLA", 2)]) == 1


# ============================================================
# GEMINI PROCESSOR TESTS
# ============================================================

class TestGeminiValidation:

    def test_valid_output(self):
        from analysis.gemini_processor import validate_output
        obj = {
            "summary": "Test summary with facts.",
            "highlights": ["Point 1", "Point 2", "Point 3"],
            "cautions": [],
        }
        row = {"entity_type": "MP", "entity_id": 1, "entity_name": "Test MP"}
        errors = validate_output(obj, row)
        assert errors == []

    def test_missing_summary(self):
        from analysis.gemini_processor import validate_output
        obj = {"highlights": ["a", "b", "c"], "cautions": []}
        row = {"entity_type": "MP", "entity_id": 1}
        errors = validate_output(obj, row)
        assert any("summary" in e for e in errors)

    def test_too_few_highlights(self):
        from analysis.gemini_processor import validate_output
        obj = {"summary": "Test", "highlights": ["a", "b"], "cautions": []}
        row = {"entity_type": "MP", "entity_id": 1}
        errors = validate_output(obj, row)
        assert any("3" in e for e in errors)

    def test_forbidden_term(self):
        from analysis.gemini_processor import validate_output
        obj = {
            "summary": "The entity has 100 works.",
            "highlights": ["Point 1", "Point 2", "Point 3"],
            "cautions": [],
        }
        row = {"entity_type": "MP", "entity_id": 1}
        errors = validate_output(obj, row)
        assert any("forbidden" in e.lower() or "entity" in e.lower() for e in errors)

    def test_extra_fields(self):
        from analysis.gemini_processor import validate_output
        obj = {
            "summary": "Test",
            "highlights": ["a", "b", "c"],
            "cautions": [],
            "extra_field": "bad",
        }
        row = {"entity_type": "MP", "entity_id": 1}
        errors = validate_output(obj, row)
        assert any("extra" in e.lower() or "forbidden" in e.lower() for e in errors)

    def test_cautions_max_two(self):
        from analysis.gemini_processor import validate_output
        obj = {
            "summary": "Test",
            "highlights": ["a", "b", "c"],
            "cautions": ["c1", "c2", "c3"],
        }
        row = {"entity_type": "MP", "entity_id": 1}
        errors = validate_output(obj, row)
        assert any("2" in e for e in errors)


class TestGeminiFilterAffected:

    def test_new_entity_is_affected(self):
        from analysis.gemini_processor import filter_affected
        evidence = [
            {"entity_type": "MP", "entity_id": 1, "evidence_hash": "abc"},
        ]
        existing = []
        affected = filter_affected(evidence, existing)
        assert len(affected) == 1

    def test_unchanged_entity_not_affected(self):
        from analysis.gemini_processor import filter_affected
        evidence = [
            {"entity_type": "MP", "entity_id": 1, "evidence_hash": "abc"},
        ]
        existing = [
            {"entity_type": "MP", "entity_id": 1, "evidence_hash": "abc",
             "prompt_version": "gemini_analysis_v5"},
        ]
        affected = filter_affected(evidence, existing)
        assert len(affected) == 0

    def test_changed_hash_is_affected(self):
        from analysis.gemini_processor import filter_affected
        evidence = [
            {"entity_type": "MP", "entity_id": 1, "evidence_hash": "new_hash"},
        ]
        existing = [
            {"entity_type": "MP", "entity_id": 1, "evidence_hash": "old_hash",
             "prompt_version": "gemini_analysis_v5"},
        ]
        affected = filter_affected(evidence, existing)
        assert len(affected) == 1

    def test_changed_prompt_version_is_affected(self):
        from analysis.gemini_processor import filter_affected
        evidence = [
            {"entity_type": "MP", "entity_id": 1, "evidence_hash": "abc"},
        ]
        existing = [
            {"entity_type": "MP", "entity_id": 1, "evidence_hash": "abc",
             "prompt_version": "gemini_analysis_v4"},
        ]
        affected = filter_affected(evidence, existing)
        assert len(affected) == 1

    def test_mixed_affected_and_unchanged(self):
        from analysis.gemini_processor import filter_affected
        evidence = [
            {"entity_type": "MP", "entity_id": 1, "evidence_hash": "abc"},
            {"entity_type": "MP", "entity_id": 2, "evidence_hash": "new"},
            {"entity_type": "MLA", "entity_id": 3, "evidence_hash": "xyz"},
        ]
        existing = [
            {"entity_type": "MP", "entity_id": 1, "evidence_hash": "abc",
             "prompt_version": "gemini_analysis_v5"},
            {"entity_type": "MP", "entity_id": 2, "evidence_hash": "old",
             "prompt_version": "gemini_analysis_v5"},
        ]
        affected = filter_affected(evidence, existing)
        ids = [(r["entity_type"], r["entity_id"]) for r in affected]
        assert ("MP", 2) in ids
        assert ("MLA", 3) in ids
        assert ("MP", 1) not in ids


class TestGeminiPromptBuilding:

    def test_prompt_contains_entity_info(self):
        from analysis.gemini_processor import build_prompt
        row = {
            "entity_type": "MP",
            "entity_id": 8,
            "entity_name": "Test MP",
            "evidence_version": 2,
            "evidence_hash": "abc123",
            "evidence": {
                "portfolio": {"total_works": 100},
                "quality": {"zero_work_member": False, "low_sample_member": False},
            },
        }
        prompt = build_prompt(row)
        assert "Test MP" in prompt
        assert "MP 8" in prompt
        assert "ENTITY EVIDENCE" in prompt

    def test_zero_work_prompt_context(self):
        from analysis.gemini_processor import build_prompt
        row = {
            "entity_type": "MP",
            "entity_id": 8,
            "entity_name": "Test MP",
            "evidence_version": 2,
            "evidence_hash": "abc123",
            "evidence": {
                "portfolio": {"total_works": 0},
                "quality": {"zero_work_member": True, "low_sample_member": False},
            },
        }
        prompt = build_prompt(row)
        assert "ZERO WORKS" in prompt

    def test_low_sample_prompt_context(self):
        from analysis.gemini_processor import build_prompt
        row = {
            "entity_type": "MP",
            "entity_id": 8,
            "entity_name": "Test MP",
            "evidence_version": 2,
            "evidence_hash": "abc123",
            "evidence": {
                "portfolio": {"total_works": 3},
                "quality": {"zero_work_member": False, "low_sample_member": True},
            },
        }
        prompt = build_prompt(row)
        assert "small portfolio" in prompt


class TestGeminiCleanJson:

    def test_clean_plain_json(self):
        from analysis.gemini_processor import clean_json
        text = '{"summary": "test", "highlights": ["a"], "cautions": []}'
        obj = clean_json(text)
        assert obj["summary"] == "test"

    def test_clean_marked_json(self):
        from analysis.gemini_processor import clean_json
        text = '```json\n{"summary": "test", "highlights": ["a"], "cautions": []}\n```'
        obj = clean_json(text)
        assert obj["summary"] == "test"

    def test_clean_invalid_json(self):
        from analysis.gemini_processor import clean_json
        with pytest.raises(json.JSONDecodeError):
            clean_json("not json at all")


# ============================================================
# ZERO-WORK HANDLING TESTS
# ============================================================

class TestZeroWorkHandling:

    def test_zero_work_evidence_has_flag(self):
        from analysis.evidence_builder import build_member_evidence
        m = _make_member(1, total_works=0, zero_work_member=True,
                         ranking_qualified=False)
        evidence = build_member_evidence([m], {}, {}, [])
        assert evidence[0]["evidence"]["quality"]["zero_work_member"] is True

    def test_zero_work_portfolio_zeros(self):
        from analysis.evidence_builder import build_member_evidence
        m = _make_member(1, total_works=0, zero_work_member=True,
                         ranking_qualified=False,
                         completed_works=0, ongoing_works=0,
                         sanctioned_works=0, recommended_works=0)
        evidence = build_member_evidence([m], {}, {}, [])
        p = evidence[0]["evidence"]["portfolio"]
        assert p["total_works"] == 0
        assert p["completed_works"] == 0

    def test_zero_work_financial_zeros(self):
        from analysis.evidence_builder import build_member_evidence
        m = _make_member(1, total_works=0, zero_work_member=True,
                         ranking_qualified=False,
                         recommended_amount=0, sanctioned_amount=0,
                         expenditure_amount=0, completion_amount=0)
        evidence = build_member_evidence([m], {}, {}, [])
        f = evidence[0]["evidence"]["financial"]
        assert f["expenditure_amount"] == 0

    def test_zero_work_not_in_anomaly_population(self):
        """Zero-work members should not appear in entity anomaly results."""
        from analysis.entity_anomaly import compute_entity_anomalies
        members = [_make_member(i, ranking_qualified=True) for i in range(10)]
        members.append(_make_member(99, total_works=0, ranking_qualified=False))
        results = compute_entity_anomalies(members, REF)
        ids = [r.entity_id for r in results]
        assert 99 not in ids


# ============================================================
# AFFECTED-ONLY PROCESSING TESTS
# ============================================================

class TestAffectedOnlyProcessing:

    def test_only_changed_entities_reprocessed(self):
        from analysis.gemini_processor import filter_affected
        evidence = [
            {"entity_type": "MP", "entity_id": i, "evidence_hash": f"hash_{i}"}
            for i in range(10)
        ]
        existing = [
            {"entity_type": "MP", "entity_id": i, "evidence_hash": f"hash_{i}",
             "prompt_version": "gemini_analysis_v5"}
            for i in range(7)
        ]
        affected = filter_affected(evidence, existing)
        assert len(affected) == 3
        affected_ids = [r["entity_id"] for r in affected]
        assert all(i >= 7 for i in affected_ids)

    def test_no_reprocessing_when_nothing_changed(self):
        from analysis.gemini_processor import filter_affected
        evidence = [
            {"entity_type": "MP", "entity_id": i, "evidence_hash": f"hash_{i}"}
            for i in range(10)
        ]
        existing = [
            {"entity_type": "MP", "entity_id": i, "evidence_hash": f"hash_{i}",
             "prompt_version": "gemini_analysis_v5"}
            for i in range(10)
        ]
        affected = filter_affected(evidence, existing)
        assert len(affected) == 0


# ============================================================
# IDEMPOTENCY TESTS
# ============================================================

class TestIdempotency:

    def test_same_input_same_hash(self):
        from analysis.evidence_builder import build_member_evidence
        m = _make_member(1, total_works=50)
        e1 = build_member_evidence([m], {}, {}, [])
        e2 = build_member_evidence([m], {}, {}, [])
        assert e1[0]["evidence_hash"] == e2[0]["evidence_hash"]

    def test_same_input_same_prompt(self):
        from analysis.gemini_processor import build_prompt
        row = {
            "entity_type": "MP", "entity_id": 1,
            "entity_name": "Test", "evidence_version": 2,
            "evidence_hash": "abc",
            "evidence": {"portfolio": {"total_works": 10},
                         "quality": {"zero_work_member": False,
                                     "low_sample_member": False}},
        }
        p1 = build_prompt(row)
        p2 = build_prompt(row)
        assert p1 == p2


# ============================================================
# GEMINI OUTPUT CONTRACT TESTS
# ============================================================

class TestOutputContract:

    def test_result_to_dict(self):
        from analysis.gemini_processor import GeminiResult
        r = GeminiResult(
            entity_type="MP", entity_id=1,
            summary="Test summary",
            highlights=["h1", "h2", "h3"],
            cautions=[],
            evidence_hash="abc",
        )
        d = r.to_dict()
        assert d["entity_type"] == "MP"
        assert d["summary"] == "Test summary"
        assert len(d["highlights"]) == 3

    def test_result_to_analysis_text(self):
        from analysis.gemini_processor import GeminiResult
        r = GeminiResult(
            entity_type="MP", entity_id=1,
            summary="Test",
            highlights=["a", "b", "c"],
            cautions=[],
            evidence_hash="abc",
        )
        text = r.to_analysis_text()
        obj = json.loads(text)
        assert obj["summary"] == "Test"
        assert len(obj["highlights"]) == 3

    def test_analysis_text_is_valid_json(self):
        from analysis.gemini_processor import GeminiResult
        r = GeminiResult(
            entity_type="STATE", entity_id=5,
            summary="State summary",
            highlights=["h1", "h2", "h3"],
            cautions=["c1"],
            evidence_hash="xyz",
        )
        text = r.to_analysis_text()
        parsed = json.loads(text)
        assert isinstance(parsed, dict)
        assert "summary" in parsed
        assert "highlights" in parsed
        assert "cautions" in parsed
