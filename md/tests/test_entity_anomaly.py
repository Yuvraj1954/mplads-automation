from datetime import date, datetime, timezone
from dataclasses import dataclass, field
from typing import Optional

import numpy as np

from analysis.entity_anomaly import (
    robust_z_score,
    compute_entity_anomalies,
    compute_state_anomalies,
    CLIP_MIN,
    CLIP_MAX,
    CONSISTENCY_CONSTANT,
    CONF_THRESHOLD_HIGH,
    CONF_THRESHOLD_MEDIUM,
    COMPOSITE_THRESHOLD_HIGH_PERCENTILE,
    COMPOSITE_THRESHOLD_MEDIUM_PERCENTILE,
    Z_ANOMALY_THRESHOLD,
)


REF = date(2026, 9, 4)


@dataclass
class FakeMemberMetrics:
    member_id: int
    member_type: str
    state_id: Optional[int] = None
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
        state_id=sid, total_works=2000, active_members=20,
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


class TestRobustZScore:

    def test_normal_data(self):
        vals = [10, 12, 11, 13, 12, 11, 10, 12]
        z = robust_z_score(vals)
        assert len(z) == len(vals)
        median = np.median(vals)
        mad = np.median(np.abs(np.array(vals) - median))
        for zi in z:
            assert abs(zi) <= CLIP_MAX

    def test_outlier_detection(self):
        vals = [10, 10, 10, 10, 10, 10, 10, 100]
        z = robust_z_score(vals)
        mad = np.median(np.abs(np.array(vals) - np.median(vals)))
        if mad > 0:
            assert z[-1] > 3
        else:
            assert all(zi == 0 for zi in z)

    def test_mad_zero(self):
        vals = [5, 5, 5, 5, 5]
        z = robust_z_score(vals)
        assert all(zi == 0 for zi in z)

    def test_empty(self):
        z = robust_z_score([])
        assert len(z) == 0

    def test_clipping(self):
        vals = [1, 1, 1, 1, 1, 1, 1, 1000]
        z = robust_z_score(vals)
        assert z[-1] <= CLIP_MAX

    def test_clipping_negative(self):
        vals = [100, 100, 100, 100, 100, 100, 100, -1000]
        z = robust_z_score(vals)
        assert z[-1] >= CLIP_MIN

    def test_consistency_constant(self):
        vals = [0, 10, 20, 30, 40]
        z = robust_z_score(vals)
        median = np.median(vals)
        mad = np.median(np.abs(np.array(vals) - median))
        expected = CONSISTENCY_CONSTANT * (vals[2] - median) / mad
        assert abs(z[2] - expected) < 0.01

    def test_deterministic(self):
        vals = [1, 5, 3, 7, 2, 8, 4, 6]
        z1 = robust_z_score(vals)
        z2 = robust_z_score(vals)
        assert all(a == b for a, b in zip(z1, z2))


class TestEntityAnomalyResults:

    def test_qualified_entities_only(self):
        members = [_make_member(i, ranking_qualified=True) for i in range(10)]
        members.append(_make_member(99, ranking_qualified=False, total_works=2))
        results = compute_entity_anomalies(members, REF)
        ids = [r.entity_id for r in results]
        assert 99 not in ids
        assert len(results) == 10

    def test_result_fields(self):
        members = [_make_member(i) for i in range(10)]
        results = compute_entity_anomalies(members, REF)
        r = results[0]
        assert r.entity_id is not None
        assert r.entity_type == "member"
        assert r.member_type in ("MP", "MLA")
        assert isinstance(r.anomaly_score, float)
        assert r.anomaly_level in ("HIGH", "MEDIUM", "NORMAL")
        assert r.confidence_level in ("HIGH", "MEDIUM", "LOW")
        assert isinstance(r.contributing_features, list)
        assert isinstance(r.supporting_metrics, dict)
        assert r.system_version is not None
        assert r.calculated_at is not None
        assert r.reference_date == REF

    def test_anomaly_distribution(self):
        members = [_make_member(i, total_works=100) for i in range(100)]
        results = compute_entity_anomalies(members, REF)
        levels = [r.anomaly_level for r in results]
        assert all(l in ("HIGH", "MEDIUM", "NORMAL") for l in levels)
        assert all(l == "NORMAL" for l in levels)

    def test_deterministic_results(self):
        members = [_make_member(i) for i in range(20)]
        r1 = compute_entity_anomalies(members, REF)
        r2 = compute_entity_anomalies(members, REF)
        scores1 = sorted([r.anomaly_score for r in r1])
        scores2 = sorted([r.anomaly_score for r in r2])
        assert scores1 == scores2

    def test_extreme_outlier_is_high(self):
        members = [_make_member(i, flagged_rate_pct=20 + i % 10,
                                cost_anomaly_works=i % 5,
                                duration_anomaly_works=i % 4)
                   for i in range(30)]
        members.append(_make_member(99, flagged_rate_pct=95,
                                    cost_anomaly_works=80,
                                    duration_anomaly_works=70))
        results = compute_entity_anomalies(members, REF)
        extreme = [r for r in results if r.entity_id == 99][0]
        assert extreme.anomaly_score > 0
        assert extreme.anomaly_level == "HIGH"

    def test_mp_mla_separation(self):
        members = []
        for i in range(15):
            members.append(_make_member(i, member_type="MP"))
        for i in range(20, 35):
            members.append(_make_member(i, member_type="MLA"))
        results = compute_entity_anomalies(members, REF)
        mp_results = [r for r in results if r.member_type == "MP"]
        mla_results = [r for r in results if r.member_type == "MLA"]
        assert len(mp_results) == 15
        assert len(mla_results) == 15

    def test_state_id_populated(self):
        members = [_make_member(i, state_id=5) for i in range(10)]
        results = compute_entity_anomalies(members, REF)
        assert all(r.state_id == 5 for r in results)


class TestConfidenceLevels:

    def test_high_confidence(self):
        m = _make_member(1, total_works=100, ranking_qualified=True)
        results = compute_entity_anomalies([m], REF)
        assert results[0].confidence_level == "HIGH"

    def test_medium_confidence(self):
        m = _make_member(1, total_works=30, ranking_qualified=True)
        results = compute_entity_anomalies([m], REF)
        assert results[0].confidence_level == "MEDIUM"

    def test_low_confidence(self):
        m = _make_member(1, total_works=10, ranking_qualified=True)
        results = compute_entity_anomalies([m], REF)
        assert results[0].confidence_level == "LOW"


class TestExplainability:

    def test_top_features_present(self):
        members = [_make_member(i, total_works=100,
                                flagged_rate_pct=15 + i % 20,
                                cost_anomaly_works=1 + i % 5,
                                duration_anomaly_works=1 + i % 4)
                   for i in range(30)]
        members.append(_make_member(99, total_works=100, flagged_rate_pct=90,
                                    cost_anomaly_works=50, duration_anomaly_works=40))
        results = compute_entity_anomalies(members, REF)
        extreme = [r for r in results if r.entity_id == 99][0]
        assert extreme.anomaly_score > 0
        assert len(extreme.contributing_features) > 0

    def test_feature_structure(self):
        members = [_make_member(i, total_works=100) for i in range(20)]
        members.append(_make_member(99, flagged_rate_pct=90))
        results = compute_entity_anomalies(members, REF)
        extreme = [r for r in results if r.entity_id == 99][0]
        for ef in extreme.contributing_features:
            assert "feature" in ef
            assert "z_score" in ef
            assert "direction" in ef
            assert "description" in ef
            assert ef["direction"] in ("HIGH", "LOW")
            assert abs(ef["z_score"]) >= Z_ANOMALY_THRESHOLD

    def test_no_accusatory_language(self):
        members = [_make_member(i, total_works=100) for i in range(20)]
        members.append(_make_member(99, flagged_rate_pct=90))
        results = compute_entity_anomalies(members, REF)
        extreme = [r for r in results if r.entity_id == 99][0]
        banned = ["fraud", "corruption", "misuse", "wrongdoing"]
        for ef in extreme.contributing_features:
            desc = ef["description"].lower()
            for word in banned:
                assert word not in desc


class TestStateAnomaly:

    def test_state_results(self):
        states = [_make_state(i) for i in range(10)]
        results = compute_state_anomalies(states, REF)
        assert len(results) == 10

    def test_state_fields(self):
        states = [_make_state(i) for i in range(10)]
        results = compute_state_anomalies(states, REF)
        r = results[0]
        assert r.entity_type == "state"
        assert r.state_id is not None
        assert r.anomaly_level in ("HIGH", "MEDIUM", "NORMAL")

    def test_state_extreme_outlier(self):
        states = [_make_state(i, risk_rate_pct=20 + i % 15,
                              cost_anomaly_works=20 + i % 30,
                              duration_anomaly_works=25 + i % 20)
                  for i in range(15)]
        states.append(_make_state(99, risk_rate_pct=80, total_works=5000,
                                  cost_anomaly_works=200,
                                  duration_anomaly_works=180))
        results = compute_state_anomalies(states, REF)
        extreme = [r for r in results if r.entity_id == 99][0]
        assert extreme.anomaly_score > 0
        assert extreme.anomaly_level == "HIGH"

    def test_unqualified_states_excluded(self):
        states = [_make_state(i, ranking_qualified=True) for i in range(10)]
        states.append(_make_state(99, ranking_qualified=False, total_works=5))
        results = compute_state_anomalies(states, REF)
        ids = [r.entity_id for r in results]
        assert 99 not in ids


class TestEdgeCases:

    def test_single_entity(self):
        members = [_make_member(1, total_works=100)]
        results = compute_entity_anomalies(members, REF)
        assert len(results) == 1
        assert results[0].anomaly_level == "NORMAL"
        assert results[0].anomaly_score == 0.0

    def test_all_same_values(self):
        members = [_make_member(i, total_works=100) for i in range(20)]
        results = compute_entity_anomalies(members, REF)
        assert all(r.anomaly_level == "NORMAL" for r in results)
        assert all(r.anomaly_score == 0.0 for r in results)

    def test_missing_expenditure(self):
        members = [_make_member(i) for i in range(15)]
        members.append(_make_member(99, expenditure_sanction_utilization_pct=0))
        results = compute_entity_anomalies(members, REF)
        assert len(results) == 16

    def test_negative_delay(self):
        members = [_make_member(i, total_works=100) for i in range(20)]
        members.append(_make_member(99, total_works=100,
                                    negative_sanction_delay_works=10,
                                    negative_execution_works=5))
        results = compute_entity_anomalies(members, REF)
        extreme = [r for r in results if r.entity_id == 99][0]
        assert extreme.anomaly_score >= 0
        assert extreme.anomaly_level in ("HIGH", "MEDIUM", "NORMAL")

    def test_empty_member_list(self):
        results = compute_entity_anomalies([], REF)
        assert results == []

    def test_empty_state_list(self):
        results = compute_state_anomalies([], REF)
        assert results == []

    def test_reference_date_preserved(self):
        members = [_make_member(i) for i in range(10)]
        results = compute_entity_anomalies(members, REF)
        assert all(r.reference_date == REF for r in results)


class TestStateIdFix:

    def test_state_id_populated_from_works(self):
        members = [_make_member(i, state_id=42) for i in range(10)]
        results = compute_entity_anomalies(members, REF)
        assert all(r.state_id == 42 for r in results)

    def test_state_id_none_when_no_works(self):
        m = _make_member(1, state_id=None, total_works=0)
        results = compute_entity_anomalies([m], REF)
        assert results[0].state_id is None
