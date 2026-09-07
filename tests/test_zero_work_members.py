"""Comprehensive tests for zero-work member injection.

Tests verify:
- Master population discovery from allocated_limit snapshots
- Zero-work member injection into MemberMetrics
- Zero-work members reach Evidence and Gemini
- Existing ZERO WORKS prompt is triggered
- Zero-work members have no fabricated metrics
- Zero-work members receive no anomaly score
- Work-bearing members remain unchanged
- Full reliable evidence/context is passed to Gemini
- Tenure/date context is preserved if it already exists
- Unknown tenure gets neutral handling
- Complete evidence population is produced
"""

import json
import tempfile
import shutil
from datetime import date
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

import pytest


REF = date(2026, 9, 6)


@dataclass
class FakeMemberMetrics:
    member_id: int
    member_type: str
    member_name: Optional[str] = None
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


def _make_work_member(mid, **overrides):
    defaults = dict(
        member_id=mid, member_type="MP", member_name=f"MP Member {mid}",
        state_id=1, total_works=50, completed_works=30,
        sanctioned_works=40, recommended_works=10,
        recommended_amount=5000000, sanctioned_amount=4000000,
        expenditure_amount=2000000, completion_amount=1500000,
        flagged_works=10, high_risk_works=2,
        completion_rate_pct=60.0, sanction_rate_pct=80.0,
        expenditure_sanction_utilization_pct=50.0,
        ranking_qualified=True,
    )
    defaults.update(overrides)
    return FakeMemberMetrics(**defaults)


def _make_zero_member(mid, member_type="MP", member_name="Zero Member",
                      state_id=1, **overrides):
    defaults = dict(
        member_id=mid, member_type=member_type, member_name=member_name,
        state_id=state_id, total_works=0, zero_work_member=True,
        ranking_qualified=False, low_sample_member=True,
    )
    defaults.update(overrides)
    return FakeMemberMetrics(**defaults)


def _create_snapshot_dir(mp_records=None, mla_records=None):
    """Create a temporary snapshot directory with allocated_limit data."""
    tmpdir = tempfile.mkdtemp()

    # Create allocated_limit directory
    mp_dir = Path(tmpdir) / "allocated_limit"
    mp_dir.mkdir()

    if mp_records is None:
        mp_records = [
            {
                "MP_NAME": "MP Alpha",
                "STATE_NAME": "Maharashtra",
                "CONSTITUENCY": "Mumbai South",
                "HOUSE_NAME": "Lok Sabha",
                "TENURE": "18th Lok Sabha",
                "TENURE_START_DATE": "Jun 4, 2024 12:00:00 AM",
                "TENURE_END_DATE": "Jun 3, 2029 11:59:59 PM",
                "ALLOCATED_AMT": 190289442.0,
            },
            {
                "MP_NAME": "MP Beta",
                "STATE_NAME": "Delhi",
                "CONSTITUENCY": "New Delhi",
                "HOUSE_NAME": "Lok Sabha",
                "TENURE": "18th Lok Sabha",
                "TENURE_START_DATE": "Jun 4, 2024 12:00:00 AM",
                "TENURE_END_DATE": "Jun 3, 2029 11:59:59 PM",
                "ALLOCATED_AMT": 150000000.0,
            },
        ]

    with open(mp_dir / "part_0001.ndjson", "w") as f:
        for rec in mp_records:
            f.write(json.dumps(rec) + "\n")

    # Create mla_allocated_limit directory
    mla_dir = Path(tmpdir) / "mla_allocated_limit"
    mla_dir.mkdir()

    if mla_records is None:
        mla_records = [
            {
                "MP_NAME": "MLA Gamma",
                "STATE_NAME": "Karnataka",
                "CONSTITUENCY": "Bangalore Central",
                "HOUSE_NAME": "Rajya Sabha",
                "TENURE": "Sitting MP",
                "TENURE_START_DATE": "Apr 10, 2026 12:00:00 AM",
                "TENURE_END_DATE": "Apr 9, 2032 11:59:59 PM",
                "ALLOCATED_AMT": 73421449.0,
            },
        ]

    with open(mla_dir / "part_0001.ndjson", "w") as f:
        for rec in mla_records:
            f.write(json.dumps(rec) + "\n")

    return tmpdir


# ============================================================
# MASTER POPULATION DISCOVERY TESTS
# ============================================================

class TestMasterPopulationDiscovery:

    def test_discover_mp_master_population(self):
        from analysis.zero_work_members import discover_master_population

        tmpdir = _create_snapshot_dir()
        try:
            result = discover_master_population(tmpdir)
            assert result["mp_count"] == 2
            assert result["mla_count"] == 1
            assert result["total_count"] == 3
        finally:
            shutil.rmtree(tmpdir)

    def test_discover_empty_snapshot(self):
        from analysis.zero_work_members import discover_master_population

        tmpdir = tempfile.mkdtemp()
        try:
            result = discover_master_population(tmpdir)
            assert result["mp_count"] == 0
            assert result["mla_count"] == 0
            assert result["total_count"] == 0
        finally:
            shutil.rmtree(tmpdir)

    def test_master_population_has_required_fields(self):
        from analysis.zero_work_members import discover_master_population

        tmpdir = _create_snapshot_dir()
        try:
            result = discover_master_population(tmpdir)
            for rec in result["all_records"]:
                assert "member_name" in rec
                assert "member_type" in rec
                assert "state_name" in rec
                assert "constituency" in rec
                assert "house_name" in rec
                assert "tenure" in rec
                assert "tenure_start_date" in rec
                assert "tenure_end_date" in rec
                assert "allocated_amount" in rec
        finally:
            shutil.rmtree(tmpdir)

    def test_mp_records_have_correct_type(self):
        from analysis.zero_work_members import discover_master_population

        tmpdir = _create_snapshot_dir()
        try:
            result = discover_master_population(tmpdir)
            for rec in result["mp_records"]:
                assert rec["member_type"] == "MP"
            for rec in result["mla_records"]:
                assert rec["member_type"] == "MLA"
        finally:
            shutil.rmtree(tmpdir)


# ============================================================
# ZERO-WORK MEMBER INJECTION TESTS
# ============================================================

class TestZeroWorkInjection:

    def _make_works_for_member(self, member_id, member_type="MP", member_name="Test Member"):
        """Create fake work records for a member (for name matching)."""
        return [{
            "work_id": member_id * 1000,
            "member_type": member_type,
            "member_id": member_id,
            "mp_name": member_name,
            "state_id": 1,
            "recommendation_date": date(2024, 1, 1),
            "sanction_date": date(2024, 2, 1),
            "completion_date": None,
            "last_expenditure_date": None,
            "recommended_amount": 500000,
            "sanction_amount": 400000,
            "expenditure_amount": 0,
            "completion_amount": None,
            "activity_name": "Test",
            "constituency_id": 101,
        }]

    def test_inject_zero_work_members(self):
        from analysis.zero_work_members import inject_zero_work_members

        tmpdir = _create_snapshot_dir()
        try:
            existing_members = [
                _make_work_member(1, member_name="MP Alpha"),
            ]
            works = self._make_works_for_member(1, "MP", "MP Alpha")

            result = inject_zero_work_members(existing_members, tmpdir, works=works)

            # Should have 1 existing + 2 new zero-work members
            assert len(result) == 3

            # Check existing member is preserved
            existing = [m for m in result if m.member_id == 1]
            assert len(existing) == 1
            assert existing[0].total_works == 50

            # Check zero-work members are injected
            zero_members = [m for m in result if m.total_works == 0]
            assert len(zero_members) == 2
        finally:
            shutil.rmtree(tmpdir)

    def test_zero_work_members_have_correct_flags(self):
        from analysis.zero_work_members import inject_zero_work_members

        tmpdir = _create_snapshot_dir()
        try:
            existing_members = []
            result = inject_zero_work_members(existing_members, tmpdir)

            for m in result:
                assert m.zero_work_member is True
                assert m.ranking_qualified is False
                assert m.low_sample_member is True
        finally:
            shutil.rmtree(tmpdir)

    def test_zero_work_members_have_no_fabricated_metrics(self):
        from analysis.zero_work_members import inject_zero_work_members

        tmpdir = _create_snapshot_dir()
        try:
            existing_members = []
            result = inject_zero_work_members(existing_members, tmpdir)

            for m in result:
                # Financial metrics should be zero
                assert m.recommended_amount == 0
                assert m.sanctioned_amount == 0
                assert m.expenditure_amount == 0
                assert m.completion_amount == 0

                # Work counts should be zero
                assert m.total_works == 0
                assert m.completed_works == 0
                assert m.ongoing_works == 0
                assert m.sanctioned_works == 0
                assert m.recommended_works == 0

                # Risk metrics should be zero
                assert m.flagged_works == 0
                assert m.high_risk_works == 0
                assert m.medium_risk_works == 0
                assert m.flagged_rate_pct == 0
                assert m.high_risk_rate_pct == 0

                # Anomaly metrics should be zero
                assert m.cost_anomaly_works == 0
                assert m.duration_anomaly_works == 0
                assert m.expenditure_over_sanction_works == 0
                assert m.negative_sanction_delay_works == 0

                # Execution metrics should be None
                assert m.avg_sanction_delay_days is None
                assert m.avg_execution_days is None
                assert m.avg_project_age_days is None
        finally:
            shutil.rmtree(tmpdir)

    def test_zero_work_members_preserve_identity(self):
        from analysis.zero_work_members import inject_zero_work_members

        tmpdir = _create_snapshot_dir()
        try:
            existing_members = []
            result = inject_zero_work_members(existing_members, tmpdir)

            names = {m.member_name for m in result}
            assert "MP Alpha" in names
            assert "MP Beta" in names
            assert "MLA Gamma" in names
        finally:
            shutil.rmtree(tmpdir)

    def test_zero_work_members_preserve_state(self):
        from analysis.zero_work_members import inject_zero_work_members

        tmpdir = _create_snapshot_dir()
        try:
            existing_members = []
            result = inject_zero_work_members(existing_members, tmpdir)

            states = {m.member_name: m.state_id for m in result}
            # All should have state_id assigned
            assert states["MP Alpha"] is not None
            assert states["MP Beta"] is not None
            assert states["MLA Gamma"] is not None
        finally:
            shutil.rmtree(tmpdir)

    def test_existing_members_not_duplicated(self):
        from analysis.zero_work_members import inject_zero_work_members

        tmpdir = _create_snapshot_dir()
        try:
            existing_members = [
                _make_work_member(1, member_name="MP Alpha"),
                _make_work_member(2, member_name="MP Beta"),
            ]
            works = (
                self._make_works_for_member(1, "MP", "MP Alpha") +
                self._make_works_for_member(2, "MP", "MP Beta")
            )

            result = inject_zero_work_members(existing_members, tmpdir, works=works)

            # Should only add 1 new zero-work member (MLA Gamma)
            assert len(result) == 3

            # Existing members should be unchanged
            alpha = [m for m in result if m.member_name == "MP Alpha"][0]
            assert alpha.total_works == 50
            assert alpha.zero_work_member is False
        finally:
            shutil.rmtree(tmpdir)

    def test_work_bearing_members_unchanged(self):
        from analysis.zero_work_members import inject_zero_work_members

        tmpdir = _create_snapshot_dir()
        try:
            original_member = _make_work_member(
                1, member_name="MP Alpha",
                total_works=100, completed_works=50,
                flagged_works=10, high_risk_works=3,
                avg_execution_days=200.0,
            )
            works = self._make_works_for_member(1, "MP", "MP Alpha")

            result = inject_zero_work_members([original_member], tmpdir, works=works)

            # Find the original member
            alpha = [m for m in result if m.member_name == "MP Alpha"][0]

            # All original values should be preserved
            assert alpha.total_works == 100
            assert alpha.completed_works == 50
            assert alpha.flagged_works == 10
            assert alpha.high_risk_works == 3
            assert alpha.avg_execution_days == 200.0
            assert alpha.ranking_qualified is True
        finally:
            shutil.rmtree(tmpdir)


# ============================================================
# TENURE/DATE CONTEXT TESTS
# ============================================================

class TestTenureContext:

    def test_tenure_dates_preserved(self):
        from analysis.zero_work_members import inject_zero_work_members

        tmpdir = _create_snapshot_dir()
        try:
            existing_members = []
            result = inject_zero_work_members(existing_members, tmpdir)

            alpha = [m for m in result if m.member_name == "MP Alpha"][0]
            assert alpha._tenure_start is not None
            assert alpha._tenure_end is not None
            assert str(alpha._tenure_start) == "2024-06-04"
            assert str(alpha._tenure_end) == "2029-06-03"
        finally:
            shutil.rmtree(tmpdir)

    def test_tenure_label_preserved(self):
        from analysis.zero_work_members import inject_zero_work_members

        tmpdir = _create_snapshot_dir()
        try:
            existing_members = []
            result = inject_zero_work_members(existing_members, tmpdir)

            alpha = [m for m in result if m.member_name == "MP Alpha"][0]
            assert alpha._tenure_label == "18th Lok Sabha"
        finally:
            shutil.rmtree(tmpdir)

    def test_allocated_amount_preserved(self):
        from analysis.zero_work_members import inject_zero_work_members

        tmpdir = _create_snapshot_dir()
        try:
            existing_members = []
            result = inject_zero_work_members(existing_members, tmpdir)

            alpha = [m for m in result if m.member_name == "MP Alpha"][0]
            assert alpha._allocated_amount == 190289442.0
        finally:
            shutil.rmtree(tmpdir)

    def test_unknown_tenure_gets_neutral_handling(self):
        from analysis.zero_work_members import inject_zero_work_members

        tmpdir = _create_snapshot_dir(
            mp_records=[
                {
                    "MP_NAME": "New MP",
                    "STATE_NAME": "Test State",
                    "CONSTITUENCY": "Test Constituency",
                    "HOUSE_NAME": "Lok Sabha",
                    "TENURE": "",
                    "TENURE_START_DATE": "",
                    "TENURE_END_DATE": "",
                    "ALLOCATED_AMT": 100000000.0,
                },
            ]
        )
        try:
            existing_members = []
            result = inject_zero_work_members(existing_members, tmpdir)

            new_mp = [m for m in result if m.member_name == "New MP"][0]
            assert new_mp._tenure_start is None
            assert new_mp._tenure_end is None
            assert new_mp._tenure_label == ""
        finally:
            shutil.rmtree(tmpdir)


# ============================================================
# EVIDENCE BUILDER TESTS
# ============================================================

class TestZeroWorkEvidence:

    def test_zero_work_evidence_has_master_context(self):
        from analysis.evidence_builder import build_member_evidence

        m = _make_zero_member(1, member_name="MP Alpha")
        m._master_record = {
            "member_name": "MP Alpha",
            "member_type": "MP",
            "state_name": "Maharashtra",
            "constituency": "Mumbai South",
            "house_name": "Lok Sabha",
            "tenure": "18th Lok Sabha",
            "tenure_start_date": "Jun 4, 2024 12:00:00 AM",
            "tenure_end_date": "Jun 3, 2029 11:59:59 PM",
            "allocated_amount": 190289442.0,
        }
        m._tenure_start = date(2024, 6, 4)
        m._tenure_end = date(2029, 6, 3)

        master_context = {
            (m.member_type, m.member_id): {
                "member_name": "MP Alpha",
                "member_type": "MP",
                "state_name": "Maharashtra",
                "constituency": "Mumbai South",
                "house_name": "Lok Sabha",
                "tenure": "18th Lok Sabha",
                "tenure_start_date": "Jun 4, 2024 12:00:00 AM",
                "tenure_end_date": "Jun 3, 2029 11:59:59 PM",
                "allocated_amount": 190289442.0,
                "tenure_start": date(2024, 6, 4),
                "tenure_end": date(2029, 6, 3),
            }
        }

        evidence = build_member_evidence(
            [m], {}, {}, [],
            master_population_context=master_context,
        )

        assert len(evidence) == 1
        ev = evidence[0]

        # Entity name should be set from master context
        assert ev["entity_name"] == "MP Alpha"

        # Master context should be in evidence
        assert ev["evidence"]["master_context"] is not None
        assert ev["evidence"]["master_context"]["member_name"] == "MP Alpha"
        assert ev["evidence"]["master_context"]["state_name"] == "Maharashtra"
        assert ev["evidence"]["master_context"]["constituency"] == "Mumbai South"
        assert ev["evidence"]["master_context"]["tenure"] == "18th Lok Sabha"

    def test_zero_work_evidence_quality_flag(self):
        from analysis.evidence_builder import build_member_evidence

        m = _make_zero_member(1)
        evidence = build_member_evidence([m], {}, {}, [])

        assert evidence[0]["evidence"]["quality"]["zero_work_member"] is True
        assert evidence[0]["evidence"]["quality"]["ranking_qualified"] is False

    def test_zero_work_evidence_no_anomaly(self):
        from analysis.evidence_builder import build_member_evidence

        m = _make_zero_member(1)
        evidence = build_member_evidence([m], {}, {}, [])

        assert evidence[0]["evidence"]["entity_anomaly"] is None

    def test_work_bearing_member_no_master_context(self):
        from analysis.evidence_builder import build_member_evidence

        m = _make_work_member(1)
        evidence = build_member_evidence([m], {}, {}, [])

        assert evidence[0]["evidence"]["master_context"] is None


# ============================================================
# GEMINI PROMPT TESTS
# ============================================================

class TestZeroWorkGeminiPrompt:

    def test_zero_work_prompt_triggers(self):
        from analysis.gemini_processor import build_prompt

        row = {
            "entity_type": "MP",
            "entity_id": 1,
            "entity_name": "MP Alpha",
            "evidence_version": 2,
            "evidence_hash": "abc123",
            "evidence": {
                "portfolio": {"total_works": 0},
                "quality": {"zero_work_member": True, "low_sample_member": True},
                "master_context": {
                    "member_name": "MP Alpha",
                    "state_name": "Maharashtra",
                    "tenure": "18th Lok Sabha",
                },
            },
        }
        prompt = build_prompt(row)
        assert "ZERO WORKS" in prompt
        assert "MP Alpha" in prompt

    def test_zero_work_prompt_includes_master_context(self):
        from analysis.gemini_processor import build_prompt

        row = {
            "entity_type": "MP",
            "entity_id": 1,
            "entity_name": "MP Alpha",
            "evidence_version": 2,
            "evidence_hash": "abc123",
            "evidence": {
                "portfolio": {"total_works": 0},
                "quality": {"zero_work_member": True, "low_sample_member": True},
                "master_context": {
                    "member_name": "MP Alpha",
                    "state_name": "Maharashtra",
                    "constituency": "Mumbai South",
                    "house_name": "Lok Sabha",
                    "tenure": "18th Lok Sabha",
                    "tenure_start_date": "Jun 4, 2024",
                    "tenure_end_date": "Jun 3, 2029",
                    "allocated_amount": 190289442.0,
                },
            },
        }
        prompt = build_prompt(row)

        # Full evidence payload is included
        assert "ENTITY EVIDENCE" in prompt
        assert "MP Alpha" in prompt

    def test_work_bearing_member_no_zero_work_context(self):
        from analysis.gemini_processor import build_prompt

        row = {
            "entity_type": "MP",
            "entity_id": 1,
            "entity_name": "MP Alpha",
            "evidence_version": 2,
            "evidence_hash": "abc123",
            "evidence": {
                "portfolio": {"total_works": 50},
                "quality": {"zero_work_member": False, "low_sample_member": False},
            },
        }
        prompt = build_prompt(row)
        # The zero work context line should NOT be added for work-bearing members
        # (The SYSTEM_INSTRUCTION contains zero work rules, but the dynamic
        # context_line with "ZERO WORKS:" prefix should not be present)
        assert "ZERO WORKS: This member has no recorded project activity" not in prompt


# ============================================================
# ENTITY ANOMALY EXCLUSION TESTS
# ============================================================

class TestZeroWorkAnomalyExclusion:

    def test_zero_work_not_in_anomaly_population(self):
        from analysis.entity_anomaly import compute_entity_anomalies

        members = [_make_work_member(i, ranking_qualified=True) for i in range(10)]
        members.append(_make_zero_member(99))

        results = compute_entity_anomalies(members, REF)
        ids = [r.entity_id for r in results]
        assert 99 not in ids

    def test_only_qualified_in_anomaly(self):
        from analysis.entity_anomaly import compute_entity_anomalies

        members = [_make_work_member(i, ranking_qualified=True) for i in range(10)]
        members.append(_make_zero_member(99, ranking_qualified=False))

        results = compute_entity_anomalies(members, REF)
        for r in results:
            assert r.entity_id != 99


# ============================================================
# EVIDENCE POPULATION COMPLETENESS TESTS
# ============================================================

class TestEvidencePopulationCompleteness:

    def test_all_master_population_in_evidence(self):
        from analysis.zero_work_members import inject_zero_work_members
        from analysis.evidence_builder import build_member_evidence

        tmpdir = _create_snapshot_dir()
        try:
            # Start with one work-bearing member
            existing_members = [_make_work_member(1, member_name="MP Alpha")]
            works = [{
                "work_id": 1000, "member_type": "MP", "member_id": 1,
                "mp_name": "MP Alpha", "state_id": 1,
            }]

            # Inject zero-work members
            all_members = inject_zero_work_members(existing_members, tmpdir, works=works)

            # Build evidence for all
            evidence = build_member_evidence(all_members, {}, {}, [])

            # Should have evidence for all 3 members
            assert len(evidence) == 3

            # Check all names are present
            entity_names = {r["entity_name"] for r in evidence}
            assert "MP Alpha" in entity_names
            assert "MP Beta" in entity_names
            assert "MLA Gamma" in entity_names
        finally:
            shutil.rmtree(tmpdir)

    def test_evidence_entity_type_matches(self):
        from analysis.zero_work_members import inject_zero_work_members
        from analysis.evidence_builder import build_member_evidence

        tmpdir = _create_snapshot_dir()
        try:
            existing_members = []
            all_members = inject_zero_work_members(existing_members, tmpdir)
            evidence = build_member_evidence(all_members, {}, {}, [])

            for ev in evidence:
                if ev["entity_name"] in ("MP Alpha", "MP Beta"):
                    assert ev["entity_type"] == "MP"
                elif ev["entity_name"] == "MLA Gamma":
                    assert ev["entity_type"] == "MLA"
        finally:
            shutil.rmtree(tmpdir)


# ============================================================
# MASTER POPULATION CONTEXT EXTRACTION TESTS
# ============================================================

class TestMasterPopulationContext:

    def test_get_master_population_context(self):
        from analysis.zero_work_members import inject_zero_work_members
        from analysis.zero_work_members import get_master_population_context

        tmpdir = _create_snapshot_dir()
        try:
            existing_members = [_make_work_member(1, member_name="MP Alpha")]
            works = [{
                "work_id": 1000, "member_type": "MP", "member_id": 1,
                "mp_name": "MP Alpha", "state_id": 1,
            }]
            all_members = inject_zero_work_members(existing_members, tmpdir, works=works)

            context = get_master_population_context(all_members)

            # Work-bearing member should have None context
            assert context[("MP", 1)] is None

            # Zero-work members should have context
            # Find zero-work members
            zero_work_members = [m for m in all_members if m.zero_work_member]
            assert len(zero_work_members) == 2

            for m in zero_work_members:
                ctx = context[(m.member_type, m.member_id)]
                assert ctx is not None
                assert ctx["member_name"] == m.member_name
                if m.member_name == "MP Alpha":
                    assert ctx["state_name"] == "Maharashtra"
                elif m.member_name == "MP Beta":
                    assert ctx["state_name"] == "Delhi"
        finally:
            shutil.rmtree(tmpdir)
