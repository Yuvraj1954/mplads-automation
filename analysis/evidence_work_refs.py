"""Evidence Work References builder.

Populates the evidence_work_refs table by linking evidence records
to specific source works based on deterministic evidence roles.

Design:
    WORK ANALYSES + EVIDENCE → EVIDENCE WORK REFS → DB2

For each member/state evidence record, identifies representative works
that contribute to the evidence categories (risk, positive_signal,
representative, financial, execution).

MLA work identity respects (work_recommendation_dtl_id, mla_id).
"""

from datetime import datetime, timezone
from typing import List, Dict, Any


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def build_evidence_work_refs(evidence_records, work_analyses,
                             member_metrics_map=None):
    """Build evidence_work_refs records.

    For each evidence record, identifies which works contributed to
    each evidence category.

    Args:
        evidence_records: list of evidence record dicts (from evidence_builder)
        work_analyses: list of WorkAnalysis objects
        member_metrics_map: optional dict (member_type, member_id) → MemberMetrics

    Returns:
        list of dicts for DB2 evidence_work_refs table
    """
    works_by_member = _group_works_by_member(work_analyses)
    works_by_state = _group_works_by_state(work_analyses)

    refs = []
    for ev in evidence_records:
        entity_type = ev["entity_type"]
        entity_id = ev["entity_id"]
        member_type = ev.get("member_type") or entity_type

        if entity_type == "STATE":
            entity_refs = _build_state_refs(ev, works_by_state)
            refs.extend(entity_refs)
        elif entity_type in ("MP", "MLA"):
            entity_refs = _build_member_refs(
                ev, works_by_member, member_type
            )
            refs.extend(entity_refs)

    return refs


def _group_works_by_member(work_analyses):
    """Group work analyses by (member_type, member_id)."""
    from collections import defaultdict
    by_member = defaultdict(list)
    for wa in work_analyses:
        by_member[(wa.member_type, wa.member_id)].append(wa)
    return dict(by_member)


def _group_works_by_state(work_analyses):
    """Group work analyses by state_id."""
    from collections import defaultdict
    by_state = defaultdict(list)
    for wa in work_analyses:
        if wa.state_id:
            by_state[wa.state_id].append(wa)
    return dict(by_state)


def _build_member_refs(ev, works_by_member, member_type):
    """Build work references for a member evidence record."""
    entity_id = ev["entity_id"]
    evidence = ev.get("evidence", {})
    works = works_by_member.get((member_type, entity_id), [])

    if not works:
        return []

    refs = []
    member_type_for_ref = member_type

    # Risk works: flagged or high-risk
    risk_works = [w for w in works if (w.flag_count or 0) >= 1 or w.risk_level == "HIGH"]
    for w in risk_works[:5]:  # Top 5
        refs.append(_make_ref(
            entity_type=member_type,
            entity_id=entity_id,
            work_id=w.work_id,
            member_type=member_type_for_ref,
            evidence_role="risk",
            reason=f"flag_count={w.flag_count}, risk_level={w.risk_level}",
        ))

    # Positive signal works: completed with good metrics
    positive_works = [
        w for w in works
        if w.status == "Completed"
        and w.expenditure_percentage and w.expenditure_percentage >= 80
    ]
    for w in positive_works[:3]:
        refs.append(_make_ref(
            entity_type=member_type,
            entity_id=entity_id,
            work_id=w.work_id,
            member_type=member_type_for_ref,
            evidence_role="positive_signal",
            reason=f"completed, expenditure_pct={w.expenditure_percentage}",
        ))

    # Representative works: largest by sanction amount
    sanctioned_works = [w for w in works if w.sanction_amount and w.sanction_amount > 0]
    sanctioned_works.sort(key=lambda w: -(w.sanction_amount or 0))
    for w in sanctioned_works[:3]:
        refs.append(_make_ref(
            entity_type=member_type,
            entity_id=entity_id,
            work_id=w.work_id,
            member_type=member_type_for_ref,
            evidence_role="representative",
            reason=f"sanction_amount={w.sanction_amount}",
        ))

    # Financial works: highest expenditure
    exp_works = [w for w in works if w.expenditure_amount and w.expenditure_amount > 0]
    exp_works.sort(key=lambda w: -(w.expenditure_amount or 0))
    for w in exp_works[:3]:
        refs.append(_make_ref(
            entity_type=member_type,
            entity_id=entity_id,
            work_id=w.work_id,
            member_type=member_type_for_ref,
            evidence_role="financial",
            reason=f"expenditure_amount={w.expenditure_amount}",
        ))

    # Execution works: longest running or most delayed
    exec_works = [
        w for w in works
        if w.sanction_delay_days is not None and w.sanction_delay_days > 0
    ]
    exec_works.sort(key=lambda w: -(w.sanction_delay_days or 0))
    for w in exec_works[:3]:
        refs.append(_make_ref(
            entity_type=member_type,
            entity_id=entity_id,
            work_id=w.work_id,
            member_type=member_type_for_ref,
            evidence_role="execution",
            reason=f"sanction_delay_days={w.sanction_delay_days}",
        ))

    return refs


def _build_state_refs(ev, works_by_state):
    """Build work references for a state evidence record."""
    entity_id = ev["entity_id"]
    state_works = works_by_state.get(entity_id, [])

    if not state_works:
        return []

    refs = []

    # Risk works for state
    risk_works = [w for w in state_works if (w.flag_count or 0) >= 1 or w.risk_level == "HIGH"]
    risk_works.sort(key=lambda w: -(w.flag_count or 0))
    for w in risk_works[:5]:
        refs.append(_make_ref(
            entity_type="STATE",
            entity_id=entity_id,
            work_id=w.work_id,
            member_type=w.member_type,
            evidence_role="risk",
            reason=f"state risk work, flag_count={w.flag_count}",
        ))

    # Representative works for state (largest)
    sanctioned_works = [w for w in state_works if w.sanction_amount and w.sanction_amount > 0]
    sanctioned_works.sort(key=lambda w: -(w.sanction_amount or 0))
    for w in sanctioned_works[:3]:
        refs.append(_make_ref(
            entity_type="STATE",
            entity_id=entity_id,
            work_id=w.work_id,
            member_type=w.member_type,
            evidence_role="representative",
            reason=f"state representative, sanction_amount={w.sanction_amount}",
        ))

    return refs


def _make_ref(entity_type, entity_id, work_id, member_type,
              evidence_role, reason):
    """Create a single evidence_work_ref record."""
    return {
        "entity_type": entity_type,
        "entity_id": entity_id,
        "work_id": work_id,
        "member_type": member_type,
        "evidence_role": evidence_role,
        "reason": reason,
        "created_at": _now_iso(),
    }
