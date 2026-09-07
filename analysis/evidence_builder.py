"""Build evidence records from deterministic analysis pipeline output.

Evidence is the bridge between deterministic analysis and Gemini interpretation.
It packages analytical findings into a structured format that Gemini can consume.

Architecture:
    SOURCE DATA → DETERMINISTIC ANALYSIS → EVIDENCE → GEMINI INTERPRETATION

Evidence never overwrites source facts or deterministic calculations.
Evidence is derived FROM deterministic output, not a replacement for it.

Affected-only design:
    build_evidence_for_entities() accepts a list of entity keys to process.
    Only those entities get new evidence records. Unchanged entities are skipped.
"""

import hashlib
import json
from datetime import datetime, timezone
from typing import Optional

EVIDENCE_VERSION = 2
EVIDENCE_SCHEMA_VERSION = "evidence_schema_v2"
SYSTEM_VERSION = "evidence_builder_v1"


def _hash_evidence(evidence_dict):
    """Deterministic hash of evidence content for idempotency."""
    canonical = json.dumps(evidence_dict, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _safe(val, default=None):
    """Return val if not None, else default."""
    return val if val is not None else default


def build_member_evidence(member_metrics, work_analyses_by_member,
                          state_metrics_by_id, statistics,
                          entity_anomaly_results=None,
                          master_population_context=None):
    """Build evidence records for all members.

    Args:
        member_metrics: list of MemberMetrics objects
        work_analyses_by_member: dict mapping (member_type, member_id) -> list of WorkAnalysis
        state_metrics_by_id: dict mapping state_id -> StateMetrics
        statistics: list of NationalStatistics
        entity_anomaly_results: optional list of EntityAnomalyResult for members
        master_population_context: optional dict mapping (member_type, member_id) → master context

    Returns:
        list of evidence record dicts ready for DB insertion
    """
    anomaly_map = {}
    if entity_anomaly_results:
        for r in entity_anomaly_results:
            anomaly_map[(r.member_type, r.entity_id)] = r

    stats_map = {}
    for s in statistics:
        key = (s.metric_name, s.member_type)
        stats_map[key] = s

    records = []
    for m in member_metrics:
        works = work_analyses_by_member.get((m.member_type, m.member_id), [])
        state = state_metrics_by_id.get(m.state_id) if m.state_id else None
        anomaly = anomaly_map.get((m.member_type, m.member_id))
        master_ctx = (master_population_context or {}).get((m.member_type, m.member_id))

        evidence = _build_member_evidence_dict(m, works, state, stats_map, anomaly, master_ctx)
        evidence_hash = _hash_evidence(evidence)

        # Use master population name if available (for zero-work members)
        entity_name = None
        if master_ctx and master_ctx.get("member_name"):
            entity_name = master_ctx["member_name"]
        elif hasattr(m, 'member_name') and m.member_name:
            entity_name = m.member_name

        records.append({
            "entity_type": m.member_type,
            "entity_id": m.member_id,
            "entity_name": entity_name,
            "member_type": m.member_type,
            "state_id": m.state_id,
            "constituency_id": m.constituency_id,
            "evidence_version": EVIDENCE_VERSION,
            "evidence_hash": evidence_hash,
            "generated_at": _now_iso(),
            "evidence": evidence,
        })

    return records


def build_state_evidence(state_metrics, work_analyses_by_state,
                         entity_anomaly_results=None):
    """Build evidence records for all states.

    Args:
        state_metrics: list of StateMetrics objects
        work_analyses_by_state: dict mapping state_id -> list of WorkAnalysis
        entity_anomaly_results: optional list of EntityAnomalyResult for states

    Returns:
        list of evidence record dicts ready for DB insertion
    """
    anomaly_map = {}
    if entity_anomaly_results:
        for r in entity_anomaly_results:
            anomaly_map[r.entity_id] = r

    records = []
    for s in state_metrics:
        works = work_analyses_by_state.get(s.state_id, [])
        anomaly = anomaly_map.get(s.state_id)

        evidence = _build_state_evidence_dict(s, works, anomaly)
        evidence_hash = _hash_evidence(evidence)

        records.append({
            "entity_type": "STATE",
            "entity_id": s.state_id,
            "entity_name": s.state_name,
            "member_type": None,
            "state_id": s.state_id,
            "constituency_id": None,
            "evidence_version": EVIDENCE_VERSION,
            "evidence_hash": evidence_hash,
            "generated_at": _now_iso(),
            "evidence": evidence,
        })

    return records


def _build_member_evidence_dict(m, works, state, stats_map, anomaly, master_ctx=None):
    """Build the evidence dict for a single member."""
    zero_work = m.total_works == 0

    portfolio = {
        "total_works": m.total_works,
        "recommended_works": m.recommended_works,
        "sanctioned_works": m.sanctioned_works,
        "completed_works": m.completed_works,
        "ongoing_works": m.ongoing_works,
        "pending_works": m.pending_works,
        "completion_rate_pct": m.completion_rate_pct,
        "sanction_rate_pct": m.sanction_rate_pct,
    }

    financial = {
        "recommended_amount": _safe(m.recommended_amount, 0),
        "sanctioned_amount": _safe(m.sanctioned_amount, 0),
        "expenditure_amount": _safe(m.expenditure_amount, 0),
        "completion_amount": _safe(m.completion_amount, 0),
        "unspent_amount": _safe(m.unspent_amount, 0),
        "expenditure_sanction_utilization_pct": _safe(m.expenditure_sanction_utilization_pct, 0),
    }

    execution = {
        "avg_sanction_delay_days": m.avg_sanction_delay_days,
        "median_sanction_delay_days": m.median_sanction_delay_days,
        "avg_execution_days": m.avg_execution_days,
        "median_execution_days": m.median_execution_days,
        "avg_project_age_days": m.avg_project_age_days,
        "max_project_age_days": m.max_project_age_days,
        "avg_pending_days": m.avg_pending_days,
        "max_pending_days": m.max_pending_days,
    }

    risk = {
        "flagged_works": m.flagged_works,
        "high_risk_works": m.high_risk_works,
        "medium_risk_works": m.medium_risk_works,
        "flagged_rate_pct": m.flagged_rate_pct,
        "high_risk_rate_pct": m.high_risk_rate_pct,
        "overdue_over_1_year": m.overdue_over_1_year,
        "overdue_over_2_years": m.overdue_over_2_years,
    }

    anomalies = {
        "cost_anomaly_works": m.cost_anomaly_works,
        "duration_anomaly_works": m.duration_anomaly_works,
        "expenditure_over_sanction_works": m.expenditure_over_sanction_works,
        "negative_sanction_delay_works": m.negative_sanction_delay_works,
        "avg_cost_percentile": m.avg_cost_percentile,
        "avg_duration_percentile": m.avg_duration_percentile,
    }

    quality = {
        "zero_work_member": zero_work,
        "low_sample_member": m.low_sample_member,
        "ranking_qualified": m.ranking_qualified,
    }

    national_context = _build_national_context(m, stats_map)

    state_context = None
    if state:
        state_context = {
            "state_id": state.state_id,
            "total_works": state.total_works,
            "active_members": state.active_members,
            "completion_rate_pct": state.completion_rate_pct,
            "risk_rate_pct": state.risk_rate_pct,
        }

    entity_anomaly = None
    if anomaly:
        entity_anomaly = {
            "anomaly_score": anomaly.anomaly_score,
            "anomaly_level": anomaly.anomaly_level,
            "confidence_level": anomaly.confidence_level,
            "contributing_features": anomaly.contributing_features,
        }

    # Master population context (for zero-work members from allocated_limit snapshots)
    master_context = None
    if master_ctx:
        master_context = {
            "member_name": master_ctx.get("member_name"),
            "member_type": master_ctx.get("member_type"),
            "state_name": master_ctx.get("state_name"),
            "constituency": master_ctx.get("constituency"),
            "house_name": master_ctx.get("house_name"),
            "tenure": master_ctx.get("tenure"),
            "tenure_start_date": master_ctx.get("tenure_start_date"),
            "tenure_end_date": master_ctx.get("tenure_end_date"),
            "allocated_amount": master_ctx.get("allocated_amount"),
        }

    evidence = {
        "portfolio": portfolio,
        "financial": financial,
        "execution": execution,
        "risk": risk,
        "anomalies": anomalies,
        "quality": quality,
        "national_context": national_context,
        "state_context": state_context,
        "entity_anomaly": entity_anomaly,
        "master_context": master_context,
    }

    return evidence


def _build_state_evidence_dict(s, works, anomaly):
    """Build the evidence dict for a single state."""
    portfolio = {
        "total_works": s.total_works,
        "active_members": s.active_members,
        "mp_active_members": s.mp_active_members,
        "mla_active_members": s.mla_active_members,
        "recommended_works": s.recommended_works,
        "sanctioned_works": s.sanctioned_works,
        "completed_works": s.completed_works,
        "ongoing_works": s.ongoing_works,
        "completion_rate_pct": s.completion_rate_pct,
        "sanction_rate_pct": s.sanction_rate_pct,
    }

    financial = {
        "recommended_amount": _safe(s.recommended_amount, 0),
        "sanctioned_amount": _safe(s.sanctioned_amount, 0),
        "expenditure_amount": _safe(s.expenditure_amount, 0),
        "completion_amount": _safe(s.completion_amount, 0),
        "expenditure_utilization_pct": _safe(s.expenditure_utilization_pct, 0),
    }

    execution = {
        "avg_sanction_delay_days": s.avg_sanction_delay_days,
        "median_sanction_delay_days": s.median_sanction_delay_days,
        "avg_execution_days": s.avg_execution_days,
        "median_execution_days": s.median_execution_days,
    }

    risk = {
        "flagged_works": s.flagged_works,
        "high_risk_works": s.high_risk_works,
        "overdue_over_1_year": s.overdue_over_1_year,
        "overdue_over_2_years": s.overdue_over_2_years,
        "risk_rate_pct": s.risk_rate_pct,
    }

    anomalies = {
        "cost_anomaly_works": s.cost_anomaly_works,
        "duration_anomaly_works": s.duration_anomaly_works,
        "expenditure_over_sanction_works": s.expenditure_over_sanction_works,
        "negative_sanction_delay_works": s.negative_sanction_delay_works,
    }

    quality = {
        "ranking_qualified": s.ranking_qualified,
    }

    entity_anomaly = None
    if anomaly:
        entity_anomaly = {
            "anomaly_score": anomaly.anomaly_score,
            "anomaly_level": anomaly.anomaly_level,
            "confidence_level": anomaly.confidence_level,
            "contributing_features": anomaly.contributing_features,
        }

    return {
        "portfolio": portfolio,
        "financial": financial,
        "execution": execution,
        "risk": risk,
        "anomalies": anomalies,
        "quality": quality,
        "entity_anomaly": entity_anomaly,
    }


def _build_national_context(m, stats_map):
    """Build national benchmark context for a member."""
    context = {}

    total_key = ("total_works", None)
    if total_key in stats_map:
        s = stats_map[total_key]
        context["national_total_works_median"] = s.median
        context["national_total_works_p90"] = s.p90

    comp_key = ("completion_rate_pct", None)
    if comp_key in stats_map:
        s = stats_map[comp_key]
        context["national_completion_rate_median"] = s.median

    delay_key = ("avg_sanction_delay_days", None)
    if delay_key in stats_map:
        s = stats_map[delay_key]
        context["national_sanction_delay_median"] = s.median

    return context if context else None


def group_works_by_member(work_analyses):
    """Group work analyses by (member_type, member_id)."""
    from collections import defaultdict
    by_member = defaultdict(list)
    for wa in work_analyses:
        by_member[(wa.member_type, wa.member_id)].append(wa)
    return dict(by_member)


def group_works_by_state(work_analyses):
    """Group work analyses by state_id."""
    from collections import defaultdict
    by_state = defaultdict(list)
    for wa in work_analyses:
        if wa.state_id:
            by_state[wa.state_id].append(wa)
    return dict(by_state)
