"""DB2 Analytics Persistence Layer.

Converts pipeline analysis output (MemberMetrics, StateMetrics,
NationalStatistics, TrendRecords, EntityAnomalyResults) into
DB2-ready records for upsert.

This is the bridge between Python deterministic analysis and DB2
application-facing analytics tables.

Design:
    PYTHON ANALYSIS → PERSISTENCE FUNCTIONS → DB2 UPSERT

All functions are pure transformations — they take pipeline objects
and return dicts ready for database insertion. They do NOT perform
any database operations themselves.
"""

from datetime import datetime, timezone
from typing import Optional, List, Dict, Any


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _safe_float(val, default=None):
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _safe_int(val, default=0):
    if val is None:
        return default
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


# ============================================================
# AUTHORITATIVE PERFORMANCE FORMULAS (single source of truth)
# ============================================================
# These are the ONLY definitions of the performance score and the
# performance classification in GovSense. They are pure functions so
# they can be imported by the analysis layer, the remediation script,
# and tests without any divergence.
#
# Concept separation (do NOT mix):
#   * performance_score / performance_classification -> outcome quality
#   * anomaly_score / anomaly_level                  -> statistical unusualness
#   * risk_level / risk_flags                        -> concrete work conditions
#
# Definition of performance_score:
#   performance_score = completion_rate_pct + fund_utilization_pct
#
# Rationale for retaining this formula: both components are direct,
# auditable MPLADS outcome measures (how much of the portfolio is
# complete; how much of sanctioned money has been disbursed). It is a
# heuristic 0-200 outcome index, NOT a statistically normalised score;
# normalisation is deferred (see audit Phase 15) rather than replaced
# with an arbitrary new formula. It is computed and persisted by the
# pipeline from current values, so it can never go stale.
PERFORMER_MIN = 160.0
AVERAGE_MIN = 120.0
NEEDS_ATTENTION_MIN = 80.0
MIN_WORKS_FOR_CLASSIFICATION = 5

PERFORMANCE_CLASSES = (
    "PERFORMER",
    "AVERAGE",
    "NEEDS_ATTENTION",
    "UNDERPERFORMER",
    "NO_DATA",
    "INSUFFICIENT_DATA",
)


def compute_performance_score(completion_rate_pct, fund_utilization_pct):
    """Authoritative 0-200 performance score. Missing inputs count as 0."""
    comp = _safe_float(completion_rate_pct, 0.0) or 0.0
    util = _safe_float(fund_utilization_pct, 0.0) or 0.0
    return round(comp + util, 2)


def classify_performance_from_score(score):
    """Authoritative performance band. Pure outcome quality, no anomaly."""
    score = _safe_float(score, 0.0) or 0.0
    if score >= PERFORMER_MIN:
        return "PERFORMER"
    if score >= AVERAGE_MIN:
        return "AVERAGE"
    if score >= NEEDS_ATTENTION_MIN:
        return "NEEDS_ATTENTION"
    return "UNDERPERFORMER"


# ============================================================
# STATISTICAL HELPERS
# ============================================================
# Used by compute_state_ranks to produce a sample-size-adjusted
# performance ranking. Sample size is accounted for through the math
# itself — no hard-coded thresholds, no work-count bonuses.

import math as _math
import statistics as _statistics


def _empirical_bayes_k(observed_rates, sample_sizes):
    """Method-of-moments estimator for the prior pseudo-count K.

    For a hierarchical binomial model y_i ~ Binomial(n_i, p_i) where
    p_i ~ Beta(alpha, beta), the posterior mean is:
        adjusted_i = (y_i + K * p_bar) / (n_i + K)
    with K = alpha + beta.

    The standard MOM estimator (Morris, 1983) is:
        K = W / (B - W)
    where:
        W = average within-group variance = mean(p_i * (1 - p_i))
        B = between-group variance of the true p_i (estimated from the
            sample variance of observed rates minus the expected within
            variance)

    Returns a small positive integer. Falls back to a conservative
    default if the data produces a non-positive estimate.
    """
    if not observed_rates:
        return 5

    p_bar = _statistics.fmean(observed_rates)
    if not (0.0 < p_bar < 1.0):
        return 5

    sample_var = _statistics.pvariance(observed_rates)
    expected_within = p_bar * (1.0 - p_bar)
    between = sample_var - expected_within * _statistics.fmean(
        1.0 / max(n, 1) for n in sample_sizes
    )

    if between <= 0:
        return 5

    k = expected_within / between
    if k <= 0:
        return 5
    return int(round(min(k, 100)))


# ============================================================
# OVERALL METRICS
# ============================================================

def build_overall_metrics(member_metrics_list, state_metrics_list,
                          member_anomalies=None):
    """Build overall_metrics records for all three scopes.

    Args:
        member_metrics_list: list of MemberMetrics (all 774)
        state_metrics_list: list of StateMetrics (all 36)
        member_anomalies: optional list of EntityAnomalyResult for members

    Returns:
        list of dicts for DB2 overall_metrics table
    """
    anomaly_map = {}
    if member_anomalies:
        for a in member_anomalies:
            if a.member_type:
                anomaly_map[(a.member_type, a.entity_id)] = a

    records = []
    for scope in ("BOTH", "MP", "MLA"):
        if scope == "BOTH":
            members = member_metrics_list
            states = state_metrics_list
        elif scope == "MP":
            members = [m for m in member_metrics_list if m.member_type == "MP"]
            states = state_metrics_list  # State metrics already cover both
        else:
            members = [m for m in member_metrics_list if m.member_type == "MLA"]
            states = state_metrics_list

        r = _aggregate_overall(members, states, scope, anomaly_map)
        records.append(r)

    return records


def _aggregate_overall(members, states, scope, anomaly_map):
    """Aggregate overall metrics from member and state lists."""
    work_bearing = [m for m in members if m.total_works > 0]
    zero_work = [m for m in members if m.zero_work_member]
    low_sample = [m for m in members if m.low_sample_member and not m.zero_work_member]
    anomaly_qualified = [m for m in members if m.ranking_qualified]

    total_works = sum(m.total_works for m in members)
    recommended = sum(m.recommended_works for m in members)
    sanctioned = sum(m.sanctioned_works for m in members)
    completed = sum(m.completed_works for m in members)
    ongoing = sum(m.ongoing_works for m in members)
    pending = sum(m.pending_works for m in members)

    recommended_amount = sum(m.recommended_amount for m in members)
    sanctioned_amount = sum(m.sanctioned_amount for m in members)
    expenditure_amount = sum(m.expenditure_amount for m in members)
    completion_amount = sum(m.completion_amount for m in members)
    unspent_amount = sanctioned_amount - expenditure_amount

    allocated = 0
    for m in members:
        master = getattr(m, '_master_record', None)
        if master:
            alloc = master.get("allocated_amount")
            if alloc:
                try:
                    allocated += float(alloc)
                except (TypeError, ValueError):
                    pass

    completion_rate = round((completed / total_works * 100), 2) if total_works > 0 else 0
    sanction_rate = round((sanctioned / total_works * 100), 2) if total_works > 0 else 0
    sanction_conversion = round((completed / sanctioned * 100), 2) if sanctioned > 0 else 0
    fund_utilization = round((expenditure_amount / sanctioned_amount * 100), 2) if sanctioned_amount > 0 else 0
    expenditure_rate = round((expenditure_amount / recommended_amount * 100), 2) if recommended_amount > 0 else 0
    expenditure_per_recommended = round((expenditure_amount / recommended_amount * 100), 2) if recommended_amount > 0 else 0

    sanction_delays = [m.avg_sanction_delay_days for m in work_bearing if m.avg_sanction_delay_days is not None]
    exec_days = [m.avg_execution_days for m in work_bearing if m.avg_execution_days is not None]
    project_ages = [m.avg_project_age_days for m in work_bearing if m.avg_project_age_days is not None]

    costs = []
    for m in work_bearing:
        if m.sanctioned_amount and m.sanctioned_works:
            costs.append(m.sanctioned_amount / m.sanctioned_works)
    costs.sort()

    flagged = sum(m.flagged_works for m in members)
    high_risk = sum(m.high_risk_works for m in members)
    medium_risk = sum(m.medium_risk_works for m in members)
    cost_anomaly = sum(m.cost_anomaly_works for m in members)
    duration_anomaly = sum(m.duration_anomaly_works for m in members)
    negative_delay = sum(m.negative_sanction_delay_works for m in members)
    overdue_1 = sum(m.overdue_over_1_year for m in members)
    overdue_2 = sum(m.overdue_over_2_years for m in members)

    return {
        "scope": scope,
        "total_members": len(members),
        "work_bearing": len(work_bearing),
        "zero_work_members": len(zero_work),
        "low_sample_members": len(low_sample),
        "total_works": total_works,
        "recommended_works": recommended,
        "sanctioned_works": sanctioned,
        "completed_works": completed,
        "ongoing_works": ongoing,
        "pending_works": pending,
        "completion_rate_pct": completion_rate,
        "sanction_rate_pct": sanction_rate,
        "sanction_conversion_pct": sanction_conversion,
        "allocated_amount": _safe_float(allocated, 0),
        "recommended_amount": _safe_float(recommended_amount, 0),
        "sanctioned_amount": _safe_float(sanctioned_amount, 0),
        "expenditure_amount": _safe_float(expenditure_amount, 0),
        "completion_amount": _safe_float(completion_amount, 0),
        "unspent_amount": _safe_float(unspent_amount, 0),
        "fund_utilization_pct": fund_utilization,
        "expenditure_rate_pct": expenditure_rate,
        "expenditure_per_recommended": expenditure_per_recommended,
        "avg_work_cost": round(sum(costs) / len(costs), 2) if costs else None,
        "median_work_cost": round(costs[len(costs) // 2], 2) if costs else None,
        "p25_work_cost": round(costs[int(len(costs) * 0.25)], 2) if costs else None,
        "p75_work_cost": round(costs[int(len(costs) * 0.75)], 2) if costs else None,
        "p90_work_cost": round(costs[int(len(costs) * 0.90)], 2) if costs else None,
        "p95_work_cost": round(costs[int(len(costs) * 0.95)], 2) if costs else None,
        "avg_sanction_delay_days": round(sum(sanction_delays) / len(sanction_delays), 2) if sanction_delays else None,
        "median_sanction_delay_days": _median(sanction_delays),
        "avg_execution_days": round(sum(exec_days) / len(exec_days), 2) if exec_days else None,
        "median_execution_days": _median(exec_days),
        "avg_project_age_days": round(sum(project_ages) / len(project_ages), 2) if project_ages else None,
        "median_project_age_days": _median(project_ages),
        "overdue_over_1_year": overdue_1,
        "overdue_over_2_years": overdue_2,
        "flagged_works": flagged,
        "high_risk_works": high_risk,
        "medium_risk_works": medium_risk,
        "cost_anomaly_works": cost_anomaly,
        "duration_anomaly_works": duration_anomaly,
        "negative_delay_count": negative_delay,
        "benchmark_qualified": len(anomaly_qualified),
        "insufficient_benchmark": len(low_sample),
        "anomaly_qualified": len(anomaly_qualified),
        "calculated_at": _now_iso(),
    }


def _median(values):
    if not values:
        return None
    s = sorted(values)
    n = len(s)
    if n % 2 == 0:
        return round((s[n // 2 - 1] + s[n // 2]) / 2, 2)
    return round(s[n // 2], 2)


# ============================================================
# MEMBER METRICS
# ============================================================

def build_member_metrics(member_metrics_list, member_anomalies=None,
                         master_population_context=None):
    """Build member_metrics records for all members.

    Args:
        member_metrics_list: list of MemberMetrics (all 774)
        member_anomalies: optional list of EntityAnomalyResult
        master_population_context: optional dict for zero-work context

    Returns:
        list of dicts for DB2 member_metrics table
    """
    anomaly_map = {}
    if member_anomalies:
        for a in member_anomalies:
            if a.member_type:
                anomaly_map[(a.member_type, a.entity_id)] = a

    master_ctx = master_population_context or {}

    records = []
    for m in member_metrics_list:
        r = _member_to_record(m, anomaly_map, master_ctx)
        records.append(r)

    return records


def _member_to_record(m, anomaly_map, master_ctx):
    """Convert a MemberMetrics object to a DB2 record dict."""
    anomaly = anomaly_map.get((m.member_type, m.member_id))

    # Get allocated amount from master context if zero-work
    allocated = 0
    master = getattr(m, '_master_record', None)
    if master:
        alloc = master.get("allocated_amount")
        if alloc:
            try:
                allocated = float(alloc)
            except (TypeError, ValueError):
                pass

    # Get member name from master context or object
    member_name = m.member_name
    if not member_name and master:
        member_name = master.get("member_name")

    # Get state name — prefer MemberMetrics.state_name (set from works data),
    # fall back to master_population_context (for zero-work members)
    state_name = getattr(m, 'state_name', None)
    if not state_name:
        master_data = master_ctx.get((m.member_type, m.member_id))
        if master_data:
            state_name = master_data.get("state_name")

    # Get tenure info
    house_name = None
    tenure = None
    tenure_start = None
    tenure_end = None
    if master:
        house_name = master.get("house_name")
        tenure = master.get("tenure")
        tenure_start = master.get("tenure_start_date")
        tenure_end = master.get("tenure_end_date")

    # Work cost — per-work sanction statistics from the aggregation layer.
    # Fall back to the member-level ratio only when the per-work fields are
    # absent (e.g. records built before these fields existed). Never fake a 0.
    avg_work_cost = _safe_float(getattr(m, "avg_work_cost", None))
    median_work_cost = _safe_float(getattr(m, "median_work_cost", None))
    if avg_work_cost is None and m.sanctioned_amount and m.sanctioned_works and m.sanctioned_works > 0:
        avg_work_cost = round(m.sanctioned_amount / m.sanctioned_works, 2)

    # Fund utilization
    fund_util = 0
    if m.sanctioned_amount and m.sanctioned_amount > 0:
        fund_util = round((m.expenditure_amount / m.sanctioned_amount) * 100, 2)

    # Authoritative performance score (outcome quality only).
    perf_score = compute_performance_score(m.completion_rate_pct, fund_util)

    # Expenditure rate
    exp_rate = 0
    if m.recommended_amount and m.recommended_amount > 0:
        exp_rate = round((m.expenditure_amount / m.recommended_amount) * 100, 2)

    # Performance classification
    perf_class = _classify_member_performance(m, anomaly)

    # Rank (computed externally and passed in, or None)
    rank = None

    return {
        "member_id": m.member_id,
        "member_type": m.member_type,
        "member_name": member_name,
        "state_id": m.state_id,
        "state_name": state_name,
        "constituency_id": m.constituency_id,
        "house_name": house_name,
        "tenure": tenure,
        "tenure_start_date": tenure_start,
        "tenure_end_date": tenure_end,
        "total_works": m.total_works,
        "recommended_works": m.recommended_works,
        "sanctioned_works": m.sanctioned_works,
        "completed_works": m.completed_works,
        "ongoing_works": m.ongoing_works,
        "pending_works": m.pending_works,
        "completion_rate_pct": m.completion_rate_pct,
        "sanction_rate_pct": m.sanction_rate_pct,
        "sanction_conversion_pct": m.sanction_conversion_pct,
        "allocated_amount": _safe_float(allocated, 0),
        "recommended_amount": _safe_float(m.recommended_amount, 0),
        "sanctioned_amount": _safe_float(m.sanctioned_amount, 0),
        "expenditure_amount": _safe_float(m.expenditure_amount, 0),
        "completion_amount": _safe_float(m.completion_amount, 0),
        "unspent_amount": _safe_float(m.unspent_amount, 0),
        "fund_utilization_pct": fund_util,
        "expenditure_rate_pct": exp_rate,
        "avg_work_cost": avg_work_cost,
        "median_work_cost": median_work_cost,
        "avg_sanction_delay_days": m.avg_sanction_delay_days,
        "median_sanction_delay_days": m.median_sanction_delay_days,
        "avg_execution_days": m.avg_execution_days,
        "median_execution_days": m.median_execution_days,
        "avg_project_age_days": m.avg_project_age_days,
        "max_project_age_days": m.max_project_age_days,
        "overdue_over_1_year": m.overdue_over_1_year,
        "overdue_over_2_years": m.overdue_over_2_years,
        "flagged_works": m.flagged_works,
        "high_risk_works": m.high_risk_works,
        "medium_risk_works": m.medium_risk_works,
        "flagged_rate_pct": m.flagged_rate_pct,
        "high_risk_rate_pct": m.high_risk_rate_pct,
        "cost_anomaly_works": m.cost_anomaly_works,
        "duration_anomaly_works": m.duration_anomaly_works,
        "anomaly_score": anomaly.anomaly_score if anomaly else None,
        "anomaly_level": anomaly.anomaly_level if anomaly else "NORMAL",
        "confidence_level": anomaly.confidence_level if anomaly else "LOW",
        "zero_work_member": m.zero_work_member,
        "low_sample_member": m.low_sample_member,
        "ranking_qualified": m.ranking_qualified,
        "performance_classification": perf_class,
        "performance_score": perf_score,
        "rank": rank,
        "scale_score": None,
        "performance_score_weighted": None,
        "calculated_at": _now_iso(),
    }


def _classify_member_performance(m, anomaly=None):
    """Classify member PERFORMANCE (outcome quality) into the 6 buckets.

    IMPORTANT (Phase 1 — concept separation): the anomaly level no longer
    overwrites the performance classification. A statistically unusual but
    high-performing member keeps their performance label; anomaly is
    represented independently in `anomaly_score` / `anomaly_level`.

    The `anomaly` argument is accepted for backward compatibility and is
    intentionally ignored.
    """
    total = m.total_works or 0
    if m.zero_work_member or total == 0:
        return "NO_DATA"
    if m.low_sample_member or total < MIN_WORKS_FOR_CLASSIFICATION:
        return "INSUFFICIENT_DATA"

    score = compute_performance_score(
        m.completion_rate_pct, m.expenditure_sanction_utilization_pct
    )
    return classify_performance_from_score(score)


# ============================================================
# STATE METRICS
# ============================================================

def build_state_metrics(state_metrics_list, state_anomalies=None,
                        member_metrics_list=None):
    """Build state_metrics records for all states.

    Args:
        state_metrics_list: list of StateMetrics (all 36)
        state_anomalies: optional list of EntityAnomalyResult for states
        member_metrics_list: optional list of MemberMetrics (for member counts)

    Returns:
        list of dicts for DB2 state_metrics table
    """
    anomaly_map = {}
    if state_anomalies:
        for a in state_anomalies:
            anomaly_map[a.entity_id] = a

    # Build state member counts
    state_member_counts = {}
    state_allocated = {}
    if member_metrics_list:
        for m in member_metrics_list:
            sid = m.state_id
            if sid not in state_member_counts:
                state_member_counts[sid] = {"total": 0, "mp": 0, "mla": 0}
            state_member_counts[sid]["total"] += 1
            if m.member_type == "MP":
                state_member_counts[sid]["mp"] += 1
            else:
                state_member_counts[sid]["mla"] += 1
            # APPENDED: aggregate allocated_amount per state from members
            master = getattr(m, "_master_record", None)
            alloc = 0.0
            if master:
                try:
                    alloc = float(master.get("allocated_amount") or 0)
                except (TypeError, ValueError):
                    alloc = 0.0
            state_allocated[sid] = state_allocated.get(sid, 0.0) + alloc

    records = []
    for s in state_metrics_list:
        r = _state_to_record(s, anomaly_map, state_member_counts,
                             state_allocated.get(s.state_id, 0.0))
        records.append(r)

    return records


def _state_to_record(s, anomaly_map, state_member_counts, allocated_amount=0.0):
    """Convert a StateMetrics object to a DB2 record dict."""
    anomaly = anomaly_map.get(s.state_id)
    counts = state_member_counts.get(s.state_id, {})

    fund_util = s.expenditure_utilization_pct
    exp_rate = 0
    if s.recommended_amount and s.recommended_amount > 0:
        exp_rate = round((s.expenditure_amount / s.recommended_amount) * 100, 2)

    perf_class = _classify_state_performance(s, anomaly)
    perf_score = compute_performance_score(s.completion_rate_pct, fund_util)

    return {
        "state_id": s.state_id,
        "state_name": s.state_name,
        "total_members": counts.get("total", 0),
        "mp_count": counts.get("mp", 0),
        "mla_count": counts.get("mla", 0),
        "active_members": s.active_members,
        "total_works": s.total_works,
        "recommended_works": s.recommended_works,
        "sanctioned_works": s.sanctioned_works,
        "completed_works": s.completed_works,
        "ongoing_works": s.ongoing_works,
        "pending_works": s.total_works - s.completed_works - s.ongoing_works,
        "completion_rate_pct": s.completion_rate_pct,
        "sanction_rate_pct": s.sanction_rate_pct,
        "allocated_amount": _safe_float(allocated_amount, 0),
        "recommended_amount": _safe_float(s.recommended_amount, 0),
        "sanctioned_amount": _safe_float(s.sanctioned_amount, 0),
        "expenditure_amount": _safe_float(s.expenditure_amount, 0),
        "completion_amount": _safe_float(s.completion_amount, 0),
        "unspent_amount": _safe_float(s.sanctioned_amount - s.expenditure_amount, 0),
        "fund_utilization_pct": fund_util,
        "expenditure_rate_pct": exp_rate,
        "avg_work_cost": _safe_float(getattr(s, "avg_work_cost", None)),
        "median_work_cost": _safe_float(getattr(s, "median_work_cost", None)),
        "avg_sanction_delay_days": s.avg_sanction_delay_days,
        "median_sanction_delay_days": s.median_sanction_delay_days,
        "avg_execution_days": s.avg_execution_days,
        "median_execution_days": s.median_execution_days,
        "avg_project_age_days": None,
        "overdue_over_1_year": s.overdue_over_1_year,
        "overdue_over_2_years": s.overdue_over_2_years,
        "flagged_works": s.flagged_works,
        "high_risk_works": s.high_risk_works,
        "risk_rate_pct": s.risk_rate_pct,
        "cost_anomaly_works": s.cost_anomaly_works,
        "duration_anomaly_works": s.duration_anomaly_works,
        "anomaly_score": anomaly.anomaly_score if anomaly else None,
        "anomaly_level": anomaly.anomaly_level if anomaly else "NORMAL",
        "confidence_level": anomaly.confidence_level if anomaly else "LOW",
        "performance_classification": perf_class,
        "performance_score": perf_score,
        "rank": None,
        "scale_score": None,
        "performance_score_weighted": None,
        "calculated_at": _now_iso(),
    }


def _classify_state_performance(s, anomaly=None):
    """Classify state PERFORMANCE (outcome quality).

    Phase 1 — the anomaly level does NOT overwrite the performance label.
    The `anomaly` argument is accepted for backward compatibility and
    intentionally ignored.
    """
    if not s.ranking_qualified:
        return "INSUFFICIENT_DATA"

    score = compute_performance_score(
        s.completion_rate_pct, s.expenditure_utilization_pct
    )
    return classify_performance_from_score(score)


# ============================================================
# NATIONAL STATISTICS
# ============================================================

def build_national_statistics(statistics_list):
    """Build national_statistics records.

    Args:
        statistics_list: list of NationalStatistics

    Returns:
        list of dicts for DB2 national_statistics table
    """
    records = []
    for s in statistics_list:
        scope = s.member_type if s.member_type else "BOTH"
        records.append({
            "metric_name": s.metric_name,
            "scope": scope,
            "sample_size": s.count,
            "mean": s.mean,
            "std_dev": s.std_dev,
            "minimum": s.minimum,
            "p25": s.p25,
            "median": s.median,
            "p75": s.p75,
            "p90": s.p90,
            "p95": s.p95,
            "maximum": s.maximum,
            "iqr": s.iqr,
            "calculated_at": _now_iso(),
        })
    return records


# ============================================================
# TRENDS
# ============================================================

def build_trends(trends_list):
    """Build trends records.

    Args:
        trends_list: list of TrendRecord

    Returns:
        list of dicts for DB2 trends table
    """
    records = []
    for t in trends_list:
        unspent = t.sanctioned_amount - t.expenditure_amount
        fund_util = round((t.expenditure_amount / t.sanctioned_amount * 100), 2) if t.sanctioned_amount > 0 else 0

        records.append({
            "year": t.year,
            "member_type": t.member_type,
            "is_partial_year": t.is_partial_year,
            "total_works": t.total_works,
            "recommended_works": t.recommended_works,
            "sanctioned_works": t.sanctioned_works,
            "completed_works": t.completed_works,
            "ongoing_works": t.ongoing_works,
            "pending_works": t.total_works - t.completed_works - t.ongoing_works,
            "recommended_amount": _safe_float(t.recommended_amount, 0),
            "sanctioned_amount": _safe_float(t.sanctioned_amount, 0),
            "expenditure_amount": _safe_float(t.expenditure_amount, 0),
            "completion_amount": _safe_float(t.completion_amount, 0),
            "completion_rate_pct": t.completion_rate_pct,
            "fund_utilization_pct": fund_util,
            "avg_sanction_delay_days": t.avg_sanction_delay_days,
            "avg_execution_days": t.avg_execution_days,
            "calculated_at": _now_iso(),
        })
    return records


def deduplicate_trends(trend_records):
    """Deduplicate trend records by (year, member_type), keeping the LAST occurrence.

    When trend records from pipeline.trends, pipeline.mp_trends, and
    pipeline.mla_trends are combined, duplicate (year, member_type) pairs
    can exist. This function removes duplicates deterministically.

    Args:
        trend_records: list of dicts with 'year' and 'member_type' keys

    Returns:
        tuple of (deduplicated_records, stats_dict)
    """
    stats = {"generated": len(trend_records), "duplicates_found": 0, "deduplicated": 0}

    if not trend_records:
        return [], stats

    seen = {}
    for i, r in enumerate(trend_records):
        key = (r["year"], r["member_type"])
        if key in seen:
            stats["duplicates_found"] += 1
        seen[key] = i

    stats["deduplicated"] = stats["duplicates_found"]

    deduplicated = []
    for i, r in enumerate(trend_records):
        key = (r["year"], r["member_type"])
        if seen[key] == i:
            deduplicated.append(r)

    return deduplicated, stats


# ============================================================
# RANKING
# ============================================================

def compute_member_ranks(member_records):
    """Compute member rank using a transparent weighted formula.

    Performance Score (drives rank):
        performance_score_weighted =
              0.40 * completion_rate_pct
            + 0.40 * fund_utilization_pct
            + 0.20 * scale_score

    scale_score is the percentile rank (0–100) of total_works across all
    ranking_qualified members, computed as:
        scale_score = (count_below + 0.5 * count_equal) / n * 100
    (midrank — standard convention for handling ties).

    Ranking only applies to ranking_qualified members (set by
    phase_a_member.py from total_works and active_members criteria).
    Non-qualified members receive rank=NULL.

    Raw metrics (completion_rate_pct, fund_utilization_pct, total_works,
    performance_score) are NEVER overwritten. New fields
    `scale_score` and `performance_score_weighted` are written alongside
    them so the ranking is fully explainable.

    Tie-handling: standard competition ranking — tied members receive
    the same rank; the next rank skips (1, 2, 2, 4). Deterministic via
    stable secondary sort on (member_id, member_type).
    """
    # Phase 2 — rank within member_type. Lok Sabha (MP) and Rajya Sabha (MLA,
    # the pipeline's internal name) are different representative bodies and
    # must not share a ranking population. Previously the populations were
    # mixed, so no MP could ever hold rank 1 and an MP's "national rank"
    # depended on Rajya Sabha scores.
    qualified = [r for r in member_records if r.get("ranking_qualified")]
    non_qualified = [r for r in member_records if not r.get("ranking_qualified")]

    for r in non_qualified:
        r["rank"] = None
        r["scale_score"] = None
        r["performance_score_weighted"] = None

    groups = {}
    for r in qualified:
        groups.setdefault(r.get("member_type") or "UNKNOWN", []).append(r)

    for group in groups.values():
        _rank_within_group(group, id_key="member_id")

    return member_records


def _rank_within_group(group, id_key):
    """Assign scale_score, performance_score_weighted and competition rank
    within a single population (one member_type, or all states).

    Invariant enforced by construction: if weighted(A) > weighted(B) then
    rank(A) is not worse than rank(B). Ties share a rank and the next rank
    skips (1, 2, 2, 4), so rank ordering is always consistent with the
    weighted score ordering.
    """
    n_q = len(group)
    if n_q == 0:
        return
    tw_sorted = sorted(_safe_int(r.get("total_works")) for r in group)
    for r in group:
        tw = _safe_int(r.get("total_works"))
        below = sum(1 for v in tw_sorted if v < tw)
        at = sum(1 for v in tw_sorted if v == tw)
        r["scale_score"] = round((below + 0.5 * at) / n_q * 100.0, 2)

    for r in group:
        comp = _safe_float(r.get("completion_rate_pct"), 0.0) or 0.0
        util = _safe_float(r.get("fund_utilization_pct"), 0.0) or 0.0
        scale = r.get("scale_score") or 0.0
        r["performance_score_weighted"] = round(
            comp * 0.40 + util * 0.40 + scale * 0.20, 2
        )

    sorted_q = sorted(
        group,
        key=lambda r: (-(r.get("performance_score_weighted") or 0), r.get(id_key) or 0),
    )
    last_score = None
    last_rank = 0
    for i, r in enumerate(sorted_q, 1):
        score = r.get("performance_score_weighted")
        if last_score is not None and score == last_score:
            r["rank"] = last_rank  # tie
        else:
            r["rank"] = i
            last_rank = i
            last_score = score


def compute_state_ranks(state_records):
    """Compute state rank using a transparent weighted formula.

    Performance Score (drives rank):
        performance_score_weighted =
              0.40 * completion_rate_pct
            + 0.40 * fund_utilization_pct
            + 0.20 * scale_score

    scale_score is the percentile rank (0–100) of total_works across all
    qualifying states, computed as:
        scale_score = (count_below + 0.5 * count_equal) / n * 100
    This handles ties via the standard "midrank" convention.

    Raw metrics (`completion_rate_pct`, `fund_utilization_pct`,
    `total_works`, `performance_score`) are NEVER overwritten. The new
    fields `scale_score` and `performance_score_weighted` are written
    alongside them so the ranking is fully explainable.

    Qualifying states are those with all three components available
    (non-NULL completion_rate_pct, fund_utilization_pct, and total_works).
    No thresholds, no size assumptions. A state with zero total_works is
    still qualifying but gets scale_score = 0.

    Tie-handling: standard "competition" ranking — tied states receive
    the same rank; the next rank skips (1, 2, 2, 4). Deterministic
    because the input sort is stable.
    """
    n_records = len(state_records)

    # --- Decide which records are qualifying (have all three components) ---
    def _has_all_three(r):
        return (
            r.get("completion_rate_pct") is not None
            and r.get("fund_utilization_pct") is not None
            and r.get("total_works") is not None
        )

    qualifying = [r for r in state_records if _has_all_three(r)]
    non_qualifying = [r for r in state_records if r not in qualifying]

    for r in non_qualifying:
        r["rank"] = None
        r["scale_score"] = None
        r["performance_score_weighted"] = None

    if qualifying:
        _rank_within_group(qualifying, id_key="state_id")

    return state_records
