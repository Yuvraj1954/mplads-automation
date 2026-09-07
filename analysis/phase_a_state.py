import numpy as np
from collections import defaultdict
from analysis.models import StateMetrics


def compute_state_metrics(work_analyses, member_metrics_by_id,
                          member_type_filter=None):
    by_state = defaultdict(list)

    for wa in work_analyses:
        if member_type_filter and wa.member_type != member_type_filter:
            continue
        sid = wa.state_id
        if sid is not None:
            by_state[sid].append(wa)

    results = []
    for state_id, works in by_state.items():
        m = _aggregate_state(state_id, works, member_metrics_by_id,
                             member_type_filter)
        results.append(m)

    return results


def _aggregate_state(state_id, works, member_metrics_by_id,
                     member_type_filter=None):
    m = StateMetrics(state_id=state_id, member_type=member_type_filter)

    m.total_works = len(works)

    members_in_state = set()
    for w in works:
        members_in_state.add((w.member_type, w.member_id))

    m.active_members = len(members_in_state)
    m.mp_active_members = sum(
        1 for mt, _ in members_in_state if mt == "MP"
    )
    m.mla_active_members = sum(
        1 for mt, _ in members_in_state if mt == "MLA"
    )

    statuses = [w.status for w in works]
    m.recommended_works = statuses.count("Recommended")
    m.sanctioned_works = sum(
        1 for w in works if w.sanction_date is not None
    )
    m.completed_works = statuses.count("Completed")
    m.ongoing_works = statuses.count("In Progress")

    m.recommended_amount = sum(w.recommended_amount or 0 for w in works)
    m.sanctioned_amount = sum(w.sanction_amount or 0 for w in works)
    m.expenditure_amount = sum(w.expenditure_amount or 0 for w in works)
    m.completion_amount = sum(w.completion_amount or 0 for w in works)

    delays = [w.sanction_delay_days for w in works
              if w.sanction_delay_days is not None]
    m.avg_sanction_delay_days = _mean(delays)
    m.median_sanction_delay_days = _median(delays)

    execs = [w.execution_days for w in works
             if w.execution_days is not None]
    m.avg_execution_days = _mean(execs)
    m.median_execution_days = _median(execs)

    m.flagged_works = sum(1 for w in works if w.flag_count >= 1)
    m.high_risk_works = sum(
        1 for w in works if w.risk_level == "HIGH"
    )

    m.overdue_over_1_year = sum(
        1 for w in works
        if w.project_age_days is not None
        and w.project_age_days > 365
        and w.status != "Completed"
    )
    m.overdue_over_2_years = sum(
        1 for w in works
        if w.project_age_days is not None
        and w.project_age_days > 730
        and w.status != "Completed"
    )

    m.completion_rate_pct = _pct(m.completed_works, m.total_works)
    m.sanction_rate_pct = _pct(m.sanctioned_works, m.total_works)

    if m.sanctioned_amount > 0:
        m.expenditure_utilization_pct = round(
            (m.expenditure_amount / m.sanctioned_amount) * 100, 2
        )
        m.sanction_conversion_pct = round(
            (m.completed_works / m.sanctioned_works * 100)
            if m.sanctioned_works > 0 else 0, 2
        )

    m.risk_rate_pct = _pct(m.flagged_works, m.total_works)

    m.cost_anomaly_works = sum(
        1 for w in works
        if w.cost_status in ("VERY_HIGH", "HIGH")
    )
    m.duration_anomaly_works = sum(
        1 for w in works
        if w.duration_status in ("VERY_LONG", "LONG")
    )
    m.expenditure_over_sanction_works = sum(
        1 for w in works
        if (w.expenditure_amount or 0) > (w.sanction_amount or 0)
        and (w.sanction_amount or 0) > 0
    )
    m.expenditure_over_recommendation_works = sum(
        1 for w in works
        if (w.expenditure_amount or 0) > (w.recommended_amount or 0)
        and (w.recommended_amount or 0) > 0
    )
    m.negative_execution_works = sum(
        1 for w in works
        if w.execution_days is not None
        and w.execution_days < 0
    )
    m.negative_sanction_delay_works = sum(
        1 for w in works
        if w.sanction_delay_days is not None
        and w.sanction_delay_days < 0
    )

    m.ranking_qualified = m.total_works >= 10 and m.active_members >= 2

    return m


def _mean(values):
    if not values:
        return None
    return round(float(np.mean(values)), 2)


def _median(values):
    if not values:
        return None
    return round(float(np.median(values)), 2)


def _pct(numerator, denominator):
    if denominator == 0:
        return 0
    return round((numerator / denominator) * 100, 2)
