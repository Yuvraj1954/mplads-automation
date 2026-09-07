import numpy as np
from collections import defaultdict
from analysis.models import MemberMetrics

MIN_RANKING_WORKS = 5


def compute_member_metrics(work_analyses, member_type_filter=None):
    by_member = defaultdict(list)

    for wa in work_analyses:
        mt = wa.member_type
        mid = wa.member_id
        if member_type_filter and mt != member_type_filter:
            continue
        by_member[(mt, mid)].append(wa)

    results = []
    for (mt, mid), works in by_member.items():
        m = _aggregate_member(mid, mt, works)
        results.append(m)

    return results


def _aggregate_member(member_id, member_type, works):
    m = MemberMetrics(
        member_id=member_id,
        member_type=member_type,
    )

    states = [w.state_id for w in works if w.state_id is not None]
    if states:
        m.state_id = states[0]

    m.total_works = len(works)

    statuses = [w.status for w in works]
    m.recommended_works = statuses.count("Recommended")
    m.sanctioned_works = sum(
        1 for w in works
        if w.sanction_date is not None
    )
    m.completed_works = statuses.count("Completed")
    m.ongoing_works = statuses.count("In Progress")
    m.pending_works = m.total_works - m.completed_works - m.ongoing_works

    m.recommended_amount = sum(w.recommended_amount or 0 for w in works)
    m.sanctioned_amount = sum(w.sanction_amount or 0 for w in works)
    m.expenditure_amount = sum(w.expenditure_amount or 0 for w in works)
    m.completion_amount = sum(w.completion_amount or 0 for w in works)
    m.unspent_amount = m.sanctioned_amount - m.expenditure_amount

    delays = [w.sanction_delay_days for w in works
              if w.sanction_delay_days is not None]
    m.avg_sanction_delay_days = _mean(delays)
    m.median_sanction_delay_days = _median(delays)

    execs = [w.execution_days for w in works
             if w.execution_days is not None]
    m.avg_execution_days = _mean(execs)
    m.median_execution_days = _median(execs)

    ages = [w.project_age_days for w in works
            if w.project_age_days is not None]
    m.avg_project_age_days = _mean(ages)
    m.max_project_age_days = max(ages) if ages else None

    pends = [w.pending_days for w in works
             if w.pending_days is not None]
    m.avg_pending_days = _mean(pends)
    m.max_pending_days = max(pends) if pends else None

    m.flagged_works = sum(1 for w in works if w.flag_count >= 1)
    m.medium_risk_works = sum(
        1 for w in works if w.risk_level == "MEDIUM"
    )
    m.high_risk_works = sum(
        1 for w in works if w.risk_level == "HIGH"
    )
    m.flagged_rate_pct = _pct(m.flagged_works, m.total_works)
    m.high_risk_rate_pct = _pct(m.high_risk_works, m.total_works)

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

    m.negative_sanction_delay_works = sum(
        1 for w in works
        if w.sanction_delay_days is not None
        and w.sanction_delay_days < 0
    )
    m.negative_execution_works = sum(
        1 for w in works
        if w.execution_days is not None
        and w.execution_days < 0
    )
    m.completion_before_sanction_works = sum(
        1 for w in works
        if (w.completion_date is not None
            and w.sanction_date is not None
            and w.completion_date < w.sanction_date)
    )
    m.expenditure_before_sanction_works = sum(
        1 for w in works
        if (w.last_expenditure_date is not None
            and w.sanction_date is not None
            and w.last_expenditure_date < w.sanction_date)
    )

    overdue_365 = sum(
        1 for w in works
        if w.project_age_days is not None
        and w.project_age_days > 365
        and w.status != "Completed"
    )
    m.overdue_over_1_year = overdue_365
    m.overdue_over_2_years = sum(
        1 for w in works
        if w.project_age_days is not None
        and w.project_age_days > 730
        and w.status != "Completed"
    )

    cost_pcts = [w.cost_percentile for w in works
                 if w.cost_percentile is not None]
    m.avg_cost_percentile = _mean(cost_pcts)

    dur_pcts = [w.duration_percentile for w in works
                if w.duration_percentile is not None]
    m.avg_duration_percentile = _mean(dur_pcts)

    cost_devs = [w.cost_deviation_from_median_percentage for w in works
                 if w.cost_deviation_from_median_percentage is not None]
    m.avg_cost_deviation_pct = _mean(cost_devs)

    dur_devs = [w.duration_deviation_from_median_percentage for w in works
                if w.duration_deviation_from_median_percentage is not None]
    m.avg_duration_deviation_pct = _mean(dur_devs)

    m.completion_rate_pct = _pct(m.completed_works, m.total_works)
    m.sanction_rate_pct = _pct(m.sanctioned_works, m.total_works)

    if m.sanctioned_amount > 0:
        m.sanction_conversion_pct = round(
            (m.completed_works / m.sanctioned_works * 100)
            if m.sanctioned_works > 0 else 0, 2
        )
        m.expenditure_sanction_utilization_pct = round(
            (m.expenditure_amount / m.sanctioned_amount) * 100, 2
        )

    if m.recommended_amount > 0:
        m.expenditure_recommendation_pct = round(
            (m.expenditure_amount / m.recommended_amount) * 100, 2
        )

    m.zero_work_member = m.total_works == 0
    m.low_sample_member = m.total_works < MIN_RANKING_WORKS
    m.ranking_qualified = m.total_works >= MIN_RANKING_WORKS

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
