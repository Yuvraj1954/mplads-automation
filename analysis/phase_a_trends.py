import numpy as np
from datetime import date
from collections import defaultdict
from analysis.models import TrendRecord


def compute_trends(work_analyses, member_type_filter=None,
                   reference_date=None):
    if reference_date is None:
        reference_date = date.today()

    by_year_type = defaultdict(list)

    for wa in work_analyses:
        if member_type_filter and wa.member_type != member_type_filter:
            continue
        year = _extract_year(wa)
        if year is None:
            continue

        by_year_type[(year, wa.member_type)].append(wa)

    results = []
    for (year, member_type), works in sorted(by_year_type.items()):
        t = _aggregate_trend(year, member_type, works, reference_date)
        results.append(t)

    return results


def _extract_year(wa):
    for attr in [
        "recommendation_date",
        "sanction_date",
        "last_expenditure_date",
        "completion_date",
    ]:
        val = getattr(wa, attr, None)
        if val is not None:
            return val.year
    return None


def _aggregate_trend(year, member_type, works, reference_date):
    t = TrendRecord(year=year, member_type=member_type)

    t.is_partial_year = (year == reference_date.year)

    t.total_works = len(works)

    statuses = [w.status for w in works]
    t.recommended_works = statuses.count("Recommended")
    t.sanctioned_works = sum(
        1 for w in works if w.sanction_date is not None
    )
    t.completed_works = statuses.count("Completed")
    t.ongoing_works = statuses.count("In Progress")

    t.recommended_amount = sum(w.recommended_amount or 0 for w in works)
    t.sanctioned_amount = sum(w.sanction_amount or 0 for w in works)
    t.expenditure_amount = sum(w.expenditure_amount or 0 for w in works)
    t.completion_amount = sum(w.completion_amount or 0 for w in works)

    delays = [w.sanction_delay_days for w in works
              if w.sanction_delay_days is not None]
    t.avg_sanction_delay_days = (
        round(float(np.mean(delays)), 2) if delays else None
    )

    execs = [w.execution_days for w in works
             if w.execution_days is not None]
    t.avg_execution_days = (
        round(float(np.mean(execs)), 2) if execs else None
    )

    t.completion_rate_pct = (
        round((t.completed_works / t.total_works) * 100, 2)
        if t.total_works > 0 else 0
    )

    return t
