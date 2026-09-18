from datetime import date, datetime, timezone

from analysis.normalize import normalize_activity
from analysis.status import classify_status
from analysis.lifecycle import compute_lifecycle
from analysis.financial import compute_financial
from analysis.benchmarks import (
    compute_benchmarks_for_group,
)
from analysis.risk import (
    compute_risk_flags,
    classify_risk_level,
    describe_risk_flags,
    compute_positive_signals,
)
from analysis.work_profile import (
    compute_days_since_last_expenditure,
    compute_financial_profile,
    compute_timeline_profile,
    compute_payment_activity_profile,
)
from analysis.affected import (
    expand_affected_works,
    expand_time_sensitive,
    compute_sanction_delay_global_distribution,
)
from analysis.models import WorkAnalysis


def analyze_works(works, reference_date=None):
    if reference_date is None:
        reference_date = date.today()

    works_by_id = {}
    for w in works:
        wid = w["work_id"]
        works_by_id[wid] = {
            **w,
            "_reference_date": reference_date,
        }

    for wid, w in works_by_id.items():
        w["normalized_activity"] = normalize_activity(
            w.get("activity_name")
        )
        w["status"] = classify_status(
            w.get("recommendation_date"),
            w.get("sanction_date"),
            w.get("completion_date"),
        )

        lifecycle = compute_lifecycle(
            w.get("recommendation_date"),
            w.get("sanction_date"),
            w.get("completion_date"),
            w["status"],
            reference_date,
        )
        w.update(lifecycle)

        financial = compute_financial(
            w.get("recommended_amount"),
            w.get("sanction_amount"),
            w.get("expenditure_amount"),
            w.get("completion_amount"),
        )
        w.update(financial)

    delay_dist = compute_sanction_delay_global_distribution(works_by_id)
    duration_p90 = _compute_global_duration_p90(works_by_id)

    mp_works_list = list(works_by_id.values())
    benchmarks = compute_benchmarks_for_group(mp_works_list, "MP")

    mla_works_list = [w for w in works_by_id.values()
                      if w.get("member_type") == "MLA"]
    mla_benchmarks = compute_benchmarks_for_group(mla_works_list, "MLA")
    benchmarks.update(mla_benchmarks)

    items = list(works_by_id.items())

    def _apply_risk(item):
        wid, w = item
        bench = benchmarks.get(wid, {})
        w.update(bench)
        flags = compute_risk_flags(w, delay_dist, duration_p90)
        w["risk_flags"] = flags
        w["flag_count"] = len(flags)
        w["risk_level"] = classify_risk_level(flags)
        w["last_calculated"] = datetime.now(timezone.utc)

    from concurrent.futures import ThreadPoolExecutor
    if len(items) < 5000:
        for item in items:
            _apply_risk(item)
    else:
        num_workers = min(8, max(1, len(items) // 10000 + 1))
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            executor.map(_apply_risk, items)

    return works_by_id


def _compute_global_duration_p90(works_by_id):
    durations = []
    for w in works_by_id.values():
        d = w.get("execution_days")
        if d is not None:
            durations.append(d)

    if not durations:
        return None

    import numpy as np
    arr = np.array(durations, dtype=float)
    sorted_arr = np.sort(arr)
    rank = 0.90 * (len(sorted_arr) - 1)
    lower = int(np.floor(rank))
    upper = int(np.ceil(rank))
    frac = rank - lower
    if lower == upper:
        return float(sorted_arr[lower])
    return float(sorted_arr[lower] * (1 - frac) + sorted_arr[upper] * frac)


def compute_work_analyses(works, reference_date=None):
    import time as _t
    t0 = _t.time()
    analyzed = analyze_works(works, reference_date)
    print(f"      [work_analysis] analyze_works: {_t.time()-t0:.1f}s", flush=True)

    t1 = _t.time()
    items = list(analyzed.items())

    def _build_work_analysis(item):
        wid, w = item
        days_since_exp = compute_days_since_last_expenditure(
            w.get("last_expenditure_date"),
            w.get("_reference_date"),
        )
        fin_profile = compute_financial_profile(
            w.get("expenditure_percentage"),
            w.get("status"),
        )
        timeline_prof = compute_timeline_profile(
            w.get("status"),
            w.get("pending_days"),
            w.get("duration_p90"),
            w.get("sanction_delay_days"),
        )
        pay_profile = compute_payment_activity_profile(
            w.get("last_expenditure_date"),
            w.get("_reference_date"),
            w.get("status"),
        )
        return WorkAnalysis(
            work_id=w["work_id"],
            member_type=w.get("member_type", ""),
            member_id=w.get("member_id", 0),
            status=w.get("status", "Unknown"),
            normalized_activity=w.get("normalized_activity"),
            state_id=w.get("state_id"),
            constituency_id=w.get("constituency_id"),
            recommendation_date=w.get("recommendation_date"),
            sanction_date=w.get("sanction_date"),
            completion_date=w.get("completion_date"),
            last_expenditure_date=w.get("last_expenditure_date"),
            recommended_amount=w.get("recommended_amount"),
            sanction_amount=w.get("sanction_amount"),
            expenditure_amount=w.get("expenditure_amount"),
            completion_amount=w.get("completion_amount"),
            sanction_delay_days=w.get("sanction_delay_days"),
            project_age_days=w.get("project_age_days"),
            execution_days=w.get("execution_days"),
            pending_days=w.get("pending_days"),
            expenditure_percentage=w.get("expenditure_percentage"),
            completion_percentage=w.get("completion_percentage"),
            benchmark_quality=w.get("benchmark_quality"),
            benchmark_peer_group=w.get("benchmark_peer_group"),
            benchmark_sample_size=w.get("benchmark_sample_size", 0),
            cost_p25=w.get("cost_p25"),
            cost_p50=w.get("cost_p50"),
            cost_p75=w.get("cost_p75"),
            cost_p90=w.get("cost_p90"),
            cost_p95=w.get("cost_p95"),
            cost_percentile=w.get("cost_percentile"),
            cost_status=w.get("cost_status"),
            cost_deviation_from_median_percentage=w.get(
                "cost_deviation_from_median_percentage"
            ),
            duration_p25=w.get("duration_p25"),
            duration_p50=w.get("duration_p50"),
            duration_p75=w.get("duration_p75"),
            duration_p90=w.get("duration_p90"),
            duration_p95=w.get("duration_p95"),
            duration_percentile=w.get("duration_percentile"),
            duration_status=w.get("duration_status"),
            duration_deviation_from_median_percentage=w.get(
                "duration_deviation_from_median_percentage"
            ),
            risk_flags=w.get("risk_flags", []),
            flag_count=w.get("flag_count", 0),
            risk_level=w.get("risk_level", "NORMAL"),
            last_calculated=w.get("last_calculated"),
            days_since_last_expenditure=days_since_exp,
            financial_profile=fin_profile,
            timeline_profile=timeline_prof,
            payment_activity_profile=pay_profile,
            risk_descriptions=describe_risk_flags(
                w.get("risk_flags", []), w
            ),
            positive_signals=compute_positive_signals(w),
        )

    from concurrent.futures import ThreadPoolExecutor
    num_workers = min(8, max(1, len(items) // 10000 + 1))
    if len(items) < 5000:
        results = [_build_work_analysis(item) for item in items]
    else:
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            results = list(executor.map(_build_work_analysis, items))
    print(f"      [work_analysis] build WorkAnalysis: {_t.time()-t1:.1f}s ({num_workers} workers)", flush=True)

    return results
