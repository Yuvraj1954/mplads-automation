import numpy as np
from collections import defaultdict


def percentile_cont(data, percent):
    if len(data) == 0:
        return None

    sorted_data = np.sort(data)
    n = len(sorted_data)

    if n == 1:
        return float(sorted_data[0])

    rank = (percent / 100) * (n - 1)
    lower = int(np.floor(rank))
    upper = int(np.ceil(rank))
    fraction = rank - lower

    if lower == upper:
        return float(sorted_data[lower])

    return float(
        sorted_data[lower] * (1 - fraction)
        + sorted_data[upper] * fraction
    )


def compute_percentiles(values):
    if not values:
        return {}

    arr = np.array(values, dtype=float)
    arr = arr[~np.isnan(arr)]

    if len(arr) == 0:
        return {}

    return {
        "p25": percentile_cont(arr, 25),
        "p50": percentile_cont(arr, 50),
        "p75": percentile_cont(arr, 75),
        "p90": percentile_cont(arr, 90),
        "p95": percentile_cont(arr, 95),
    }


def build_benchmark_groups(works, member_type):
    state_groups = defaultdict(lambda: {
        "sanction_amounts": [],
        "durations": [],
        "works": [],
    })
    national_groups = defaultdict(lambda: {
        "sanction_amounts": [],
        "durations": [],
        "works": [],
    })

    for w in works:
        if (w.get("sanction_amount") or 0) <= 0:
            continue
        if w.get("sanction_date") is None:
            continue

        norm = w.get("normalized_activity")
        state = w.get("state_id")
        san = w["sanction_amount"]

        dur = None
        if (w.get("completion_date") is not None
                and w["completion_date"] >= w["sanction_date"]):
            dur = (w["completion_date"] - w["sanction_date"]).days

        if norm and state is not None:
            key = f"{norm}|{state}"
            state_groups[key]["sanction_amounts"].append(san)
            if dur is not None:
                state_groups[key]["durations"].append(dur)
            state_groups[key]["works"].append(w)

        if norm:
            national_groups[norm]["sanction_amounts"].append(san)
            if dur is not None:
                national_groups[norm]["durations"].append(dur)
            national_groups[norm]["works"].append(w)

    return dict(state_groups), dict(national_groups)


def select_benchmark(normalized_activity, state_id,
                     state_groups, national_groups):
    state_key = f"{normalized_activity}|{state_id}" if state_id is not None else None

    state_data = state_groups.get(state_key) if state_key else None
    national_data = national_groups.get(normalized_activity)

    state_sample = len(state_data["sanction_amounts"]) if state_data else 0
    national_sample = len(national_data["sanction_amounts"]) if national_data else 0

    if state_sample >= 20:
        quality = "ACTIVITY_STATE_STRONG"
        peer = "ACTIVITY + STATE"
        sample = state_sample
    elif state_sample >= 10:
        quality = "ACTIVITY_STATE_GOOD"
        peer = "ACTIVITY + STATE"
        sample = state_sample
    elif state_sample >= 5:
        quality = "ACTIVITY_STATE_LIMITED"
        peer = "ACTIVITY + STATE"
        sample = state_sample
    elif national_sample >= 20:
        quality = "ACTIVITY_NATIONAL_STRONG"
        peer = "ACTIVITY NATIONAL"
        sample = national_sample
    elif national_sample >= 10:
        quality = "ACTIVITY_NATIONAL_GOOD"
        peer = "ACTIVITY NATIONAL"
        sample = national_sample
    elif national_sample >= 5:
        quality = "ACTIVITY_NATIONAL_LIMITED"
        peer = "ACTIVITY NATIONAL"
        sample = national_sample
    else:
        quality = "INSUFFICIENT"
        peer = "NONE"
        sample = 0

    if state_sample >= 5:
        cost_pcts = compute_percentiles(state_data["sanction_amounts"])
        dur_pcts = compute_percentiles(state_data["durations"])
    elif national_sample >= 5:
        cost_pcts = compute_percentiles(national_data["sanction_amounts"])
        dur_pcts = compute_percentiles(national_data["durations"])
    else:
        cost_pcts = {}
        dur_pcts = {}

    return {
        "benchmark_quality": quality,
        "benchmark_peer_group": peer,
        "benchmark_sample_size": sample,
        "cost_p25": cost_pcts.get("p25"),
        "cost_p50": cost_pcts.get("p50"),
        "cost_p75": cost_pcts.get("p75"),
        "cost_p90": cost_pcts.get("p90"),
        "cost_p95": cost_pcts.get("p95"),
        "duration_p25": dur_pcts.get("p25"),
        "duration_p50": dur_pcts.get("p50"),
        "duration_p75": dur_pcts.get("p75"),
        "duration_p90": dur_pcts.get("p90"),
        "duration_p95": dur_pcts.get("p95"),
    }


def classify_percentile(value, percentiles):
    if value is None or not percentiles:
        return None

    p25 = percentiles.get("p25")
    p50 = percentiles.get("p50")
    p75 = percentiles.get("p75")
    p90 = percentiles.get("p90")
    p95 = percentiles.get("p95")

    thresholds = [
        (p25, 10),
        (p50, 25),
        (p75, 50),
        (p90, 75),
        (p95, 90),
    ]

    for pval, score in thresholds:
        if pval is not None and value <= pval:
            return score

    return 97.5


def classify_cost_status(cost_percentile, benchmark_sample_size, status):
    if benchmark_sample_size < 5:
        return "NO_RELIABLE_BENCHMARK"

    if status != "Completed":
        return "NOT_APPLICABLE"

    if cost_percentile is None:
        return "NOT_APPLICABLE"

    if cost_percentile >= 95:
        return "VERY_HIGH"
    elif cost_percentile >= 90:
        return "HIGH"
    elif cost_percentile >= 75:
        return "ABOVE_NORMAL"
    elif cost_percentile >= 25:
        return "NORMAL"
    elif cost_percentile >= 10:
        return "BELOW_NORMAL"
    else:
        return "VERY_LOW"


def classify_duration_status(duration_percentile, benchmark_sample_size,
                             status):
    if benchmark_sample_size < 5:
        return "NO_RELIABLE_BENCHMARK"

    if status != "Completed":
        return "NOT_APPLICABLE"

    if duration_percentile is None:
        return "NOT_APPLICABLE"

    if duration_percentile >= 95:
        return "VERY_LONG"
    elif duration_percentile >= 90:
        return "LONG"
    elif duration_percentile >= 75:
        return "ABOVE_NORMAL"
    elif duration_percentile >= 25:
        return "NORMAL"
    elif duration_percentile >= 10:
        return "BELOW_NORMAL"
    else:
        return "VERY_SHORT"


def compute_median_deviation(value, median_value):
    if value is None or median_value is None or median_value == 0:
        return None
    return round(((value - median_value) / median_value) * 100, 2)


def compute_benchmarks_for_group(works, member_type):
    state_groups, national_groups = build_benchmark_groups(
        works, member_type
    )

    results = {}
    for w in works:
        wid = w["work_id"]
        norm = w.get("normalized_activity")
        state = w.get("state_id")
        status = w.get("status")

        bench = select_benchmark(
            norm, state, state_groups, national_groups
        )

        san = w.get("sanction_amount")
        cost_pcts = {
            "p25": bench["cost_p25"],
            "p50": bench["cost_p50"],
            "p75": bench["cost_p75"],
            "p90": bench["cost_p90"],
            "p95": bench["cost_p95"],
        }
        cost_pct = classify_percentile(san, cost_pcts)
        cost_status = classify_cost_status(
            cost_pct, bench["benchmark_sample_size"], status
        )

        dur = None
        if (w.get("sanction_date") is not None
                and w.get("completion_date") is not None
                and w["completion_date"] >= w["sanction_date"]):
            dur = (w["completion_date"] - w["sanction_date"]).days

        dur_pcts = {
            "p25": bench["duration_p25"],
            "p50": bench["duration_p50"],
            "p75": bench["duration_p75"],
            "p90": bench["duration_p90"],
            "p95": bench["duration_p95"],
        }
        dur_pct = classify_percentile(dur, dur_pcts)
        dur_status = classify_duration_status(
            dur_pct, bench["benchmark_sample_size"], status
        )

        cost_dev = compute_median_deviation(san, bench["cost_p50"])
        dur_dev = compute_median_deviation(dur, bench["duration_p50"])

        results[wid] = {
            **bench,
            "cost_percentile": cost_pct,
            "cost_status": cost_status,
            "cost_deviation_from_median_percentage": cost_dev,
            "duration_percentile": dur_pct,
            "duration_status": dur_status,
            "duration_deviation_from_median_percentage": dur_dev,
        }

    return results
