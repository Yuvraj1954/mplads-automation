from collections import defaultdict
from datetime import date


def expand_affected_works(delta_work_ids, works_by_id,
                          existing_analyses, reference_date):
    affected = set(delta_work_ids)

    works_list = []
    for wid in affected:
        w = works_by_id.get(wid)
        if w:
            works_list.append(w)

    benchmark_groups = defaultdict(set)
    for w in works_list:
        norm = w.get("normalized_activity")
        state = w.get("state_id")
        if norm and state is not None:
            key = (norm, state)
            benchmark_groups[key].add("state")
        if norm:
            benchmark_groups[(norm, None)].add("national")

    for (norm, state), scope_set in benchmark_groups.items():
        for w in works_by_id.values():
            if w.get("normalized_activity") != norm:
                continue
            if "state" in scope_set and w.get("state_id") == state:
                affected.add(w["work_id"])
            elif "national" in scope_set and state is None:
                affected.add(w["work_id"])

    affected_members = set()
    for wid in affected:
        w = works_by_id.get(wid)
        if w:
            affected_members.add((w.get("member_type"), w.get("member_id")))

    return affected, affected_members


def expand_time_sensitive(works_by_id, existing_analyses, reference_date,
                         affected_members=None):
    """Identify works whose status may change soon or need fresh analysis.

    Uses canonical date fields from raw work records (which lack a 'status'
    field). The logic mirrors classify_status() in analysis/status.py:
      - IN_PROGRESS: has sanction_date but no completion_date
      - RECOMMENDED: has recommendation_date but no sanction/completion
      - COMPLETED: has completion_date (still time-sensitive if recent)

    When affected_members is provided, only works belonging to those members
    are considered. This prevents unrelated members from being pulled in.
    """
    time_sensitive = set()

    for wid, w in works_by_id.items():
        if affected_members is not None:
            member_key = (w.get("member_type"), w.get("member_id"))
            if member_key not in affected_members:
                continue

        sanction_date = w.get("sanction_date")
        completion_date = w.get("completion_date")
        recommendation_date = w.get("recommendation_date")

        # In Progress: sanctioned but not completed
        if sanction_date is not None and completion_date is None:
            time_sensitive.add(wid)

        # Recommended: only recommended, no sanction yet
        if recommendation_date is not None and sanction_date is None and completion_date is None:
            time_sensitive.add(wid)

        # Any work with recent expenditure activity
        if w.get("last_expenditure_date") is not None:
            time_sensitive.add(wid)

    return time_sensitive


def compute_sanction_delay_global_distribution(works_by_id):
    valid_delays = []
    for w in works_by_id.values():
        d = w.get("sanction_delay_days")
        if d is not None and d >= 0:
            valid_delays.append(d)

    if not valid_delays:
        return {"delay_p75": None, "delay_p90": None, "delay_p95": None}

    import numpy as np
    arr = np.array(valid_delays, dtype=float)
    n = len(arr)

    def pct(data, percent):
        sorted_d = np.sort(data)
        rank = (percent / 100) * (len(sorted_d) - 1)
        lower = int(np.floor(rank))
        upper = int(np.ceil(rank))
        frac = rank - lower
        if lower == upper:
            return float(sorted_d[lower])
        return float(sorted_d[lower] * (1 - frac) + sorted_d[upper] * frac)

    return {
        "delay_p75": pct(arr, 75),
        "delay_p90": pct(arr, 90),
        "delay_p95": pct(arr, 95),
    }
