#!/usr/bin/env python3
"""Backfill `rank` column in DB2 member_metrics and state_metrics.

Member ranks: raw completion% + utilization% (only ranking_qualified members).
State ranks: statistical lower-bound CI on completion% + utilization%
  (Wilson score for completion, normal-approx for utilization).
  Sample size is accounted for via the math itself — no hard-coded
  size thresholds, no work-count bonuses. Workload/scale is shown
  separately on the frontend.

Usage:
    python automation/backfill_ranks.py
"""

import math
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
from supabase import create_client

load_dotenv(override=False)

DB2_URL = os.environ.get("DB2_URL") or os.environ.get("SUPABASE_URL")
DB2_KEY = os.environ.get("DB2_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not DB2_URL or not DB2_KEY:
    print("ERROR: DB2_URL and DB2_SERVICE_ROLE_KEY (or SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY) must be set", file=sys.stderr)
    sys.exit(1)

BATCH = 1000

# z = 1.96 → 95% confidence
_Z_95 = 1.96


def fetch_all(client, table: str, columns: str = "*"):
    """Fetch all rows from a table with pagination."""
    rows = []
    offset = 0
    while True:
        result = client.table(table).select(columns).range(offset, offset + BATCH - 1).execute()
        if not result.data:
            break
        rows.extend(result.data)
        if len(result.data) < BATCH:
            break
        offset += BATCH
    return rows


def perf_score(row):
    comp = row.get("completion_rate_pct") or 0
    util = row.get("fund_utilization_pct") or 0
    try:
        return float(comp) + float(util)
    except (TypeError, ValueError):
        return 0.0


def wilson_lower_bound(successes, total, z=_Z_95):
    """Lower bound of the Wilson score interval for a binomial proportion."""
    if total <= 0:
        return 0.0
    p = successes / total
    denom = 1 + z * z / total
    center = (p + z * z / (2 * total)) / denom
    spread = z * math.sqrt(
        p * (1 - p) / total + z * z / (4 * total * total)
    ) / denom
    return max(0.0, center - spread)


def normal_lower_bound_pct(p_pct, n, z=_Z_95):
    """Lower bound of normal-approx CI for a proportion (percent)."""
    if n <= 0:
        return 0.0
    p = max(0.0, min(1.0, p_pct / 100.0))
    se = math.sqrt(p * (1 - p) / n)
    return max(0.0, p_pct - z * se * 100)


def compute_ranks(records):
    """Member ranks: raw completion + utilization, only for ranking_qualified."""
    qualified = [r for r in records if r.get("ranking_qualified")]
    qualified.sort(key=lambda r: -perf_score(r))
    ranks = {}
    for i, r in enumerate(qualified, 1):
        key = (r.get("member_id"), r.get("member_type"))
        ranks[key] = i
    return ranks


def compute_state_ranks(records):
    """State ranks via statistical lower-bound CI (Wilson + normal approx).

    Must mirror analysis/db2_analytics_persistence.compute_state_ranks:
        rank_score = lower_bound_95CI(completion%) + lower_bound_95CI(utilization%)
    Sample size is accounted for via the math itself. No hard-coded size
    thresholds. No work-count bonus. Workload/scale stays separate.
    """
    def _rank_score(r):
        comp_lb = wilson_lower_bound(
            int(r.get("completed_works") or 0),
            int(r.get("total_works") or 0),
        ) * 100.0
        util_pct = float(r.get("fund_utilization_pct") or 0)
        util_lb = normal_lower_bound_pct(
            util_pct,
            int(r.get("sanctioned_works") or 0),
        )
        return comp_lb + util_lb

    sorted_records = sorted(records, key=_rank_score, reverse=True)
    ranks = {}
    for i, r in enumerate(sorted_records, 1):
        ranks[r.get("state_id")] = i
    return ranks


def update_ranks(client, table: str, records, ranks_by_key, key_fields):
    """Update the `rank` column for each record."""
    updated = 0
    batch_size = 200
    batch = []
    for r in records:
        if len(key_fields) == 2:
            key = (r.get(key_fields[0]), r.get(key_fields[1]))
            rank = ranks_by_key.get(key)
        else:
            key = r.get(key_fields[0])
            rank = ranks_by_key.get(key)
        update_row = {key_fields[0]: r[key_fields[0]]}
        if len(key_fields) == 2:
            update_row[key_fields[1]] = r[key_fields[1]]
        update_row["rank"] = rank

        batch.append(update_row)
        if len(batch) >= batch_size:
            client.table(table).upsert(batch, on_conflict=",".join(key_fields)).execute()
            updated += len(batch)
            batch = []

    if batch:
        client.table(table).upsert(batch, on_conflict=",".join(key_fields)).execute()
        updated += len(batch)
    return updated


def main():
    print("=" * 70)
    print("BACKFILL RANKS — DB2 member_metrics + state_metrics")
    print("=" * 70)

    client = create_client(DB2_URL, DB2_KEY)

    # --- Member metrics ---
    print("\n[1/2] Fetching member_metrics...")
    members = fetch_all(
        client,
        "member_metrics",
        "member_id, member_type, completion_rate_pct, fund_utilization_pct, ranking_qualified",
    )
    print(f"  Fetched {len(members)} member rows")
    member_ranks = compute_ranks(members)
    print(f"  Computed {len(member_ranks)} member ranks (ranking_qualified=True)")

    print("\n[2/2] Updating member_metrics.rank...")
    updated = update_ranks(
        client,
        "member_metrics",
        members,
        member_ranks,
        key_fields=["member_id", "member_type"],
    )
    print(f"  Updated {updated} member rows")

    # --- State metrics ---
    print("\n[3/4] Fetching state_metrics...")
    states = fetch_all(
        client,
        "state_metrics",
        "state_id, total_works, completed_works, sanctioned_works, fund_utilization_pct",
    )
    print(f"  Fetched {len(states)} state rows")
    state_ranks = compute_state_ranks(states)
    print(f"  Computed {len(state_ranks)} state ranks (all 36 states/UTs)")

    print("\n[4/4] Updating state_metrics.rank...")
    updated = update_ranks(
        client,
        "state_metrics",
        states,
        state_ranks,
        key_fields=["state_id"],
    )
    print(f"  Updated {updated} state rows")

    print("\n" + "=" * 70)
    print("BACKFILL COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
