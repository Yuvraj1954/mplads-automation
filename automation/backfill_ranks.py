#!/usr/bin/env python3
"""Backfill `rank` column in DB2 member_metrics and state_metrics.

Member ranks:
    raw completion% + utilization%, only ranking_qualified members.

State ranks:
    Empirical-Bayes shrinkage with proper denominators:
        completion%   uses total_works      as the sample-size measure
        utilization%  uses sanctioned_works as the sample-size measure
    K is derived from the data via MOM estimator (Morris, 1983). K adapts
    to the actual data — no hard-coded thresholds.

    Only the `rank` column is updated. Raw metrics
    (completion_rate_pct, fund_utilization_pct, performance_score) are
    preserved untouched, so PERFORMANCE / EVIDENCE / SCALE remain visible
    on every row through the existing columns:
        PERFORMANCE: completion_rate_pct + fund_utilization_pct
        EVIDENCE:    total_works (completion denom) + sanctioned_works (util denom)
        SCALE:       active_members + sanctioned_amount + expenditure_amount

Usage:
    python automation/backfill_ranks.py
"""

import os
import sys
import statistics
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


def empirical_bayes_k(observed_rates, sample_sizes):
    """Method-of-moments estimator for the prior pseudo-count K."""
    if not observed_rates:
        return 5
    p_bar = statistics.fmean(observed_rates)
    if not (0.0 < p_bar < 1.0):
        return 5
    sample_var = statistics.pvariance(observed_rates)
    expected_within = p_bar * (1.0 - p_bar)
    between = sample_var - expected_within * statistics.fmean(
        1.0 / max(n, 1) for n in sample_sizes
    )
    if between <= 0:
        return 5
    k = expected_within / between
    if k <= 0:
        return 5
    return int(round(min(k, 100)))


def compute_ranks(records):
    """Member ranks: raw completion + utilization, only for ranking_qualified."""
    qualified = [r for r in records if r.get("ranking_qualified")]
    qualified.sort(key=lambda r: -perf_score(r))
    ranks = {}
    for i, r in enumerate(qualified, 1):
        key = (r.get("member_id"), r.get("member_type"))
        ranks[key] = i
    return ranks


def compute_state_ranks_eb(records):
    """State ranks via empirical-Bayes shrinkage.

    Mirrors analysis/db2_analytics_persistence.compute_state_ranks.
    Returns dict state_id -> rank (or None for zero-evidence states).
    """
    comp_rates, comp_n = [], []
    util_rates, util_n = [], []
    for r in records:
        c = r.get("completion_rate_pct")
        u = r.get("fund_utilization_pct")
        if c is not None:
            comp_rates.append(c / 100.0)
            comp_n.append(max(1, int(r.get("total_works") or 0)))
        if u is not None:
            util_rates.append(u / 100.0)
            util_n.append(max(1, int(r.get("sanctioned_works") or 0)))

    comp_prior = statistics.fmean(comp_rates) if comp_rates else 0.0
    util_prior = statistics.fmean(util_rates) if util_rates else 0.0
    K_comp = empirical_bayes_k(comp_rates, comp_n)
    K_util = empirical_bayes_k(util_rates, util_n)

    enriched = []
    for r in records:
        n_c = int(r.get("total_works") or 0)
        n_u = int(r.get("sanctioned_works") or 0)
        c_raw = float(r.get("completion_rate_pct") or 0)
        u_raw = float(r.get("fund_utilization_pct") or 0)
        comp_adj = ((c_raw * n_c + comp_prior * 100.0 * K_comp) / (n_c + K_comp)) if n_c > 0 else comp_prior * 100.0
        util_adj = ((u_raw * n_u + util_prior * 100.0 * K_util) / (n_u + K_util)) if n_u > 0 else util_prior * 100.0
        eligible = n_c > 0 or n_u > 0
        enriched.append((r["state_id"], comp_adj + util_adj, eligible))

    eligible = sorted(
        [(sid, score) for sid, score, ok in enriched if ok],
        key=lambda x: x[1], reverse=True,
    )
    ranks = {}
    for i, (sid, _) in enumerate(eligible, 1):
        ranks[sid] = i
    for sid, _, ok in enriched:
        if not ok:
            ranks[sid] = None
    return ranks


def update_ranks(client, table, records, ranks_by_key, key_fields):
    updated = 0
    batch_size = 200
    batch = []
    for r in records:
        if len(key_fields) == 2:
            key = (r.get(key_fields[0]), r.get(key_fields[1]))
            rank = ranks_by_key.get(key)
            update_row = {
                key_fields[0]: r[key_fields[0]],
                key_fields[1]: r[key_fields[1]],
                "rank": rank,
            }
        else:
            key = r.get(key_fields[0])
            rank = ranks_by_key.get(key)
            update_row = {key_fields[0]: r[key_fields[0]], "rank": rank}
        batch.append(update_row)
        if len(batch) >= batch_size:
            client.table(table).upsert(batch, on_conflict=",".join(key_fields)).execute()
            updated += len(batch); batch = []
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
        client, "member_metrics", members, member_ranks,
        key_fields=["member_id", "member_type"],
    )
    print(f"  Updated {updated} member rows")

    # --- State metrics ---
    print("\n[3/4] Fetching state_metrics...")
    states = fetch_all(
        client,
        "state_metrics",
        "state_id, total_works, sanctioned_works, completion_rate_pct, fund_utilization_pct",
    )
    print(f"  Fetched {len(states)} state rows")
    state_ranks = compute_state_ranks_eb(states)
    print(f"  Computed {sum(1 for v in state_ranks.values() if v is not None)} state ranks")

    print("\n[4/4] Updating state_metrics.rank...")
    updated = update_ranks(
        client, "state_metrics", states, state_ranks,
        key_fields=["state_id"],
    )
    print(f"  Updated {updated} state rows")

    print("\n" + "=" * 70)
    print("BACKFILL COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
