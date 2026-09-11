#!/usr/bin/env python3
"""Backfill `rank` column in DB2 member_metrics and state_metrics.

Ranks are computed based on PERFORMANCE (completion_rate_pct + fund_utilization_pct):
  Rank 1 = best performer (highest score)

Only ranking_qualified records receive a rank; others are set to NULL.

The previous pipeline implementation ranked by anomaly_score, which produced
inverted rankings (rank 1 = most anomalous = worst performer). This script
recomputes all existing ranks using the correct performance-based metric.

Usage:
    python automation/backfill_ranks.py
"""

import os
import sys
import time
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


def compute_ranks(records):
    """Return dict of primary_key -> rank, where rank 1 = best performance."""
    qualified = [r for r in records if r.get("ranking_qualified")]
    qualified.sort(key=lambda r: -perf_score(r))
    ranks = {}
    for i, r in enumerate(qualified, 1):
        key = (r.get("member_id"), r.get("member_type"))
        ranks[key] = i
    return ranks


def compute_state_ranks(records):
    """Return dict of state_id -> rank for state records."""
    qualified = [r for r in records if r.get("ranking_qualified")]
    qualified.sort(key=lambda r: -perf_score(r))
    ranks = {}
    for i, r in enumerate(qualified, 1):
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
        "state_id, completion_rate_pct, fund_utilization_pct, ranking_qualified",
    )
    print(f"  Fetched {len(states)} state rows")
    state_ranks = compute_state_ranks(states)
    print(f"  Computed {len(state_ranks)} state ranks (ranking_qualified=True)")

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
