#!/usr/bin/env python3
"""Backfill `rank`, `scale_score`, and `performance_score_weighted` columns
in DB2 state_metrics.

State ranking formula:
    performance_score_weighted =
          0.40 * completion_rate_pct
        + 0.40 * fund_utilization_pct
        + 0.20 * scale_score
    scale_score = percentile rank (midrank, 0-100) of total_works across
                  all qualifying states.
    rank = by performance_score_weighted (descending, competition ranking).

Member ranks remain raw completion + utilization, only for ranking_qualified.

Usage:
    python automation/backfill_ranks.py
"""

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


def compute_member_ranks(records):
    qualified = [r for r in records if r.get("ranking_qualified")]
    qualified.sort(key=lambda r: -perf_score(r))
    ranks = {}
    for i, r in enumerate(qualified, 1):
        key = (r.get("member_id"), r.get("member_type"))
        ranks[key] = i
    return ranks


def compute_state_payload(records):
    """Compute rank + scale_score + performance_score_weighted for each state.

    Mirrors analysis/db2_analytics_persistence.compute_state_ranks exactly.
    Returns dict state_id -> {rank, scale_score, performance_score_weighted}.
    """
    def _has_all_three(r):
        return (
            r.get("completion_rate_pct") is not None
            and r.get("fund_utilization_pct") is not None
            and r.get("total_works") is not None
        )

    qualifying = [r for r in records if _has_all_three(r)]
    non_qualifying = [r for r in records if r not in qualifying]

    payload = {r["state_id"]: {"rank": None, "scale_score": None, "performance_score_weighted": None}
               for r in non_qualifying}

    if not qualifying:
        return payload

    n = len(qualifying)
    tw_sorted = sorted(int(r.get("total_works") or 0) for r in qualifying)
    for r in qualifying:
        tw = int(r.get("total_works") or 0)
        below = sum(1 for v in tw_sorted if v < tw)
        at = sum(1 for v in tw_sorted if v == tw)
        scale = round((below + 0.5 * at) / n * 100.0, 2)
        comp = float(r.get("completion_rate_pct") or 0)
        util = float(r.get("fund_utilization_pct") or 0)
        score = round(comp * 0.40 + util * 0.40 + scale * 0.20, 2)
        payload[r["state_id"]] = {
            "scale_score": scale,
            "performance_score_weighted": score,
        }

    sorted_q = sorted(
        qualifying,
        key=lambda r: (-payload[r["state_id"]]["performance_score_weighted"], r["state_id"]),
    )
    last_score = None
    last_rank = 0
    for i, r in enumerate(sorted_q, 1):
        score = payload[r["state_id"]]["performance_score_weighted"]
        if last_score is not None and score == last_score:
            payload[r["state_id"]]["rank"] = last_rank
        else:
            payload[r["state_id"]]["rank"] = i
            last_rank = i
            last_score = score

    return payload


def update_member_ranks(client, table, records, ranks_by_key):
    updated = 0
    batch_size = 200
    batch = []
    for r in records:
        key = (r.get("member_id"), r.get("member_type"))
        rank = ranks_by_key.get(key)
        batch.append({
            "member_id": r["member_id"],
            "member_type": r["member_type"],
            "rank": rank,
        })
        if len(batch) >= batch_size:
            client.table(table).upsert(batch, on_conflict="member_id,member_type").execute()
            updated += len(batch); batch = []
    if batch:
        client.table(table).upsert(batch, on_conflict="member_id,member_type").execute()
        updated += len(batch)
    return updated


def update_state_metrics(client, table, payload):
    updated = 0
    batch_size = 100
    batch = []
    for sid, fields in payload.items():
        row = {"state_id": sid, **fields}
        batch.append(row)
        if len(batch) >= batch_size:
            client.table(table).upsert(batch, on_conflict="state_id").execute()
            updated += len(batch); batch = []
    if batch:
        client.table(table).upsert(batch, on_conflict="state_id").execute()
        updated += len(batch)
    return updated


def main():
    print("=" * 70)
    print("BACKFILL RANKS — DB2 member_metrics + state_metrics")
    print("=" * 70)

    client = create_client(DB2_URL, DB2_KEY)

    # --- Member metrics ---
    print("\n[1/4] Fetching member_metrics...")
    members = fetch_all(
        client,
        "member_metrics",
        "member_id, member_type, completion_rate_pct, fund_utilization_pct, ranking_qualified",
    )
    print(f"  Fetched {len(members)} member rows")
    member_ranks = compute_member_ranks(members)
    print(f"  Computed {len(member_ranks)} member ranks (ranking_qualified=True)")

    print("\n[2/4] Updating member_metrics.rank...")
    updated = update_member_ranks(client, "member_metrics", members, member_ranks)
    print(f"  Updated {updated} member rows")

    # --- State metrics (with weighted score fields) ---
    print("\n[3/4] Fetching state_metrics...")
    states = fetch_all(
        client,
        "state_metrics",
        "state_id, total_works, completion_rate_pct, fund_utilization_pct",
    )
    print(f"  Fetched {len(states)} state rows")
    state_payload = compute_state_payload(states)
    print(f"  Computed {sum(1 for v in state_payload.values() if v['rank'] is not None)} state ranks")

    print("\n[4/4] Updating state_metrics.rank + scale_score + performance_score_weighted...")
    updated = update_state_metrics(client, "state_metrics", state_payload)
    print(f"  Updated {updated} state rows")

    print("\n" + "=" * 70)
    print("BACKFILL COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
