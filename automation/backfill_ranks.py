#!/usr/bin/env python3
"""Backfill `rank`, `scale_score`, and `performance_score_weighted` columns
in DB2 member_metrics and state_metrics.

Ranking formula (members and states use the same formula):
    performance_score_weighted =
          0.40 * completion_rate_pct
        + 0.40 * fund_utilization_pct
        + 0.20 * scale_score
    scale_score = percentile rank (midrank, 0-100) of total_works across
                  all ranking_qualified members / all qualifying states.
    rank = by performance_score_weighted (descending, competition ranking).

Members: ranking_qualified=True is required (set by phase_a_member.py).
States:  must have non-NULL completion_rate_pct, fund_utilization_pct,
         and total_works.

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


def _percentile_rank(values, target):
    """Midrank percentile: (count_below + 0.5 * count_equal) / n * 100."""
    n = len(values)
    if n == 0:
        return 0.0
    below = sum(1 for v in values if v < target)
    at = sum(1 for v in values if v == target)
    return round((below + 0.5 * at) / n * 100.0, 2)


def _competition_rank(records, key_fn):
    """Rank records descending by key_fn; ties share a rank; deterministic."""
    sorted_recs = sorted(records, key=lambda r: (-key_fn(r), _secondary_key(r)))
    last_score = None
    last_rank = 0
    for i, r in enumerate(sorted_recs, 1):
        score = key_fn(r)
        if last_score is not None and score == last_score:
            r["rank"] = last_rank
        else:
            r["rank"] = i
            last_rank = i
            last_score = score


def _secondary_key(r):
    """Secondary sort key for deterministic ordering."""
    if "state_id" in r:
        return r.get("state_id") or 0
    return (r.get("member_type") or "", r.get("member_id") or 0)


def compute_member_payload(records):
    """Compute rank + scale_score + performance_score_weighted for each member.

    Mirrors analysis/db2_analytics_persistence.compute_member_ranks.
    Returns dict (member_id, member_type) -> {rank, scale_score, performance_score_weighted}.
    """
    qualified = [r for r in records if r.get("ranking_qualified")]
    non_qualified = [r for r in records if not r.get("ranking_qualified")]

    payload = {
        (r["member_id"], r["member_type"]): {
            "rank": None,
            "scale_score": None,
            "performance_score_weighted": None,
        }
        for r in non_qualified
    }

    if not qualified:
        return payload

    tw_values = [int(r.get("total_works") or 0) for r in qualified]
    for r in qualified:
        tw = int(r.get("total_works") or 0)
        scale = _percentile_rank(tw_values, tw)
        comp = float(r.get("completion_rate_pct") or 0)
        util = float(r.get("fund_utilization_pct") or 0)
        score = round(comp * 0.40 + util * 0.40 + scale * 0.20, 2)
        payload[(r["member_id"], r["member_type"])] = {
            "scale_score": scale,
            "performance_score_weighted": score,
        }

    _competition_rank(
        qualified,
        key_fn=lambda r: payload[(r["member_id"], r["member_type"])]["performance_score_weighted"] or 0,
    )
    for r in qualified:
        key = (r["member_id"], r["member_type"])
        payload[key]["rank"] = r["rank"]

    return payload


def compute_state_payload(records):
    """Compute rank + scale_score + performance_score_weighted for each state.

    Mirrors analysis/db2_analytics_persistence.compute_state_ranks.
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

    tw_values = [int(r.get("total_works") or 0) for r in qualifying]
    for r in qualifying:
        tw = int(r.get("total_works") or 0)
        scale = _percentile_rank(tw_values, tw)
        comp = float(r.get("completion_rate_pct") or 0)
        util = float(r.get("fund_utilization_pct") or 0)
        score = round(comp * 0.40 + util * 0.40 + scale * 0.20, 2)
        payload[r["state_id"]] = {
            "scale_score": scale,
            "performance_score_weighted": score,
        }

    _competition_rank(
        qualifying,
        key_fn=lambda r: payload[r["state_id"]]["performance_score_weighted"] or 0,
    )
    for r in qualifying:
        payload[r["state_id"]]["rank"] = r["rank"]

    return payload


def upsert_member(client, table, payload):
    updated = 0
    batch_size = 200
    batch = []
    for (mid, mtype), fields in payload.items():
        batch.append({"member_id": mid, "member_type": mtype, **fields})
        if len(batch) >= batch_size:
            client.table(table).upsert(batch, on_conflict="member_id,member_type").execute()
            updated += len(batch); batch = []
    if batch:
        client.table(table).upsert(batch, on_conflict="member_id,member_type").execute()
        updated += len(batch)
    return updated


def upsert_state(client, table, payload):
    updated = 0
    batch_size = 100
    batch = []
    for sid, fields in payload.items():
        batch.append({"state_id": sid, **fields})
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
        "member_id, member_type, total_works, "
        "completion_rate_pct, fund_utilization_pct, ranking_qualified",
    )
    print(f"  Fetched {len(members)} member rows")
    member_payload = compute_member_payload(members)
    print(f"  Computed {sum(1 for v in member_payload.values() if v['rank'] is not None)} member ranks")

    print("\n[2/4] Updating member_metrics (rank + scale_score + performance_score_weighted)...")
    updated = upsert_member(client, "member_metrics", member_payload)
    print(f"  Updated {updated} member rows")

    # --- State metrics ---
    print("\n[3/4] Fetching state_metrics...")
    states = fetch_all(
        client,
        "state_metrics",
        "state_id, total_works, completion_rate_pct, fund_utilization_pct",
    )
    print(f"  Fetched {len(states)} state rows")
    state_payload = compute_state_payload(states)
    print(f"  Computed {sum(1 for v in state_payload.values() if v['rank'] is not None)} state ranks")

    print("\n[4/4] Updating state_metrics (rank + scale_score + performance_score_weighted)...")
    updated = upsert_state(client, "state_metrics", state_payload)
    print(f"  Updated {updated} state rows")

    print("\n" + "=" * 70)
    print("BACKFILL COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
