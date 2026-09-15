#!/usr/bin/env python3
"""One-time, reversible analytics remediation backfill.

    python automation/remediate_analytics.py            # dry-run (default)
    python automation/remediate_analytics.py --apply    # write changes

Requires DATABASE_URL (DB1) and DB2_DATABASE_URL (DB2) in the environment
(the direct Postgres DSNs).

What it fixes (GovSense Analytics Foundation, Phases 1-4, 12):

1. Identity (Phase 3). The loader changed its member-id scheme between runs.
   Canonical analytical identities are the ids used by the analysis tables:
     MP  -> member_id 1..538          (work_analysis)
     MLA -> member_id 100000+         (mla_work_analysis, i.e. Rajya Sabha)
   Rows in member_metrics/ai_analysis that carry the OTHER scheme for the
   same person are stale orphans (no backing works). They are archived into
   backup tables before deletion, so the operation is reversible.

2. performance_score (Phase 1). Recomputed from current values using the
   authoritative formula in analysis.db2_analytics_persistence and persisted
   (it was previously written only by a manual frontend script and had gone
   stale).

3. performance_classification (Phase 1). Recomputed from performance ONLY.
   Anomaly no longer overwrites the performance label.

4. avg_work_cost / median_work_cost. Recomputed from per-work sanction
   amounts in DB1 (were NULL).

5. rank / scale_score / performance_score_weighted (Phase 2). Recomputed for
   the whole canonical population, within member_type, so that score order
   and rank order agree.

6. overall_metrics population counts (Phase 4). Recomputed from the canonical
   member_metrics population.

All writes are idempotent. Raw source data (DB1 works/amounts) is never
modified.
"""

import argparse
import asyncio
import importlib.util
import os
import statistics
import sys
from datetime import datetime, timezone

PIPELINE_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
D2_PATH = os.path.join(PIPELINE_ROOT, "analysis", "db2_analytics_persistence.py")


def _load_authoritative():
    """Load db2_analytics_persistence without importing the analysis package
    (avoids pulling in numpy via analysis/__init__.py)."""
    spec = importlib.util.spec_from_file_location("govsense_d2", D2_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


D2 = _load_authoritative()


def _now():
    return datetime.now(timezone.utc).isoformat()


async def _work_cost_by_member(db1):
    """(member_type, member_id) -> (avg, median) sanction cost per work."""
    acc = {}
    for table, mt in (("work_analysis", "MP"), ("mla_work_analysis", "MLA")):
        rows = await db1.fetch(
            f"SELECT member_id, sanction_amount FROM public.{table} "
            f"WHERE sanction_amount IS NOT NULL AND sanction_amount > 0"
        )
        for r in rows:
            acc.setdefault((mt, r["member_id"]), []).append(float(r["sanction_amount"]))
    out = {}
    for k, vals in acc.items():
        out[k] = (round(statistics.fmean(vals), 2), round(statistics.median(vals), 2))
    return out


async def _work_cost_by_state(db1):
    acc = {}
    for table in ("work_analysis", "mla_work_analysis"):
        rows = await db1.fetch(
            f"SELECT state_id, sanction_amount FROM public.{table} "
            f"WHERE sanction_amount IS NOT NULL AND sanction_amount > 0"
        )
        for r in rows:
            acc.setdefault(r["state_id"], []).append(float(r["sanction_amount"]))
    out = {}
    for k, vals in acc.items():
        out[k] = (round(statistics.fmean(vals), 2), round(statistics.median(vals), 2))
    return out


def _classify(total_works, zero, low, score):
    if zero or (total_works or 0) == 0:
        return "NO_DATA"
    if low or (total_works or 0) < D2.MIN_WORKS_FOR_CLASSIFICATION:
        return "INSUFFICIENT_DATA"
    return D2.classify_performance_from_score(score)


async def _archive(db2, table, backup, tag):
    await db2.execute(
        f'CREATE TABLE IF NOT EXISTS public."{backup}" AS SELECT * FROM public."{table}" WHERE false'
    )
    # Only archive once per tag; idempotent re-runs replace nothing.
    n = await db2.fetchval(f'SELECT COUNT(*) FROM public."{backup}"')
    if n == 0:
        await db2.execute(
            f'INSERT INTO public."{backup}" SELECT * FROM public."{table}"'
        )
        print(f"  Archived {table} -> {backup}")
    else:
        print(f"  Backup {backup} already exists ({n} rows); not overwriting")


async def _find_orphans(db2):
    """Return stale/orphan identities based on the canonical id scheme.

    Canonical analytical ids (the ones actually referenced by the analysis
    tables) are:
        MP  -> member_id 1..538 (low)          mla -> member_id >= 100000
    Therefore all MP rows with id >= 100000 and all MLA rows with id < 100000
    are stale. Additionally, a zero-work row that duplicates a work-bearing
    person of the same (member_type, name) is a spurious master duplicate.
    All are archived before deletion (reversible).
    """
    mp = await db2.fetch(
        "SELECT member_id FROM public.member_metrics WHERE member_type='MP' AND member_id>=100000"
    )
    mla = await db2.fetch(
        "SELECT member_id FROM public.member_metrics WHERE member_type='MLA' AND member_id<100000"
    )
    dup = await db2.fetch(
        """
        SELECT z.member_id, z.member_type FROM public.member_metrics z
        WHERE z.zero_work_member
          AND EXISTS (
            SELECT 1 FROM public.member_metrics c
            WHERE c.member_name = z.member_name AND c.member_type = z.member_type
              AND c.total_works > 0)
        """
    )
    groups = {}
    for r in mp:
        groups.setdefault("MP", set()).add(r["member_id"])
    for r in mla:
        groups.setdefault("MLA", set()).add(r["member_id"])
    for r in dup:
        groups.setdefault(r["member_type"], set()).add(r["member_id"])
    return [(mt, sorted(ids)) for mt, ids in groups.items()]


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write changes (default: dry-run)")
    args = ap.parse_args()
    apply = args.apply

    db1_url = os.environ.get("DATABASE_URL")
    db2_url = os.environ.get("DB2_DATABASE_URL")
    if not db1_url or not db2_url:
        print("ERROR: DATABASE_URL and DB2_DATABASE_URL must be set")
        sys.exit(1)

    import asyncpg
    db1 = await asyncpg.create_pool(dsn=db1_url, min_size=1, max_size=3,
                                    statement_cache_size=0, command_timeout=180)
    db2 = await asyncpg.create_pool(dsn=db2_url, min_size=1, max_size=3,
                                    statement_cache_size=0, command_timeout=180)
    tag = datetime.now(timezone.utc).strftime("%Y%m%d")
    print(f"=== GovSense analytics remediation ({'APPLY' if apply else 'DRY-RUN'}) ===")

    # ---------------- Phase 3: identity cleanup ----------------
    orphans = await _find_orphans(db2)
    total_orphans = sum(len(ids) for _, ids in orphans)
    print(f"\n[Phase 3] orphan member_metrics rows: {total_orphans}")
    for mt, ids in orphans:
        print(f"  {mt}: {len(ids)} orphan ids (sample {ids[:5]})")

    if apply and total_orphans:
        await _archive(db2, "member_metrics", f"govsense_backup_member_metrics_{tag}", tag)
        await _archive(db2, "ai_analysis", f"govsense_backup_ai_analysis_{tag}", tag)
        for mt, ids in orphans:
            await db2.execute(
                "DELETE FROM public.member_metrics WHERE member_type=$1 AND member_id = ANY($2::int[])",
                mt, ids,
            )
            # Remove orphan AI rows for the same identities.
            await db2.execute(
                "DELETE FROM public.ai_analysis WHERE entity_type=$1 AND entity_id = ANY($2::bigint[])",
                mt, ids,
            )
        print(f"  Deleted {total_orphans} orphan metric rows and their AI rows")

    # ---------------- Phase 1/2: members ----------------
    costs = await _work_cost_by_member(db1)
    members = await db2.fetch(
        """SELECT member_id, member_type, total_works, completion_rate_pct,
                  fund_utilization_pct, zero_work_member, low_sample_member,
                  ranking_qualified
           FROM public.member_metrics"""
    )
    mrecs = []
    for r in members:
        rec = dict(r)
        comp = float(rec["completion_rate_pct"] or 0)
        util = float(rec["fund_utilization_pct"] or 0)
        score = D2.compute_performance_score(comp, util)
        rec["performance_score"] = score
        rec["performance_classification"] = _classify(
            rec["total_works"], rec["zero_work_member"], rec["low_sample_member"], score
        )
        c = costs.get((rec["member_type"], rec["member_id"]))
        rec["avg_work_cost"] = c[0] if c else None
        rec["median_work_cost"] = c[1] if c else None
        mrecs.append(rec)
    D2.compute_member_ranks(mrecs)

    print(f"\n[Phase 1/2] member rows to update: {len(mrecs)}")
    by_type = {}
    for r in mrecs:
        by_type[r["member_type"]] = by_type.get(r["member_type"], 0) + 1
    print(f"  by type: {by_type}")
    print(f"  with median_work_cost: {sum(1 for r in mrecs if r['median_work_cost'] is not None)}")

    if apply:
        await db2.executemany(
            """UPDATE public.member_metrics
               SET performance_score=$1, performance_classification=$2,
                   avg_work_cost=$3, median_work_cost=$4,
                   rank=$5, scale_score=$6, performance_score_weighted=$7,
                   calculated_at=NOW()
               WHERE member_id=$8 AND member_type=$9""",
            [
                (r["performance_score"], r["performance_classification"],
                 r["avg_work_cost"], r["median_work_cost"],
                 r.get("rank"), r.get("scale_score"), r.get("performance_score_weighted"),
                 r["member_id"], r["member_type"])
                for r in mrecs
            ],
        )
        print("  members updated")

    # ---------------- Phase 1/2: states ----------------
    scosts = await _work_cost_by_state(db1)
    states = await db2.fetch(
        """SELECT state_id, total_works, completion_rate_pct, fund_utilization_pct
           FROM public.state_metrics"""
    )
    srecs = []
    for r in states:
        rec = dict(r)
        comp = float(rec["completion_rate_pct"] or 0)
        util = float(rec["fund_utilization_pct"] or 0)
        score = D2.compute_performance_score(comp, util)
        rec["performance_score"] = score
        rec["performance_classification"] = D2.classify_performance_from_score(score)
        c = scosts.get(rec["state_id"])
        rec["avg_work_cost"] = c[0] if c else None
        rec["median_work_cost"] = c[1] if c else None
        srecs.append(rec)
    D2.compute_state_ranks(srecs)

    print(f"\n[Phase 1/2] state rows to update: {len(srecs)}")
    if apply:
        await db2.executemany(
            """UPDATE public.state_metrics
               SET performance_score=$1, performance_classification=$2,
                   avg_work_cost=$3, median_work_cost=$4,
                   rank=$5, scale_score=$6, performance_score_weighted=$7,
                   calculated_at=NOW()
               WHERE state_id=$8""",
            [
                (r["performance_score"], r["performance_classification"],
                 r["avg_work_cost"], r["median_work_cost"],
                 r.get("rank"), r.get("scale_score"), r.get("performance_score_weighted"),
                 r["state_id"])
                for r in srecs
            ],
        )
        print("  states updated")

    # ---------------- Phase 4: overall population ----------------
    pop = await db2.fetchrow(
        """SELECT COUNT(*) AS total,
                  COUNT(*) FILTER (WHERE total_works > 0) AS work_bearing,
                  COUNT(*) FILTER (WHERE zero_work_member) AS zero_work,
                  COUNT(*) FILTER (WHERE low_sample_member AND NOT zero_work_member) AS low_sample
           FROM public.member_metrics"""
    )
    mp_pop = await db2.fetchrow(
        """SELECT COUNT(*) AS total,
                  COUNT(*) FILTER (WHERE total_works > 0) AS work_bearing,
                  COUNT(*) FILTER (WHERE zero_work_member) AS zero_work
           FROM public.member_metrics WHERE member_type='MP'"""
    )
    mla_pop = await db2.fetchrow(
        """SELECT COUNT(*) AS total,
                  COUNT(*) FILTER (WHERE total_works > 0) AS work_bearing,
                  COUNT(*) FILTER (WHERE zero_work_member) AS zero_work
           FROM public.member_metrics WHERE member_type='MLA'"""
    )
    print(f"\n[Phase 4] canonical population BOTH={pop['total']} "
          f"work_bearing={pop['work_bearing']} zero_work={pop['zero_work']} "
          f"| MP={mp_pop['total']} | MLA={mla_pop['total']}")
    if apply:
        await db2.execute(
            """UPDATE public.overall_metrics SET total_members=$1, work_bearing=$2,
                      zero_work_members=$3, low_sample_members=$4 WHERE scope='BOTH'""",
            pop["total"], pop["work_bearing"], pop["zero_work"], pop["low_sample"],
        )
        await db2.execute(
            """UPDATE public.overall_metrics SET total_members=$1, work_bearing=$2,
                      zero_work_members=$3 WHERE scope='MP'""",
            mp_pop["total"], mp_pop["work_bearing"], mp_pop["zero_work"],
        )
        await db2.execute(
            """UPDATE public.overall_metrics SET total_members=$1, work_bearing=$2,
                      zero_work_members=$3 WHERE scope='MLA'""",
            mla_pop["total"], mla_pop["work_bearing"], mla_pop["zero_work"],
        )
        print("  overall_metrics population updated")

    print(f"\n=== {'APPLIED' if apply else 'DRY-RUN COMPLETE'} ===")
    await db1.close()
    await db2.close()


if __name__ == "__main__":
    asyncio.run(main())
