#!/usr/bin/env python3
"""GovSense AI — Intelligence layer backfill / orchestrator.

Runs all intelligence stages end-to-end, idempotently. Writes to DB2 and DB1
per the migration 2026_09_15_intelligence*.sql.

Stages:
  1. Allocation matching (DB1 allocation tables -> DB2 member_metrics)
  2. Performance score 0-100 + label + confidence (member_intelligence,
     state_intelligence)
  3. National rank + percentile (within member_type for members)
  4. K-Means profiling (representative + state) -> cluster_id, cluster_label
  5. XGBoost slow-completion project model + per-work delay probability
  6. Isolation Forest anomaly per work
  7. Risk engine (predictive + anomaly + statistical) per member / state
  8. Model registry persistence

Idempotent: each stage upserts. Re-runs produce the same authoritative values.

Required env:
  DATABASE_URL     (DB1 direct Postgres DSN)
  DB2_DATABASE_URL (DB2 direct Postgres DSN)

Run:
  python automation/intelligence_backfill.py [--apply]
  (default: dry-run; prints a summary of computed values without writing.)
"""

import argparse
import asyncio
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

import asyncpg
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from automation.db_pool import init_pools, close_pools, get_db1_pool, get_db2_pool
from automation.observability import PipelineTimer, StageMetrics

# Total stages for the progress indicator.
TOTAL_STAGES = 7


def _now():
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def _hr(seconds):
    """Human-readable duration."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    m, s = divmod(seconds, 60)
    if m < 60:
        return f"{int(m)}m{s:04.1f}s"
    h, m = divmod(int(m), 60)
    return f"{int(h)}h{m:02d}m{s:02.0f}s"


class StageTimer:
    """Cumulative per-stage timing and pretty-printing."""

    def __init__(self):
        self._t0 = time.monotonic()
        self._stage_t0 = None

    def start(self, idx, name):
        self._stage_t0 = time.monotonic()
        print(f"\n[{_now()}] === STAGE {idx}/{TOTAL_STAGES}: {name} ===", flush=True)

    def done(self, name):
        if self._stage_t0 is None:
            return
        d = time.monotonic() - self._stage_t0
        total = time.monotonic() - self._t0
        print(f"[{_now()}]    stage '{name}' done in {_hr(d)} (total elapsed {_hr(total)})",
              flush=True)
        self._stage_t0 = None


TIMER = StageTimer()


def _step(msg):
    print(f"[{_now()}]    {msg}", flush=True)


def _timed(label):
    """Context-manager-style timing helper for sub-steps."""
    return _TimedCtx(label)


class _TimedCtx:
    def __init__(self, label):
        self.label = label
        self.t0 = None

    def __enter__(self):
        self.t0 = time.monotonic()
        print(f"[{_now()}]    >> {self.label} ...", flush=True)
        return self

    def __exit__(self, exc_type, exc, tb):
        d = time.monotonic() - (self.t0 or time.monotonic())
        status = "ok" if exc_type is None else f"FAILED ({exc_type.__name__})"
        print(f"[{_now()}]    << {self.label} -> {status} in {_hr(d)}", flush=True)


from analysis.intelligence import allocation as alloc_mod
from analysis.intelligence import score as score_mod
from analysis.intelligence import profiling as prof_mod
from analysis.intelligence import project_model as proj_mod
from analysis.intelligence import anomaly_model as anom_mod
from analysis.intelligence import risk as risk_mod
from analysis.intelligence.registry import upsert_registry


DATA_VERSION = "2026-09-14"


def _safe(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


async def _load_db1(db1):
    with _timed("DB1: load allocation tables"):
        alloc_rows = await db1.fetch(
            """SELECT 'MP'::text AS member_type, mp_id AS master_id, allocated_amount,
                      source_tenure, source_house_of_parliament::text AS source_house_of_parliament
               FROM public.mp_allocations
               UNION ALL
               SELECT 'MLA'::text, mla_id, allocated_amount, source_tenure,
                      source_house_of_parliament::text
               FROM public.mla_allocations""")
        _step(f"alloc tables: {len(alloc_rows)} rows")
    with _timed("DB1: load masters (mps + mlas)"):
        master_rows = await db1.fetch(
            """SELECT 'MP'::text AS member_type, mp_id AS master_id, mp_name AS name,
                      tenure, house_name::text AS house, constituency_id
               FROM public.mps
               UNION ALL
               SELECT 'MLA'::text, mla_id, mla_name, tenure, house_name::text,
                      constituency_id
               FROM public.mlas""")
        _step(f"masters: {len(master_rows)} rows")
    with _timed("DB1: load works (UNION ALL ~133k rows)"):
        works = await db1.fetch(
            """SELECT work_id, member_id, member_type, state_id,
                      recommended_amount, sanction_amount, expenditure_amount,
                      cost_percentile, duration_percentile,
                      execution_days, project_age_days, sanction_delay_days,
                      recommendation_date, sanction_date, completion_date,
                      work_category, normalized_activity
               FROM public.work_analysis WHERE member_type='MP'
               UNION ALL
               SELECT work_id, member_id, member_type, state_id,
                      recommended_amount, sanction_amount, expenditure_amount,
                      cost_percentile, duration_percentile,
                      execution_days, project_age_days, sanction_delay_days,
                      recommendation_date, sanction_date, completion_date,
                      work_category, normalized_activity
               FROM public.mla_work_analysis WHERE member_type='MLA'""",
            timeout=500,
        )
        _step(f"works: {len(works)} rows")
    return [dict(r) for r in alloc_rows], [dict(r) for r in master_rows], [dict(r) for r in works]


async def _fetch_with_timeout(pool, sql, label, timeout_s=600):
    """Execute a large query with a server-side statement_timeout override.
    Uses a dedicated connection (acquired from pool) and raises the server-side
    statement_timeout so Supabase does not cancel the query mid-stream."""
    async with pool.acquire() as c:
        # Raise server-side statement timeout on this connection.
        await c.execute(f"SET statement_timeout = '{timeout_s * 1000}'")  # Postgres wants ms
        # Verify it took effect.
        current = await c.fetchval("SHOW statement_timeout")
        t0 = time.monotonic()
        print(f"[{_now()}]    >> DB1: {label} (stmt_timeout={current}) ...", flush=True)
        rows = await c.fetch(sql)
        print(f"[{_now()}]    << {label}: {len(rows)} rows in {_hr(time.monotonic()-t0)}", flush=True)
        return [dict(r) for r in rows]


async def _load_works(db1):
    """Load the minimal works projection needed by the project model and
    anomaly stages. Fetched in PARALLEL from two dedicated connections,
    each with a raised statement_timeout so Supabase does not cancel."""
    cols = """work_id, member_id, member_type, state_id,
              recommended_amount, sanction_amount, expenditure_amount,
              cost_percentile, duration_percentile,
              execution_days, project_age_days, sanction_delay_days,
              recommendation_date, sanction_date, completion_date,
              work_category, normalized_activity"""
    t0 = time.monotonic()
    print(f"[{_now()}]    >> DB1: loading work_analysis + mla_work_analysis IN PARALLEL ...", flush=True)
    mp_rows, mla_rows = await asyncio.gather(
        _fetch_with_timeout(db1, f"SELECT {cols} FROM public.work_analysis",
                            "work_analysis (MP ~108k)"),
        _fetch_with_timeout(db1, f"SELECT {cols} FROM public.mla_work_analysis",
                            "mla_work_analysis (MLA ~25k)"),
    )
    combined = mp_rows + mla_rows
    print(f"[{_now()}]    works combined: {len(combined)} rows (total {_hr(time.monotonic()-t0)})", flush=True)
    return combined


async def _load_db2(db2):
    print(f"[{_now()}]    >> DB2: load member_metrics ...", flush=True)
    t0 = time.monotonic()
    members = await db2.fetch(
        """SELECT member_id, member_type, member_name, house_name, tenure,
                  constituency_id, total_works, completed_works,
                  fund_utilization_pct, avg_sanction_delay_days,
                  flagged_rate_pct, overdue_over_1_year, cost_anomaly_works,
                  anomaly_score, performance_score, performance_classification
           FROM public.member_metrics""")
    print(f"[{_now()}]    << DB2 member_metrics: {len(members)} rows in {_hr(time.monotonic()-t0)}", flush=True)
    print(f"[{_now()}]    >> DB2: load state_metrics ...", flush=True)
    t1 = time.monotonic()
    states = await db2.fetch(
        """SELECT state_id, state_name, total_works, completed_works,
                  fund_utilization_pct, avg_sanction_delay_days,
                  overdue_over_1_year, flagged_works, risk_rate_pct,
                  cost_anomaly_works, performance_score, performance_classification
           FROM public.state_metrics""")
    print(f"[{_now()}]    << DB2 state_metrics: {len(states)} rows in {_hr(time.monotonic()-t1)}", flush=True)
    return [dict(r) for r in members], [dict(r) for r in states]


async def stage_allocation(db1, db2, apply):
    TIMER.start(1, "Allocation matching")
    alloc_rows = await db1.fetch(
        """SELECT 'MP'::text AS member_type, mp_id AS master_id, allocated_amount,
                  source_tenure, source_house_of_parliament::text AS source_house_of_parliament
           FROM public.mp_allocations
           UNION ALL
           SELECT 'MLA'::text, mla_id, allocated_amount, source_tenure,
                  source_house_of_parliament::text
           FROM public.mla_allocations"""
    )
    _step(f"alloc tables loaded: {len(alloc_rows)} rows")
    master_rows = await db1.fetch(
        """SELECT 'MP'::text AS member_type, mp_id AS master_id, mp_name AS name,
                  tenure, house_name::text AS house, constituency_id
           FROM public.mps
           UNION ALL
           SELECT 'MLA'::text, mla_id, mla_name, tenure, house_name::text,
                  constituency_id
           FROM public.mlas"""
    )
    _step(f"masters loaded: {len(master_rows)} rows")
    members, _ = await _load_db2(db2)

    # Pre-fill missing tenure/house on MLA member_metrics from mlas by exact
    # name (best-effort, unique match only). Improves MLA coverage when
    # _master_record attachment was missed.
    from collections import defaultdict
    mlas_by_name = defaultdict(list)
    for m in master_rows:
        if m.get("member_type") == "MLA" and m.get("name"):
            mlas_by_name[alloc_mod._norm(m["name"])].append(m)
    # Pre-fill missing tenure/house on MLA member_metrics from mlas.
    # Note: the allocator matches by alloc_mod._norm(name); we must use the
    # same normalisation here so the key spaces align.
    prefill_count = 0
    for mem in members:
        if mem.get("member_type") != "MLA":
            continue
        if mem.get("tenure") and mem.get("house_name"):
            continue
        cands = mlas_by_name.get(alloc_mod._norm(mem.get("member_name")), [])
        if len(cands) == 1:
            mem["tenure"] = cands[0]["tenure"]
            mem["house_name"] = cands[0].get("house") or "Rajya Sabha"
            prefill_count += 1
        # ambiguous (multiple masters with same normalised name): leave unmatched
    for mem in members:
        if mem.get("member_type") != "MLA":
            continue
        if mem.get("tenure") and mem.get("house_name"):
            continue
        cands = mlas_by_name.get((mem.get("member_name") or "").strip().lower(), [])
        unique = {c["tenure"] for c in cands} if cands else set()
        if len(cands) == 1:
            mem["tenure"] = cands[0]["tenure"]
            mem["house_name"] = cands[0].get("house") or "Rajya Sabha"
            prefill_count += 1
        elif len(unique) == 1 and len(cands) > 1:
            # multiple master entries with same name+tenure -> ambiguous; leave unmatched
            pass
    _step(f"MLA tenure/house pre-filled for {prefill_count} members")

    with _timed("build canonical allocation mapping"):
        updates, counts = alloc_mod.build_canonical_allocation(
            alloc_rows, master_rows, members,
        )
    print(f"  matched: MP={counts['mp']} MLA={counts['mla']} total_updates={len(updates)}", flush=True)
    if apply:
        async with db2.acquire() as c2:
            await c2.executemany(
                """UPDATE public.member_metrics
                   SET allocated_amount=$1, allocated_source=$2, allocated_confidence=$3
                   WHERE member_id=$4 AND member_type=$5""",
                [(u["allocated_amount"], u["allocated_source"],
                  u["allocated_confidence"], u["member_id"], u["member_type"])
                 for u in updates],
            )
            _step(f"DB2 UPDATE member_metrics: {len(updates)} rows")
            # member allocations (for state aggregation, in Python because
            # state aggregation requires a cross-DB join: works.state_id from
            # DB1 + member allocated from DB2).
            mem_alloc = {r["member_id"]: r["allocated_amount"] for r in
                         await c2.fetch(
                             "SELECT member_id, allocated_amount FROM public.member_metrics"
                             " WHERE allocated_amount > 0")}
            _step(f"re-fetched {len(mem_alloc)} non-zero allocations for state aggregation")
        with _timed("DB1 load works->state map (DISTINCT, ~737 rows)"):
            work_map = await db1.fetch(
                """SELECT DISTINCT member_id, member_type, state_id FROM public.work_analysis
                   UNION SELECT DISTINCT member_id, member_type, state_id FROM public.mla_work_analysis""",
                timeout=500,
            )
        state_totals = defaultdict(float)
        for w in work_map:
            amt = mem_alloc.get(w["member_id"]) if w["member_type"] == "MP" else mem_alloc.get(w["member_id"])
            if amt and w["state_id"]:
                state_totals[w["state_id"]] += float(amt)
        _step(f"state allocation aggregated for {len(state_totals)} states")
        async with db2.acquire() as c2:
            await c2.executemany(
                """UPDATE public.state_metrics
                   SET allocated_amount = $1,
                       allocated_source = 'allocation_table',
                       allocated_confidence = 'HIGH'
                   WHERE state_id = $2""",
                [(float(total), int(sid)) for sid, total in state_totals.items()],
            )
            await c2.execute(
                """UPDATE public.state_metrics
                   SET allocated_source='unmatched', allocated_confidence='LOW'
                   WHERE allocated_amount IS NULL OR allocated_amount = 0"""
            )
        _step("DB2 UPDATE state_metrics allocated_* done")
    TIMER.done("Allocation matching")
    return counts


def _to_float(v):
    return _safe(v, None)


async def stage_score_and_ranking(db2, apply):
    TIMER.start(2, "Performance score 0-100 + rank + percentile")
    members, states = await _load_db2(db2)
    with _timed("score: members"):
        m_scores = score_mod.member_scores(members)
    with _timed("score: states"):
        s_scores = score_mod.state_scores(states)

    # National rank + percentile within each member_type
    by_type = defaultdict(list)
    for k, v in m_scores.items():
        if v["score"] is not None:
            by_type[k[0]].append((k, v["score"]))
    _step(f"computing member ranks within {len(by_type)} populations")
    ranks_m = {}
    for mt, items in by_type.items():
        ranks = score_mod.rank_within(dict(items), higher_better=True)
        ranks_m.update(ranks)
    with _timed("rank: states"):
        s_ranks = score_mod.rank_within(
            {sid: v["score"] for sid, v in s_scores.items()}, higher_better=True
        )

    member_intel = []
    for (mt, mid), s in m_scores.items():
        nr, npct = ranks_m.get((mt, mid), (None, None))
        member_intel.append({
            "member_id": mid, "member_type": mt,
            "performance_score_100": s["score"],
            "performance_label": s["label"],
            "performance_confidence": s["confidence"],
            "national_rank": nr,
            "national_percentile": npct,
            "sample_size": s["n"],
        })

    state_intel = []
    for sid, s in s_scores.items():
        rk, pct = s_ranks.get(sid, (None, None))
        state_intel.append({
            "state_id": sid,
            "performance_score_100": s["score"],
            "performance_label": s["label"],
            "performance_confidence": s["confidence"],
            "rank": rk,
            "national_percentile": pct,
            "sample_size": s["n"],
        })
    _step(f"member scores computed: {sum(1 for v in m_scores.values() if v['score'] is not None)} "
          f"of {len(m_scores)}")
    _step(f"state scores computed:  {sum(1 for v in s_scores.values() if v['score'] is not None)} "
          f"of {len(s_scores)}")
    if apply:
        async with db2.acquire() as c:
            await c.executemany(
                """INSERT INTO public.member_intelligence
                       (member_id, member_type, performance_score_100, performance_label,
                        performance_confidence, national_rank, national_percentile, sample_size,
                        calculated_at)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8, NOW())
                   ON CONFLICT (member_id, member_type) DO UPDATE SET
                        performance_score_100 = EXCLUDED.performance_score_100,
                        performance_label = EXCLUDED.performance_label,
                        performance_confidence = EXCLUDED.performance_confidence,
                        national_rank = EXCLUDED.national_rank,
                        national_percentile = EXCLUDED.national_percentile,
                        sample_size = EXCLUDED.sample_size,
                        calculated_at = EXCLUDED.calculated_at""",
                [(m["member_id"], m["member_type"], m["performance_score_100"],
                  m["performance_label"], m["performance_confidence"],
                  m["national_rank"], m["national_percentile"], m["sample_size"])
                 for m in member_intel],
            )
            await c.executemany(
                """INSERT INTO public.state_intelligence
                       (state_id, performance_score_100, performance_label,
                        performance_confidence, rank, national_percentile, sample_size,
                        calculated_at)
                   VALUES ($1,$2,$3,$4,$5,$6,$7, NOW())
                   ON CONFLICT (state_id) DO UPDATE SET
                        performance_score_100 = EXCLUDED.performance_score_100,
                        performance_label = EXCLUDED.performance_label,
                        performance_confidence = EXCLUDED.performance_confidence,
                        rank = EXCLUDED.rank,
                        national_percentile = EXCLUDED.national_percentile,
                        sample_size = EXCLUDED.sample_size,
                        calculated_at = EXCLUDED.calculated_at""",
                [(s["state_id"], s["performance_score_100"], s["performance_label"],
                  s["performance_confidence"], s["rank"], s["national_percentile"],
                  s["sample_size"]) for s in state_intel],
            )
        print("  member_intelligence + state_intelligence updated")
    return m_scores, s_scores


async def stage_profiling(db2, m_scores, s_scores, apply):
    TIMER.start(4, "K-Means profiling")
    members, states = await _load_db2(db2)
    members_for_prof = [m for m in members
                        if m_scores.get((m["member_type"], m["member_id"]),
                                        {}).get("n", 0) >= 5]
    states_for_prof = [s for s in states
                       if s_scores.get(int(s["state_id"]), {}).get("n", 0) >= 5]
    _step(f"K-Means inputs: {len(members_for_prof)} members, {len(states_for_prof)} states")
    with _timed("K-Means fit (members, k by silhouette, ARI stability)"):
        m_clu = prof_mod.profile_members(members_for_prof) if members_for_prof else {}
    with _timed("K-Means fit (states)"):
        s_clu = prof_mod.profile_states(states_for_prof) if states_for_prof else {}

    n_m_clu = sum(1 for v in m_clu.values() if v.get("cluster_label") not in (None, "insufficient"))
    n_s_clu = sum(1 for v in s_clu.values() if v.get("cluster_label") not in (None, "insufficient"))
    print(f"  member clusters: {n_m_clu}/{len(m_clu)} labelled; state clusters: {n_s_clu}/{len(s_clu)}", flush=True)

    # Peer rank/percentile depends on cluster assignment, so compute it here.
    with _timed("compute peer rank/percentile within member_type + cluster"):
        peer_ranks = score_mod.peer_ranks(m_scores, m_clu)
    _step(f"peer ranks computed for {len(peer_ranks)} members")

    if apply:
        async with db2.acquire() as c:
            # Batch member clusters in chunks of 50 to avoid 719 round-trips.
            m_items = list(m_clu.items())
            for chunk_start in range(0, len(m_items), 50):
                chunk = m_items[chunk_start:chunk_start + 50]
                await c.executemany(
                    """UPDATE public.member_intelligence
                       SET cluster_id=$1, cluster_label=$2, calculated_at=NOW()
                       WHERE member_id=$3 AND member_type=$4""",
                    [(v.get("cluster_id"), v.get("cluster_label"), mid, mt)
                     for (mt, mid), v in chunk],
                )
                _step(f"member clusters persisted: {min(chunk_start + 50, len(m_items))}/{len(m_items)}")
            for sid, v in s_clu.items():
                await c.execute(
                    """UPDATE public.state_intelligence
                       SET cluster_id=$1, cluster_label=$2, calculated_at=NOW()
                       WHERE state_id=$3""",
                    v.get("cluster_id"), v.get("cluster_label"), int(sid),
                )
            # Persist peer ranks.
            if peer_ranks:
                pr_items = list(peer_ranks.items())
                for chunk_start in range(0, len(pr_items), 50):
                    chunk = pr_items[chunk_start:chunk_start + 50]
                    await c.executemany(
                        """UPDATE public.member_intelligence
                           SET peer_rank=$1, peer_percentile=$2, calculated_at=NOW()
                           WHERE member_id=$3 AND member_type=$4""",
                        [(rk, pct, mid, mt) for (mt, mid), (rk, pct) in chunk],
                    )
                    _step(f"peer ranks persisted: {min(chunk_start + 50, len(pr_items))}/{len(pr_items)}")
        _step(f"clusters + peer ranks persisted to member_intelligence + state_intelligence")
    TIMER.done("K-Means profiling")
    return m_clu, s_clu


def _aggregate_work_risks(works_predictions, works_iso):
    """Returns dict (member_type, member_id) and dict state_id ->
    {mean_delay_prob, isolation_95, n}."""
    # predictions by (member_type, member_id)
    pred_by_key = defaultdict(list)
    iso_by_key = defaultdict(list)
    pred_by_state = defaultdict(list)
    iso_by_state = defaultdict(list)
    # join predictions with iso at the work_id level
    iso_map = {w["work_id"]: w for w in works_iso}
    for p in works_predictions:
        k = (p["member_type"], p["work_id"])  # work_id may not match member_id; load later
    # We need work->member/state mapping. Simpler: load from DB1 works once more.
    return None  # placeholder; computed below with DB1 join


async def stage_project_and_anomaly(db1, db2, apply):
    TIMER.start(5, "Project delay model (XGBoost)")
    TIMER.start(6, "Isolation Forest anomaly")
    # We run both models in this stage (5 + 6).
    works = [dict(w) for w in await _load_works(db1)]

    # XGBoost project model
    print(f"  >> XGBoost: training on {len(works)} works ...", flush=True)
    t0 = time.monotonic()
    res, model, fnames, rows = proj_mod.train_and_evaluate(works)
    _step(f"XGBoost trained in {_hr(time.monotonic()-t0)} "
          f"(status={res['status']} n_total={res['n_total']} n_train={res.get('n_train','?')} "
          f"n_test={res.get('n_test','?')} prev={res.get('target_prevalence',0):.3f})")
    _step(f"XGBoost metrics: {res['metrics']}")
    work_predictions = []
    if res["status"] == "READY" and model is not None:
        work_predictions = proj_mod.predict_works(works, model, fnames)
        _step(f"XGBoost predictions: {len(work_predictions)} works scored")
    elif res["status"] != "READY":
        _step("XGBoost NOT_READY -> NO predictions persisted (no fake ML).")

    # Isolation Forest
    print(f"  >> IsolationForest: fitting on {len(works)} works ...", flush=True)
    t1 = time.monotonic()
    iso_results = anom_mod.fit_and_score(works)
    _step(f"IsolationForest fit in {_hr(time.monotonic()-t1)}: {len(iso_results)} works; "
          f"HIGHLY_UNUSUAL={sum(1 for r in iso_results if r['isolation_level']=='HIGHLY_UNUSUAL')}, "
          f"UNUSUAL={sum(1 for r in iso_results if r['isolation_level']=='UNUSUAL')}, "
          f"NORMAL={sum(1 for r in iso_results if r['isolation_level']=='NORMAL')}")

    if apply:
        with _timed("DB1 UPDATE per-work ML outputs (batched)"):
            # Health check the shared pool
            try:
                async with db1.acquire() as c:
                    await c.execute("SELECT 1")
                _step("DB1 connection health check passed")
            except Exception as e:
                _step(f"DB1 connection health check failed: {e}")
                # Reconnect via db_pool module
                from automation.db_pool import db1_reconnect
                await db1_reconnect()
                db1 = get_db1_pool()
                _step("DB1 pool reconnected via db_pool")
            async with db1.acquire() as c:
                # Separate MP and MLA rows, batch executemany in chunks of 500.
                # Only write isolation fields (XGBoost NOT_READY = no predictions).
                mp_iso = [(i["isolation_score"], i["isolation_level"], i["work_id"])
                           for i in iso_results if i["member_type"] == "MP"]
                mla_iso = [(i["isolation_score"], i["isolation_level"], i["work_id"])
                           for i in iso_results if i["member_type"] == "MLA"]
                if work_predictions:
                    mp_pred = [(p["delay_probability"], p["delay_risk_band"], p["work_id"])
                               for p in work_predictions if p["member_type"] == "MP"]
                    mla_pred = [(p["delay_probability"], p["delay_risk_band"], p["work_id"])
                                for p in work_predictions if p["member_type"] == "MLA"]
                else:
                    mp_pred = []; mla_pred = []
                for label, rows, sql_template in [
                    ("MP isolation", mp_iso, "UPDATE public.work_analysis SET isolation_score=$1, isolation_level=$2 WHERE work_id=$3"),
                    ("MLA isolation", mla_iso, "UPDATE public.mla_work_analysis SET isolation_score=$1, isolation_level=$2 WHERE work_id=$3"),
                    ("MP delay_pred", mp_pred, "UPDATE public.work_analysis SET delay_probability=$1, delay_risk_band=$2 WHERE work_id=$3"),
                    ("MLA delay_pred", mla_pred, "UPDATE public.mla_work_analysis SET delay_probability=$1, delay_risk_band=$2 WHERE work_id=$3"),
                ]:
                    if not rows:
                        continue
                    _step(f"{label}: {len(rows)} rows, batching 500 ...")
                    for start in range(0, len(rows), 500):
                        chunk = rows[start:start+500]
                        await c.executemany(sql_template, chunk)
                        _step(f"{label}: {min(start+500, len(rows))}/{len(rows)}")
        # Registry entry for project model + isolation forest
        with _timed("DB2 UPSERT model_registry (project_delay_xgb + isolation_forest)"):
            async with db2.acquire() as c2:
                await upsert_registry(c2,
                    name="project_delay_xgb",
                    version=res.get("version", "xgb-unknown"),
                    model_type="XGBClassifier",
                    training_date=datetime.now(timezone.utc),
                    training_observations=res.get("n_train"),
                    features=fnames,
                    target="slow_completion (execution_days > 365)",
                    validation_method="temporal split by recommendation_date",
                    metrics={k: v for k, v in res.get("metrics", {}).items() if v is not None},
                    threshold={"LOW": 0.30, "MODERATE": 0.50, "HIGH": 0.70, "CRITICAL": 0.70},
                    calibration="default 0.5; bands fixed",
                    status=res["status"],
                    data_version=DATA_VERSION)
                await upsert_registry(c2,
                    name="isolation_forest",
                    version=f"if-{datetime.now(timezone.utc).strftime('%Y%m%d')}",
                    model_type="IsolationForest",
                    training_date=datetime.now(timezone.utc),
                    training_observations=len(iso_results),
                    features=anom_mod.FEATURE_NAMES,
                    target=None,
                    validation_method="unsupervised; level thresholds from quantiles",
                    metrics={"contamination": 0.05},
                    threshold={"NORMAL": 0.33, "UNUSUAL": 0.66},
                    calibration="min-max within dataset",
                    status="READY",
                    data_version=DATA_VERSION)
        _step("DB1 per-work columns + DB2 model_registry updated")
    TIMER.done("Project delay model + Isolation Forest")
    return work_predictions, iso_results, works


async def stage_risk(db1, db2, m_scores, s_scores, work_predictions, iso_results, apply, works=None):
    TIMER.start(7, "Risk engine (predictive + anomaly + statistical)")
    # Aggregate per-work predictions/iso to per-entity
    pred_by_member = defaultdict(list)
    pred_by_state = defaultdict(list)
    iso_by_member = defaultdict(list)
    iso_by_state = defaultdict(list)

    if works is not None:
        _step(f"reusing {len(works)} works already in memory for risk aggregation")
    else:
        with _timed("DB1 load work->member,state mapping (parallel)"):
            mp_wm, mla_wm = await asyncio.gather(
                _fetch_with_timeout(db1,
                    "SELECT work_id, member_id, member_type, state_id FROM public.work_analysis",
                    "work_analysis member/state map (MP)"),
                _fetch_with_timeout(db1,
                    "SELECT work_id, member_id, member_type, state_id FROM public.mla_work_analysis",
                    "mla_work_analysis member/state map (MLA)"),
            )
            works = mp_wm + mla_wm
            _step(f"work map loaded: {len(mp_wm)} MP + {len(mla_wm)} MLA = {len(works)}")
    w2map = {w["work_id"]: dict(w) for w in works}
    for p in work_predictions:
        m = w2map.get(p["work_id"])
        if not m:
            continue
        pred_by_member[(m["member_type"], m["member_id"])].append(p["delay_probability"])
        if m.get("state_id"):
            pred_by_state[m["state_id"]].append(p["delay_probability"])
    for i in iso_results:
        m = w2map.get(i["work_id"])
        if not m:
            continue
        iso_by_member[(m["member_type"], m["member_id"])].append(i["isolation_score"])
        if m.get("state_id"):
            iso_by_state[m["state_id"]].append(i["isolation_score"])
    _step(f"aggregated per-work evidence: delay={len(pred_by_member)} members/{len(pred_by_state)} states; "
          f"iso={len(iso_by_member)} members/{len(iso_by_state)} states")

    def _agg(delay_vals, iso_vals):
        """Merge delay probabilities and isolation scores into one evidence record."""
        out = {"mean_delay_prob": None, "isolation_95": None, "n": 0}
        if delay_vals:
            arr = np.array(delay_vals, dtype=float)
            out["mean_delay_prob"] = float(np.mean(arr))
            out["n"] += int(arr.size)
        if iso_vals:
            arr = np.array(iso_vals, dtype=float)
            out["isolation_95"] = float(np.percentile(arr, 95))
            out["n"] += int(arr.size)
        return out

    member_keys = set(pred_by_member) | set(iso_by_member)
    work_risk_m = {k: _agg(pred_by_member.get(k, []), iso_by_member.get(k, [])) for k in member_keys}
    state_keys = set(pred_by_state) | set(iso_by_state)
    work_risk_s = {sid: _agg(pred_by_state.get(sid, []), iso_by_state.get(sid, [])) for sid in state_keys}

    members, states = await _load_db2(db2)
    ent_anom_m = {(m["member_type"], m["member_id"]): float(m.get("anomaly_score") or 0)
                  for m in members}
    ent_anom_s = {int(s["state_id"]): float(s.get("anomaly_score") or 0) for s in states}

    with _timed("compute member risks"):
        m_risks = risk_mod.compute_member_risks(members, work_risk_m, ent_anom_m)
    with _timed("compute state risks"):
        s_risks = risk_mod.compute_state_risks(states, work_risk_s, ent_anom_s)

    n_member_risked = sum(1 for v in m_risks.values() if v.get("score") is not None)
    n_state_risked = sum(1 for v in s_risks.values() if v.get("score") is not None)
    print(f"  member risks: {n_member_risked} computed; state risks: {n_state_risked}", flush=True)

    if apply:
        with _timed("DB2 UPSERT risk_* into member_intelligence + state_intelligence"):
            async with db2.acquire() as c:
                m_items = list(m_risks.items())
                for chunk_start in range(0, len(m_items), 100):
                    chunk = m_items[chunk_start:chunk_start + 100]
                    await c.executemany(
                        """UPDATE public.member_intelligence SET risk_score=$1, risk_level=$2,
                                risk_confidence=$3, risk_evidence=$4::jsonb, calculated_at=NOW()
                           WHERE member_id=$5 AND member_type=$6""",
                        [(r["score"], r["level"], r["confidence"],
                          json.dumps(r["evidence"]), mid, mt)
                         for (mt, mid), r in chunk],
                    )
                    _step(f"member risks persisted: {min(chunk_start + 100, len(m_items))}/{len(m_items)}")
                s_items = list(s_risks.items())
                for chunk_start in range(0, len(s_items), 100):
                    chunk = s_items[chunk_start:chunk_start + 100]
                    await c.executemany(
                        """UPDATE public.state_intelligence SET risk_score=$1, risk_level=$2,
                                risk_confidence=$3, risk_evidence=$4::jsonb, calculated_at=NOW()
                           WHERE state_id=$5""",
                        [(r["score"], r["level"], r["confidence"],
                          json.dumps(r["evidence"]), int(sid))
                         for sid, r in chunk],
                    )
                    _step(f"state risks persisted: {min(chunk_start + 100, len(s_items))}/{len(s_items)}")
        _step("risks persisted")
    TIMER.done("Risk engine")
    return m_risks, s_risks


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="Write changes (default: dry-run)")
    ap.add_argument("--scope", type=str, default=None,
                    help="JSON file with AffectedScope for incremental run. "
                         "If provided, only processes affected members/states.")
    ap.add_argument("--parallel", action="store_true", default=True,
                    help="Enable parallel independent stages (default: True)")
    ap.add_argument("--no-parallel", dest="parallel", action="store_false",
                    help="Disable parallel independent stages")
    args = ap.parse_args()
    apply = args.apply
    enable_parallel = args.parallel

    db1_url = os.environ.get("DATABASE_URL")
    db2_url = os.environ.get("DB2_DATABASE_URL")
    if not db1_url or not db2_url:
        print("ERROR: DATABASE_URL and DB2_DATABASE_URL required", flush=True)
        sys.exit(1)

    # Load scope if provided
    scope = None
    if args.scope:
        try:
            with open(args.scope) as f:
                scope = json.load(f)
            _step(f"Loaded affected scope: {scope.get('affected_works', 0)} works, "
                  f"{scope.get('affected_members', 0)} members")
        except Exception as e:
            _step(f"WARNING: Could not load scope file: {e}, running full backfill")
            scope = None

    print("==============================================================", flush=True)
    print(f" GovSense intelligence backfill  ({'APPLY' if apply else 'DRY-RUN'})", flush=True)
    print(f" started: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}", flush=True)
    print(f" db1 host: {db1_url.split('@')[-1] if '@' in db1_url else 'env DATABASE_URL'}", flush=True)
    print(f" db2 host: {db2_url.split('@')[-1] if '@' in db2_url else 'env DB2_DATABASE_URL'}", flush=True)
    print(f" parallel: {enable_parallel}", flush=True)
    print(f" scope: {'incremental' if scope else 'full'}", flush=True)
    print("==============================================================", flush=True)

    timer = PipelineTimer(run_id="intelligence_backfill",
                          mode="incremental" if scope else "full")

    # EARLY EXIT: If scope is provided but has 0 affected works/members,
    # skip all DB work entirely. No pools needed, no stages to run.
    if scope and scope.get("affected_works", 0) == 0 and scope.get("affected_members", 0) == 0:
        print("\n==============================================================", flush=True)
        print(" SKIPPING: scope has 0 affected works and 0 affected members", flush=True)
        print(" No intelligence backfill needed.", flush=True)
        print("==============================================================", flush=True)
        return

    try:
        # Use shared pools from db_pool module
        await init_pools(db1_url=db1_url, db2_url=db2_url)
        db1 = get_db1_pool()
        db2 = get_db2_pool()
        _step("Shared DB1 + DB2 pools initialized")

        t_total = time.monotonic()

        # Stage 1: Allocation matching
        m = timer.begin("allocation")
        counts = await stage_allocation(db1, db2, apply)
        m.affected_count = counts.get("mp", 0) + counts.get("mla", 0)
        timer.end("allocation")

        # Stage 2: Performance score + rank
        m = timer.begin("score_ranking")
        m_scores, s_scores = await stage_score_and_ranking(db2, apply)
        m.affected_count = len(m_scores)
        timer.end("score_ranking")

        # Stage 3: K-Means profiling (depends on score)
        m = timer.begin("profiling")
        m_clu, s_clu = await stage_profiling(db2, m_scores, s_scores, apply)
        m.affected_count = len(m_clu)
        timer.end("profiling")

        # Stages 4-5: XGBoost + Isolation Forest (independent of profiling)
        # Can run in parallel with stage 6 (risk) if no data dependency
        m45 = timer.begin("xgb_isolation")
        preds, iso, works = await stage_project_and_anomaly(db1, db2, apply)
        m45.affected_count = len(iso)
        timer.end("xgb_isolation")

        # Stage 6: Risk engine (depends on score + anomaly results)
        m6 = timer.begin("risk")
        await stage_risk(db1, db2, m_scores, s_scores, preds, iso, apply, works=works)
        m6.affected_count = len(m_scores)
        timer.end("risk")

        total = time.monotonic() - t_total
        print("\n==============================================================", flush=True)
        print(f" {'APPLIED' if apply else 'DRY-RUN COMPLETE'}", flush=True)
        print(f" total wall time: {_hr(total)}", flush=True)
        print("==============================================================", flush=True)

        timer.print_summary()

    finally:
        await close_pools()
        _step("Shared connection pools closed")


if __name__ == "__main__":
    asyncio.run(main())
