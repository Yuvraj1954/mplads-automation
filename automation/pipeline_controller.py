"""End-to-end pipeline controller for two-database architecture.

DB1 = CORE / SOURCE   (source records, ingestion, operational state)
DB2 = INTELLIGENCE     (analysis results, evidence, Gemini, AI outputs)

Orchestrates the complete flow:
    1. Fetch            (DB1 storage)
    2. Compare          (local SQLite)
    3. Delta            (local files)
    4. DB1 Ingestion    (Edge Functions → DB1)
    5. Affected Entities (delta → affected set)
    6. Deterministic Analysis (Python pipeline)
    7. Entity Anomaly   (robust z-score scoring)
    8. Evidence Update  (→ DB2)
    9. Gemini Processing (affected-only → DB2)
    10. DB2 Persistence  (upsert results)
    11. Verification     (both databases)
    12. Cleanup          (temporary artifacts only)

Design principles:
    - Affected-only processing at every stage
    - Idempotent at every stage
    - Verification gates before cleanup
    - Checkpoint/resume support
    - SOURCE ABSENCE ≠ DATABASE DELETION
    - No cross-database SQL joins
    - Python bridges DB1 read → calculation → DB2 write
"""

import asyncio
import json
import os
import sys
import time
import shutil
import urllib.request
import urllib.parse
import ssl
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from automation.db_config import load_config, get_config, get_db1, get_db2, SSL_CTX

PIPELINE_VERSION = "pipeline_v2"
STAGES = [
    "fetch", "compare", "delta", "ingest", "affected",
    "analyze", "work_analysis_persist", "anomaly", "analytics_persist",
    "evidence", "evidence_work_refs", "gemini",
    "persist", "verify", "cleanup",
]

BUCKET = "mplads-raw"


def sb_get(url, key, table, params=None):
    """Supabase REST GET."""
    query = ""
    if params:
        query = "?" + urllib.parse.urlencode(params, doseq=True)
    req = urllib.request.Request(
        f"{url}/rest/v1/{table}{query}",
        headers={
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=60, context=SSL_CTX) as resp:
        return json.loads(resp.read())


def sb_post(url, key, table, body, extra_headers=None):
    """Supabase REST POST."""
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }
    if extra_headers:
        headers.update(extra_headers)
    req = urllib.request.Request(
        f"{url}/rest/v1/{table}",
        data=json.dumps(body).encode(),
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60, context=SSL_CTX) as resp:
        return resp.status


_sb_clients = {}

def sb_upsert(url, key, table, body, conflict_cols=None):
    """Supabase UPSERT via supabase-py client (bulk upsert)."""
    from supabase import create_client
    client_key = url + key
    if client_key not in _sb_clients:
        _sb_clients[client_key] = create_client(url, key)
    client = _sb_clients[client_key]
    if not body:
        return 200
    try:
        if conflict_cols:
            client.table(table).upsert(body, on_conflict=",".join(conflict_cols)).execute()
        else:
            client.table(table).upsert(body).execute()
    except Exception as e:
        print(f"  WARNING: bulk upsert to {table} failed ({len(body)} rows): {e}")
        for row in body:
            try:
                if conflict_cols:
                    client.table(table).upsert(row, on_conflict=",".join(conflict_cols)).execute()
                else:
                    client.table(table).upsert(row).execute()
            except Exception as e2:
                print(f"  WARNING: upsert to {table} failed for {row.get('entity_type','?')}:{row.get('entity_id','?')}: {e2}")
    return 200


def sb_delete(url, key, table, filters):
    """Supabase REST DELETE with filters."""
    params = "&".join(f"{k}={v}" for k, v in filters.items())
    req = urllib.request.Request(
        f"{url}/rest/v1/{table}?{params}",
        headers={
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        },
        method="DELETE",
    )
    with urllib.request.urlopen(req, timeout=60, context=SSL_CTX) as resp:
        return resp.status


def load_table(url, key, table, select="*", filters=None):
    """Load all rows from a table with pagination."""
    params = {"select": select, "limit": "1000", "offset": "0"}
    if filters:
        params.update(filters)
    all_rows = []
    offset = 0
    while True:
        params["offset"] = str(offset)
        batch = sb_get(url, key, table, params)
        if not batch:
            break
        all_rows.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
    return all_rows


def recompute_all_ranks(db2_url, db2_key):
    """Recompute rank/scale_score/performance_score_weighted for the WHOLE
    member_metrics and state_metrics tables.

    Phase 2 optimized: Uses SQL window functions when asyncpg is available,
    falls back to Python computation otherwise.

    The SQL approach:
    - Computes RANK() and PERCENT_RANK() in a single pass per member_type
    - No data transfer to Python — all computation in Postgres
    - Writes only the 3 ranking columns via UPDATE ... FROM subquery

    Only the three ranking columns are written; raw metrics are untouched.
    """
    try:
        import asyncpg
        import os
        return _recompute_all_ranks_sql(db2_url)
    except (ImportError, Exception) as e:
        print(f"  SQL ranking unavailable ({e}), falling back to Python")
        return _recompute_all_ranks_python(db2_url, db2_key)


def _recompute_all_ranks_sql(db2_url):
    """SQL-based ranking using Postgres window functions.

    Single SQL pass per member_type for members, single pass for states.
    No data transfer to Python — all computation in Postgres.

    db2_url is the Supabase REST URL (for interface compatibility), but
    asyncpg needs the direct PostgreSQL DSN. We read DB2_DATABASE_URL
    from the environment for the actual connection.
    """
    import asyncio
    import asyncpg
    import os

    pg_dsn = os.environ.get("DB2_DATABASE_URL", "")
    if not pg_dsn:
        raise RuntimeError("DB2_DATABASE_URL not set — cannot run SQL ranking")

    async def _run():
        pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2,
                                          statement_cache_size=0, command_timeout=120)
        try:
            async with pool.acquire() as conn:
                # Member ranking: RANK() within each member_type
                # scale_score is computed by db2_analytics_persistence.compute_member_ranks()
                # and must NOT be overwritten here.
                await conn.execute("""
                    WITH ranked AS (
                        SELECT
                            member_id,
                            member_type,
                            RANK() OVER (
                                PARTITION BY member_type
                                ORDER BY performance_score_weighted DESC NULLS LAST
                            ) AS new_rank
                        FROM member_metrics
                        WHERE performance_score_weighted IS NOT NULL
                          AND ranking_qualified = true
                    )
                    UPDATE member_metrics m
                    SET
                        rank = r.new_rank
                    FROM ranked r
                    WHERE m.member_id = r.member_id
                      AND m.member_type = r.member_type
                """)
                member_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM member_metrics WHERE rank IS NOT NULL"
                )
                print(f"  SQL member ranking: {member_count} rows updated")

                # State ranking
                # scale_score is computed by db2_analytics_persistence.compute_state_ranks()
                # and must NOT be overwritten here.
                await conn.execute("""
                    WITH ranked AS (
                        SELECT
                            state_id,
                            RANK() OVER (
                                ORDER BY performance_score_weighted DESC NULLS LAST
                            ) AS new_rank
                        FROM state_metrics
                        WHERE performance_score_weighted IS NOT NULL
                    )
                    UPDATE state_metrics s
                    SET
                        rank = r.new_rank
                    FROM ranked r
                    WHERE s.state_id = r.state_id
                """)
                state_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM state_metrics WHERE rank IS NOT NULL"
                )
                print(f"  SQL state ranking: {state_count} rows updated")

        finally:
            await pool.close()

    try:
        try:
            loop = asyncio.get_running_loop()
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(asyncio.run, _run())
                future.result(timeout=120)
        except RuntimeError:
            asyncio.run(_run())
    except Exception as e:
        raise RuntimeError(f"SQL ranking failed: {e}") from e


def _recompute_all_ranks_python(db2_url, db2_key):
    """Python-based ranking fallback (original implementation)."""
    from analysis.db2_analytics_persistence import (
        compute_member_ranks,
        compute_state_ranks,
    )

    member_cols = (
        "member_id,member_type,total_works,completion_rate_pct,"
        "fund_utilization_pct,ranking_qualified"
    )
    members = load_table(db2_url, db2_key, "member_metrics", select=member_cols)
    if members:
        compute_member_ranks(members)
        payload = [
            {
                "member_id": r["member_id"],
                "member_type": r["member_type"],
                "rank": r.get("rank"),
                "scale_score": r.get("scale_score"),
                "performance_score_weighted": r.get("performance_score_weighted"),
            }
            for r in members
        ]
        for start in range(0, len(payload), 200):
            sb_upsert(db2_url, db2_key, "member_metrics",
                      payload[start:start + 200],
                      conflict_cols=["member_id", "member_type"])
        print(f"  Re-ranked {len(payload)} member rows across all populations")

    state_cols = (
        "state_id,total_works,completion_rate_pct,fund_utilization_pct"
    )
    states = load_table(db2_url, db2_key, "state_metrics", select=state_cols)
    if states:
        compute_state_ranks(states)
        payload = [
            {
                "state_id": r["state_id"],
                "rank": r.get("rank"),
                "scale_score": r.get("scale_score"),
                "performance_score_weighted": r.get("performance_score_weighted"),
            }
            for r in states
        ]
        sb_upsert(db2_url, db2_key, "state_metrics", payload,
                  conflict_cols=["state_id"])
        print(f"  Re-ranked {len(payload)} state rows")

def _checkpoint_path(run_id):
    """Return path to checkpoint file for a run."""
    return ROOT / ".pipeline_checkpoints" / f"{run_id}.json"


def save_checkpoint(run_id, stage, data=None):
    """Save pipeline checkpoint after completing a stage."""
    cp_dir = ROOT / ".pipeline_checkpoints"
    cp_dir.mkdir(exist_ok=True)

    path = _checkpoint_path(run_id)
    checkpoint = {}
    if path.exists():
        checkpoint = json.loads(path.read_text())

    checkpoint["last_completed_stage"] = stage
    checkpoint["last_updated"] = _now_iso()
    if data:
        checkpoint.setdefault("stage_data", {})[stage] = data

    path.write_text(json.dumps(checkpoint, indent=2))


def load_checkpoint(run_id):
    """Load checkpoint for a run. Returns dict or empty dict."""
    path = _checkpoint_path(run_id)
    if path.exists():
        return json.loads(path.read_text())
    return {}


def next_stage_after(run_id, current_stage):
    """Determine the next stage to run based on checkpoint."""
    checkpoint = load_checkpoint(run_id)
    last = checkpoint.get("last_completed_stage")
    if last is None:
        return STAGES[0]
    try:
        idx = STAGES.index(last)
        if idx + 1 < len(STAGES):
            return STAGES[idx + 1]
    except ValueError:
        pass
    return current_stage

def _timer():
    return time.time()


def _elapsed(start):
    return time.time() - start


def _print_timing(timing, label="STAGE"):
    for key, val in timing.items():
        print(f"  [{label}] {key}: {val:.1f}s")


def stage_affected(delta_dir=None, run_id=None):
    """Stage 5: Determine affected works/entities from delta.

    Reads delta files to identify which members/states are affected
    by newly added or updated works.

    Uses WORK_RECOMMENDATION_DTL_ID to look up internal work_ids via DB1
    (not raw mp_id/mla_id from government records).

    Returns:
        dict with affected entity keys
    """
    print("\n=== STAGE 5: AFFECTED ENTITIES ===")

    affected_work_dtls = []
    affected_work_ids = set()

    if delta_dir and Path(delta_dir).exists():
        for dataset_dir in Path(delta_dir).iterdir():
            if not dataset_dir.is_dir():
                continue
            is_mla = dataset_dir.name.startswith("mla_")
            for part_file in dataset_dir.glob("*_part_*.ndjson"):
                with open(part_file) as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            record = json.loads(line)
                        except json.JSONDecodeError:
                            continue

                        dtl_id = record.get("WORK_RECOMMENDATION_DTL_ID")
                        if dtl_id:
                            dtl_str = str(dtl_id)
                            affected_work_dtls.append(dtl_str)
                            snapshot_work_id = int(dtl_str) + (1_000_000 if is_mla else 0)
                            affected_work_ids.add(snapshot_work_id)

    print(f"  Delta work DTL IDs: {len(affected_work_dtls)}")
    print(f"  Snapshot work IDs: {len(affected_work_ids)}")

    # Member and state identity is derived in stage_analyze_affected()
    # after pipeline runs, from affected_work_ids + pipeline.work_analyses.
    # No DB1 lookup needed — DTL_ID is the universal identity bridge.
    affected_members = set()
    affected_states = set()

    result = {
        "affected_members": affected_members,
        "affected_states": affected_states,
        "affected_work_ids": affected_work_ids,
        "affected_work_dtls": affected_work_dtls,
        "member_count": len(affected_members),
        "state_count": len(affected_states),
        "work_count": len(affected_work_ids),
    }

    print(f"  Affected members: {result['member_count']}")
    print(f"  Affected states: {result['state_count']}")
    print(f"  Affected works (internal): {result['work_count']}")

    return result


def stage_analyze(snapshot_dir, reference_date=None):
    """Stage 6: Run deterministic analysis on a snapshot directory.

    Reads from local snapshot (originally from DB1 storage).
    Produces work_analyses, member_metrics, state_metrics, statistics, trends.
    Injects zero-work members from allocated_limit snapshots.

    Returns:
        dict with analysis results
    """
    from analysis.pipeline import AnalysisPipeline
    from analysis.snapshot_loader import load_all_data, build_work_records, build_db1_member_map
    from analysis.entity_anomaly import compute_entity_anomalies, compute_state_anomalies
    from analysis.evidence_builder import (
        build_member_evidence, build_state_evidence,
        group_works_by_member, group_works_by_state,
    )
    from analysis.zero_work_members import inject_zero_work_members, get_master_population_context

    import time as _t
    t_stage = _t.time()
    print("\n=== STAGE 6: ANALYZE ===", flush=True)

    # Build authoritative member ID mapping from DB1
    print("  [1/7] Building DB1 member map...", end=" ", flush=True)
    t0 = _t.time()
    db1_url, db1_key = get_db1()
    db1_member_map = build_db1_member_map(db1_url, db1_key)
    print(f"OK ({len(db1_member_map)} members, {_t.time()-t0:.1f}s)", flush=True)

    # Load data directly from NDJSON snapshot files
    print("  [2/7] Loading NDJSON snapshot...", end=" ", flush=True)
    t0 = _t.time()
    data = load_all_data(snapshot_dir)
    print(f"OK ({_t.time()-t0:.1f}s)", flush=True)

    print("  [3/7] Building work records...", end=" ", flush=True)
    t0 = _t.time()
    works, state_map, constituency_map, member_map = build_work_records(
        *data, db1_member_map=db1_member_map
    )
    print(f"OK ({len(works)} works, {_t.time()-t0:.1f}s)", flush=True)

    # Run pipeline on work-bearing members
    print("  [4/7] Running AnalysisPipeline.run_full()...", end=" ", flush=True)
    t0 = _t.time()
    pipeline = AnalysisPipeline(works)
    pipeline.run_full(reference_date)
    print(f"OK ({len(pipeline.member_metrics)} members, {_t.time()-t0:.1f}s)", flush=True)

    print(f"  Members analyzed (work-bearing): {len(pipeline.member_metrics)}", flush=True)
    print(f"  States analyzed: {len(pipeline.state_metrics)}", flush=True)
    print(f"  Work analyses: {len(pipeline.work_analyses)}", flush=True)

    # Populate member_name on MemberMetrics from works data
    print("  [5/7] Populating names/states...", end=" ", flush=True)
    t0 = _t.time()
    member_name_map = {}
    for w in works:
        key = (w.get("member_type"), w.get("member_id"))
        if key not in member_name_map:
            mp_name = w.get("mp_name", "")
            if mp_name:
                member_name_map[key] = mp_name
    for m in pipeline.member_metrics:
        if not m.member_name:
            m.member_name = member_name_map.get((m.member_type, m.member_id))

    # Populate state_name on StateMetrics and MemberMetrics from works data
    state_name_map = {}
    for w in works:
        state_id = w.get("state_id")
        state_name = w.get("state_name", "")
        if state_id and state_name and state_id not in state_name_map:
            state_name_map[state_id] = state_name
    for s in pipeline.state_metrics:
        if not s.state_name:
            s.state_name = state_name_map.get(s.state_id)
    for m in pipeline.member_metrics:
        if not m.state_name and m.state_id:
            m.state_name = state_name_map.get(m.state_id)
    print(f"OK ({_t.time()-t0:.1f}s)", flush=True)

    # Inject zero-work members from DB1 canonical population
    print("  [6/7] Injecting zero-work members...", end=" ", flush=True)
    t0 = _t.time()
    pipeline.member_metrics = inject_zero_work_members(
        pipeline.member_metrics,
        snapshot_dir,
        db1_member_map=db1_member_map,
        state_metrics_by_id={s.state_id: s for s in pipeline.state_metrics},
    )
    print(f"OK ({_t.time()-t0:.1f}s)", flush=True)

    # Update member_metrics_by_id to include zero-work members
    pipeline.member_metrics_by_id = {
        (m.member_type, m.member_id): m
        for m in pipeline.member_metrics
    }

    # Get master population context for evidence building
    print("  [7/7] Building master population context...", end=" ", flush=True)
    t0 = _t.time()
    master_context = get_master_population_context(pipeline.member_metrics)
    print(f"OK ({_t.time()-t0:.1f}s)", flush=True)

    print(f"  Total members (after injection): {len(pipeline.member_metrics)}", flush=True)
    print(f"  STAGE 6 TOTAL: {_t.time()-t_stage:.1f}s", flush=True)

    return {
        "pipeline": pipeline,
        "work_analyses": pipeline.work_analyses,
        "master_population_context": master_context,
    }


def stage_analyze_affected(snapshot_dir, affected_result, reference_date=None):
    """Stage 6a: Run deterministic analysis for affected members only.

    For affected mode, we still load the full snapshot (member metrics
    depend on ALL works belonging to a member), but only persist the
    affected subset.

    The key optimization is that affected-mode skips stages that are
    only needed for full recompute (e.g., full evidence rebuild).

    Returns:
        dict with analysis results (same structure as stage_analyze)
    """
    from analysis.pipeline import AnalysisPipeline
    from analysis.snapshot_loader import load_all_data, build_work_records, build_db1_member_map

    print("\n=== STAGE 6a: ANALYZE (AFFECTED ONLY) ===")

    affected_members = affected_result.get("affected_members", set())
    affected_states = affected_result.get("affected_states", set())

    # Build authoritative member ID mapping from DB1
    db1_url, db1_key = get_db1()
    db1_member_map = build_db1_member_map(db1_url, db1_key)

    data = load_all_data(snapshot_dir)
    works, state_map, constituency_map, member_map = build_work_records(
        *data, db1_member_map=db1_member_map
    )

    pipeline = AnalysisPipeline(works)
    pipeline.run_full(reference_date)

    print(f"  Members analyzed (full population): {len(pipeline.member_metrics)}")
    print(f"  States analyzed (full population): {len(pipeline.state_metrics)}")

    member_name_map = {}
    for w in works:
        key = (w.get("member_type"), w.get("member_id"))
        if key not in member_name_map:
            mp_name = w.get("mp_name", "")
            if mp_name:
                member_name_map[key] = mp_name
    for m in pipeline.member_metrics:
        if not m.member_name:
            m.member_name = member_name_map.get((m.member_type, m.member_id))

    state_name_map = {}
    for w in works:
        state_id = w.get("state_id")
        state_name = w.get("state_name", "")
        if state_id and state_name and state_id not in state_name_map:
            state_name_map[state_id] = state_name
    for s in pipeline.state_metrics:
        if not s.state_name:
            s.state_name = state_name_map.get(s.state_id)
    for m in pipeline.member_metrics:
        if not m.state_name and m.state_id:
            m.state_name = state_name_map.get(m.state_id)

    # Phase 4: inject the authoritative zero-work master population in
    # affected mode too, exactly as the full path does. Without this,
    # pipeline.member_metrics only contains work-bearing members and
    # build_overall_metrics would overwrite overall_metrics.total_members
    # with the work-bearing count (the observed population mismatch).
    try:
        from analysis.zero_work_members import inject_zero_work_members
        pipeline.member_metrics = inject_zero_work_members(
            pipeline.member_metrics,
            snapshot_dir,
            db1_member_map=db1_member_map,
            state_metrics_by_id={s.state_id: s for s in pipeline.state_metrics},
        )
        print(f"  Total members (after injection): {len(pipeline.member_metrics)}")
    except Exception as _e:
        print(f"  [warn] zero-work injection skipped: {_e}")

    # Filter work_analyses to affected works only
    affected_work_ids = affected_result.get("affected_work_ids", set())
    affected_work_analyses = [
        wa for wa in pipeline.work_analyses
        if wa.work_id in affected_work_ids
    ] if affected_work_ids else pipeline.work_analyses

    # Also include time-sensitive works — only for members already affected
    # by the delta. Without this filter, expand_time_sensitive scans the
    # entire snapshot and pulls in hundreds of unrelated members.
    from analysis.affected import expand_time_sensitive
    works_by_id = {w["work_id"]: w for w in works}

    delta_members = set()
    for wid in affected_work_ids:
        w = works_by_id.get(wid)
        if w:
            delta_members.add((w.get("member_type"), w.get("member_id")))

    time_sensitive = expand_time_sensitive(works_by_id, {}, reference_date,
                                          affected_members=delta_members)
    time_sensitive_analyses = [
        wa for wa in pipeline.work_analyses
        if wa.work_id in time_sensitive and wa.work_id not in affected_work_ids
    ]
    affected_work_analyses.extend(time_sensitive_analyses)

    # Derive affected member/state identity from affected work analyses.
    # This bridges DTL_ID → snapshot work_id → (member_type, member_id) / state_id.
    derived_members = set()
    derived_states = set()
    for wa in affected_work_analyses:
        derived_members.add((wa.member_type, wa.member_id))
        if wa.state_id:
            derived_states.add(wa.state_id)

    # Update affected_result in-place so downstream stages get correct identity
    if derived_members:
        affected_result["affected_members"] = derived_members
        affected_result["member_count"] = len(derived_members)
    if derived_states:
        affected_result["affected_states"] = derived_states
        affected_result["state_count"] = len(derived_states)

    # Re-filter using derived identity
    affected_members = affected_result.get("affected_members", set())
    affected_states = affected_result.get("affected_states", set())

    affected_member_metrics = [
        m for m in pipeline.member_metrics
        if (m.member_type, m.member_id) in affected_members
    ]
    affected_state_metrics = [
        s for s in pipeline.state_metrics
        if s.state_id in affected_states
    ]

    print(f"  Affected member metrics: {len(affected_member_metrics)}")
    print(f"  Affected state metrics: {len(affected_state_metrics)}")
    print(f"  Affected work analyses: {len(affected_work_analyses)}")

    return {
        "pipeline": pipeline,
        "work_analyses": affected_work_analyses,
        "affected_member_metrics": affected_member_metrics,
        "affected_state_metrics": affected_state_metrics,
        "is_affected_mode": True,
    }


def stage_work_analysis_persist(work_analyses, affected_work_ids=None):
    """Stage 6b: Persist work analysis to DB2.work_analysis / DB2.mla_work_analysis.

    DB2 is the canonical destination for work_analysis. The backend reads
    from DB2; DB1 is raw/source only.

    For affected mode: deletes old records first (to satisfy the unique
    constraint on work_id), then inserts fresh records.

    ML lifecycle: Before DELETE, fetches existing ML columns + feature_fingerprint
    for affected work_ids. After INSERT, restores ML values WHERE the feature
    fingerprint matches (deterministic features unchanged → ML prediction still
    valid). ML values are left NULL where features changed (stale) or for new
    works (uncomputed). Intelligence backfill later fills NULLs.

    Uses bounded parallelism for the delete phase (the bottleneck) when
    there are affected work_ids. MP and MLA tables are processed in parallel.

    Args:
        work_analyses: list of WorkAnalysis objects from stage_analyze
        affected_work_ids: set of snapshot work_ids that are affected.
                          If None, persists all (full mode).

    Returns:
        dict with write stats including ML preservation metrics
    """
    import hashlib
    import threading
    from concurrent.futures import ThreadPoolExecutor, as_completed

    print("\n=== STAGE 6b: WORK ANALYSIS PERSIST (DB2) ===")

    db2_url, db2_key = get_db2()
    now_str = datetime.now(timezone.utc).isoformat()

    # Isolation Forest feature columns used for fingerprint computation.
    # These 8 features are the sole ML inputs for anomaly detection.
    # If any change, the isolation_score/isolation_level may be stale.
    _ML_FEATURE_COLS = (
        "sanction_amount", "recommended_amount", "expenditure_amount",
        "sanction_delay_days", "execution_days", "project_age_days",
        "cost_percentile", "duration_percentile",
    )

    def _compute_feature_fingerprint(wa):
        """Compute SHA-256 fingerprint of ML-relevant deterministic features.

        Returns a hex digest string. If any feature is None, uses empty string.
        Two works with identical ML inputs will have identical fingerprints.
        """
        parts = []
        for col in _ML_FEATURE_COLS:
            val = getattr(wa, col, None)
            parts.append(f"{col}={val}")
        raw = "|".join(parts)
        return hashlib.sha256(raw.encode()).hexdigest()

    # Pre-check: verify both tables exist on DB2 before attempting writes.
    # Retry once after 3s if PostgREST schema cache is stale (PGRST205).
    import time as _time
    from supabase import create_client as _cc
    _probe = _cc(db2_url, db2_key)
    for tbl in ("work_analysis", "mla_work_analysis"):
        ok = False
        for attempt in range(2):
            try:
                _probe.table(tbl).select("work_id", count="exact").limit(0).execute()
                ok = True
                break
            except Exception as e:
                if attempt == 0:
                    print(f"  WARNING: table '{tbl}' not visible in schema cache (attempt 1/2), retrying in 3s ...")
                    _time.sleep(3)
                else:
                    print(f"  FATAL: table '{tbl}' not accessible on DB2: {e}")
                    raise RuntimeError(f"Table '{tbl}' missing from DB2.")
        if ok:
            print(f"  table '{tbl}': ACCESSIBLE")

    # Separate MP and MLA
    mp_analyses = [wa for wa in work_analyses if wa.member_type == "MP"]
    mla_analyses = [wa for wa in work_analyses if wa.member_type == "MLA"]

    if affected_work_ids is not None:
        mp_analyses = [wa for wa in mp_analyses if wa.work_id in affected_work_ids]
        mla_analyses = [wa for wa in mla_analyses if wa.work_id in affected_work_ids]

    print(f"  MP analyses to persist: {len(mp_analyses)}")
    print(f"  MLA analyses to persist: {len(mla_analyses)}")

    # ── ML preservation: fetch existing ML + fingerprints before DELETE ──
    # In affected mode, we need to save ML values for works whose deterministic
    # features haven't changed (fingerprint match). This prevents the DELETE+INSERT
    # from wiping valid ML predictions.
    existing_ml = {}  # work_id → {delay_probability, delay_risk_band, isolation_score, isolation_level, feature_fingerprint}
    if affected_work_ids is not None and affected_work_ids:
        ml_fetch_start = time.time()
        for table_name in ("work_analysis", "mla_work_analysis"):
            try:
                all_ids = list(affected_work_ids)
                batch_size = 200
                for start in range(0, len(all_ids), batch_size):
                    batch = all_ids[start:start + batch_size]
                    resp = (
                        _probe.table(table_name)
                        .select("work_id,delay_probability,delay_risk_band,"
                                "isolation_score,isolation_level,feature_fingerprint")
                        .in_("work_id", batch)
                        .execute()
                    )
                    for row in (resp.data or []):
                        wid = row["work_id"]
                        existing_ml[wid] = {
                            "delay_probability": row.get("delay_probability"),
                            "delay_risk_band": row.get("delay_risk_band"),
                            "isolation_score": row.get("isolation_score"),
                            "isolation_level": row.get("isolation_level"),
                            "feature_fingerprint": row.get("feature_fingerprint"),
                        }
            except Exception as e:
                print(f"  WARNING: could not fetch existing ML for {table_name}: {e}")
                # If feature_fingerprint column doesn't exist yet, that's OK —
                # all works will be treated as needing recomputation.
        ml_fetch_elapsed = time.time() - ml_fetch_start
        print(f"  ML preservation: fetched {len(existing_ml)} existing records ({ml_fetch_elapsed:.1f}s)")

    def _work_analysis_to_record(wa):
        """Convert WorkAnalysis dataclass to DB2 record dict."""
        return {
            "work_id": wa.work_id,
            "member_id": wa.member_id,
            "member_type": wa.member_type,
            "constituency_id": wa.constituency_id,
            "state_id": wa.state_id,
            "state_name": getattr(wa, "state_name", ""),
            "work_category": getattr(wa, "work_category", ""),
            "activity_name": getattr(wa, "activity_name", ""),
            "normalized_activity": wa.normalized_activity,
            "work_description": getattr(wa, "work_description", ""),
            "status": wa.status,
            "recommended_amount": wa.recommended_amount,
            "sanction_amount": wa.sanction_amount,
            "expenditure_amount": wa.expenditure_amount,
            "completion_amount": wa.completion_amount,
            "recommendation_date": str(wa.recommendation_date) if wa.recommendation_date else None,
            "sanction_date": str(wa.sanction_date) if wa.sanction_date else None,
            "first_expenditure_date": str(getattr(wa, "first_expenditure_date", None)) if getattr(wa, "first_expenditure_date", None) else None,
            "last_expenditure_date": str(wa.last_expenditure_date) if wa.last_expenditure_date else None,
            "completion_date": str(wa.completion_date) if wa.completion_date else None,
            "sanction_delay_days": wa.sanction_delay_days,
            "project_age_days": wa.project_age_days,
            "execution_days": wa.execution_days,
            "pending_days": wa.pending_days,
            "expenditure_percentage": wa.expenditure_percentage,
            "completion_percentage": wa.completion_percentage,
            "benchmark_peer_group": wa.benchmark_peer_group,
            "benchmark_quality": wa.benchmark_quality,
            "benchmark_sample_size": wa.benchmark_sample_size,
            "cost_p25": wa.cost_p25,
            "cost_p50": wa.cost_p50,
            "cost_p75": wa.cost_p75,
            "cost_p90": wa.cost_p90,
            "cost_p95": wa.cost_p95,
            "duration_p25": wa.duration_p25,
            "duration_p50": wa.duration_p50,
            "duration_p75": wa.duration_p75,
            "duration_p90": wa.duration_p90,
            "duration_p95": wa.duration_p95,
            "cost_percentile": wa.cost_percentile,
            "duration_percentile": wa.duration_percentile,
            "cost_status": wa.cost_status,
            "duration_status": wa.duration_status,
            "cost_deviation_from_median_percentage": wa.cost_deviation_from_median_percentage,
            "duration_deviation_from_median_percentage": wa.duration_deviation_from_median_percentage,
            "risk_flags": wa.risk_flags if isinstance(wa.risk_flags, list) else [],
            "flag_count": wa.flag_count,
            "risk_level": wa.risk_level,
            "last_calculated": now_str,
            "feature_fingerprint": _compute_feature_fingerprint(wa),
        }

    def _insert_records(client, table_name, records, label):
        """Insert records in batches of 1000 with retry. Returns count inserted."""
        import time as _t
        batch_size = 1000
        batches = [records[start:start + batch_size] for start in range(0, len(records), batch_size)]
        inserted = 0
        t0 = time.time()

        for i, batch in enumerate(batches):
            for attempt in range(3):
                try:
                    client.table(table_name).insert(batch).execute()
                    inserted += len(batch)
                    break
                except Exception as e:
                    if attempt < 2:
                        wait = (attempt + 1) * 2
                        print(f"  {label} batch {i+1}/{len(batches)} retry {attempt+1} in {wait}s: {e}", flush=True)
                        _t.sleep(wait)
                    else:
                        print(f"  WARNING: batch {i+1}/{len(batches)} failed for {label}: {e}", flush=True)
                        # Fallback: try up to 10 rows individually
                        for row in batch[:10]:
                            try:
                                client.table(table_name).insert(row).execute()
                                inserted += 1
                            except Exception:
                                pass
            # Small delay between batches to avoid overwhelming the server
            if i < len(batches) - 1:
                _t.sleep(0.3)

        elapsed = time.time() - t0
        print(f"  {label}: inserted {inserted}/{len(records)} in {len(batches)} batches "
              f"({elapsed:.1f}s)", flush=True)
        return inserted

    def _restore_ml_values(client, table_name, records, label):
        """Restore ML values for works whose feature fingerprint matches.

        After INSERT, ML columns are NULL. For works where the deterministic
        features haven't changed (fingerprint matches), restore the existing
        ML values. For works where features changed or are new, leave NULL
        (intelligence backfill will recompute).

        Returns dict with preserved/stale/new counts.
        """
        if not existing_ml:
            # No existing ML data found for any affected work — all are new
            return {"preserved": 0, "stale": 0, "new": len(records)}

        preserved = 0
        stale = 0
        new = 0
        to_restore = []

        for rec in records:
            wid = rec["work_id"]
            old = existing_ml.get(wid)
            if old is None:
                new += 1
                continue
            old_fp = old.get("feature_fingerprint")
            new_fp = rec.get("feature_fingerprint")
            if old_fp and new_fp and old_fp == new_fp:
                # Features unchanged — safe to preserve ML values
                ml_update = {}
                for col in ("delay_probability", "delay_risk_band",
                            "isolation_score", "isolation_level"):
                    if old.get(col) is not None:
                        ml_update[col] = old[col]
                if ml_update:
                    ml_update["work_id"] = wid
                    to_restore.append(ml_update)
                    preserved += 1
                else:
                    # Old ML values were all NULL — treat as new
                    new += 1
            else:
                # Features changed — old ML prediction is stale
                stale += 1

        # Batched restore via upsert (1000 per batch, sequential with retry)
        if to_restore:
            import time as _t
            batch_size = 1000
            batches = [to_restore[start:start + batch_size] for start in range(0, len(to_restore), batch_size)]

            for i, batch in enumerate(batches):
                for attempt in range(3):
                    try:
                        client.table(table_name).upsert(batch, on_conflict="work_id").execute()
                        break
                    except Exception as e:
                        if attempt < 2:
                            _t.sleep((attempt + 1) * 2)
                        else:
                            print(f"  WARNING: ML restore batch {i+1} failed for {label}: {e}")
                            for row in batch[:10]:
                                try:
                                    wid = row.pop("work_id")
                                    client.table(table_name).update(row).eq("work_id", wid).execute()
                                except Exception:
                                    pass
                if i < len(batches) - 1:
                    _t.sleep(0.3)

        return {"preserved": preserved, "stale": stale, "new": new}

    def _delete_work_ids_partition(partition, table_name, label, worker_idx):
        """Delete old records for a partition of work_ids. Returns count deleted.

        Uses batched in-clause deletes (200 per batch) instead of
        individual REST calls for ~5-10x throughput improvement.
        """
        deleted = 0
        batch_size = 200
        client_key = db2_url + db2_key
        if client_key not in _sb_clients:
            from supabase import create_client
            _sb_clients[client_key] = create_client(db2_url, db2_key)
        client = _sb_clients[client_key]
        for start in range(0, len(partition), batch_size):
            batch = partition[start:start + batch_size]
            try:
                client.table(table_name).delete().in_("work_id", batch).execute()
                deleted += len(batch)
            except Exception:
                for wid in batch:
                    try:
                        sb_delete(db2_url, db2_key, table_name,
                                  {"work_id": f"eq.{wid}"})
                        deleted += 1
                    except Exception:
                        pass
        return deleted

    def _persist_table_parallel(table_name, analyses, label):
        """Persist analyses for one table with parallel delete phase.

        In affected mode, deletes old records first (to satisfy the unique
        constraint on work_id), then inserts fresh records, then restores
        ML values for works whose deterministic features haven't changed.
        """
        if not analyses:
            print(f"  {label}: no analyses to persist")
            return {"inserted": 0, "ml": {"preserved": 0, "stale": 0, "new": 0}}

        t_start = time.time()
        records = [_work_analysis_to_record(wa) for wa in analyses]
        work_ids = [r["work_id"] for r in records]

        # Step 1: Delete old records first (required for unique work_id constraint)
        total_deleted = 0
        if affected_work_ids is not None and work_ids:
            del_start = time.time()
            num_workers = min(5, len(work_ids))
            partitions = [[] for _ in range(num_workers)]
            for i, wid in enumerate(work_ids):
                partitions[i % num_workers].append(wid)

            delete_errors = []

            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                futures = {}
                for idx, partition in enumerate(partitions):
                    if partition:
                        f = executor.submit(
                            _delete_work_ids_partition,
                            partition, table_name, label, idx
                        )
                        futures[f] = idx

                for f in as_completed(futures):
                    try:
                        total_deleted += f.result()
                    except Exception as exc:
                        delete_errors.append(str(exc))

            del_elapsed = time.time() - del_start
            print(f"  {label}: deleted {total_deleted} old records ({del_elapsed:.1f}s, "
                  f"{num_workers} workers)")

        # Step 2: Insert fresh records (sequential, already batched)
        from supabase import create_client
        client = create_client(db2_url, db2_key)
        inserted = _insert_records(client, table_name, records, label)

        # Step 3: Restore ML values for works with unchanged features
        ml_stats = {"preserved": 0, "stale": 0, "new": 0}
        if affected_work_ids is not None and records:
            ml_stats = _restore_ml_values(client, table_name, records, label)

        insert_elapsed = time.time() - t_start
        print(f"  {label}: inserted {inserted} records, "
              f"ML preserved={ml_stats['preserved']} stale={ml_stats['stale']} "
              f"new={ml_stats['new']} ({insert_elapsed:.1f}s)")

        return {"inserted": inserted, "ml": ml_stats}

    # Run MP and MLA tables in parallel
    ml_totals = {"preserved": 0, "stale": 0, "new": 0}
    with ThreadPoolExecutor(max_workers=2) as executor:
        mp_future = executor.submit(
            _persist_table_parallel, "work_analysis", mp_analyses, "MP work_analysis"
        )
        mla_future = executor.submit(
            _persist_table_parallel, "mla_work_analysis", mla_analyses, "MLA mla_work_analysis"
        )
        mp_result = mp_future.result()
        mla_result = mla_future.result()

    for k in ml_totals:
        ml_totals[k] = mp_result["ml"][k] + mla_result["ml"][k]

    total = mp_result["inserted"] + mla_result["inserted"]
    print(f"  Total work analysis persisted: {total}")
    print(f"  ML lifecycle: preserved={ml_totals['preserved']} "
          f"stale={ml_totals['stale']} new={ml_totals['new']} "
          f"(stale/new will be recomputed by intelligence backfill)")
    return {
        "mp_written": mp_result["inserted"],
        "mla_written": mla_result["inserted"],
        "total": total,
        "ml_preserved": ml_totals["preserved"],
        "ml_stale": ml_totals["stale"],
        "ml_new": ml_totals["new"],
    }


def stage_anomaly(pipeline_result, reference_date=None):
    """Stage 7: Compute entity anomaly scores.

    Uses robust z-score method. Global reference statistics computed
    across qualified population, but only affected entity results persisted.

    Returns:
        dict with member and state anomaly results
    """
    from analysis.entity_anomaly import compute_entity_anomalies, compute_state_anomalies

    print("\n=== STAGE 7: ENTITY ANOMALY ===")

    pipeline = pipeline_result["pipeline"]

    member_anomalies = compute_entity_anomalies(pipeline.member_metrics, reference_date)
    state_anomalies = compute_state_anomalies(pipeline.state_metrics, reference_date)

    print(f"  Member anomalies: {len(member_anomalies)}")
    print(f"  State anomalies: {len(state_anomalies)}")

    return {
        "member_anomalies": member_anomalies,
        "state_anomalies": state_anomalies,
    }


def stage_evidence(pipeline_result, anomaly_result):
    """Stage 8: Build evidence records from deterministic + anomaly output.

    Evidence bridges deterministic analysis and Gemini interpretation.
    Built from Python output, not directly from DB.

    Returns:
        list of evidence record dicts
    """
    from analysis.evidence_builder import (
        build_member_evidence, build_state_evidence,
        group_works_by_member, group_works_by_state,
    )

    print("\n=== STAGE 8: EVIDENCE ===")

    pipeline = pipeline_result["pipeline"]
    master_context = pipeline_result.get("master_population_context")

    work_by_member = group_works_by_member(pipeline.work_analyses)
    work_by_state = group_works_by_state(pipeline.work_analyses)

    member_evidence = build_member_evidence(
        pipeline.member_metrics, work_by_member,
        {s.state_id: s for s in pipeline.state_metrics},
        pipeline.statistics, anomaly_result["member_anomalies"],
        master_population_context=master_context,
    )
    state_evidence = build_state_evidence(
        pipeline.state_metrics, work_by_state, anomaly_result["state_anomalies"],
    )

    all_evidence = member_evidence + state_evidence
    print(f"  Evidence records: {len(all_evidence)}")

    # Count zero-work evidence
    zero_work_count = sum(
        1 for r in member_evidence
        if r["evidence"].get("quality", {}).get("zero_work_member", False)
    )
    print(f"  Zero-work member evidence: {zero_work_count}")

    return all_evidence


def stage_analytics_persist(pipeline_result, anomaly_result):
    """Stage 7b: Persist all analytics to DB2.

    Writes:
    - overall_metrics (3 rows: MP, MLA, BOTH)
    - member_metrics (774 rows)
    - state_metrics (36 rows)
    - national_statistics (~50 rows)
    - trends (~10 rows)

    All writes are upserts (idempotent).
    Returns:
        dict with write stats
    """
    from analysis.db2_analytics_persistence import (
        build_overall_metrics,
        build_member_metrics,
        build_state_metrics,
        build_national_statistics,
        build_trends,
        compute_member_ranks,
        compute_state_ranks,
    )

    print("\n=== STAGE 7b: ANALYTICS PERSIST ===")

    db2_url, db2_key = get_db2()

    pipeline = pipeline_result["pipeline"]
    master_ctx = pipeline_result.get("master_population_context")
    member_anomalies = anomaly_result.get("member_anomalies", [])
    state_anomalies = anomaly_result.get("state_anomalies", [])

    # Build all records
    overall = build_overall_metrics(
        pipeline.member_metrics, pipeline.state_metrics, member_anomalies
    )
    members = build_member_metrics(
        pipeline.member_metrics, member_anomalies, master_ctx
    )
    states = build_state_metrics(
        pipeline.state_metrics, state_anomalies, pipeline.member_metrics
    )
    stats = build_national_statistics(pipeline.statistics)
    trends_raw = build_trends(pipeline.trends)
    trends_raw.extend(build_trends(pipeline.mp_trends))
    trends_raw.extend(build_trends(pipeline.mla_trends))

    from analysis.db2_analytics_persistence import deduplicate_trends
    trends, trends_dedup = deduplicate_trends(trends_raw)

    # Also build MP-only and MLA-only statistics
    stats.extend(build_national_statistics(pipeline.mp_statistics))
    stats.extend(build_national_statistics(pipeline.mla_statistics))

    # Compute rankings
    compute_member_ranks(members)
    compute_state_ranks(states)

    print(f"  Overall metrics: {len(overall)}")
    print(f"  Member metrics: {len(members)}")
    print(f"  State metrics: {len(states)}")
    print(f"  National statistics: {len(stats)}")
    print(f"  Trends generated: {trends_dedup['generated']}")
    print(f"  Trends duplicates: {trends_dedup['duplicates_found']}")
    print(f"  Trends after dedup: {len(trends)}")

    # Upsert all records to DB2
    written = 0

    has_real_data = any(
        r.get("total_works", 0) > 0 or r.get("sanctioned_amount", 0) > 0
        for r in overall
    )
    if has_real_data:
        sb_upsert(db2_url, db2_key, "overall_metrics", overall,
                  conflict_cols=["scope"])
        written += len(overall)
    else:
        print("  WARNING: Skipping overall_metrics upsert (all zeros - empty snapshot?)")

    # Member metrics in batches (sequential with retry)
    t_member = time.time()
    import time as _t
    batch_size = 500
    member_batches = [members[start:start + batch_size] for start in range(0, len(members), batch_size)]

    for i, batch in enumerate(member_batches):
        for attempt in range(3):
            try:
                sb_upsert(db2_url, db2_key, "member_metrics", batch,
                          conflict_cols=["member_id", "member_type"])
                break
            except Exception as e:
                if attempt < 2:
                    _t.sleep((attempt + 1) * 2)
                else:
                    print(f"  WARNING: member_metrics batch {i+1} upsert failed: {e}", flush=True)
        if i < len(member_batches) - 1:
            _t.sleep(0.3)
    written += len(members)
    print(f"  Member metrics upserted: {len(members)} in {len(member_batches)} batches "
          f"({time.time()-t_member:.1f}s)", flush=True)

    # State metrics
    sb_upsert(db2_url, db2_key, "state_metrics", states,
              conflict_cols=["state_id"])
    written += len(states)

    # Phase 2: re-rank the full tables from DB values so any rows not in the
    # current in-memory population (e.g. legacy rows) are also consistent.
    try:
        recompute_all_ranks(db2_url, db2_key)
    except Exception as _e:
        print(f"  WARNING: full re-rank skipped: {_e}")

    # National statistics (delete old + insert new for each scope) — sequential with retry
    t_stats = time.time()
    import time as _t

    for scope in ("BOTH", "MP", "MLA", None):
        scope_stats = [s for s in stats if s.get("scope") == scope]
        if scope_stats:
            for attempt in range(3):
                try:
                    sb_upsert(db2_url, db2_key, "national_statistics", scope_stats,
                              conflict_cols=["metric_name", "scope"])
                    break
                except Exception as e:
                    if attempt < 2:
                        _t.sleep((attempt + 1) * 2)
                    else:
                        print(f"  WARNING: national_stats scope={scope} upsert failed: {e}", flush=True)
            written += len(scope_stats)
            _t.sleep(0.3)
    print(f"  National statistics upserted in {time.time()-t_stats:.1f}s", flush=True)

    # Trends (upsert by year + member_type)
    if trends:
        trends_written = 0
        trends_failed = 0
        for mt in ("MP", "MLA"):
            mt_trends = [t for t in trends if t.get("member_type") == mt]
            if mt_trends:
                try:
                    sb_upsert(db2_url, db2_key, "trends", mt_trends,
                              conflict_cols=["year", "member_type"])
                    trends_written += len(mt_trends)
                except Exception as exc:
                    print(f"  WARNING: trends upsert failed for {mt}: {exc}")
                    trends_failed += len(mt_trends)
        print(f"  Trends written: {trends_written}, failed: {trends_failed}")
        written += trends_written

    print(f"  Total written: {written} analytics records")
    return {
        "overall": len(overall),
        "members": len(members),
        "states": len(states),
        "statistics": len(stats),
        "trends": len(trends),
        "total_written": written,
    }


def stage_evidence_work_refs(evidence_records, work_analyses):
    """Stage 8b: Build and persist evidence_work_refs.

    Links evidence records to specific source works.
    Writes batches sequentially to avoid overwhelming the server.
    Returns:
        dict with write stats
    """
    from analysis.evidence_work_refs import build_evidence_work_refs

    print("\n=== STAGE 8b: EVIDENCE WORK REFS ===")

    db2_url, db2_key = get_db2()

    t_start = time.time()
    refs = build_evidence_work_refs(evidence_records, work_analyses)
    build_elapsed = time.time() - t_start

    if refs:
        # Delete old refs (single call for full rebuild)
        try:
            sb_delete(db2_url, db2_key, "evidence_work_refs",
                      {"ref_id": "gte.0"})
        except Exception:
            pass

        # Write sequentially in batches with retry
        import time as _t
        batch_size = 500
        batches = [refs[start:start + batch_size]
                   for start in range(0, len(refs), batch_size)]

        written = 0
        for batch in batches:
            for attempt in range(3):
                try:
                    sb_upsert(db2_url, db2_key, "evidence_work_refs", batch)
                    written += len(batch)
                    break
                except Exception as e:
                    if attempt < 2:
                        _t.sleep((attempt + 1) * 2)
                    else:
                        print(f"  WARNING: evidence_work_refs batch failed: {e}", flush=True)

        print(f"  Evidence work refs: {written} (build={build_elapsed:.1f}s)")
    else:
        written = 0
        print("  No evidence work refs to write")

    return {"written": written}


def stage_gemini(evidence_records, api_keys=None, models=None):
    """Stage 9: Process evidence through Gemini (affected-only, parallel).

    Only processes entities whose evidence_hash has changed since
    last analysis. Uses idempotency via evidence_hash + prompt_version.
    Processes evidence in parallel across multiple API keys and models
    with bounded concurrency and per-lane rate limiting.

    Args:
        evidence_records: list of evidence record dicts
        api_keys: list of API key strings (default: from config)
        models: list of model names (default: ["gemini-3.1-flash-lite", "gemini-3.5-flash-lite"])

    Returns:
        dict with processing stats
    """
    import os
    from analysis.gemini_scheduler import GeminiScheduler
    from analysis.gemini_processor import filter_affected

    print("\n=== STAGE 9: GEMINI (Parallel) ===")

    cfg = get_config()
    keys = api_keys or cfg.gemini_keys
    if not keys:
        print("  No Gemini API keys. Skipping Gemini stage.")
        return {"skipped": True, "reason": "no_api_keys", "processed": 0,
                "success": 0, "failed": 0}

    db2_url, db2_key = get_db2()

    # Load existing analyses from DB2 to determine affected set
    existing = load_table(db2_url, db2_key, "ai_analysis",
                          "entity_type,entity_id,evidence_hash,prompt_version")

    affected = filter_affected(evidence_records, existing)
    print(f"  Total evidence: {len(evidence_records)}")
    print(f"  Already up-to-date: {len(evidence_records) - len(affected)}")
    print(f"  Need processing: {len(affected)}")

    if not affected:
        print("  Nothing to process.")
        return {"processed": 0, "skipped": len(evidence_records),
                "success": 0, "failed": 0}

    if models is None:
        models_env = os.environ.get("GEMINI_MODELS")
        if models_env:
            models = [m.strip() for m in models_env.split(",") if m.strip()]
        else:
            models = ["gemini-3.1-flash-lite", "gemini-3.5-flash-lite"]

    concurrency = int(os.environ.get("GEMINI_CONCURRENCY", "12"))
    items_per_req = int(os.environ.get("GEMINI_ITEMS_PER_REQUEST", "5"))

    scheduler = GeminiScheduler(
        api_keys=keys,
        models=models,
        rpm_per_lane=15,
        max_workers=concurrency,
        items_per_request=items_per_req,
        max_attempts=3,
    )

    def on_success(result):
        # Persist to DB2 via upsert (idempotent on entity_type + entity_id)
        sb_upsert(db2_url, db2_key, "ai_analysis", [{
            "entity_type": result.entity_type,
            "entity_id": result.entity_id,
            "evidence_version": 2,
            "evidence_hash": result.evidence_hash,
            "model": result.model,
            "prompt_version": result.prompt_version,
            "analysis_text": result.to_analysis_text(),
            "generated_at": result.generated_at,
        }], conflict_cols=["entity_type", "entity_id"])

    batch_result = scheduler.process(affected, on_success=on_success)

    print(f"  Successful: {batch_result['success_count']}")
    print(f"  Failed: {batch_result['failure_count']}")
    print(f"  Retries: {batch_result.get('retries', 0)}")

    return {
        "processed": len(affected),
        "skipped": len(evidence_records) - len(affected),
        "success": batch_result["success_count"],
        "failed": batch_result["failure_count"],
        "failures": batch_result["failures"],
    }



def stage_persist(evidence_records, affected_keys=None):
    """Stage 10: Write evidence to DB2.

    If affected_keys provided, only those entities are upserted.
    Otherwise, full rebuild (delete-all + insert-all).

    Returns:
        dict with write stats
    """
    print("\n=== STAGE 10: DB2 PERSIST ===")

    db2_url, db2_key = get_db2()

    if affected_keys is not None:
        affected_records = [
            r for r in evidence_records
            if (r["entity_type"], r["entity_id"]) in affected_keys
        ]
        print(f"  Affected-only: {len(affected_records)}/{len(evidence_records)} records")
        records_to_write = affected_records
    else:
        print("  Full rebuild: deleting old evidence...")
        # Only delete entity_evidence — evidence_work_refs are managed by
        # stage_evidence_work_refs (Stage 8b) and must NOT be wiped here
        sb_delete(db2_url, db2_key, "entity_evidence", {"evidence_id": "gte.0"})
        records_to_write = evidence_records

    # Write in batches via upsert (idempotent on entity_type + entity_id)
    batch_size = 500
    written = 0
    for start in range(0, len(records_to_write), batch_size):
        batch = records_to_write[start:start + batch_size]
        sb_upsert(db2_url, db2_key, "entity_evidence", batch,
                  conflict_cols=["entity_type", "entity_id"])
        written += len(batch)

    print(f"  Written: {written} evidence records to DB2")
    return {"written": written}


def stage_analytics_persist_affected(pipeline_result, anomaly_result, affected_result):
    """Stage 7b-affected: Persist analytics for affected members/states only.

    Writes only the affected subset of:
    - overall_metrics (always full — small table, safe to rebuild)
    - member_metrics (affected members only)
    - state_metrics (affected states only)
    - national_statistics (full rebuild — small table)
    - trends (full rebuild — small table)

    All writes are upserts (idempotent).
    """
    from analysis.db2_analytics_persistence import (
        build_overall_metrics,
        build_member_metrics,
        build_state_metrics,
        build_national_statistics,
        build_trends,
        compute_member_ranks,
        compute_state_ranks,
    )

    print("\n=== STAGE 7b: ANALYTICS PERSIST (AFFECTED) ===")

    db2_url, db2_key = get_db2()

    pipeline = pipeline_result["pipeline"]
    master_ctx = pipeline_result.get("master_population_context")
    anomaly_result_data = anomaly_result

    member_anomalies = anomaly_result_data.get("member_anomalies", [])
    state_anomalies = anomaly_result_data.get("state_anomalies", [])

    # Overall metrics — always full rebuild (small table)
    overall = build_overall_metrics(
        pipeline.member_metrics, pipeline.state_metrics, member_anomalies
    )

    # Affected member metrics only
    affected_member_metrics = pipeline_result.get("affected_member_metrics", pipeline.member_metrics)
    members = build_member_metrics(affected_member_metrics, member_anomalies, master_ctx)

    # Affected state metrics only
    affected_state_metrics = pipeline_result.get("affected_state_metrics", pipeline.state_metrics)
    states = build_state_metrics(affected_state_metrics, state_anomalies, pipeline.member_metrics)

    # National statistics and trends — full rebuild (small tables)
    stats = build_national_statistics(pipeline.statistics)
    stats.extend(build_national_statistics(pipeline.mp_statistics))
    stats.extend(build_national_statistics(pipeline.mla_statistics))
    trends_raw = build_trends(pipeline.trends)
    trends_raw.extend(build_trends(pipeline.mp_trends))
    trends_raw.extend(build_trends(pipeline.mla_trends))

    from analysis.db2_analytics_persistence import deduplicate_trends
    trends, trends_dedup = deduplicate_trends(trends_raw)

    compute_member_ranks(members)
    compute_state_ranks(states)

    print(f"  Overall metrics (full): {len(overall)}")
    print(f"  Member metrics (affected): {len(members)}")
    print(f"  State metrics (affected): {len(states)}")
    print(f"  National statistics (full): {len(stats)}")
    print(f"  Trends generated: {trends_dedup['generated']}")
    print(f"  Trends duplicates: {trends_dedup['duplicates_found']}")
    print(f"  Trends after dedup: {len(trends)}")

    written = 0

    has_real_data = any(
        r.get("total_works", 0) > 0 or r.get("sanctioned_amount", 0) > 0
        for r in overall
    )
    if has_real_data:
        sb_upsert(db2_url, db2_key, "overall_metrics", overall,
                  conflict_cols=["scope"])
        written += len(overall)
    else:
        print("  WARNING: Skipping overall_metrics upsert (all zeros)")

    batch_size = 200
    for start in range(0, len(members), batch_size):
        batch = members[start:start + batch_size]
        sb_upsert(db2_url, db2_key, "member_metrics", batch,
                  conflict_cols=["member_id", "member_type"])
        written += len(batch)

    sb_upsert(db2_url, db2_key, "state_metrics", states,
              conflict_cols=["state_id"])
    written += len(states)

    # Phase 2: after writing the affected subset, re-rank the FULL tables so
    # affected and untouched rows share one consistent ranking population.
    # Fail-safe: a ranking refresh error must not abort the persist stage.
    try:
        recompute_all_ranks(db2_url, db2_key)
    except Exception as _e:
        print(f"  WARNING: full re-rank skipped: {_e}")

    for scope in ("BOTH", "MP", "MLA", None):
        scope_stats = [s for s in stats if s.get("scope") == scope]
        if scope_stats:
            # Upsert first to avoid data loss if upsert fails
            sb_upsert(db2_url, db2_key, "national_statistics", scope_stats,
                      conflict_cols=["metric_name", "scope"])

    # Trends (upsert by year + member_type)
    if trends:
        trends_written = 0
        trends_failed = 0
        for mt in ("MP", "MLA"):
            mt_trends = [t for t in trends if t.get("member_type") == mt]
            if mt_trends:
                try:
                    sb_upsert(db2_url, db2_key, "trends", mt_trends,
                              conflict_cols=["year", "member_type"])
                    trends_written += len(mt_trends)
                except Exception as exc:
                    print(f"  WARNING: trends upsert failed for {mt}: {exc}")
                    trends_failed += len(mt_trends)
        print(f"  Trends written: {trends_written}, failed: {trends_failed}")
        written += trends_written

    print(f"  Total written: {written} analytics records")
    return {
        "overall": len(overall),
        "members": len(members),
        "states": len(states),
        "statistics": len(stats),
        "trends": len(trends),
        "total_written": written,
    }


def stage_evidence_affected(pipeline_result, anomaly_result, affected_result):
    """Stage 8-affected: Build evidence for affected entities only.

    Returns evidence records only for affected members/states.
    """
    from analysis.evidence_builder import (
        build_member_evidence, build_state_evidence,
        group_works_by_member, group_works_by_state,
    )

    print("\n=== STAGE 8: EVIDENCE (AFFECTED) ===")

    pipeline = pipeline_result["pipeline"]
    master_context = pipeline_result.get("master_population_context")

    affected_members = affected_result.get("affected_members", set())
    affected_states = affected_result.get("affected_states", set())

    # Use full work set for grouping (evidence needs complete context)
    work_by_member = group_works_by_member(pipeline.work_analyses)
    work_by_state = group_works_by_state(pipeline.work_analyses)

    # Filter member metrics to affected only
    affected_member_metrics = [
        m for m in pipeline.member_metrics
        if (m.member_type, m.member_id) in affected_members
    ]
    affected_state_metrics = [
        s for s in pipeline.state_metrics
        if s.state_id in affected_states
    ]

    member_evidence = build_member_evidence(
        affected_member_metrics, work_by_member,
        {s.state_id: s for s in pipeline.state_metrics},
        pipeline.statistics, anomaly_result["member_anomalies"],
        master_population_context=master_context,
    )
    state_evidence = build_state_evidence(
        affected_state_metrics, work_by_state, anomaly_result["state_anomalies"],
    )

    all_evidence = member_evidence + state_evidence
    print(f"  Affected evidence records: {len(all_evidence)}")

    zero_work_count = sum(
        1 for r in member_evidence
        if r["evidence"].get("quality", {}).get("zero_work_member", False)
    )
    print(f"  Zero-work member evidence: {zero_work_count}")

    return all_evidence


def stage_evidence_work_refs_affected(evidence_records, work_analyses, affected_result):
    """Stage 8b-affected: Build evidence_work_refs for affected entities only.

    Only replaces refs for affected entities, preserves all others.
    Writes batches sequentially to avoid overwhelming the server.
    """
    from analysis.evidence_work_refs import build_evidence_work_refs

    print("\n=== STAGE 8b: EVIDENCE WORK REFS (AFFECTED) ===")

    db2_url, db2_key = get_db2()

    t_start = time.time()
    refs = build_evidence_work_refs(evidence_records, work_analyses)
    build_elapsed = time.time() - t_start

    if refs:
        affected_members = affected_result.get("affected_members", set())
        affected_states = affected_result.get("affected_states", set())

        # Collect all delete tasks
        delete_tasks = []
        for member_type, member_id in affected_members:
            delete_tasks.append(
                (db2_url, db2_key, "evidence_work_refs",
                 {"entity_type": f"eq.{member_type}", "entity_id": f"eq.{member_id}"})
            )
        for state_id in affected_states:
            delete_tasks.append(
                (db2_url, db2_key, "evidence_work_refs",
                 {"entity_type": "eq.STATE", "entity_id": f"eq.{state_id}"})
            )

        # Sequential delete
        deleted = 0
        for task in delete_tasks:
            url, key, tbl, filters = task
            try:
                sb_delete(url, key, tbl, filters)
                deleted += 1
            except Exception:
                pass

        print(f"  Cleaned affected refs: {deleted} entities ({time.time() - t_start:.1f}s)")

        # Sequential batch insert
        batch_size = 500
        batches = [refs[start:start + batch_size]
                   for start in range(0, len(refs), batch_size)]

        written = 0
        for batch in batches:
            sb_upsert(db2_url, db2_key, "evidence_work_refs", batch)
            written += len(batch)

        print(f"  Evidence work refs written: {written} ({time.time() - t_start:.1f}s total)")
    else:
        written = 0
        print("  No evidence work refs to write")

    return {"written": written}


def stage_persist_affected(evidence_records, affected_keys):
    """Stage 10-affected: Persist evidence for affected entities only.

    Only upserts records for affected members/states. Leaves all other
    evidence untouched.
    """
    print("\n=== STAGE 10: DB2 PERSIST (AFFECTED) ===")

    db2_url, db2_key = get_db2()

    affected_records = [
        r for r in evidence_records
        if (r["entity_type"], r["entity_id"]) in affected_keys
    ]
    print(f"  Affected-only: {len(affected_records)}/{len(evidence_records)} records")

    batch_size = 500
    written = 0
    for start in range(0, len(affected_records), batch_size):
        batch = affected_records[start:start + batch_size]
        sb_upsert(db2_url, db2_key, "entity_evidence", batch,
                  conflict_cols=["entity_type", "entity_id"])
        written += len(batch)

    print(f"  Written: {written} evidence records to DB2")
    return {"written": written}


def stage_verify(evidence_records, pipeline_result, anomaly_result,
                 gemini_result=None, persist_result=None,
                 analytics_result=None, work_refs_result=None,
                 affected_result=None, work_analysis_result=None):
    """Stage 11: Verify pipeline outputs before cleanup.

    Checks:
    1. Evidence records exist for all analyzed entities
    2. No duplicate evidence hashes
    3. Entity counts match between analysis and evidence
    4. Deterministic invariants hold
    5. DB2 writes successful (if applicable)
    6. No source deletion occurred

    In affected mode, expected counts come from the affected subset,
    not the full population.

    Returns:
        dict with verification results
    """
    print("\n=== STAGE 11: VERIFY ===")

    issues = []
    warnings = []

    # Check evidence is non-empty
    if not evidence_records:
        issues.append("No evidence records to verify")

    # Check for duplicate hashes
    hashes = [r["evidence_hash"] for r in evidence_records]
    if len(hashes) != len(set(hashes)):
        issues.append("Duplicate evidence hashes found")

    # Check member evidence count matches pipeline
    member_count = len([r for r in evidence_records if r["entity_type"] in ("MP", "MLA")])
    state_count = len([r for r in evidence_records if r["entity_type"] == "STATE"])

    pipeline = pipeline_result.get("pipeline")
    is_affected = pipeline_result.get("is_affected_mode", False)

    # In affected mode, compare against affected subset; in full mode, full population
    if is_affected and affected_result:
        expected_members = affected_result.get("member_count", 0)
        expected_states = affected_result.get("state_count", 0)
    elif pipeline:
        expected_members = len(pipeline.member_metrics)
        expected_states = len(pipeline.state_metrics)
    else:
        expected_members = 0
        expected_states = 0

    if pipeline:
        if member_count != expected_members:
            issues.append(
                f"Member evidence count mismatch: {member_count} evidence vs "
                f"{expected_members} expected"
            )
        if state_count != expected_states:
            issues.append(
                f"State evidence count mismatch: {state_count} evidence vs "
                f"{expected_states} expected"
            )

    # Check anomaly counts
    if anomaly_result:
        member_anomaly_count = len(anomaly_result.get("member_anomalies", []))
        state_anomaly_count = len(anomaly_result.get("state_anomalies", []))
        if member_anomaly_count == 0 and expected_members > 5:
            issues.append("No member anomalies computed (expected some)")
        if state_anomaly_count == 0 and expected_states > 3:
            issues.append("No state anomalies computed (expected some)")

    # Check persist result
    if persist_result:
        if persist_result.get("written", 0) == 0 and len(evidence_records) > 0:
            issues.append("Persist wrote 0 records but evidence exists")

    # Check analytics result
    if analytics_result:
        if analytics_result.get("total_written", 0) == 0:
            issues.append("Analytics persist wrote 0 records")
        if analytics_result.get("members", 0) == 0:
            issues.append("No member metrics persisted")
        if analytics_result.get("states", 0) == 0:
            issues.append("No state metrics persisted")

    # Check work refs result
    if work_refs_result:
        if work_refs_result.get("written", 0) == 0 and len(evidence_records) > 0:
            issues.append("No evidence work refs written (expected some)")

    # Check gemini result — partial failures are warnings, not hard failures.
    # One unresolved AI explanation should NOT undo validated deterministic analytics.
    if gemini_result and not gemini_result.get("skipped"):
        if gemini_result.get("failed", 0) > 0:
            warnings.append(f"Gemini had {gemini_result['failed']} failures (partial AI)")

    # Check ML column completeness — affected works must have ML values after
    # intelligence backfill runs. If ML is NULL, the pipeline status should
    # reflect this as a warning (not a hard failure, since the backfill may
    # have been skipped or failed independently).
    if work_analysis_result:
        ml_preserved = work_analysis_result.get("ml_preserved", 0)
        ml_stale = work_analysis_result.get("ml_stale", 0)
        ml_new = work_analysis_result.get("ml_new", 0)
        ml_need_recompute = ml_stale + ml_new
        if ml_preserved > 0:
            print(f"  ML preserved: {ml_preserved} works (features unchanged)")
        if ml_need_recompute > 0:
            print(f"  ML needs recomputation: {ml_need_recompute} works "
                  f"(stale={ml_stale}, new={ml_new})")

    # Check no source deletion (invariant)
    # This is a structural check — we never delete from DB1 in this pipeline
    # If we reach here, the invariant holds

    passed = len(issues) == 0
    status = "PASS" if not warnings else "SUCCESS_WITH_WARNINGS"
    if not passed:
        status = "FAIL"
    print(f"  Evidence records: {len(evidence_records)}")
    print(f"  Member evidence: {member_count}")
    print(f"  State evidence: {state_count}")
    print(f"  Issues: {len(issues)}")
    for issue in issues:
        print(f"    - {issue}")
    if warnings:
        print(f"  Warnings: {len(warnings)}")
        for w in warnings:
            print(f"    - {w}")
    print(f"  Result: {status}")

    return {"passed": passed, "status": status, "issues": issues, "warnings": warnings}


def stage_cleanup(run_id=None, delta_dir=None, local_snapshot=None):
    """Stage 12: Clean up temporary pipeline artifacts.

    ONLY cleans up:
    - Downloaded delta files
    - Temporary intermediate files
    - Temporary Gemini request/response artifacts
    - Checkpoint files for completed runs

    Does NOT clean up:
    - Database tables
    - Source data
    - Historical records

    SOURCE ABSENCE ≠ DATABASE DELETION
    """
    print("\n=== STAGE 12: CLEANUP ===")

    cleaned = []

    # Clean delta directory
    if delta_dir and Path(delta_dir).exists():
        try:
            shutil.rmtree(delta_dir)
            cleaned.append(f"delta_dir: {delta_dir}")
        except Exception as exc:
            print(f"  WARNING: Could not remove delta dir {delta_dir}: {exc}")

    # Clean local snapshot
    if local_snapshot and Path(local_snapshot).exists():
        try:
            shutil.rmtree(local_snapshot)
            cleaned.append(f"local_snapshot: {local_snapshot}")
        except Exception as exc:
            print(f"  WARNING: Could not remove local snapshot {local_snapshot}: {exc}")

    # Clean checkpoint for completed run
    if run_id:
        cp_path = _checkpoint_path(run_id)
        if cp_path.exists():
            try:
                cp_path.unlink()
                cleaned.append(f"checkpoint: {cp_path}")
            except Exception as exc:
                print(f"  WARNING: Could not remove checkpoint {cp_path}: {exc}")

    print(f"  Cleaned: {len(cleaned)} items")
    for item in cleaned:
        print(f"    - {item}")

    return {"cleaned": len(cleaned), "items": cleaned}

def run_pipeline(snapshot_dir=None, reference_date=None,
                 delta_dir=None, run_id=None,
                 skip_gemini=False, skip_ingest=False,
                 dry_run=False, resume=False,
                 mode="affected"):
    """Run the complete two-database pipeline.

    Args:
        snapshot_dir: path to NDJSON snapshot directory
        reference_date: date object for analysis reference
        delta_dir: path to delta directory (from comparator)
        run_id: unique run identifier for checkpointing
        skip_gemini: if True, skip the Gemini stage
        skip_ingest: if True, skip DB1 ingestion stages (1-4)
        dry_run: if True, run analysis only, no DB writes
        resume: if True, resume from last checkpoint
        mode: "full" for complete recompute, "affected" for delta-only

    Returns:
        dict with full pipeline results
    """
    cfg = load_config(require_db2=not dry_run)
    timing = {}
    pipeline_timing = {}

    if run_id is None:
        run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    print("=" * 70, flush=True)
    print(f"MPLADS PIPELINE — {PIPELINE_VERSION}", flush=True)
    print(f"Run ID: {run_id}", flush=True)
    print(f"Mode: {mode}", flush=True)
    print(f"DB1: {cfg.db1_url}", flush=True)
    print(f"DB2: {'CONFIGURED' if cfg.db2_ready else 'NOT CONFIGURED (dry-run)'}", flush=True)
    print(f"Gemini keys: {len(cfg.gemini_keys)}", flush=True)
    print(f"Started: {_now_iso()}", flush=True)
    print("=" * 70, flush=True)

    results = {
        "pipeline_version": PIPELINE_VERSION,
        "run_id": run_id,
        "started_at": _now_iso(),
        "mode": mode,
        "stages": {},
    }

    try:
        # Determine starting stage
        start_stage = "analyze" if skip_ingest else "affected"
        if resume:
            start_stage = next_stage_after(run_id, start_stage)
            print(f"\nResuming from stage: {start_stage}")

        affected_result = None
        analysis_result = None
        anomaly_result = None
        evidence_records = []
        gemini_result = None
        persist_result = None
        analytics_result = None
        work_refs_result = None

        if mode == "affected" and delta_dir:

            # Stage 5: Affected entities
            if start_stage in ("affected", "analyze"):
                t5 = _timer()
                affected_result = stage_affected(delta_dir, run_id)
                results["stages"]["affected"] = affected_result
                save_checkpoint(run_id, "affected", {
                    "member_count": affected_result["member_count"],
                    "state_count": affected_result["state_count"],
                })
                timing["affected"] = _elapsed(t5)

            # Check if there are any affected entities
            if affected_result and affected_result["work_count"] == 0:
                print("\n=== NO AFFECTED ENTITIES — SKIPPING ANALYSIS ===")
                results["stages"]["analyze"] = {"skipped": True, "reason": "no_affected_entities"}
                results["stages"]["anomaly"] = {"skipped": True, "reason": "no_affected_entities"}
                results["stages"]["analytics_persist"] = {"skipped": True, "reason": "no_affected_entities"}
                results["stages"]["evidence"] = {"skipped": True, "reason": "no_affected_entities"}
                results["stages"]["evidence_work_refs"] = {"skipped": True, "reason": "no_affected_entities"}
                results["stages"]["gemini"] = {"skipped": True, "reason": "no_affected_entities"}
                results["stages"]["persist"] = {"skipped": True, "reason": "no_affected_entities"}
                results["stages"]["verify"] = {"passed": True, "issues": []}
                results["status"] = "SUCCESS"
                results["completed_at"] = _now_iso()
                results["timing"] = timing
                return results

            # Stage 6a: Analyze affected
            t6 = _timer()
            analysis_result = stage_analyze_affected(
                snapshot_dir, affected_result, reference_date
            )
            results["stages"]["analyze"] = {
                "members": len(analysis_result["pipeline"].member_metrics),
                "states": len(analysis_result["pipeline"].state_metrics),
                "affected_members": len(analysis_result.get("affected_member_metrics", [])),
                "affected_states": len(analysis_result.get("affected_state_metrics", [])),
                "work_analyses": len(analysis_result["work_analyses"]),
            }
            save_checkpoint(run_id, "analyze", results["stages"]["analyze"])
            timing["analyze"] = _elapsed(t6)

            # Stage 7: Entity Anomaly (use full pipeline for reference statistics)
            # Runs even in dry_run — it's deterministic with no DB writes.
            t7 = _timer()
            anomaly_result = stage_anomaly(analysis_result, reference_date)
            results["stages"]["anomaly"] = {
                "member_anomalies": len(anomaly_result["member_anomalies"]),
                "state_anomalies": len(anomaly_result["state_anomalies"]),
            }
            save_checkpoint(run_id, "anomaly", results["stages"]["anomaly"])
            timing["anomaly"] = _elapsed(t7)

            if dry_run:
                print("\n=== DRY RUN: Skipping DB writes, evidence, and Gemini ===")
                results["stages"]["work_analysis_persist"] = {"skipped": True, "reason": "dry_run"}
                results["stages"]["analytics_persist"] = {"skipped": True, "reason": "dry_run"}
                results["stages"]["evidence"] = {"skipped": True, "reason": "dry_run"}
                results["stages"]["evidence_work_refs"] = {"skipped": True, "reason": "dry_run"}
                results["stages"]["gemini"] = {"skipped": True, "reason": "dry_run"}
                results["stages"]["persist"] = {"skipped": True, "reason": "dry_run"}
                results["stages"]["verify"] = {"passed": True, "issues": []}
                results["stages"]["cleanup"] = {"skipped": True, "reason": "dry_run"}
                results["status"] = "DRY_RUN"
                results["completed_at"] = _now_iso()
                results["timing"] = timing
                return results

            # Stage 6b: Persist affected work analysis to DB2
            t6b = _timer()
            work_analysis_result = stage_work_analysis_persist(
                analysis_result["work_analyses"],
                affected_work_ids=affected_result.get("affected_work_ids") if affected_result else None,
            )
            results["stages"]["work_analysis_persist"] = work_analysis_result
            save_checkpoint(run_id, "work_analysis_persist", work_analysis_result)
            timing["work_analysis_persist"] = _elapsed(t6b)

            # Stage 7b: Persist analytics (affected members/states only)
            t7b = _timer()
            analytics_result = stage_analytics_persist_affected(
                analysis_result, anomaly_result, affected_result
            )
            results["stages"]["analytics_persist"] = analytics_result
            save_checkpoint(run_id, "analytics_persist", analytics_result)
            timing["analytics_persist"] = _elapsed(t7b)

            # Stage 8: Evidence (affected entities only)
            t8 = _timer()
            evidence_records = stage_evidence_affected(
                analysis_result, anomaly_result, affected_result
            )
            results["stages"]["evidence"] = {"records": len(evidence_records)}
            save_checkpoint(run_id, "evidence", results["stages"]["evidence"])
            timing["evidence"] = _elapsed(t8)

            # Stage 8b: Evidence work refs (targeted for affected entities)
            t8b = _timer()
            work_refs_result = stage_evidence_work_refs_affected(
                evidence_records, analysis_result["work_analyses"], affected_result
            )
            results["stages"]["evidence_work_refs"] = work_refs_result
            save_checkpoint(run_id, "evidence_work_refs", work_refs_result)
            timing["evidence_work_refs"] = _elapsed(t8b)

            # Stage 9: Gemini (affected-only, skip if no evidence changes)
            t9 = _timer()
            if skip_gemini:
                print("\n=== GEMINI SKIPPED ===")
                gemini_result = {"skipped": True, "reason": "skip_gemini_flag",
                                 "processed": 0, "success": 0, "failed": 0}
            else:
                gemini_result = stage_gemini(evidence_records)
            results["stages"]["gemini"] = gemini_result
            save_checkpoint(run_id, "gemini", {
                "processed": gemini_result.get("processed", 0),
                "success": gemini_result.get("success", 0),
            })
            timing["gemini"] = _elapsed(t9)

            # Stage 10: Persist evidence (affected entities only)
            t10 = _timer()
            affected_keys = set()
            if affected_result:
                affected_keys = set(affected_result.get("affected_members", set()))
                for state_id in affected_result.get("affected_states", set()):
                    affected_keys.add(("STATE", state_id))
            persist_result = stage_persist_affected(evidence_records, affected_keys)
            results["stages"]["persist"] = persist_result
            save_checkpoint(run_id, "persist", persist_result)
            timing["persist"] = _elapsed(t10)

            # Stage 11: Verify
            t11 = _timer()
            verify_result = stage_verify(
                evidence_records, analysis_result, anomaly_result,
                gemini_result, persist_result,
                analytics_result=analytics_result,
                work_refs_result=work_refs_result,
                affected_result=affected_result,
                work_analysis_result=work_analysis_result,
            )
            results["stages"]["verify"] = verify_result
            save_checkpoint(run_id, "verify", verify_result)
            timing["verify"] = _elapsed(t11)

        else:

            # Stage 5: Affected entities
            if start_stage in ("affected", "analyze"):
                t5 = _timer()
                print("\n>>> STAGE 5: AFFECTED ENTITIES...", flush=True)
                affected_result = stage_affected(delta_dir, run_id)
                results["stages"]["affected"] = affected_result
                save_checkpoint(run_id, "affected", {
                    "member_count": affected_result["member_count"],
                    "state_count": affected_result["state_count"],
                })
                timing["affected"] = _elapsed(t5)
                print(f">>> STAGE 5 done: {_elapsed(t5)}", flush=True)

            # Stage 6: Analyze (full)
            t6 = _timer()
            print("\n>>> STAGE 6: FULL ANALYZE...", flush=True)
            analysis_result = stage_analyze(snapshot_dir, reference_date)
            results["stages"]["analyze"] = {
                "members": len(analysis_result["pipeline"].member_metrics),
                "states": len(analysis_result["pipeline"].state_metrics),
                "work_analyses": len(analysis_result["work_analyses"]),
            }
            save_checkpoint(run_id, "analyze", results["stages"]["analyze"])
            timing["analyze"] = _elapsed(t6)
            print(f">>> STAGE 6 done: {_elapsed(t6)}", flush=True)

            # Stage 7: Entity Anomaly
            t7 = _timer()
            print("\n>>> STAGE 7: ENTITY ANOMALY...", flush=True)
            anomaly_result = stage_anomaly(analysis_result, reference_date)
            results["stages"]["anomaly"] = {
                "member_anomalies": len(anomaly_result["member_anomalies"]),
                "state_anomalies": len(anomaly_result["state_anomalies"]),
            }
            save_checkpoint(run_id, "anomaly", results["stages"]["anomaly"])
            timing["anomaly"] = _elapsed(t7)
            print(f">>> STAGE 7 done: {_elapsed(t7)}", flush=True)

            if dry_run:
                print("\n=== DRY RUN: Skipping DB writes, evidence, and Gemini ===")
                results["stages"]["work_analysis_persist"] = {"skipped": True, "reason": "dry_run"}
                results["stages"]["analytics_persist"] = {"skipped": True, "reason": "dry_run"}
                results["stages"]["evidence"] = {"skipped": True, "reason": "dry_run"}
                results["stages"]["evidence_work_refs"] = {"skipped": True, "reason": "dry_run"}
                results["stages"]["gemini"] = {"skipped": True, "reason": "dry_run"}
                results["stages"]["persist"] = {"skipped": True, "reason": "dry_run"}
                results["stages"]["verify"] = {"passed": True, "issues": []}
                results["stages"]["cleanup"] = {"skipped": True, "reason": "dry_run"}
                results["status"] = "DRY_RUN"
                results["completed_at"] = _now_iso()
                results["timing"] = timing
                return results

            # Stage 6b: Persist all work analysis to DB2 (full mode)
            t6b = _timer()
            print("\n>>> STAGE 6b: WORK ANALYSIS PERSIST...", flush=True)
            work_analysis_result = stage_work_analysis_persist(
                analysis_result["work_analyses"],
            )
            results["stages"]["work_analysis_persist"] = work_analysis_result
            save_checkpoint(run_id, "work_analysis_persist", work_analysis_result)
            timing["work_analysis_persist"] = _elapsed(t6b)
            print(f">>> STAGE 6b done: {_elapsed(t6b)}", flush=True)

            # Stage 7b: Persist analytics to DB2
            t7b = _timer()
            print("\n>>> STAGE 7b: ANALYTICS PERSIST...", flush=True)
            analytics_result = stage_analytics_persist(analysis_result, anomaly_result)
            results["stages"]["analytics_persist"] = analytics_result
            save_checkpoint(run_id, "analytics_persist", analytics_result)
            timing["analytics_persist"] = _elapsed(t7b)
            print(f">>> STAGE 7b done: {_elapsed(t7b)}", flush=True)

            # Stage 8: Evidence
            t8 = _timer()
            print("\n>>> STAGE 8: EVIDENCE...", flush=True)
            evidence_records = stage_evidence(analysis_result, anomaly_result)
            results["stages"]["evidence"] = {"records": len(evidence_records)}
            save_checkpoint(run_id, "evidence", results["stages"]["evidence"])
            timing["evidence"] = _elapsed(t8)
            print(f">>> STAGE 8 done: {_elapsed(t8)}", flush=True)

            # Stage 8b: Evidence work refs
            t8b = _timer()
            print("\n>>> STAGE 8b: EVIDENCE WORK REFS...", flush=True)
            work_refs_result = stage_evidence_work_refs(
                evidence_records, analysis_result["work_analyses"]
            )
            results["stages"]["evidence_work_refs"] = work_refs_result
            save_checkpoint(run_id, "evidence_work_refs", work_refs_result)
            timing["evidence_work_refs"] = _elapsed(t8b)
            print(f">>> STAGE 8b done: {_elapsed(t8b)}", flush=True)

            # Stage 9: Gemini
            t9 = _timer()
            print("\n>>> STAGE 9: GEMINI...", flush=True)
            if skip_gemini:
                print("\n=== GEMINI SKIPPED ===", flush=True)
                gemini_result = {"skipped": True, "reason": "skip_gemini_flag",
                                 "processed": 0, "success": 0, "failed": 0}
            else:
                gemini_result = stage_gemini(evidence_records)
            results["stages"]["gemini"] = gemini_result
            save_checkpoint(run_id, "gemini", {
                "processed": gemini_result.get("processed", 0),
                "success": gemini_result.get("success", 0),
            })
            timing["gemini"] = _elapsed(t9)
            print(f">>> STAGE 9 done: {_elapsed(t9)}", flush=True)

            # Stage 10: Persist to DB2
            t10 = _timer()
            print("\n>>> STAGE 10: PERSIST EVIDENCE...", flush=True)
            persist_result = stage_persist(evidence_records)
            results["stages"]["persist"] = persist_result
            save_checkpoint(run_id, "persist", persist_result)
            timing["persist"] = _elapsed(t10)
            print(f">>> STAGE 10 done: {_elapsed(t10)}", flush=True)

            # Stage 11: Verify
            t11 = _timer()
            print("\n>>> STAGE 11: VERIFY...", flush=True)
            verify_result = stage_verify(
                evidence_records, analysis_result, anomaly_result,
                gemini_result, persist_result,
                analytics_result=analytics_result,
                work_refs_result=work_refs_result,
                work_analysis_result=work_analysis_result,
            )
            results["stages"]["verify"] = verify_result
            save_checkpoint(run_id, "verify", verify_result)
            timing["verify"] = _elapsed(t11)
            print(f">>> STAGE 11 done: {_elapsed(t11)}", flush=True)

        if not verify_result["passed"]:
            print("\n=== VERIFICATION FAILED ===")
            print("Aborting. Temporary artifacts preserved for debugging.")
            results["status"] = "FAILED_VERIFICATION"
            results["timing"] = timing
            return results

        # Stage 12: Cleanup (only after verification passes)
        t12 = _timer()
        cleanup_result = stage_cleanup(run_id, delta_dir)
        results["stages"]["cleanup"] = cleanup_result
        save_checkpoint(run_id, "cleanup", cleanup_result)
        timing["cleanup"] = _elapsed(t12)

        if verify_result.get("warnings"):
            results["status"] = "SUCCESS_WITH_WARNINGS"
            results["warnings"] = verify_result["warnings"]
        else:
            results["status"] = "SUCCESS"
        results["completed_at"] = _now_iso()
        results["timing"] = timing

    except Exception as exc:
        results["status"] = "FAILED"
        results["error"] = str(exc)
        results["failed_at"] = _now_iso()
        raise

    finally:
        print("\n" + "=" * 70, flush=True)
        print(f"PIPELINE STATUS: {results.get('status', 'UNKNOWN')}", flush=True)
        print(f"Run ID: {run_id}", flush=True)
        print(f"Mode: {mode}", flush=True)
        print(f"Completed: {results.get('completed_at', results.get('failed_at', 'N/A'))}", flush=True)
        if timing:
            total = sum(timing.values())
            print(f"Total analysis time: {total:.1f}s", flush=True)
            for key, val in timing.items():
                print(f"  {key}: {val:.1f}s", flush=True)
        print("=" * 70, flush=True)

    return results


def _now_iso():
    return datetime.now(timezone.utc).isoformat()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="MPLADS Pipeline Controller (Two-DB)")
    parser.add_argument("--snapshot", help="Path to NDJSON snapshot directory")
    parser.add_argument("--delta-dir", help="Path to delta directory from comparator")
    parser.add_argument("--run-id", help="Unique run ID for checkpointing")
    parser.add_argument("--reference-date", help="Reference date (YYYY-MM-DD)")
    parser.add_argument("--skip-gemini", action="store_true", help="Skip Gemini processing")
    parser.add_argument("--skip-ingest", action="store_true", help="Skip DB1 ingestion (stages 1-4)")
    parser.add_argument("--dry-run", action="store_true", help="Analyze only, no DB writes")
    parser.add_argument("--resume", action="store_true", help="Resume from last checkpoint")
    parser.add_argument("--mode", choices=["full", "affected"], default="affected",
                        help="Pipeline mode: full or affected (default: affected)")
    args = parser.parse_args()

    ref_date = None
    if args.reference_date:
        from datetime import date
        ref_date = date.fromisoformat(args.reference_date)

    result = run_pipeline(
        snapshot_dir=args.snapshot,
        reference_date=ref_date,
        delta_dir=args.delta_dir,
        run_id=args.run_id,
        skip_gemini=args.skip_gemini,
        skip_ingest=args.skip_ingest,
        dry_run=args.dry_run,
        resume=args.resume,
        mode=args.mode,
    )

    if result["status"] not in ("SUCCESS", "DRY_RUN"):
        sys.exit(1)
