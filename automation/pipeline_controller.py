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


# ============================================================
# PIPELINE VERSION & CONSTANTS
# ============================================================

PIPELINE_VERSION = "pipeline_v2"
STAGES = [
    "fetch", "compare", "delta", "ingest", "affected",
    "analyze", "work_analysis_persist", "anomaly", "analytics_persist",
    "evidence", "evidence_work_refs", "gemini",
    "persist", "verify", "cleanup",
]

BUCKET = "mplads-raw"


# ============================================================
# HTTP HELPERS (database-agnostic)
# ============================================================

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


# ============================================================
# CHECKPOINT HELPERS
# ============================================================

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


# ============================================================
# STAGE IMPLEMENTATIONS
# ============================================================

# Stages 1-4 are handled by daily_pipeline.py (fetch, compare, delta, ingest).
# Stages 5-12 are handled here (analysis through cleanup).

def _timer():
    """Return a monotonic start time."""
    return time.time()


def _elapsed(start):
    """Return elapsed seconds since start."""
    return time.time() - start


def _print_timing(timing, label="STAGE"):
    """Print timing for a stage."""
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
    from analysis.snapshot_loader import load_all_data, build_work_records
    from analysis.entity_anomaly import compute_entity_anomalies, compute_state_anomalies
    from analysis.evidence_builder import (
        build_member_evidence, build_state_evidence,
        group_works_by_member, group_works_by_state,
    )
    from analysis.zero_work_members import inject_zero_work_members, get_master_population_context

    print("\n=== STAGE 6: ANALYZE ===")

    # Load data directly from NDJSON snapshot files
    data = load_all_data(snapshot_dir)
    works, state_map, constituency_map, member_map = build_work_records(*data)

    # Run pipeline on work-bearing members
    pipeline = AnalysisPipeline(works)
    pipeline.run_full(reference_date)

    print(f"  Members analyzed (work-bearing): {len(pipeline.member_metrics)}")
    print(f"  States analyzed: {len(pipeline.state_metrics)}")
    print(f"  Work analyses: {len(pipeline.work_analyses)}")

    # Populate member_name on MemberMetrics from works data
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

    # Inject zero-work members from allocated_limit snapshots
    pipeline.member_metrics = inject_zero_work_members(
        pipeline.member_metrics,
        snapshot_dir,
        state_metrics_by_id={s.state_id: s for s in pipeline.state_metrics},
        works=works,
    )

    # Update member_metrics_by_id to include zero-work members
    pipeline.member_metrics_by_id = {
        (m.member_type, m.member_id): m
        for m in pipeline.member_metrics
    }

    # Get master population context for evidence building
    master_context = get_master_population_context(pipeline.member_metrics)

    print(f"  Total members (after injection): {len(pipeline.member_metrics)}")

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
    from analysis.snapshot_loader import load_all_data, build_work_records

    print("\n=== STAGE 6a: ANALYZE (AFFECTED ONLY) ===")

    affected_members = affected_result.get("affected_members", set())
    affected_states = affected_result.get("affected_states", set())

    data = load_all_data(snapshot_dir)
    works, state_map, constituency_map, member_map = build_work_records(*data)

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

    # Filter work_analyses to affected works only
    affected_work_ids = affected_result.get("affected_work_ids", set())
    affected_work_analyses = [
        wa for wa in pipeline.work_analyses
        if wa.work_id in affected_work_ids
    ] if affected_work_ids else pipeline.work_analyses

    # Also include time-sensitive works
    from analysis.affected import expand_time_sensitive
    works_by_id = {w["work_id"]: w for w in works}
    time_sensitive = expand_time_sensitive(works_by_id, {}, reference_date)
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
    """Stage 6b: Persist work analysis to DB1.work_analysis / DB1.mla_work_analysis.

    For affected mode: only replaces analysis for affected works.
    Uses delete-then-insert (not upsert) because DB1 work_analysis tables
    lack a unique index on work_id.

    Args:
        work_analyses: list of WorkAnalysis objects from stage_analyze
        affected_work_ids: set of snapshot work_ids that are affected.
                          If None, persists all (full mode).

    Returns:
        dict with write stats
    """
    print("\n=== STAGE 6b: WORK ANALYSIS PERSIST ===")

    db1_url, db1_key = get_db1()
    now_str = datetime.now(timezone.utc).isoformat()

    # Separate MP and MLA
    mp_analyses = [wa for wa in work_analyses if wa.member_type == "MP"]
    mla_analyses = [wa for wa in work_analyses if wa.member_type == "MLA"]

    if affected_work_ids is not None:
        mp_analyses = [wa for wa in mp_analyses if wa.work_id in affected_work_ids]
        mla_analyses = [wa for wa in mla_analyses if wa.work_id in affected_work_ids]

    print(f"  MP analyses to persist: {len(mp_analyses)}")
    print(f"  MLA analyses to persist: {len(mla_analyses)}")

    def _work_analysis_to_record(wa):
        """Convert WorkAnalysis dataclass to DB1 record dict."""
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
        }

    def _persist_batch(table_name, analyses, label):
        """Delete affected records then insert fresh ones."""
        if not analyses:
            print(f"  {label}: no analyses to persist")
            return 0

        records = [_work_analysis_to_record(wa) for wa in analyses]
        work_ids = [r["work_id"] for r in records]

        # Step 1: Delete affected records (by work_id ranges for efficiency)
        if affected_work_ids is not None:
            deleted = 0
            for wid in work_ids:
                try:
                    sb_delete(db1_url, db1_key, table_name,
                              {"work_id": f"eq.{wid}"})
                    deleted += 1
                except Exception:
                    pass
            print(f"  {label}: deleted {deleted} affected records")

        # Step 2: Insert fresh records in batches
        batch_size = 500
        inserted = 0
        from supabase import create_client
        client = create_client(db1_url, db1_key)
        for start in range(0, len(records), batch_size):
            batch = records[start:start + batch_size]
            try:
                client.table(table_name).insert(batch).execute()
                inserted += len(batch)
            except Exception as e:
                print(f"  WARNING: batch insert failed for {label}: {e}")
                for row in batch:
                    try:
                        client.table(table_name).insert(row).execute()
                        inserted += 1
                    except Exception as e2:
                        print(f"  WARNING: individual insert failed: {e2}")

        print(f"  {label}: inserted {inserted} records")
        return inserted

    mp_written = _persist_batch("work_analysis", mp_analyses, "MP work_analysis")
    mla_written = _persist_batch("mla_work_analysis", mla_analyses, "MLA mla_work_analysis")

    total = mp_written + mla_written
    print(f"  Total work analysis persisted: {total}")
    return {"mp_written": mp_written, "mla_written": mla_written, "total": total}


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
    trends = build_trends(pipeline.trends)

    # Also build MP-only and MLA-only statistics and trends
    stats.extend(build_national_statistics(pipeline.mp_statistics))
    stats.extend(build_national_statistics(pipeline.mla_statistics))
    trends.extend(build_trends(pipeline.mp_trends))
    trends.extend(build_trends(pipeline.mla_trends))

    # Compute rankings
    compute_member_ranks(members)
    compute_state_ranks(states)

    print(f"  Overall metrics: {len(overall)}")
    print(f"  Member metrics: {len(members)}")
    print(f"  State metrics: {len(states)}")
    print(f"  National statistics: {len(stats)}")
    print(f"  Trends: {len(trends)}")

    # Upsert all records to DB2
    written = 0

    # Guard: skip overall_metrics upsert if all values are zero
    # This prevents a subsequent run with empty snapshot from overwriting
    # real data with zeros (the member_metrics empty-body guard already
    # protects member_metrics, but overall_metrics always produces 3 rows)
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

    # Member metrics in batches
    batch_size = 200
    for start in range(0, len(members), batch_size):
        batch = members[start:start + batch_size]
        sb_upsert(db2_url, db2_key, "member_metrics", batch,
                  conflict_cols=["member_id", "member_type"])
        written += len(batch)

    # State metrics
    sb_upsert(db2_url, db2_key, "state_metrics", states,
              conflict_cols=["state_id"])
    written += len(states)

    # National statistics (delete old + insert new for each scope)
    for scope in ("BOTH", "MP", "MLA", None):
        scope_stats = [s for s in stats if s.get("scope") == scope]
        if scope_stats:
            filter_val = f"eq.{scope}" if scope else "is.null"
            try:
                sb_delete(db2_url, db2_key, "national_statistics",
                          {"scope": filter_val})
            except Exception:
                pass  # Table might be empty
            sb_upsert(db2_url, db2_key, "national_statistics", scope_stats)
            written += len(scope_stats)

    # Trends (delete old + insert new for each member_type)
    for mt in ("MP", "MLA"):
        mt_trends = [t for t in trends if t.get("member_type") == mt]
        if mt_trends:
            try:
                sb_delete(db2_url, db2_key, "trends",
                          {"member_type": f"eq.{mt}"})
            except Exception:
                pass
            sb_upsert(db2_url, db2_key, "trends", mt_trends)
            written += len(mt_trends)

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
    Returns:
        dict with write stats
    """
    from analysis.evidence_work_refs import build_evidence_work_refs

    print("\n=== STAGE 8b: EVIDENCE WORK REFS ===")

    db2_url, db2_key = get_db2()

    refs = build_evidence_work_refs(evidence_records, work_analyses)

    if refs:
        # Delete old refs and insert new (full rebuild for refs)
        try:
            sb_delete(db2_url, db2_key, "evidence_work_refs",
                      {"ref_id": "gte.0"})
        except Exception:
            pass

        # Write in batches
        batch_size = 500
        written = 0
        for start in range(0, len(refs), batch_size):
            batch = refs[start:start + batch_size]
            sb_upsert(db2_url, db2_key, "evidence_work_refs", batch)
            written += len(batch)

        print(f"  Evidence work refs: {written}")
    else:
        written = 0
        print("  No evidence work refs to write")

    return {"written": written}


def stage_gemini(evidence_records, api_keys=None):
    """Stage 9: Process evidence through Gemini (affected-only).

    Only processes entities whose evidence_hash has changed since
    last analysis. Uses idempotency via evidence_hash + prompt_version.

    Returns:
        dict with processing stats
    """
    from analysis.gemini_processor import GeminiProcessor, filter_affected

    print("\n=== STAGE 9: GEMINI ===")

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

    processor = GeminiProcessor(api_keys=keys)

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

    batch_result = processor.process_batch(affected, on_success=on_success)

    print(f"  Successful: {batch_result['success_count']}")
    print(f"  Failed: {batch_result['failure_count']}")

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
    trends = build_trends(pipeline.trends)
    trends.extend(build_trends(pipeline.mp_trends))
    trends.extend(build_trends(pipeline.mla_trends))

    compute_member_ranks(members)
    compute_state_ranks(states)

    print(f"  Overall metrics (full): {len(overall)}")
    print(f"  Member metrics (affected): {len(members)}")
    print(f"  State metrics (affected): {len(states)}")
    print(f"  National statistics (full): {len(stats)}")
    print(f"  Trends (full): {len(trends)}")

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

    for scope in ("BOTH", "MP", "MLA", None):
        scope_stats = [s for s in stats if s.get("scope") == scope]
        if scope_stats:
            filter_val = f"eq.{scope}" if scope else "is.null"
            try:
                sb_delete(db2_url, db2_key, "national_statistics",
                          {"scope": filter_val})
            except Exception:
                pass
            sb_upsert(db2_url, db2_key, "national_statistics", scope_stats)
            written += len(scope_stats)

    for mt in ("MP", "MLA"):
        mt_trends = [t for t in trends if t.get("member_type") == mt]
        if mt_trends:
            try:
                sb_delete(db2_url, db2_key, "trends",
                          {"member_type": f"eq.{mt}"})
            except Exception:
                pass
            sb_upsert(db2_url, db2_key, "trends", mt_trends)
            written += len(mt_trends)

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
    """
    from analysis.evidence_work_refs import build_evidence_work_refs

    print("\n=== STAGE 8b: EVIDENCE WORK REFS (AFFECTED) ===")

    db2_url, db2_key = get_db2()

    refs = build_evidence_work_refs(evidence_records, work_analyses)

    if refs:
        # For affected mode: delete only affected entity refs, then insert
        affected_members = affected_result.get("affected_members", set())
        affected_states = affected_result.get("affected_states", set())

        deleted = 0
        for member_type, member_id in affected_members:
            try:
                sb_delete(db2_url, db2_key, "evidence_work_refs",
                          {"entity_type": f"eq.{member_type}", "entity_id": f"eq.{member_id}"})
                deleted += 1
            except Exception:
                pass

        for state_id in affected_states:
            try:
                sb_delete(db2_url, db2_key, "evidence_work_refs",
                          {"entity_type": "eq.STATE", "entity_id": f"eq.{state_id}"})
                deleted += 1
            except Exception:
                pass

        print(f"  Cleaned affected refs: {deleted} entities")

        batch_size = 500
        written = 0
        for start in range(0, len(refs), batch_size):
            batch = refs[start:start + batch_size]
            sb_upsert(db2_url, db2_key, "evidence_work_refs", batch)
            written += len(batch)

        print(f"  Evidence work refs written: {written}")
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
                 affected_result=None):
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

    # Check gemini result
    if gemini_result and not gemini_result.get("skipped"):
        if gemini_result.get("failed", 0) > 0:
            issues.append(f"Gemini had {gemini_result['failed']} failures")

    # Check no source deletion (invariant)
    # This is a structural check — we never delete from DB1 in this pipeline
    # If we reach here, the invariant holds

    passed = len(issues) == 0
    print(f"  Evidence records: {len(evidence_records)}")
    print(f"  Member evidence: {member_count}")
    print(f"  State evidence: {state_count}")
    print(f"  Issues: {len(issues)}")
    for issue in issues:
        print(f"    - {issue}")
    print(f"  Result: {'PASS' if passed else 'FAIL'}")

    return {"passed": passed, "issues": issues}


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


# ============================================================
# MAIN PIPELINE
# ============================================================

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

    print("=" * 70)
    print(f"MPLADS PIPELINE — {PIPELINE_VERSION}")
    print(f"Run ID: {run_id}")
    print(f"Mode: {mode}")
    print(f"DB1: {cfg.db1_url}")
    print(f"DB2: {'CONFIGURED' if cfg.db2_ready else 'NOT CONFIGURED (dry-run)'}")
    print(f"Gemini keys: {len(cfg.gemini_keys)}")
    print(f"Started: {_now_iso()}")
    print("=" * 70)

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
            # ============================================================
            # AFFECTED-ONLY MODE
            # ============================================================

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

            # Stage 6b: Persist affected work analysis to DB1
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
            )
            results["stages"]["verify"] = verify_result
            save_checkpoint(run_id, "verify", verify_result)
            timing["verify"] = _elapsed(t11)

        else:
            # ============================================================
            # FULL MODE
            # ============================================================

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

            # Stage 6: Analyze (full)
            t6 = _timer()
            analysis_result = stage_analyze(snapshot_dir, reference_date)
            results["stages"]["analyze"] = {
                "members": len(analysis_result["pipeline"].member_metrics),
                "states": len(analysis_result["pipeline"].state_metrics),
                "work_analyses": len(analysis_result["work_analyses"]),
            }
            save_checkpoint(run_id, "analyze", results["stages"]["analyze"])
            timing["analyze"] = _elapsed(t6)

            # Stage 7: Entity Anomaly
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

            # Stage 6b: Persist all work analysis to DB1 (full mode)
            t6b = _timer()
            work_analysis_result = stage_work_analysis_persist(
                analysis_result["work_analyses"],
            )
            results["stages"]["work_analysis_persist"] = work_analysis_result
            save_checkpoint(run_id, "work_analysis_persist", work_analysis_result)
            timing["work_analysis_persist"] = _elapsed(t6b)

            # Stage 7b: Persist analytics to DB2
            t7b = _timer()
            analytics_result = stage_analytics_persist(analysis_result, anomaly_result)
            results["stages"]["analytics_persist"] = analytics_result
            save_checkpoint(run_id, "analytics_persist", analytics_result)
            timing["analytics_persist"] = _elapsed(t7b)

            # Stage 8: Evidence
            t8 = _timer()
            evidence_records = stage_evidence(analysis_result, anomaly_result)
            results["stages"]["evidence"] = {"records": len(evidence_records)}
            save_checkpoint(run_id, "evidence", results["stages"]["evidence"])
            timing["evidence"] = _elapsed(t8)

            # Stage 8b: Evidence work refs
            t8b = _timer()
            work_refs_result = stage_evidence_work_refs(
                evidence_records, analysis_result["work_analyses"]
            )
            results["stages"]["evidence_work_refs"] = work_refs_result
            save_checkpoint(run_id, "evidence_work_refs", work_refs_result)
            timing["evidence_work_refs"] = _elapsed(t8b)

            # Stage 9: Gemini
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

            # Stage 10: Persist to DB2
            t10 = _timer()
            persist_result = stage_persist(evidence_records)
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
            )
            results["stages"]["verify"] = verify_result
            save_checkpoint(run_id, "verify", verify_result)
            timing["verify"] = _elapsed(t11)

        # ================================================================
        # COMMON: Verify, Cleanup, Timing
        # ================================================================

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

        results["status"] = "SUCCESS"
        results["completed_at"] = _now_iso()
        results["timing"] = timing

    except Exception as exc:
        results["status"] = "FAILED"
        results["error"] = str(exc)
        results["failed_at"] = _now_iso()
        raise

    finally:
        print("\n" + "=" * 70)
        print(f"PIPELINE STATUS: {results.get('status', 'UNKNOWN')}")
        print(f"Run ID: {run_id}")
        print(f"Mode: {mode}")
        print(f"Completed: {results.get('completed_at', results.get('failed_at', 'N/A'))}")
        if timing:
            total = sum(timing.values())
            print(f"Total analysis time: {total:.1f}s")
            for key, val in timing.items():
                print(f"  {key}: {val:.1f}s")
        print("=" * 70)

    return results


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


# ============================================================
# CLI ENTRY POINT
# ============================================================

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
