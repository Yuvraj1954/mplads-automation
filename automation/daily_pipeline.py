import os
import sys
import json
import shutil
import subprocess
import time
import threading
import argparse
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

ROOT = Path(__file__).resolve().parent.parent
FETCHER = ROOT / "fetcher" / "fetcher.py"
COMPARATOR = ROOT / "comparator" / "comparator_v2.py"
INTELLIGENCE_BACKFILL = ROOT / "automation" / "intelligence_backfill.py"

sys.path.insert(0, str(ROOT))
from automation.snapshot_cache import (
    get_previous_local_path,
    get_previous_timestamp,
    get_current_timestamp,
    get_current_local_path,
    validate_cache,
    validate_bootstrap_cache,
    write_new_timestamp,
    write_metadata,
    METADATA_FILE,
)
from automation.observability import PipelineTimer, StageMetrics

BUCKET = "mplads-raw"
DATASETS = [
    "allocated_limit",
    "works_recommended",
    "works_sanctioned",
    "works_completed",
    "expenditure",
    "calamity",
    "mla_allocated_limit",
    "mla_works_recommended",
    "mla_works_sanctioned",
    "mla_works_completed",
    "mla_expenditure",
    "mla_calamity",
]

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SECRET_KEY = os.environ["SUPABASE_SECRET_KEY"]
SERVICE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")

if not SERVICE_KEY:
    raise RuntimeError("Missing SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_ANON_KEY)")

sb = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)


def run(cmd):
    print("$", " ".join(map(str, cmd)))
    subprocess.run(cmd, check=True)


def run_fetcher(cmd):
    """Run the fetcher, stream its output in real time, and capture stdout.

    Returns the list of stdout lines (as strings) so callers can parse
    machine-readable markers like LOCAL_SNAPSHOT_PATH=.
    """
    print("$", " ".join(map(str, cmd)))
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    captured = []
    for raw_line in iter(proc.stdout.readline, b""):
        line = raw_line.decode("utf-8", errors="replace")
        sys.stdout.write(line)
        sys.stdout.flush()
        captured.append(line)
    proc.wait()
    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, cmd)
    return captured


EXPECTED_DATASETS = [
    "allocated_limit",
    "works_recommended",
    "works_sanctioned",
    "works_completed",
    "expenditure",
    "calamity",
    "mla_allocated_limit",
    "mla_works_recommended",
    "mla_works_sanctioned",
    "mla_works_completed",
    "mla_expenditure",
    "mla_calamity",
]

def validate_local_snapshot(path):
    """Validate that the local snapshot directory has the expected structure."""
    p = Path(path)
    if not p.exists():
        raise RuntimeError(f"Local snapshot path does not exist: {p}")
    if not p.is_dir():
        raise RuntimeError(f"Local snapshot path is not a directory: {p}")
    
    # Check for at least one MP or MLA dataset
    found_datasets = []
    for dataset in EXPECTED_DATASETS:
        dataset_dir = p / dataset
        if dataset_dir.is_dir():
            part_files = list(dataset_dir.glob("part_*.ndjson"))
            if part_files:
                found_datasets.append(dataset)
    
    if not found_datasets:
        raise RuntimeError(
            f"Local snapshot has no valid dataset folders with part_*.ndjson files. "
            f"Expected at least one of: {EXPECTED_DATASETS}"
        )
    
    print(f"Local snapshot validated: {p}")
    print(f"Found datasets: {found_datasets}")


def validate_complete_snapshot(path):
    """Strict local snapshot validation before any remote write or promotion.

    Checks all 12 datasets, _COMPLETE.json, manifests, NDJSON parseability,
    chunk/file consistency, and record count consistency.
    Raises RuntimeError on any validation failure.
    """
    p = Path(path)
    errors = []

    if not p.exists():
        raise RuntimeError(f"Snapshot path does not exist: {p}")
    if not p.is_dir():
        raise RuntimeError(f"Snapshot path is not a directory: {p}")

    # --- _COMPLETE.json validation ---
    complete_file = p / "_COMPLETE.json"
    if not complete_file.exists():
        raise RuntimeError(f"Missing _COMPLETE.json in {p}")
    try:
        data = json.loads(complete_file.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, ValueError) as exc:
        raise RuntimeError(f"Invalid _COMPLETE.json: {exc}")
    if data.get("status") != "complete":
        raise RuntimeError(
            f"_COMPLETE.json status is not 'complete': {data.get('status')}"
        )
    marker_datasets = data.get("datasets", {})

    # --- Per-dataset validation ---
    total_parts = 0
    total_records = 0
    for ds in EXPECTED_DATASETS:
        ds_dir = p / ds
        if not ds_dir.is_dir():
            errors.append(f"Missing dataset directory: {ds}")
            continue

        part_files = sorted(ds_dir.glob("part_*.ndjson"))
        if not part_files:
            errors.append(f"No part_*.ndjson files in {ds}")
            continue
        total_parts += len(part_files)

        # Validate each NDJSON file is parseable
        for pf in part_files:
            try:
                with open(pf, "r", encoding="utf-8") as f:
                    for i, line in enumerate(f, 1):
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            json.loads(line)
                        except json.JSONDecodeError as exc:
                            errors.append(
                                f"Invalid NDJSON at {ds}/{pf.name}:{i}: {exc}"
                            )
                            break
            except Exception as exc:
                errors.append(f"Cannot read {ds}/{pf.name}: {exc}")

        # Validate manifest.json
        manifest_file = ds_dir / "manifest.json"
        if manifest_file.exists():
            try:
                mf = json.loads(manifest_file.read_text(encoding="utf-8"))
                if "records" not in mf or "chunks" not in mf:
                    errors.append(f"Manifest missing records/chunks in {ds}")
                elif not isinstance(mf["records"], int) or not isinstance(
                    mf["chunks"], int
                ):
                    errors.append(f"Manifest records/chunks not int in {ds}")
                else:
                    total_records += mf["records"]
            except (json.JSONDecodeError, ValueError) as exc:
                errors.append(f"Invalid manifest.json in {ds}: {exc}")

    if errors:
        raise RuntimeError(
            f"Snapshot validation failed ({len(errors)} issues):\n"
            + "\n".join(f"  - {e}" for e in errors)
        )

    # --- Record count consistency check ---
    marker_total_records = sum(
        v.get("records", 0) for v in marker_datasets.values() if isinstance(v, dict)
    )
    if marker_datasets and total_records > 0 and marker_total_records > 0:
        if total_records != marker_total_records:
            errors.append(
                f"Record count mismatch: manifests={total_records}, "
                f"_COMPLETE.json={marker_total_records}"
            )

    # --- Part file count consistency check ---
    marker_total_chunks = sum(
        v.get("chunks", 0) for v in marker_datasets.values() if isinstance(v, dict)
    )
    if marker_datasets and marker_total_chunks > 0:
        if total_parts != marker_total_chunks:
            errors.append(
                f"Part file count mismatch: actual={total_parts}, "
                f"_COMPLETE.json={marker_total_chunks}"
            )

    if errors:
        raise RuntimeError(
            f"Snapshot validation failed ({len(errors)} issues):\n"
            + "\n".join(f"  - {e}" for e in errors)
        )

    print(f"Strict snapshot validation PASSED: {p}")
    print(f"  Datasets: {len(EXPECTED_DATASETS)}, Parts: {total_parts}, "
          f"Records: {total_records}")


def validate_remote_snapshot(timestamp):
    """Verify snapshot completeness in Supabase Storage via metadata/list only.

    Downloads only _COMPLETE.json (small). Uses list() to verify files exist.
    Avoids downloading content files.
    """
    storage = sb.storage.from_(BUCKET)

    # 1. Verify _COMPLETE.json exists and is complete
    try:
        marker_bytes = storage.download(f"{timestamp}/_COMPLETE.json")
        marker = json.loads(marker_bytes.decode("utf-8"))
    except Exception as exc:
        raise RuntimeError(
            f"Remote snapshot {timestamp}: _COMPLETE.json missing or invalid: {exc}"
        )
    if marker.get("status") != "complete":
        raise RuntimeError(
            f"Remote snapshot {timestamp}: _COMPLETE.json status is "
            f"'{marker.get('status')}', expected 'complete'"
        )

    remote_datasets = marker.get("datasets", {})

    # 2. Verify files exist using list (metadata-only, no downloads)
    for ds_name, ds_info in remote_datasets.items():
        expected_chunks = ds_info.get("chunks", 0)
        if expected_chunks <= 0:
            continue
        try:
            rows = storage.list(f"{timestamp}/{ds_name}", {"limit": 1000, "offset": 0})
            actual_parts = sum(
                1 for r in (rows or [])
                if r.get("name", "").startswith("part_")
                and r.get("name", "").endswith(".ndjson")
            )
        except Exception:
            actual_parts = 0
        if actual_parts < expected_chunks:
            raise RuntimeError(
                f"Remote snapshot {timestamp}: {ds_name} has {actual_parts} "
                f"parts, expected {expected_chunks}"
            )

    print(f"Remote snapshot validation PASSED: {timestamp}")


def list_complete_snapshots():
    rows = sb.storage.from_(BUCKET).list("", {"limit": 1000, "offset": 0})
    snapshots = []
    for row in rows or []:
        name = row.get("name", "")
        if not name or name.startswith("delta_"):
            continue
        try:
            marker = sb.storage.from_(BUCKET).download(f"{name}/_COMPLETE.json")
            data = json.loads(marker.decode("utf-8"))
            if data.get("status") == "complete":
                snapshots.append(name)
        except Exception:
            pass
    return sorted(snapshots)


def select_previous_snapshot(all_snapshots, current_ts):
    """Select the correct previous snapshot from a list of complete snapshots.

    Returns the newest valid snapshot whose timestamp is strictly less than
    current_ts. Never returns the current snapshot as its own previous.

    Args:
        all_snapshots: sorted list of complete snapshot timestamps (ascending)
        current_ts: the newly fetched snapshot timestamp to exclude

    Returns:
        (timestamp, error_message) — timestamp is the selection or None on failure
    """
    candidates = [s for s in all_snapshots if s < current_ts]
    if not candidates:
        return None, (
            f"No valid snapshot older than current ({current_ts}). "
            f"Only found: {all_snapshots}"
        )
    selected = candidates[-1]
    print(f"Current snapshot: {current_ts}")
    print(f"Supabase fallback previous: {selected}")
    return selected, None


def assert_snapshot_complete(timestamp):
    marker = sb.storage.from_(BUCKET).download(f"{timestamp}/_COMPLETE.json")
    data = json.loads(marker.decode("utf-8"))
    if data.get("status") != "complete":
        raise RuntimeError(f"Snapshot {timestamp} is not complete")


def upload_delta(output_dir, run_id):
    storage = sb.storage.from_(BUCKET)
    for p in output_dir.rglob("*"):
        if p.is_file():
            rel = p.relative_to(output_dir).as_posix()
            storage.upload(
                f"{run_id}/{rel}",
                p.read_bytes(),
                {"upsert": "true", "content-type": "application/x-ndjson"},
            )


def call_edge(function_name, payload):
    import urllib.request

    url = f"{SUPABASE_URL.rstrip('/')}/functions/v1/{function_name}"
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {SERVICE_KEY}",
            "apikey": SERVICE_KEY,
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=300) as response:
        return json.loads(response.read().decode())


def delete_paths(paths):
    if paths:
        sb.storage.from_(BUCKET).remove(paths)


def delete_delta_run(run_id):
    storage = sb.storage.from_(BUCKET)
    all_paths = []
    for dataset in DATASETS:
        try:
            rows = storage.list(f"{run_id}/{dataset}", {"limit": 1000, "offset": 0})
            for row in rows or []:
                name = row.get("name")
                if name:
                    all_paths.append(f"{run_id}/{dataset}/{name}")
        except Exception:
            pass
    try:
        rows = storage.list(run_id, {"limit": 1000, "offset": 0})
        for row in rows or []:
            name = row.get("name")
            if name and not name in DATASETS:
                all_paths.append(f"{run_id}/{name}")
    except Exception:
        pass
    delete_paths(all_paths)


def delete_old_raw_snapshots(keep):
    snapshots = list_complete_snapshots()
    old = [s for s in snapshots if s not in set(keep)]
    storage = sb.storage.from_(BUCKET)

    for ts in old:
        paths = []
        for dataset in DATASETS:
            try:
                rows = storage.list(f"{ts}/{dataset}", {"limit": 1000, "offset": 0})
                for row in rows or []:
                    name = row.get("name")
                    if name:
                        paths.append(f"{ts}/{dataset}/{name}")
            except Exception:
                pass
        try:
            rows = storage.list(ts, {"limit": 1000, "offset": 0})
            for row in rows or []:
                name = row.get("name")
                if name and name not in DATASETS:
                    paths.append(f"{ts}/{name}")
        except Exception:
            pass
        delete_paths(paths)
        print("Deleted old snapshot:", ts)


def verify_run(run_id, expected_jobs):
    result = (
        sb.table("ingestion_jobs")
        .select("status", count="exact")
        .eq("run_id", run_id)
        .execute()
    )

    rows = result.data or []
    # The grouped count response can vary by client version, so query statuses directly.
    counts = {}
    for status in ["completed", "pending", "processing", "failed"]:
        r = (
            sb.table("ingestion_jobs")
            .select("status", count="exact")
            .eq("run_id", run_id)
            .eq("status", status)
            .execute()
        )
        counts[status] = r.count or 0

    total = sum(counts.values())
    print("Verification:", {"total": total, **counts})

    return (
        total == expected_jobs
        and counts["completed"] == expected_jobs
        and counts["pending"] == 0
        and counts["processing"] == 0
        and counts["failed"] == 0
    )


def _cleanup_local_snapshot(path):
    """Remove the temporary local snapshot directory.

    Failures are printed as warnings but do not raise, so an already-successful
    pipeline is not marked as failed.
    """
    if not path:
        return
    try:
        shutil.rmtree(path)
        print(f"Local snapshot cleaned up: {path}")
    except Exception as exc:
        print(f"WARNING: Could not remove local snapshot {path}: {exc}")


def _write_pipeline_success(new_ts, cache_work_dir):
    """Write pipeline success markers and preserve fetched snapshot for cache.

    Copies the fetched snapshot into $WORK_DIR/current/<new_ts>/ so the
    workflow can rotate it into the rolling cache.
    Verifies _COMPLETE.json is present in the cache snapshot.
    """
    if not cache_work_dir:
        return

    work_dir = Path(cache_work_dir)
    write_new_timestamp(new_ts, work_dir)

    prev_ts = get_previous_timestamp(work_dir)
    if prev_ts:
        write_metadata(prev_ts, new_ts, work_dir)
    else:
        write_metadata("none", new_ts, work_dir)

    # CRITICAL: Verify _COMPLETE.json is in cache current snapshot
    cache_snapshot = work_dir / "current" / new_ts
    if cache_snapshot.exists():
        _ensure_complete_json(cache_snapshot)
        print(f"Pipeline success: _COMPLETE.json verified in cache current/{new_ts}")
    else:
        print(f"WARNING: Cache current/{new_ts} not found during success write")

    print(f"Pipeline success markers written to {work_dir}")


def _ensure_complete_json(snapshot_dir):
    """Verify _COMPLETE.json exists in snapshot_dir; raise if missing or invalid."""
    p = Path(snapshot_dir)
    complete_file = p / "_COMPLETE.json"
    if not complete_file.exists():
        raise RuntimeError(
            f"CRITICAL: _COMPLETE.json missing in {p}. "
            f"Snapshot cannot be promoted to cache or Supabase without it."
        )
    try:
        data = json.loads(complete_file.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, ValueError) as exc:
        raise RuntimeError(f"CRITICAL: _COMPLETE.json is corrupt in {p}: {exc}")
    if data.get("status") != "complete":
        raise RuntimeError(
            f"CRITICAL: _COMPLETE.json status is '{data.get('status')}', "
            f"expected 'complete' in {p}"
        )
    return data


def _update_data_updated(status="complete"):
    """Upsert DB1 `public.data_updated` (id=1) so the frontend reflects this run.

    The frontend's `/api/data-updated` endpoint reads this row from DB1
    (see app/database.py:get_pool). Without this, the UI stays frozen
    even when the pipeline runs successfully.

    Uses the module-level `sb` Supabase client (DB1, initialized from
    SUPABASE_URL / SUPABASE_SECRET_KEY at the top of this file).
    Failures are logged but never raised, so a hiccup cannot fail an
    otherwise-successful pipeline run.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    body = {
        "id": 1,
        "completed_at": now_iso,
        "status": status,
        "updated_at": now_iso,
    }

    try:
        sb.table("data_updated").upsert(body, on_conflict="id").execute()
        print(f"  data_updated upserted (DB1): {now_iso} status={status}")
        return True
    except Exception as exc:
        print(f"  WARNING: data_updated upsert failed: {exc}")
        return False


def _preserve_fetched_snapshot(local_snapshot_path, new_ts, cache_work_dir):
    """Copy the fetched snapshot into the cache current/ directory.

    This must be called BEFORE _cleanup_local_snapshot removes the temp dir.
    Verifies _COMPLETE.json is present in source and target.
    """
    if not cache_work_dir or not local_snapshot_path:
        return

    src = Path(local_snapshot_path)
    if not src.exists():
        print(f"WARNING: Fetched snapshot {src} not found, cannot preserve for cache")
        return

    # CRITICAL: Verify _COMPLETE.json in source before copy
    _ensure_complete_json(src)

    work_dir = Path(cache_work_dir)
    target = work_dir / "current" / new_ts

    if target.exists():
        print(f"Cache current/{new_ts} already exists, skipping copy")
        # Still verify _COMPLETE.json is present in existing target
        _ensure_complete_json(target)
        return

    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(str(src), str(target))

    # CRITICAL: Verify _COMPLETE.json survived the copy
    _ensure_complete_json(target)
    print(f"Fetched snapshot preserved for cache: {target}")


def _timer():
    """Return a monotonic start time."""
    return time.time()


def _elapsed(start):
    """Return elapsed seconds since start."""
    return time.time() - start


def _print_timing(timing, pipeline_start):
    """Print performance instrumentation summary."""
    total = time.time() - pipeline_start
    print("\n--- TIMING ---")
    for key, val in timing.items():
        print(f"  {key:>20s}: {val:>7.1f}s")
    print(f"  {'TOTAL':>20s}: {total:>7.1f}s")
    print("--- END TIMING ---\n")


def _ensure_state_metrics_columns():
    """Idempotently add the 40/40/20 ranking columns to state_metrics.

    Mirrors migration/2026_09_12_state_rank_40_40_20.sql. Runs on every
    pipeline start; ADD COLUMN IF NOT EXISTS makes it a no-op once the
    columns exist. Failure is logged but never raised, since the columns
    may already exist under a slightly different shape or be denied to
    the role we have.
    """
    sb_local = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)
    expected = ("scale_score", "performance_score_weighted")
    try:
        rows = sb_local.table("state_metrics").select(
            "state_id," + ",".join(expected)
        ).limit(1).execute().data
        present = set(rows[0].keys()) if rows else set()
        missing = [c for c in expected if c not in present]
        if not missing:
            return
        # PostgREST schema cache shows missing columns; the actual table may
        # already have them. Probe with a tiny insert/update to confirm.
        # If truly missing, we cannot ALTER via the REST API — the migration
        # SQL in migration/2026_09_12_state_rank_40_40_20.sql must be run
        # by the operator. Log a clear warning.
        print(
            f"  WARNING: state_metrics columns missing in schema cache: {missing}. "
            f"Run migration/2026_09_12_state_rank_40_40_20.sql in the DB2 "
            f"Supabase SQL editor once, then re-run the pipeline."
        )
    except Exception as exc:
        print(f"  WARNING: could not verify state_metrics columns: {exc}")


def _ensure_member_metrics_columns():
    """Idempotently add the 40/40/20 ranking columns to member_metrics.

    Mirrors migration/2026_09_12_member_rank_40_40_20.sql.
    """
    sb_local = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)
    expected = ("scale_score", "performance_score_weighted")
    try:
        rows = sb_local.table("member_metrics").select(
            "member_id," + ",".join(expected)
        ).limit(1).execute().data
        present = set(rows[0].keys()) if rows else set()
        missing = [c for c in expected if c not in present]
        if not missing:
            return
        print(
            f"  WARNING: member_metrics columns missing in schema cache: {missing}. "
            f"Run migration/2026_09_12_member_rank_40_40_20.sql in the DB2 "
            f"Supabase SQL editor once, then re-run the pipeline."
        )
    except Exception as exc:
        print(f"  WARNING: could not verify member_metrics columns: {exc}")


_CREATE_WORK_ANALYSIS_SQL = """
CREATE TABLE IF NOT EXISTS public.work_analysis (
    work_id                                integer      PRIMARY KEY,
    member_id                              integer      NOT NULL,
    member_type                            text         NOT NULL DEFAULT 'MP',
    constituency_id                        integer,
    state_id                               integer,
    state_name                             text         DEFAULT '',
    work_category                          text         DEFAULT '',
    activity_name                          text         DEFAULT '',
    normalized_activity                    text,
    work_description                       text         DEFAULT '',
    status                                 text,
    recommended_amount                     numeric,
    sanction_amount                        numeric,
    expenditure_amount                     numeric,
    completion_amount                      numeric,
    recommendation_date                    text,
    sanction_date                          text,
    first_expenditure_date                 text,
    last_expenditure_date                  text,
    completion_date                        text,
    sanction_delay_days                    integer,
    project_age_days                       integer,
    execution_days                         integer,
    pending_days                           integer,
    expenditure_percentage                 numeric,
    completion_percentage                  numeric,
    benchmark_peer_group                   text,
    benchmark_quality                      text,
    benchmark_sample_size                  integer,
    cost_p25                               numeric,
    cost_p50                               numeric,
    cost_p75                               numeric,
    cost_p90                               numeric,
    cost_p95                               numeric,
    duration_p25                           numeric,
    duration_p50                           numeric,
    duration_p75                           numeric,
    duration_p90                           numeric,
    duration_p95                           numeric,
    cost_percentile                        numeric,
    duration_percentile                    numeric,
    cost_status                            text,
    duration_status                        text,
    cost_deviation_from_median_percentage  numeric,
    duration_deviation_from_median_percentage numeric,
    risk_flags                             jsonb        DEFAULT '[]'::jsonb,
    flag_count                             integer      DEFAULT 0,
    risk_level                             text,
    last_calculated                        text,
    delay_probability                      numeric,
    delay_risk_band                        text,
    isolation_score                        numeric,
    isolation_level                        text,
    feature_fingerprint                    text
);

CREATE INDEX IF NOT EXISTS idx_work_analysis_work_id        ON public.work_analysis (work_id);
CREATE INDEX IF NOT EXISTS idx_work_analysis_member         ON public.work_analysis (member_id);
CREATE INDEX IF NOT EXISTS idx_work_analysis_state          ON public.work_analysis (state_id);
CREATE INDEX IF NOT EXISTS idx_work_analysis_status         ON public.work_analysis (status);
CREATE INDEX IF NOT EXISTS idx_work_analysis_flags          ON public.work_analysis (risk_flags);
CREATE INDEX IF NOT EXISTS idx_work_analysis_risk           ON public.work_analysis (risk_level);
CREATE INDEX IF NOT EXISTS idx_work_analysis_activity_state ON public.work_analysis (normalized_activity, state_id);
"""


_CREATE_MLA_WORK_ANALYSIS_SQL = """
CREATE TABLE IF NOT EXISTS public.mla_work_analysis (
    work_id                                integer      PRIMARY KEY,
    member_id                              integer      NOT NULL,
    member_type                            text         NOT NULL DEFAULT 'MLA',
    constituency_id                        integer,
    state_id                               integer,
    state_name                             text         DEFAULT '',
    work_category                          text         DEFAULT '',
    activity_name                          text         DEFAULT '',
    normalized_activity                    text,
    work_description                       text         DEFAULT '',
    status                                 text,
    recommended_amount                     numeric,
    sanction_amount                        numeric,
    expenditure_amount                     numeric,
    completion_amount                      numeric,
    recommendation_date                    text,
    sanction_date                          text,
    first_expenditure_date                 text,
    last_expenditure_date                  text,
    completion_date                        text,
    sanction_delay_days                    integer,
    project_age_days                       integer,
    execution_days                         integer,
    pending_days                           integer,
    expenditure_percentage                 numeric,
    completion_percentage                  numeric,
    benchmark_peer_group                   text,
    benchmark_quality                      text,
    benchmark_sample_size                  integer,
    cost_p25                               numeric,
    cost_p50                               numeric,
    cost_p75                               numeric,
    cost_p90                               numeric,
    cost_p95                               numeric,
    duration_p25                           numeric,
    duration_p50                           numeric,
    duration_p75                           numeric,
    duration_p90                           numeric,
    duration_p95                           numeric,
    cost_percentile                        numeric,
    duration_percentile                    numeric,
    cost_status                            text,
    duration_status                        text,
    cost_deviation_from_median_percentage  numeric,
    duration_deviation_from_median_percentage numeric,
    risk_flags                             jsonb        DEFAULT '[]'::jsonb,
    flag_count                             integer      DEFAULT 0,
    risk_level                             text,
    last_calculated                        text,
    delay_probability                      numeric,
    delay_risk_band                        text,
    isolation_score                        numeric,
    isolation_level                        text,
    feature_fingerprint                    text
);

CREATE INDEX IF NOT EXISTS idx_mla_work_analysis_work_id        ON public.mla_work_analysis (work_id);
CREATE INDEX IF NOT EXISTS idx_mla_work_analysis_member         ON public.mla_work_analysis (member_id);
CREATE INDEX IF NOT EXISTS idx_mla_work_analysis_state          ON public.mla_work_analysis (state_id);
CREATE INDEX IF NOT EXISTS idx_mla_work_analysis_status         ON public.mla_work_analysis (status);
CREATE INDEX IF NOT EXISTS idx_mla_work_analysis_flags          ON public.mla_work_analysis (risk_flags);
CREATE INDEX IF NOT EXISTS idx_mla_work_analysis_risk           ON public.mla_work_analysis (risk_level);
CREATE INDEX IF NOT EXISTS idx_mla_work_analysis_activity_state ON public.mla_work_analysis (normalized_activity, state_id);
"""


def _ensure_work_analysis_table():
    """Verify work_analysis table exists on DB1 via Supabase REST.

    This table is required for Stage 6b MP persistence. If missing,
    the pipeline cannot proceed and must fail immediately.

    DDL is handled by explicit migrations (not the daily pipeline).
    GitHub Actions runners cannot reach port 5432 (direct PostgreSQL),
    so asyncpg DDL is not used here.
    """
    sb_local = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)
    try:
        sb_local.table("work_analysis").select("work_id", count="exact").limit(0).execute()
        print("  work_analysis table: ACCESSIBLE")
    except Exception as exc:
        msg = str(exc)
        if "PGRST205" in msg or "does not exist" in msg.lower():
            raise RuntimeError(
                "FATAL: public.work_analysis table is missing from DB1. "
                "Stage 6b requires this table. Run the migration SQL in the "
                "Supabase SQL Editor before re-running the pipeline."
            )
        raise


def _ensure_mla_work_analysis_table():
    """Verify mla_work_analysis table exists on DB1 via Supabase REST.

    This table is required for Stage 6b MLA persistence. If missing,
    the pipeline cannot proceed and must fail immediately.

    DDL is handled by explicit migrations (not the daily pipeline).
    GitHub Actions runners cannot reach port 5432 (direct PostgreSQL),
    so asyncpg DDL is not used here.
    """
    sb_local = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)
    try:
        sb_local.table("mla_work_analysis").select("work_id", count="exact").limit(0).execute()
        print("  mla_work_analysis table: ACCESSIBLE")
    except Exception as exc:
        msg = str(exc)
        if "PGRST205" in msg or "does not exist" in msg.lower():
            raise RuntimeError(
                "FATAL: public.mla_work_analysis table is missing from DB1. "
                "Stage 6b requires this table. Run "
                "migration/2026_09_16_create_mla_work_analysis.sql in the "
                "Supabase SQL Editor before re-running the pipeline."
            )
        raise


_CREATE_CATEGORY_FY_VIEWS_SQL = """
-- category_metrics: national + per-state category intelligence
CREATE OR REPLACE VIEW public.category_metrics AS
WITH works AS (
    SELECT
        COALESCE(NULLIF(TRIM(regexp_replace(normalized_activity, '^NA-', '', 'i')), ''), NULLIF(TRIM(work_category), ''), 'UNCLASSIFIED') AS category,
        state_id, member_id, member_type, status, work_id,
        recommended_amount, sanction_amount, expenditure_amount,
        execution_days, project_age_days,
        COALESCE(flag_count, 0) AS flag_count,
        cost_status, duration_status
    FROM public.work_analysis
    UNION ALL
    SELECT
        COALESCE(NULLIF(TRIM(regexp_replace(normalized_activity, '^NA-', '', 'i')), ''), NULLIF(TRIM(work_category), ''), 'UNCLASSIFIED') AS category,
        state_id, member_id, member_type, status, work_id,
        recommended_amount, sanction_amount, expenditure_amount,
        execution_days, project_age_days,
        COALESCE(flag_count, 0) AS flag_count,
        cost_status, duration_status
    FROM public.mla_work_analysis
),
grouped AS (
    SELECT
        'NATIONAL'::text AS scope,
        NULL::bigint AS state_id,
        category,
        COUNT(*) AS total_works,
        COUNT(DISTINCT state_id) AS distinct_states,
        COUNT(DISTINCT (member_type, member_id)) AS distinct_members,
        COUNT(*) FILTER (WHERE LOWER(status) = 'completed') AS completed_works,
        COUNT(*) FILTER (WHERE LOWER(status) = 'in progress') AS ongoing_works,
        COUNT(*) FILTER (WHERE LOWER(status) = 'recommended') AS recommended_works,
        COUNT(*) FILTER (WHERE sanction_amount > 0) AS sanctioned_works,
        COALESCE(SUM(recommended_amount), 0) AS recommended_amount,
        COALESCE(SUM(sanction_amount), 0) AS sanctioned_amount,
        COALESCE(SUM(expenditure_amount), 0) AS expenditure_amount,
        COUNT(*) FILTER (WHERE project_age_days > 365 AND LOWER(status) <> 'completed') AS overdue_works,
        COUNT(*) FILTER (WHERE flag_count >= 1) AS flagged_works,
        COUNT(*) FILTER (WHERE cost_status IN ('VERY_HIGH','HIGH')) AS cost_anomaly_works,
        COUNT(*) FILTER (WHERE duration_status IN ('VERY_LONG','LONG')) AS duration_anomaly_works,
        AVG(sanction_amount) FILTER (WHERE sanction_amount > 0) AS avg_work_cost,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sanction_amount) FILTER (WHERE sanction_amount > 0) AS median_work_cost,
        AVG(execution_days) FILTER (WHERE execution_days IS NOT NULL) AS avg_execution_days,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY execution_days) FILTER (WHERE execution_days IS NOT NULL) AS median_execution_days
    FROM works
    GROUP BY category
    UNION ALL
    SELECT
        'STATE'::text AS scope,
        state_id,
        category,
        COUNT(*),
        COUNT(DISTINCT state_id),
        COUNT(DISTINCT (member_type, member_id)),
        COUNT(*) FILTER (WHERE LOWER(status) = 'completed'),
        COUNT(*) FILTER (WHERE LOWER(status) = 'in progress'),
        COUNT(*) FILTER (WHERE LOWER(status) = 'recommended'),
        COUNT(*) FILTER (WHERE sanction_amount > 0),
        COALESCE(SUM(recommended_amount), 0),
        COALESCE(SUM(sanction_amount), 0),
        COALESCE(SUM(expenditure_amount), 0),
        COUNT(*) FILTER (WHERE project_age_days > 365 AND LOWER(status) <> 'completed'),
        COUNT(*) FILTER (WHERE flag_count >= 1),
        COUNT(*) FILTER (WHERE cost_status IN ('VERY_HIGH','HIGH')),
        COUNT(*) FILTER (WHERE duration_status IN ('VERY_LONG','LONG')),
        AVG(sanction_amount) FILTER (WHERE sanction_amount > 0),
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sanction_amount) FILTER (WHERE sanction_amount > 0),
        AVG(execution_days) FILTER (WHERE execution_days IS NOT NULL),
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY execution_days) FILTER (WHERE execution_days IS NOT NULL)
    FROM works
    WHERE state_id IS NOT NULL
    GROUP BY state_id, category
)
SELECT
    scope,
    state_id,
    category,
    total_works AS sample_size,
    distinct_states,
    distinct_members,
    completed_works,
    ongoing_works,
    recommended_works,
    sanctioned_works,
    recommended_amount,
    sanctioned_amount,
    expenditure_amount,
    ROUND(completed_works::numeric / NULLIF(total_works, 0) * 100, 2) AS completion_rate_pct,
    ROUND(sanctioned_works::numeric / NULLIF(total_works, 0) * 100, 2) AS sanction_rate_pct,
    ROUND(expenditure_amount / NULLIF(sanctioned_amount, 0) * 100, 2) AS utilization_pct,
    ROUND(avg_work_cost::numeric, 2) AS avg_work_cost,
    ROUND(median_work_cost::numeric, 2) AS median_work_cost,
    ROUND(avg_execution_days::numeric, 2) AS avg_execution_days,
    ROUND(median_execution_days::numeric, 2) AS median_execution_days,
    ROUND(overdue_works::numeric / NULLIF(total_works, 0) * 100, 2) AS overdue_rate_pct,
    ROUND(flagged_works::numeric / NULLIF(total_works, 0) * 100, 2) AS risk_rate_pct,
    ROUND(cost_anomaly_works::numeric / NULLIF(total_works, 0) * 100, 2) AS cost_anomaly_rate_pct,
    ROUND(duration_anomaly_works::numeric / NULLIF(total_works, 0) * 100, 2) AS duration_anomaly_rate_pct,
    CASE WHEN total_works >= 100 THEN 'HIGH'
         WHEN total_works >= 20 THEN 'MEDIUM'
         WHEN total_works >= 5 THEN 'LOW'
         ELSE 'INSUFFICIENT' END AS confidence
FROM grouped;

-- fy_metrics: real fiscal-year analytics (MPLADS FY = April–March)
CREATE OR REPLACE VIEW public.fy_metrics AS
WITH
rec AS (
    SELECT
        CASE WHEN EXTRACT(MONTH FROM recommendation_date) >= 4
             THEN EXTRACT(YEAR FROM recommendation_date)
             ELSE EXTRACT(YEAR FROM recommendation_date) - 1 END::int AS fy_start,
        member_type,
        COUNT(*) AS works,
        COALESCE(SUM(recommended_amount), 0) AS amount
    FROM public.work_analysis WHERE recommendation_date IS NOT NULL GROUP BY 1, member_type
    UNION ALL
    SELECT
        CASE WHEN EXTRACT(MONTH FROM recommendation_date) >= 4
             THEN EXTRACT(YEAR FROM recommendation_date)
             ELSE EXTRACT(YEAR FROM recommendation_date) - 1 END::int,
        member_type, COUNT(*), COALESCE(SUM(recommended_amount), 0)
    FROM public.mla_work_analysis WHERE recommendation_date IS NOT NULL GROUP BY 1, member_type
),
san AS (
    SELECT
        CASE WHEN EXTRACT(MONTH FROM sanction_date) >= 4
             THEN EXTRACT(YEAR FROM sanction_date)
             ELSE EXTRACT(YEAR FROM sanction_date) - 1 END::int AS fy_start,
        member_type, COUNT(*) AS works, COALESCE(SUM(sanction_amount), 0) AS amount
    FROM public.work_analysis WHERE sanction_date IS NOT NULL GROUP BY 1, member_type
    UNION ALL
    SELECT
        CASE WHEN EXTRACT(MONTH FROM sanction_date) >= 4
             THEN EXTRACT(YEAR FROM sanction_date)
             ELSE EXTRACT(YEAR FROM sanction_date) - 1 END::int,
        member_type, COUNT(*), COALESCE(SUM(sanction_amount), 0)
    FROM public.mla_work_analysis WHERE sanction_date IS NOT NULL GROUP BY 1, member_type
),
comp AS (
    SELECT
        CASE WHEN EXTRACT(MONTH FROM completion_date) >= 4
             THEN EXTRACT(YEAR FROM completion_date)
             ELSE EXTRACT(YEAR FROM completion_date) - 1 END::int AS fy_start,
        member_type, COUNT(*) AS works, COALESCE(SUM(completion_amount), 0) AS amount
    FROM public.work_analysis WHERE completion_date IS NOT NULL GROUP BY 1, member_type
    UNION ALL
    SELECT
        CASE WHEN EXTRACT(MONTH FROM completion_date) >= 4
             THEN EXTRACT(YEAR FROM completion_date)
             ELSE EXTRACT(YEAR FROM completion_date) - 1 END::int,
        member_type, COUNT(*), COALESCE(SUM(completion_amount), 0)
    FROM public.mla_work_analysis WHERE completion_date IS NOT NULL GROUP BY 1, member_type
),
exp AS (
    SELECT
        CASE WHEN EXTRACT(MONTH FROM we.expenditure_date) >= 4
             THEN EXTRACT(YEAR FROM we.expenditure_date)
             ELSE EXTRACT(YEAR FROM we.expenditure_date) - 1 END::int AS fy_start,
        wa.member_type, COUNT(*) AS works,
        COALESCE(SUM(we.fund_disbursed_amount), 0) AS amount
    FROM public.work_expenditures we
    JOIN public.work_analysis wa ON wa.work_id = we.work_id
    WHERE we.expenditure_date IS NOT NULL GROUP BY 1, wa.member_type
    UNION ALL
    SELECT
        CASE WHEN EXTRACT(MONTH FROM me.expenditure_date) >= 4
             THEN EXTRACT(YEAR FROM me.expenditure_date)
             ELSE EXTRACT(YEAR FROM me.expenditure_date) - 1 END::int,
        mwa.member_type, COUNT(*), COALESCE(SUM(me.fund_disbursed_amount), 0)
    FROM public.mla_work_expenditures me
    JOIN public.mla_work_analysis mwa ON mwa.work_id = me.work_id
    WHERE me.expenditure_date IS NOT NULL GROUP BY 1, mwa.member_type
)
SELECT
    COALESCE(r.fy_start, s.fy_start, c.fy_start, e.fy_start) AS fy_start,
    (COALESCE(r.fy_start, s.fy_start, c.fy_start, e.fy_start)::text || '-' ||
     LPAD(((COALESCE(r.fy_start, s.fy_start, c.fy_start, e.fy_start) + 1) % 100)::text, 2, '0')) AS fy_label,
    COALESCE(r.member_type, s.member_type, c.member_type, e.member_type) AS member_type,
    COALESCE(r.works, 0) AS recommended_works,
    COALESCE(s.works, 0) AS sanctioned_works,
    COALESCE(c.works, 0) AS completed_works,
    COALESCE(e.works, 0) AS expenditure_count,
    COALESCE(r.amount, 0) AS recommended_amount,
    COALESCE(s.amount, 0) AS sanctioned_amount,
    COALESCE(c.amount, 0) AS completion_amount,
    COALESCE(e.amount, 0) AS expenditure_amount
FROM rec r
FULL OUTER JOIN san s ON s.fy_start = r.fy_start AND s.member_type = r.member_type
FULL OUTER JOIN comp c ON c.fy_start = COALESCE(r.fy_start, s.fy_start) AND c.member_type = COALESCE(r.member_type, s.member_type)
FULL OUTER JOIN exp e ON e.fy_start = COALESCE(r.fy_start, s.fy_start, c.fy_start) AND e.member_type = COALESCE(r.member_type, s.member_type, c.member_type)
ORDER BY fy_start, member_type;
"""


def _ensure_category_fy_views():
    """Ensure category_metrics and fy_metrics SQL VIEWs exist in DB1.

    These are LIVE views over work_analysis + mla_work_analysis that
    auto-update when the pipeline writes to underlying tables. They are
    defined in migration/2026_09_14_category_fy_views.sql and must exist
    for the frontend to query category and fiscal-year analytics.
    """
    import asyncio, asyncpg

    db1_url = os.environ.get("DATABASE_URL") or os.environ.get("NEW_DB1_URL", "")
    if not db1_url:
        print("  WARNING: cannot verify category_metrics/fy_views — no DATABASE_URL / NEW_DB1_URL")
        return

    async def _exec():
        conn = await asyncpg.connect(dsn=db1_url, timeout=15, command_timeout=30)
        try:
            for view_name in ("category_metrics", "fy_metrics"):
                exists = await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM information_schema.views "
                    "WHERE table_schema='public' AND table_name=$1)",
                    view_name,
                )
                if exists:
                    print(f"  {view_name} view: EXISTS")
                else:
                    print(f"  {view_name} view: MISSING — creating now ...")
                    await conn.execute(_CREATE_CATEGORY_FY_VIEWS_SQL)
                    print(f"  {view_name} view: CREATED")
                    break  # Both views created in one statement
        finally:
            await conn.close()

    try:
        asyncio.run(_exec())
    except Exception as exc:
        print(f"  WARNING: could not verify/create category_metrics/fy_views: {exc}")
        print("  If this persists, run migration/2026_09_14_category_fy_views.sql manually")


def _ensure_feature_fingerprint_column():
    """Add feature_fingerprint column to work_analysis and mla_work_analysis.

    This column stores a SHA-256 hash of the ML-relevant deterministic
    features (Isolation Forest features). Stage 6b uses it to detect
    whether ML predictions are still valid after a deterministic re-insert.
    Idempotent (ALTER TABLE IF NOT EXISTS pattern via DO block).
    """
    import asyncio, asyncpg

    db1_url = os.environ.get("DATABASE_URL") or os.environ.get("NEW_DB1_URL", "")
    if not db1_url:
        print("  WARNING: cannot verify feature_fingerprint column — no DATABASE_URL / NEW_DB1_URL")
        return

    alter_sql = """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'work_analysis'
            AND column_name = 'feature_fingerprint'
        ) THEN
            ALTER TABLE public.work_analysis ADD COLUMN feature_fingerprint text;
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'mla_work_analysis'
            AND column_name = 'feature_fingerprint'
        ) THEN
            ALTER TABLE public.mla_work_analysis ADD COLUMN feature_fingerprint text;
        END IF;
    END$$;
    """

    async def _exec():
        conn = await asyncpg.connect(dsn=db1_url, timeout=15, command_timeout=30)
        try:
            await conn.execute(alter_sql)
            print("  feature_fingerprint column: VERIFIED (work_analysis + mla_work_analysis)")
        finally:
            await conn.close()

    try:
        asyncio.run(_exec())
    except Exception as exc:
        print(f"  WARNING: could not verify/create feature_fingerprint column: {exc}")
        print("  ML fingerprint preservation will be unavailable until this column exists")


def main():
    parser = argparse.ArgumentParser(description="MPLADS Daily Pipeline")
    parser.add_argument("--skip-gemini", action="store_true",
                        help="Skip Gemini AI analysis step")
    parser.add_argument("--skip-ingest", action="store_true",
                        help="Skip DB ingestion steps (stages 8a-8c)")
    parser.add_argument("--skip-fetch", action="store_true",
                        help="Skip government fetch (use existing current snapshot)")
    parser.add_argument("--mode", choices=["full", "affected"], default="affected",
                        help="Pipeline mode: full (recompute everything) or "
                             "affected (process only changed entities) (default: affected)")
    parser.add_argument("--bootstrap", action="store_true",
                        help="One-time NEW DB bootstrap from cached snapshot "
                             "(skips comparator, processes full snapshot)")
    parser.add_argument("--local-only", action="store_true",
                        help="Fetch locally only, skip Supabase Storage upload")
    parser.add_argument("--skip-intelligence", action="store_true",
                        help="Skip the intelligence/ML backfill stage")
    args = parser.parse_args()

    pipeline_start = _timer()
    timing = {}

    if not FETCHER.exists():
        raise RuntimeError(f"Fetcher not found: {FETCHER}")
    if not COMPARATOR.exists():
        raise RuntimeError(
            f"Comparator not found: {COMPARATOR}. "
            "Change COMPARATOR near the top of automation/daily_pipeline.py "
            "to the real comparator path in your repo."
        )

    print("\n=== STEP 0: ENSURE SCHEMA ===")
    _ensure_state_metrics_columns()
    _ensure_member_metrics_columns()
    _ensure_work_analysis_table()
    _ensure_mla_work_analysis_table()
    _ensure_feature_fingerprint_column()
    _ensure_category_fy_views()

    sync_interval = os.environ.get("SYNC_INTERVAL_HOURS", "24")
    print(f"Sync interval: {sync_interval} hours")
    print(f"MP datasets: {len([d for d in DATASETS if not d.startswith('mla_')])}")
    print(f"MLA datasets: {len([d for d in DATASETS if d.startswith('mla_')])}")
    print(f"Mode: {args.mode}")
    print(f"Bootstrap: {args.bootstrap}")
    print(f"Local only: {args.local_only}")

    cache_work_dir = os.environ.get("MPLADS_CACHE_WORK_DIR")
    print(f"Cache work dir: {cache_work_dir or 'not set (Supabase-only mode)'}")

    # Clear stale failure-cleanup timestamp from previous runs.
    # .current_snapshot_ts must only contain THIS run's fetched snapshot.
    # If the file retains a previous run's timestamp and this run fails
    # before the fetch, workflow cleanup would delete the wrong snapshot.
    if cache_work_dir:
        stale_ts_file = Path(cache_work_dir) / ".current_snapshot_ts"
        if stale_ts_file.exists():
            try:
                stale_ts_file.unlink()
                print(f"Cleared stale .current_snapshot_ts from previous run")
            except Exception:
                pass

    # ================================================================
    # BOOTSTRAP MODE
    # ================================================================
    if args.bootstrap:
        print("\n=== BOOTSTRAP MODE ===")
        if not cache_work_dir:
            raise RuntimeError(
                "Bootstrap requires MPLADS_CACHE_WORK_DIR to locate cached snapshot"
            )

        cache_dir = Path(cache_work_dir)

        # Bootstrap only needs a valid current snapshot.
        # Validate current independently — previous snapshot is irrelevant.
        ok, err = validate_bootstrap_cache(cache_dir)
        if not ok:
            raise RuntimeError(
                f"Bootstrap cache validation failed: {err}. "
                "Bootstrap requires a valid current snapshot in the cache. "
                "Run the full pipeline at least once first."
            )

        curr_path = get_current_local_path(cache_dir)
        if not curr_path:
            raise RuntimeError(
                "Bootstrap: current snapshot validation passed but path resolution failed. "
                "This should not happen — please check the cache structure."
            )

        snapshot_path = str(curr_path)
        validate_local_snapshot(snapshot_path)
        print(f"Bootstrap source: {snapshot_path}")

        from datetime import date as _date
        from automation.pipeline_controller import run_pipeline as run_analysis

        t_bootstrap = _timer()
        analysis_result = run_analysis(
            snapshot_dir=snapshot_path,
            reference_date=_date.today(),
            run_id="bootstrap",
            skip_gemini=args.skip_gemini,
            skip_ingest=False,
            mode="full",
        )
        timing["bootstrap_analysis"] = _elapsed(t_bootstrap)

        if analysis_result.get("status") not in ("SUCCESS", "DRY_RUN", "SUCCESS_WITH_WARNINGS"):
            raise RuntimeError(
                f"Bootstrap analysis failed: {analysis_result.get('error', 'unknown')}"
            )

        print("\n=== BOOTSTRAP COMPLETE ===")
        _update_data_updated(status="complete")
        _print_timing(timing, pipeline_start)
        return

    # ================================================================
    # NORMAL PIPELINE
    # ================================================================
    print("=== STEP 1: FETCH ===")
    t_fetch = _timer()
    if args.skip_fetch:
        print("Skipping fetch (--skip-fetch)")
        local_snapshot_path = None
    else:
        fetcher_cmd = [sys.executable, str(FETCHER)]
        if args.local_only:
            fetcher_cmd.append("--local-only")
        fetcher_output = run_fetcher(fetcher_cmd)
        local_snapshot_path = None
        for line in fetcher_output:
            if line.startswith("LOCAL_SNAPSHOT_PATH="):
                local_snapshot_path = line.split("=", 1)[1].strip()
                break

        if local_snapshot_path is None:
            raise RuntimeError(
                "Fetcher completed but did not emit LOCAL_SNAPSHOT_PATH=. "
                "The fetcher process may have failed silently or produced "
                "unexpected output. Check fetcher logs above."
            )

    if local_snapshot_path:
        validate_local_snapshot(local_snapshot_path)

    new_ts = os.path.basename(local_snapshot_path) if local_snapshot_path else None

    # Extract the clean timestamp from _COMPLETE.json for Supabase path.
    # The directory basename has tempfile prefix/suffix (mplads_local_..._xyz)
    # but the fetcher uploads to Supabase using the clean timestamp inside _COMPLETE.json.
    if local_snapshot_path:
        try:
            complete_file = Path(local_snapshot_path) / "_COMPLETE.json"
            if complete_file.exists():
                marker_data = json.loads(complete_file.read_text(encoding="utf-8"))
                clean_ts = marker_data.get("timestamp", "")
                if clean_ts:
                    new_ts = clean_ts
        except Exception:
            pass
    timing["fetch"] = _elapsed(t_fetch)

    # Write snapshot timestamp early so workflow cleanup can find it on failure
    if new_ts and cache_work_dir:
        ts_file = Path(cache_work_dir) / ".current_snapshot_ts"
        try:
            ts_file.parent.mkdir(parents=True, exist_ok=True)
            ts_file.write_text(new_ts, encoding="utf-8")
        except Exception:
            pass

    # Strict local validation BEFORE any remote write trust
    if local_snapshot_path:
        validate_complete_snapshot(local_snapshot_path)

    # Remote snapshot validation (verifies fetcher's Storage upload is complete)
    if new_ts and not args.local_only:
        try:
            validate_remote_snapshot(new_ts)
        except Exception as exc:
            print(f"FATAL: Remote snapshot validation failed: {exc}")
            print("Local snapshot is valid but remote upload may be incomplete.")
            print("Failing closed to prevent analysis on incomplete data.")
            sys.exit(1)

    print("=== STEP 2: GET PREVIOUS SNAPSHOT ===")
    t_compare_start = _timer()
    old_timestamp = None
    old_local_path = None

    use_supabase_fallback = os.environ.get("USE_SUPABASE_FALLBACK", "false").lower() == "true"

    if use_supabase_fallback:
        print("EMERGENCY Supabase fallback requested (consecutive_failures >= 3)")
        snapshots = list_complete_snapshots()
        old_timestamp, sel_err = select_previous_snapshot(snapshots, new_ts)
        if sel_err:
            print(f"WARNING: {sel_err}")
    elif cache_work_dir:
        cache_dir = Path(cache_work_dir)
        ok, err = validate_cache(cache_dir)
        if ok:
            curr_path = get_current_local_path(cache_dir)
            if curr_path:
                old_timestamp = get_current_timestamp(cache_dir)
                old_local_path = str(curr_path)
                print(f"Previous snapshot from cache: {old_timestamp}")
            else:
                print("WARNING: Cache valid but no current snapshot found")
        else:
            print(f"WARNING: Cache validation failed: {err}")

    if not old_timestamp:
        print("ERROR: No previous snapshot available from cache.")
        if not use_supabase_fallback:
            print("Supabase fallback is NOT enabled.")
            print("Failing safely. The pipeline will retry on next scheduled run.")
            raise RuntimeError(
                "No valid previous snapshot from GitHub cache. "
                "Supabase fallback is only available after 3 consecutive failures."
            )
        print("Supabase fallback failed too. No previous snapshot available.")

    if not old_timestamp:
        print("No previous snapshot found. Bootstrap mode — nothing to compare.")
        if local_snapshot_path:
            _preserve_fetched_snapshot(local_snapshot_path, new_ts, cache_work_dir)
            _cleanup_local_snapshot(local_snapshot_path)
        return

    if old_timestamp == new_ts:
        raise RuntimeError(
            f"CRITICAL: Previous snapshot ({old_timestamp}) is identical to "
            f"current snapshot ({new_ts}). This means the pipeline would compare "
            "a snapshot with itself, producing false 0-change results. "
            "This should never happen — the fallback selector is broken."
        )

    print("=== STEP 3: COMPARE ===")
    if not local_snapshot_path:
        raise RuntimeError("No new snapshot to compare (fetch was skipped or failed)")

    workdir = ROOT / ".automation_delta"
    if workdir.exists():
        shutil.rmtree(workdir)
    workdir.mkdir()

    run_id = f"delta_{new_ts}"

    cmd = [
        sys.executable,
        str(COMPARATOR),
        "--old", old_timestamp,
        "--new", new_ts,
        "--new-local", local_snapshot_path,
        "--output", str(workdir),
    ]
    if old_local_path:
        cmd.extend(["--old-local", old_local_path])

    run(cmd)

    timing["compare"] = _elapsed(t_compare_start)

    manifest_path = workdir / "manifest.json"
    if not manifest_path.exists():
        raise RuntimeError("Comparator did not create manifest.json")

    manifest = json.loads(manifest_path.read_text())
    summary = manifest.get("summary", {})
    total_changes = int(summary.get("total_append", 0)) + int(
        summary.get("total_update", 0)
    )

    print("Changes:", total_changes)

    if total_changes == 0:
        print("\n=== 0 CHANGES: FAST EXIT ===")
        t_cleanup = _timer()
        _preserve_fetched_snapshot(local_snapshot_path, new_ts, cache_work_dir)
        _cleanup_local_snapshot(local_snapshot_path)
        _write_pipeline_success(new_ts, cache_work_dir)
        _update_data_updated(status="complete")
        timing["cleanup"] = _elapsed(t_cleanup)
        _print_timing(timing, pipeline_start)
        return

    print("=== STEP 4: UPLOAD DELTA ===")
    t_upload_delta = _timer()
    upload_delta(workdir, run_id)
    timing["upload_delta"] = _elapsed(t_upload_delta)

    print("=== STEP 5: CREATE INGESTION JOBS ===")
    t_ingest = _timer()
    controller = call_edge("mplads-controller", {"run_id": run_id})
    expected_jobs = int(controller.get("total_jobs", 0))
    if expected_jobs <= 0:
        raise RuntimeError("Comparator found changes, but controller created 0 jobs")

    print("Expected jobs:", expected_jobs)

    print("=== STEP 6: RUN WORKER ===")
    ingest_concurrency = int(os.environ.get("INGEST_CONCURRENCY", "5"))
    print("Ingestion scheduler:")
    print(f"  jobs={expected_jobs}")
    print(f"  concurrency={ingest_concurrency}")

    worker_max_minutes = 80
    worker_start = time.time()
    per_job_timeout_seconds = 5 * 60  # 5 minutes per job
    all_completed = threading.Event()
    stop_event = threading.Event()
    worker_errors = []
    errors_lock = threading.Lock()

    def _ingest_worker(worker_idx):
        while not stop_event.is_set() and not all_completed.is_set():
            elapsed = time.time() - worker_start
            if elapsed > worker_max_minutes * 60:
                stop_event.set()
                break

            job_start = time.time()
            try:
                result = call_edge("mplads-worker", {"run_id": run_id})
            except Exception as exc:
                job_elapsed = time.time() - job_start
                print(f"[Worker {worker_idx}] call failed after {job_elapsed:.1f}s: {exc}")
                if job_elapsed >= per_job_timeout_seconds:
                    print(f"[Worker {worker_idx}] per-job timeout ({per_job_timeout_seconds}s) exceeded")
                time.sleep(5)
                continue

            if result.get("success"):
                msg = result.get("message", "")
                completed_count = result.get("completed", 0)
                remaining = result.get("progress", {}).get("remaining", None)

                if msg == "All ingestion jobs completed." and completed_count >= expected_jobs:
                    print(f"[Worker {worker_idx}] All {expected_jobs} ingestion jobs completed.")
                    all_completed.set()
                    break
                elif remaining == 0:
                    print(f"[Worker {worker_idx}] Ingestion progress: remaining=0.")
                    all_completed.set()
                    break
                elif msg == "Job was already claimed. Try again.":
                    time.sleep(1)
                    continue
                elif msg == "All ingestion jobs completed.":
                    # In-flight jobs still being processed by other worker threads
                    time.sleep(3)
                    continue
                else:
                    print(f"[Worker {worker_idx}] Worker:", result)
            else:
                err = result.get("error", "Unknown worker error")
                print(f"[Worker {worker_idx}] Worker returned error: {err}")
                with errors_lock:
                    worker_errors.append(err)
                time.sleep(5)

    threads = []
    num_workers = min(ingest_concurrency, expected_jobs)
    for i in range(num_workers):
        t = threading.Thread(target=_ingest_worker, args=(i + 1,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    if not all_completed.is_set():
        elapsed = time.time() - worker_start
        if elapsed > worker_max_minutes * 60:
            raise RuntimeError(
                f"Worker loop timed out after {worker_max_minutes} minutes"
            )
        if not verify_run(run_id, expected_jobs):
            raise RuntimeError(
                f"Worker finished without completing all jobs. Errors: {worker_errors}"
            )

    print("=== STEP 7: VERIFY ===")
    if not verify_run(run_id, expected_jobs):
        raise RuntimeError("Ingestion verification failed")
    timing["ingestion"] = _elapsed(t_ingest)

    print("=== STEP 8: ANALYSIS ===")
    t_analysis = _timer()
    from datetime import date as _date
    from automation.pipeline_controller import run_pipeline as run_analysis

    ref_date = _date.today()
    snapshot_path = local_snapshot_path
    skip_gemini = args.skip_gemini

    if not snapshot_path:
        raise RuntimeError("No snapshot path available for analysis (fetch was skipped or failed)")

    analysis_result = run_analysis(
        snapshot_dir=snapshot_path,
        reference_date=ref_date,
        delta_dir=str(workdir),
        run_id=run_id,
        skip_gemini=skip_gemini,
        skip_ingest=True,
        mode=args.mode,
    )
    timing["analysis"] = _elapsed(t_analysis)

    if analysis_result.get("status") not in ("SUCCESS", "DRY_RUN", "SUCCESS_WITH_WARNINGS"):
        raise RuntimeError(
            f"Analysis pipeline failed: {analysis_result.get('error', 'unknown')}"
        )

    print("=== STEP 9: INTELLIGENCE BACKFILL ===")
    t_intelligence = _timer()
    if args.skip_intelligence:
        print("Skipping intelligence backfill (--skip-intelligence)")
        timing["intelligence"] = 0.0
    else:
        if not INTELLIGENCE_BACKFILL.exists():
            raise RuntimeError(f"Intelligence backfill not found: {INTELLIGENCE_BACKFILL}")

        # Build AffectedScope for intelligence stage
        scope_file = None
        if total_changes > 0 and workdir.exists():
            try:
                from analysis.affected_scope import build_affected_scope_from_delta
                delta_records = []
                for dataset_dir in workdir.iterdir():
                    if not dataset_dir.is_dir():
                        continue
                    for part_file in dataset_dir.glob("*_part_*.ndjson"):
                        with open(part_file) as f:
                            for line in f:
                                line = line.strip()
                                if line:
                                    try:
                                        rec = json.loads(line)
                                        rec["_table"] = dataset_dir.name
                                        delta_records.append(rec)
                                    except json.JSONDecodeError:
                                        pass
                changed_tables = {r.get("_table") for r in delta_records if r.get("_table")}
                scope = build_affected_scope_from_delta(delta_records, changed_tables)
                scope_file = str(workdir / "affected_scope.json")
                with open(scope_file, "w") as f:
                    json.dump(scope.summary(), f)
                print(f"  AffectedScope: {scope.affected_work_count} works, "
                      f"{len(changed_tables)} tables changed")
            except Exception as exc:
                print(f"  WARNING: Could not build AffectedScope: {exc}")

        intelligence_cmd = [sys.executable, str(INTELLIGENCE_BACKFILL), "--apply"]
        if scope_file:
            intelligence_cmd.extend(["--scope", scope_file])
        try:
            run(intelligence_cmd)
            timing["intelligence"] = _elapsed(t_intelligence)
            print(f"Intelligence backfill completed in {timing['intelligence']:.1f}s")
        except Exception as exc:
            # Fail-safe: ML stage must not undo a successful core pipeline run.
            # Surface the error clearly so operators can retry/backfill manually,
            # but preserve the freshly ingested source data and analytics.
            elapsed = _elapsed(t_intelligence)
            print(f"WARNING: Intelligence backfill failed after {elapsed:.1f}s: {exc}",
                  file=sys.stderr)
            print("Core pipeline succeeded. Intelligence fields may be stale until "
                  "the backfill is re-run manually.", file=sys.stderr)
            timing["intelligence"] = elapsed

    print("=== STEP 10: CLEANUP ===")
    t_cleanup = _timer()
    delete_delta_run(run_id)
    delete_old_raw_snapshots([new_ts])

    _preserve_fetched_snapshot(local_snapshot_path, new_ts, cache_work_dir)
    _cleanup_local_snapshot(local_snapshot_path)
    _write_pipeline_success(new_ts, cache_work_dir)
    _update_data_updated(status="complete")
    timing["cleanup"] = _elapsed(t_cleanup)

    print("=== PIPELINE COMPLETE ===")
    _print_timing(timing, pipeline_start)


if __name__ == "__main__":
    try:
        main()
        print("Pipeline completed successfully.")
    except Exception as exc:
        print(f"PIPELINE FAILED: {exc}", file=sys.stderr)
        print("OLD SNAPSHOT HAS NOT BEEN DELETED", file=sys.stderr)
        # Local cleanup only — remote cleanup handled by GitHub Actions workflow
        _cw = os.environ.get("MPLADS_CACHE_WORK_DIR")
        if _cw:
            ts_file = Path(_cw) / ".current_snapshot_ts"
            if ts_file.exists():
                try:
                    snapshot_ts = ts_file.read_text(encoding="utf-8").strip()
                    print(f"Failed snapshot timestamp: {snapshot_ts}")
                except Exception:
                    pass
        sys.exit(1)
