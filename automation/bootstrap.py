"""One-time production bootstrap for NEW DB1 and NEW DB2.

End-to-end flow:
    MPLADS government server
        → fresh complete snapshot
        → validate ALL datasets
        → write snapshot to GitHub rolling cache
        → upload full snapshot to Supabase Storage
        → full ingestion into NEW DB1 via edge functions
        → verify DB1 ingestion
        → full deterministic analysis
        → full evidence generation
        → full Gemini/AI analysis
        → persist everything into NEW DB2
        → final verification
        → SUCCESS

This is a ONE-TIME destructive operation.
Do NOT use for daily incremental updates.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

ROOT = Path(__file__).resolve().parent.parent
FETCHER = ROOT / "fetcher" / "fetcher.py"

sys.path.insert(0, str(ROOT))

from automation.snapshot_cache import (
    ensure_cache_dirs,
    write_metadata,
    write_new_timestamp,
)

# ============================================================
# Configuration
# ============================================================

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
SECRET_KEY = os.environ.get("SUPABASE_SECRET_KEY")
DB2_URL = os.environ.get("DB2_URL")
DB2_KEY = os.environ.get("DB2_SERVICE_ROLE_KEY")

BUCKET = "mplads-raw"

REQUIRED_DATASETS = [
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


# ============================================================
# Helpers
# ============================================================

def _timer():
    return time.time()


def _elapsed(start):
    return time.time() - start


def run(cmd, **kwargs):
    """Run a subprocess, raise on failure."""
    result = subprocess.run(cmd, capture_output=True, text=True, **kwargs)
    if result.returncode != 0:
        print(f"COMMAND FAILED: {' '.join(cmd)}")
        print(f"STDOUT:\n{result.stdout}")
        print(f"STDERR:\n{result.stderr}")
        raise RuntimeError(f"Command failed with exit code {result.returncode}")
    return result.stdout


def run_fetcher(extra_args=None):
    """Run the fetcher and capture LOCAL_SNAPSHOT_PATH from output."""
    cmd = [sys.executable, str(FETCHER)]
    if extra_args:
        cmd.extend(extra_args)

    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )
    lines = []
    local_snapshot_path = None
    for line in proc.stdout:
        line = line.rstrip()
        print(line)
        lines.append(line)
        if line.startswith("LOCAL_SNAPSHOT_PATH="):
            local_snapshot_path = line.split("=", 1)[1].strip()
    proc.wait()
    if proc.returncode != 0:
        raise RuntimeError("Fetcher failed")
    return local_snapshot_path


# ============================================================
# STAGE 1: FRESH SNAPSHOT
# ============================================================

def stage_fetch():
    """Fetch a fresh snapshot from the MPLADS government server."""
    print("\n" + "=" * 70)
    print("STAGE 1: FETCH FRESH SNAPSHOT")
    print("=" * 70)

    local_snapshot_path = run_fetcher()
    if not local_snapshot_path:
        raise RuntimeError("Fetcher did not return LOCAL_SNAPSHOT_PATH")

    snapshot_dir = Path(local_snapshot_path)
    if not snapshot_dir.exists():
        raise RuntimeError(f"Snapshot directory does not exist: {snapshot_dir}")

    print(f"Fresh snapshot: {snapshot_dir}")
    return str(snapshot_dir)


# ============================================================
# STAGE 2: VALIDATE ALL DATASETS
# ============================================================

def stage_validate(snapshot_path):
    """Validate that all 12 expected datasets exist and are non-empty."""
    print("\n" + "=" * 70)
    print("STAGE 2: VALIDATE ALL DATASETS")
    print("=" * 70)

    snapshot_dir = Path(snapshot_path)
    missing = []
    empty = []

    for dataset in REQUIRED_DATASETS:
        dataset_dir = snapshot_dir / dataset
        if not dataset_dir.exists():
            missing.append(dataset)
            continue

        part_files = list(dataset_dir.glob("part_*.ndjson"))
        if not part_files:
            empty.append(dataset)
            continue

        total_records = 0
        for pf in part_files:
            with open(pf) as f:
                total_records += sum(1 for line in f if line.strip())

        if total_records == 0:
            empty.append(dataset)
        else:
            print(f"  ✓ {dataset}: {total_records} records")

    if missing:
        raise RuntimeError(f"Missing datasets: {missing}")
    if empty:
        raise RuntimeError(f"Empty datasets (no records): {empty}")

    # Validate _COMPLETE.json
    complete_path = snapshot_dir / "_COMPLETE.json"
    if not complete_path.exists():
        raise RuntimeError("Missing _COMPLETE.json")

    marker = json.loads(complete_path.read_text())
    if marker.get("status") != "complete":
        raise RuntimeError(f"_COMPLETE.json status is not 'complete': {marker.get('status')}")

    print(f"  ✓ _COMPLETE.json present and valid")
    print(f"  ✓ All {len(REQUIRED_DATASETS)} datasets validated")


# ============================================================
# STAGE 3: WRITE TO GITHUB ROLLING CACHE
# ============================================================

def stage_cache(snapshot_path, cache_work_dir):
    """Write the fresh snapshot to the GitHub rolling cache."""
    print("\n" + "=" * 70)
    print("STAGE 3: WRITE TO GITHUB ROLLING CACHE")
    print("=" * 70)

    if not cache_work_dir:
        print("No cache work dir specified — skipping cache write")
        return

    work_dir = Path(cache_work_dir)
    ensure_cache_dirs(work_dir)

    snapshot_dir = Path(snapshot_path)
    timestamp = snapshot_dir.name

    target = work_dir / "current" / timestamp
    if target.exists():
        print(f"Cache target already exists: {target}")
    else:
        shutil.copytree(str(snapshot_dir), str(target))
        print(f"Copied snapshot to cache: {target}")

    write_new_timestamp(timestamp, work_dir)
    write_metadata("none", timestamp, work_dir)
    print(f"Cache metadata written: current={timestamp}")


# ============================================================
# STAGE 4: UPLOAD FULL SNAPSHOT TO SUPABASE STORAGE
# ============================================================

def stage_upload(snapshot_path, run_id):
    """Upload the full snapshot to Supabase Storage for edge function ingestion."""
    print("\n" + "=" * 70)
    print("STAGE 4: UPLOAD FULL SNAPSHOT TO SUPABASE STORAGE")
    print("=" * 70)

    storage = create_client(SUPABASE_URL, SECRET_KEY).storage.from_(BUCKET)
    snapshot_dir = Path(snapshot_path)
    uploaded = 0

    for p in snapshot_dir.rglob("*"):
        if p.is_file():
            rel = p.relative_to(snapshot_dir).as_posix()
            storage.upload(
                f"{run_id}/{rel}",
                p.read_bytes(),
                {"upsert": "true", "content-type": "application/x-ndjson"},
            )
            uploaded += 1
            if uploaded % 20 == 0:
                print(f"  Uploaded {uploaded} files...")

    print(f"  ✓ Uploaded {uploaded} files to Supabase Storage under run_id={run_id}")


# ============================================================
# STAGE 5: FULL DB1 INGESTION VIA EDGE FUNCTIONS
# ============================================================

# Per-job timeout: 5 minutes.  Transport-level retry only.
# If a single edge function call takes longer, we recover the stuck
# job and move on.  This does NOT terminate the overall bootstrap.
PER_JOB_TIMEOUT_SECONDS = 5 * 60

# Maximum retry passes after the first pass.  Each retry only
# re-processes jobs that failed or timed out (never completed).
MAX_RETRY_PASSES = 3

# Poll interval between worker calls.
WORKER_POLL_INTERVAL = 15

# HTTP request timeout for individual edge function calls.
HTTP_TIMEOUT = 120


def _recover_stuck_jobs(sb, run_id, per_job_timeout):
    """Reset jobs stuck in 'processing' past the per-job timeout.

    The edge function has no per-job timeout.  If the client's HTTP
    request times out while the edge function is still running, the
    job stays stuck in 'processing' forever.  This resets those jobs
    back to 'pending' so the next worker call can claim them.

    Safety: only resets jobs whose started_at is older than
    per_job_timeout seconds AND whose attempts are below the retry
    limit.  Jobs that genuinely completed on the server side will be
    marked 'completed' by the edge function's own completion path.
    """
    result = (
        sb.table("ingestion_jobs")
        .select("job_id, started_at, attempts")
        .eq("run_id", run_id)
        .eq("status", "processing")
        .execute()
    )
    stuck = result.data or []
    recovered = 0
    now = time.time()

    for job in stuck:
        started = job.get("started_at")
        attempts = int(job.get("attempts", 0) or 0)
        if not started or attempts >= MAX_RETRY_PASSES:
            continue
        try:
            started_ts = datetime.fromisoformat(
                started.replace("Z", "+00:00")
            ).timestamp()
        except (ValueError, TypeError):
            continue
        if now - started_ts > per_job_timeout:
            sb.table("ingestion_jobs").update({
                "status": "pending",
                "error_message": f"Timed out after {per_job_timeout}s (attempt {attempts})",
            }).eq("job_id", job["job_id"]).execute()
            recovered += 1

    return recovered


def _query_job_counts(sb, run_id):
    """Return {status: count} for the given run_id."""
    counts = {}
    for status in ["completed", "pending", "processing", "failed"]:
        r = (
            sb.table("ingestion_jobs")
            .select("job_id", count="exact")
            .eq("run_id", run_id)
            .eq("status", status)
            .execute()
        )
        counts[status] = r.count or 0
    return counts


def stage_ingest(run_id):
    """Run full DB1 ingestion via edge functions.

    NO global time ceiling.  Local bootstrap waits until all jobs
    finish, regardless of elapsed time.

    Retry model:
        - Per-job: 5 minutes.  If a worker call takes longer than
          this, we recover the stuck job and move on.
        - After first pass: retry failed/timed-out jobs up to
          MAX_RETRY_PASSES times with the same per-job limit.
        - Final gate: ALL jobs must be completed.  No silent skips.
    """
    print("\n" + "=" * 70)
    print("STAGE 5: FULL DB1 INGESTION")
    print("=" * 70)

    url = f"{SUPABASE_URL.rstrip('/')}/functions/v1"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {SERVICE_KEY}",
        "apikey": SERVICE_KEY,
    }

    import ssl
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    sb = create_client(SUPABASE_URL, SERVICE_KEY)

    # Create ingestion jobs
    print("  Creating ingestion jobs...")
    body = json.dumps({"run_id": run_id}).encode()
    req = urllib.request.Request(
        f"{url}/mplads-controller",
        data=body, headers=headers, method="POST",
    )
    with urllib.request.urlopen(req, timeout=120, context=ctx) as resp:
        controller = json.loads(resp.read().decode())

    expected_jobs = int(controller.get("total_jobs", 0))
    if expected_jobs <= 0:
        raise RuntimeError(f"Controller created 0 jobs (response: {controller})")
    print(f"  Expected jobs: {expected_jobs}")

    pipeline_start = time.time()

    # ── FIRST PASS ────────────────────────────────────────────
    print("  --- First pass ---")
    _run_worker_loop(
        run_id, url, headers, ctx, sb,
        expected_jobs, pipeline_start,
    )

    # ── RETRY PASSES ──────────────────────────────────────────
    for retry_num in range(1, MAX_RETRY_PASSES + 1):
        counts = _query_job_counts(sb, run_id)
        non_completed = counts["pending"] + counts["processing"] + counts["failed"]
        if non_completed == 0:
            break

        elapsed_min = (time.time() - pipeline_start) / 60

        print(f"  --- Retry pass {retry_num}/{MAX_RETRY_PASSES} "
              f"({non_completed} jobs to retry, {elapsed_min:.1f}m elapsed) ---")

        # Recover any stuck processing jobs before retrying
        recovered = _recover_stuck_jobs(sb, run_id, PER_JOB_TIMEOUT_SECONDS)
        if recovered:
            print(f"  Recovered {recovered} stuck jobs")

        _run_worker_loop(
            run_id, url, headers, ctx, sb,
            expected_jobs, pipeline_start,
        )

    # ── FINAL GATE ────────────────────────────────────────────
    print("  --- Final verification ---")
    counts = _query_job_counts(sb, run_id)
    total = sum(counts.values())
    elapsed_min = (time.time() - pipeline_start) / 60

    print(f"  Counts after {elapsed_min:.1f}m: {counts}")

    if total != expected_jobs:
        raise RuntimeError(
            f"Final gate FAILED: total jobs {total} != expected {expected_jobs}"
        )
    if counts["completed"] != expected_jobs:
        raise RuntimeError(
            f"Final gate FAILED: {counts['completed']}/{expected_jobs} completed "
            f"(pending={counts['pending']}, processing={counts['processing']}, "
            f"failed={counts['failed']})"
        )
    if counts["pending"] > 0 or counts["processing"] > 0 or counts["failed"] > 0:
        raise RuntimeError(
            f"Final gate FAILED: incomplete ingestion — {counts}"
        )

    print(f"  ✓ DB1 ingestion complete: {expected_jobs}/{expected_jobs} jobs "
          f"in {elapsed_min:.1f}m")


def _run_worker_loop(run_id, url, headers, ctx, sb, expected_jobs, pipeline_start):
    """Worker loop with per-job timeout and stuck recovery.

    NO global time ceiling.  Each iteration calls the mplads-worker
    edge function which claims and processes ONE pending job.  If the
    call takes longer than PER_JOB_TIMEOUT_SECONDS, we recover the
    stuck job on the next iteration.
    """
    worker_start = time.time()

    while True:
        # Recover stuck processing jobs
        recovered = _recover_stuck_jobs(sb, run_id, PER_JOB_TIMEOUT_SECONDS)
        if recovered:
            print(f"  Recovered {recovered} stuck jobs")

        # Check remaining work
        counts = _query_job_counts(sb, run_id)
        remaining = counts["pending"] + counts["processing"] + counts["failed"]
        completed = counts["completed"]

        if remaining == 0:
            print(f"  All {completed}/{expected_jobs} jobs completed")
            break

        global_elapsed = time.time() - pipeline_start
        elapsed_min = global_elapsed / 60
        print(
            f"  Progress: {completed}/{expected_jobs} completed, "
            f"{remaining} remaining, "
            f"{elapsed_min:.1f}m elapsed"
        )

        # Call worker with per-job timeout
        job_start = time.time()
        try:
            body = json.dumps({"run_id": run_id}).encode()
            req = urllib.request.Request(
                f"{url}/mplads-worker",
                data=body, headers=headers, method="POST",
            )
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT, context=ctx) as resp:
                result = json.loads(resp.read().decode())

            job_elapsed = time.time() - job_start
            print(f"  Worker call: {job_elapsed:.1f}s — {result}")

            # If worker says all done, break
            if result.get("success") and (
                result.get("message") == "All ingestion jobs completed."
                or result.get("progress", {}).get("remaining", 1) == 0
            ):
                break

        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError) as exc:
            job_elapsed = time.time() - job_start
            print(f"  Worker call failed after {job_elapsed:.1f}s: {exc}")

            # If the HTTP call itself timed out, the edge function may
            # still be running.  Recover stuck jobs on next iteration.
            if job_elapsed >= PER_JOB_TIMEOUT_SECONDS:
                print(f"  Per-job timeout ({PER_JOB_TIMEOUT_SECONDS}s) exceeded — "
                      f"will recover stuck jobs next iteration")

        time.sleep(WORKER_POLL_INTERVAL)


# ============================================================
# STAGE 6: FULL ANALYSIS + EVIDENCE + GEMINI + DB2 PERSIST
# ============================================================

def stage_full_analysis(snapshot_path, run_id, skip_gemini=False):
    """Run the complete analysis pipeline in full mode."""
    print("\n" + "=" * 70)
    print("STAGE 6: FULL ANALYSIS PIPELINE")
    print("=" * 70)

    from automation.pipeline_controller import run_pipeline

    result = run_pipeline(
        snapshot_dir=snapshot_path,
        reference_date=date.today(),
        run_id=run_id,
        skip_gemini=skip_gemini,
        skip_ingest=True,
        mode="full",
    )

    if result.get("status") not in ("SUCCESS", "DRY_RUN"):
        raise RuntimeError(
            f"Analysis pipeline failed: {result.get('error', 'unknown')}"
        )

    print(f"  ✓ Analysis pipeline complete: {result['status']}")
    return result


# ============================================================
# STAGE 7: VERIFY BOTH DATABASES
# ============================================================

def stage_verify_db1():
    """Verify NEW DB1 has expected data."""
    print("\n" + "=" * 70)
    print("STAGE 7a: VERIFY NEW DB1")
    print("=" * 70)

    client = create_client(SUPABASE_URL, SERVICE_KEY)

    checks = {
        "works": "works",
        "work_analysis": "work_analysis",
    }

    for label, table in checks.items():
        result = client.table(table).select("*", count="exact").limit(0).execute()
        count = result.count or 0
        status = "✓" if count > 0 else "✗"
        print(f"  {status} {table}: {count} rows")
        if count == 0:
            raise RuntimeError(f"DB1 verification failed: {table} is empty")


def stage_verify_db2():
    """Verify NEW DB2 has expected data."""
    print("\n" + "=" * 70)
    print("STAGE 7b: VERIFY NEW DB2")
    print("=" * 70)

    if not DB2_URL or not DB2_KEY:
        print("  ⚠ DB2 credentials not set — skipping DB2 verification")
        return

    client = create_client(DB2_URL, DB2_KEY)

    checks = {
        "overall_metrics": "overall_metrics",
        "member_metrics": "member_metrics",
        "state_metrics": "state_metrics",
        "national_statistics": "national_statistics",
        "trends": "trends",
        "entity_evidence": "entity_evidence",
        "evidence_work_refs": "evidence_work_refs",
        "ai_analysis": "ai_analysis",
    }

    for label, table in checks.items():
        result = client.table(table).select("*", count="exact").limit(0).execute()
        count = result.count or 0
        status = "✓" if count > 0 else "✗"
        print(f"  {status} {table}: {count} rows")
        if count == 0:
            raise RuntimeError(f"DB2 verification failed: {table} is empty")


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="MPLADS Production Bootstrap (one-time NEW DB initialization)"
    )
    parser.add_argument(
        "--confirm",
        required=True,
        help="Must be exactly 'BOOTSTRAP_NEW_DATABASES' to proceed",
    )
    parser.add_argument(
        "--cache-work-dir",
        default=os.environ.get("MPLADS_CACHE_WORK_DIR"),
        help="GitHub cache work directory",
    )
    parser.add_argument(
        "--skip-gemini",
        action="store_true",
        help="Skip Gemini/AI analysis (NOT recommended for bootstrap)",
    )
    args = parser.parse_args()

    if args.confirm != "BOOTSTRAP_NEW_DATABASES":
        print("ERROR: --confirm must be exactly 'BOOTSTRAP_NEW_DATABASES'")
        print("This is a destructive operation. Confirmation required.")
        sys.exit(1)

    pipeline_start = time.time()
    timing = {}

    print("=" * 70)
    print("MPLADS PRODUCTION BOOTSTRAP")
    print("One-time NEW DB1 + NEW DB2 initialization")
    print("=" * 70)
    print(f"Started: {datetime.now(timezone.utc).isoformat()}")
    print(f"Cache work dir: {args.cache_work_dir}")
    print(f"Skip Gemini: {args.skip_gemini}")
    print("=" * 70)

    run_id = f"bootstrap_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    try:
        # Stage 1: Fetch fresh snapshot
        t = _timer()
        snapshot_path = stage_fetch()
        timing["fetch"] = _elapsed(t)

        # Stage 2: Validate all datasets
        t = _timer()
        stage_validate(snapshot_path)
        timing["validate"] = _elapsed(t)

        # Stage 3: Write to GitHub cache
        t = _timer()
        stage_cache(snapshot_path, args.cache_work_dir)
        timing["cache"] = _elapsed(t)

        # Stage 4: Upload to Supabase Storage
        t = _timer()
        stage_upload(snapshot_path, run_id)
        timing["upload"] = _elapsed(t)

        # Stage 5: Full DB1 ingestion
        t = _timer()
        stage_ingest(run_id)
        timing["ingest"] = _elapsed(t)

        # Stage 6: Full analysis + evidence + Gemini + DB2 persist
        t = _timer()
        analysis_result = stage_full_analysis(
            snapshot_path, run_id, skip_gemini=args.skip_gemini
        )
        timing["analysis"] = _elapsed(t)

        # Stage 7: Verify both databases
        t = _timer()
        stage_verify_db1()
        stage_verify_db2()
        timing["verify"] = _elapsed(t)

    except Exception as exc:
        print(f"\n{'=' * 70}")
        print(f"BOOTSTRAP FAILED: {exc}")
        print(f"{'=' * 70}")
        _print_timing(timing, pipeline_start)
        sys.exit(1)

    print(f"\n{'=' * 70}")
    print("BOOTSTRAP COMPLETE — NEW DB1 + NEW DB2 INITIALIZED")
    print(f"{'=' * 70}")
    _print_timing(timing, pipeline_start)


def _print_timing(timing, pipeline_start):
    total = time.time() - pipeline_start
    print(f"\n--- TIMING ---")
    for key, val in timing.items():
        print(f"  {key}: {val:.1f}s")
    print(f"  TOTAL: {total:.1f}s")


if __name__ == "__main__":
    main()
