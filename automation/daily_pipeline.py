import os
import sys
import json
import shutil
import subprocess
import time
import argparse
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

ROOT = Path(__file__).resolve().parent.parent
FETCHER = ROOT / "fetcher" / "fetcher.py"
COMPARATOR = ROOT / "comparator" / "comparator_v2.py"

sys.path.insert(0, str(ROOT / "automation"))
from snapshot_cache import (
    get_previous_local_path,
    get_previous_timestamp,
    get_current_timestamp,
    validate_cache,
    write_new_timestamp,
    write_metadata,
    METADATA_FILE,
)

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
    with urllib.request.urlopen(req, timeout=120) as response:
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

    print(f"Pipeline success markers written to {work_dir}")


def _preserve_fetched_snapshot(local_snapshot_path, new_ts, cache_work_dir):
    """Copy the fetched snapshot into the cache current/ directory.

    This must be called BEFORE _cleanup_local_snapshot removes the temp dir.
    """
    if not cache_work_dir or not local_snapshot_path:
        return

    work_dir = Path(cache_work_dir)
    target = work_dir / "current" / new_ts

    if target.exists():
        print(f"Cache current/{new_ts} already exists, skipping copy")
        return

    src = Path(local_snapshot_path)
    if not src.exists():
        print(f"WARNING: Fetched snapshot {src} not found, cannot preserve for cache")
        return

    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(str(src), str(target))
    print(f"Fetched snapshot preserved for cache: {target}")


def main():
    parser = argparse.ArgumentParser(description="MPLADS Daily Pipeline")
    parser.add_argument("--skip-gemini", action="store_true",
                        help="Skip Gemini AI analysis step")
    parser.add_argument("--skip-ingest", action="store_true",
                        help="Skip DB ingestion steps (stages 8a-8c)")
    parser.add_argument("--skip-fetch", action="store_true",
                        help="Skip government fetch (use existing current snapshot)")
    args = parser.parse_args()

    if not FETCHER.exists():
        raise RuntimeError(f"Fetcher not found: {FETCHER}")
    if not COMPARATOR.exists():
        raise RuntimeError(
            f"Comparator not found: {COMPARATOR}. "
            "Change COMPARATOR near the top of automation/daily_pipeline.py "
            "to the real comparator path in your repo."
        )

    sync_interval = os.environ.get("SYNC_INTERVAL_HOURS", "24")
    print(f"Sync interval: {sync_interval} hours")
    print(f"MP datasets: {len([d for d in DATASETS if not d.startswith('mla_')])}")
    print(f"MLA datasets: {len([d for d in DATASETS if d.startswith('mla_')])}")

    cache_work_dir = os.environ.get("MPLADS_CACHE_WORK_DIR")
    print(f"Cache work dir: {cache_work_dir or 'not set (Supabase-only mode)'}")

    print("=== STEP 1: FETCH ===")
    if args.skip_fetch:
        print("Skipping fetch (--skip-fetch)")
        local_snapshot_path = None
    else:
        fetcher_output = run_fetcher([sys.executable, str(FETCHER)])
        local_snapshot_path = None
        for line in fetcher_output:
            if line.startswith("LOCAL_SNAPSHOT_PATH="):
                local_snapshot_path = line.split("=", 1)[1].strip()
                break

    if local_snapshot_path:
        validate_local_snapshot(local_snapshot_path)

    new_ts = os.path.basename(local_snapshot_path) if local_snapshot_path else None

    print("=== STEP 2: GET PREVIOUS SNAPSHOT ===")
    old_timestamp = None
    old_local_path = None

    use_supabase_fallback = os.environ.get("USE_SUPABASE_FALLBACK", "false").lower() == "true"

    if use_supabase_fallback:
        print("Supabase fallback requested by workflow — skipping cache")
    elif cache_work_dir:
        cache_dir = Path(cache_work_dir)
        ok, err = validate_cache(cache_dir)
        if ok:
            prev_path = get_previous_local_path(cache_dir)
            if prev_path:
                old_timestamp = get_previous_timestamp(cache_dir)
                old_local_path = str(prev_path)
                print(f"Previous snapshot from cache: {old_timestamp}")
            else:
                print("WARNING: Cache valid but no previous snapshot found")
        else:
            print(f"WARNING: Cache validation failed: {err}")

    if not old_timestamp:
        print("Previous snapshot from Supabase Storage (fallback)")
        snapshots = list_complete_snapshots()
        if snapshots:
            old_timestamp = snapshots[-1]
            print(f"Previous snapshot from Supabase: {old_timestamp}")

    if not old_timestamp:
        print("No previous snapshot found. Bootstrap mode — nothing to compare.")
        if local_snapshot_path:
            _preserve_fetched_snapshot(local_snapshot_path, new_ts, cache_work_dir)
            _cleanup_local_snapshot(local_snapshot_path)
        return

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

    manifest_path = workdir / "manifest.json"
    if not manifest_path.exists():
        raise RuntimeError("Comparator did not create manifest.json")

    manifest = json.loads(manifest_path.read_text())
    summary = manifest.get("summary", {})
    total_changes = int(summary.get("total_append", 0)) + int(
        summary.get("total_update", 0)
    )

    print("Changes:", total_changes)

    print("=== STEP 4: UPLOAD DELTA ===")
    upload_delta(workdir, run_id)

    if total_changes == 0:
        print("No changes. No ingestion needed.")
        delete_delta_run(run_id)
        _preserve_fetched_snapshot(local_snapshot_path, new_ts, cache_work_dir)
        _cleanup_local_snapshot(local_snapshot_path)
        _write_pipeline_success(new_ts, cache_work_dir)
        return

    print("=== STEP 5: CREATE INGESTION JOBS ===")
    controller = call_edge("mplads-controller", {"run_id": run_id})
    expected_jobs = int(controller.get("total_jobs", 0))
    if expected_jobs <= 0:
        raise RuntimeError("Comparator found changes, but controller created 0 jobs")

    print("Expected jobs:", expected_jobs)

    print("=== STEP 6: RUN WORKER ===")
    worker_max_minutes = 30
    worker_start = time.time()
    worker_interval = 60

    while True:
        elapsed = time.time() - worker_start
        if elapsed > worker_max_minutes * 60:
            raise RuntimeError(
                f"Worker loop timed out after {worker_max_minutes} minutes"
            )

        result = call_edge("mplads-worker", {"run_id": run_id})
        print("Worker:", result)
        if result.get("success") and (
            result.get("message") == "All ingestion jobs completed."
            or result.get("progress", {}).get("remaining", 1) == 0
        ):
            break

        remaining = worker_max_minutes - (elapsed / 60)
        print(f"Worker remaining budget: {remaining:.1f} minutes")
        time.sleep(worker_interval)

    print("=== STEP 7: VERIFY ===")
    if not verify_run(run_id, expected_jobs):
        raise RuntimeError("Ingestion verification failed")

    print("=== STEP 8: ANALYSIS ===")
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
    )

    if analysis_result.get("status") not in ("SUCCESS", "DRY_RUN"):
        raise RuntimeError(
            f"Analysis pipeline failed: {analysis_result.get('error', 'unknown')}"
        )

    print("=== STEP 9: CLEANUP ===")
    delete_delta_run(run_id)

    _preserve_fetched_snapshot(local_snapshot_path, new_ts, cache_work_dir)
    _cleanup_local_snapshot(local_snapshot_path)
    _write_pipeline_success(new_ts, cache_work_dir)

    print("=== PIPELINE COMPLETE ===")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"PIPELINE FAILED: {exc}", file=sys.stderr)
        print("OLD SNAPSHOT HAS NOT BEEN DELETED", file=sys.stderr)
        sys.exit(1)
