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

    sync_interval = os.environ.get("SYNC_INTERVAL_HOURS", "24")
    print(f"Sync interval: {sync_interval} hours")
    print(f"MP datasets: {len([d for d in DATASETS if not d.startswith('mla_')])}")
    print(f"MLA datasets: {len([d for d in DATASETS if d.startswith('mla_')])}")
    print(f"Mode: {args.mode}")
    print(f"Bootstrap: {args.bootstrap}")
    print(f"Local only: {args.local_only}")

    cache_work_dir = os.environ.get("MPLADS_CACHE_WORK_DIR")
    print(f"Cache work dir: {cache_work_dir or 'not set (Supabase-only mode)'}")

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

        if analysis_result.get("status") not in ("SUCCESS", "DRY_RUN"):
            raise RuntimeError(
                f"Bootstrap analysis failed: {analysis_result.get('error', 'unknown')}"
            )

        print("\n=== BOOTSTRAP COMPLETE ===")
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
            print(f"WARNING: Remote snapshot validation failed: {exc}")
            print("Local snapshot is valid but remote upload may be incomplete.")

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
    worker_max_minutes = 80
    worker_start = time.time()
    worker_interval = 60
    per_job_timeout_seconds = 5 * 60  # 5 minutes per job
    max_retries = 3

    while True:
        elapsed = time.time() - worker_start
        if elapsed > worker_max_minutes * 60:
            raise RuntimeError(
                f"Worker loop timed out after {worker_max_minutes} minutes"
            )

        job_start = time.time()
        try:
            result = call_edge("mplads-worker", {"run_id": run_id})
        except Exception as exc:
            job_elapsed = time.time() - job_start
            print(f"Worker call failed after {job_elapsed:.1f}s: {exc}")
            if job_elapsed >= per_job_timeout_seconds:
                print(f"Per-job timeout ({per_job_timeout_seconds}s) exceeded")
            time.sleep(worker_interval)
            continue

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

    if analysis_result.get("status") not in ("SUCCESS", "DRY_RUN"):
        raise RuntimeError(
            f"Analysis pipeline failed: {analysis_result.get('error', 'unknown')}"
        )

    print("=== STEP 9: CLEANUP ===")
    t_cleanup = _timer()
    delete_delta_run(run_id)

    _preserve_fetched_snapshot(local_snapshot_path, new_ts, cache_work_dir)
    _cleanup_local_snapshot(local_snapshot_path)
    _write_pipeline_success(new_ts, cache_work_dir)
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
        if cache_work_dir:
            ts_file = Path(cache_work_dir) / ".current_snapshot_ts"
            if ts_file.exists():
                try:
                    snapshot_ts = ts_file.read_text(encoding="utf-8").strip()
                    print(f"Failed snapshot timestamp: {snapshot_ts}")
                except Exception:
                    pass
        sys.exit(1)
