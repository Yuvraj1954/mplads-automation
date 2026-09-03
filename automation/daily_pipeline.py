import os
import sys
import json
import shutil
import subprocess
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

ROOT = Path(__file__).resolve().parent.parent
FETCHER = ROOT / "fetcher" / "fetcher.py"
COMPARATOR = ROOT / "comparator" / "comparator_v2.py"

BUCKET = "mplads-raw"
DATASETS = [
    "allocated_limit",
    "works_recommended",
    "works_sanctioned",
    "works_completed",
    "expenditure",
    "calamity",
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
]


def validate_local_snapshot(path):
    """Validate that the local snapshot directory has the expected structure."""
    p = Path(path)
    if not p.exists():
        raise RuntimeError(f"Local snapshot path does not exist: {p}")
    if not p.is_dir():
        raise RuntimeError(f"Local snapshot path is not a directory: {p}")
    for dataset in EXPECTED_DATASETS:
        dataset_dir = p / dataset
        if not dataset_dir.is_dir():
            raise RuntimeError(
                f"Local snapshot missing dataset folder: {dataset}"
            )
        part_files = list(dataset_dir.glob("part_*.ndjson"))
        if not part_files:
            raise RuntimeError(
                f"Local snapshot dataset '{dataset}' has no part_*.ndjson files"
            )
    print(f"Local snapshot validated: {p}")


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
            .select("id", count="exact")
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


def main():
    if not FETCHER.exists():
        raise RuntimeError(f"Fetcher not found: {FETCHER}")
    if not COMPARATOR.exists():
        raise RuntimeError(
            f"Comparator not found: {COMPARATOR}. "
            "Change COMPARATOR near the top of automation/daily_pipeline.py "
            "to the real comparator path in your repo."
        )

    print("=== STEP 1: FETCH ===")
    fetcher_output = run_fetcher([sys.executable, str(FETCHER)])

    local_snapshot_path = None
    for line in fetcher_output:
        if line.startswith("LOCAL_SNAPSHOT_PATH="):
            local_snapshot_path = line.split("=", 1)[1].strip()
            break

    if not local_snapshot_path:
        raise RuntimeError(
            "Fetcher completed but LOCAL_SNAPSHOT_PATH not found in output"
        )

    validate_local_snapshot(local_snapshot_path)

    snapshots = list_complete_snapshots()
    if not snapshots:
        raise RuntimeError("No complete snapshot exists after fetch")

    for ts in snapshots[-2:]:
        assert_snapshot_complete(ts)

    if len(snapshots) < 2:
        print("Bootstrap: only one complete snapshot exists. Nothing to compare.")
        return

    old_ts, new_ts = snapshots[-2], snapshots[-1]
    print("Comparing:", old_ts, "->", new_ts)

    workdir = ROOT / ".automation_delta"
    if workdir.exists():
        shutil.rmtree(workdir)
    workdir.mkdir()

    run_id = f"delta_{new_ts}"

    print("=== STEP 2: COMPARE ===")
    run([
        sys.executable,
        str(COMPARATOR),
        "--old", old_ts,
        "--new", new_ts,
        "--new-local", local_snapshot_path,
        "--output", str(workdir),
    ])

    manifest_path = workdir / "manifest.json"
    if not manifest_path.exists():
        raise RuntimeError("Comparator did not create manifest.json")

    manifest = json.loads(manifest_path.read_text())
    summary = manifest.get("summary", {})
    total_changes = int(summary.get("total_append", 0)) + int(
        summary.get("total_update", 0)
    )

    print("Changes:", total_changes)

    print("=== STEP 3: UPLOAD DELTA ===")
    upload_delta(workdir, run_id)

    if total_changes == 0:
        print("No changes. No ingestion needed.")
        delete_delta_run(run_id)
        delete_old_raw_snapshots(snapshots[-2:])
        _cleanup_local_snapshot(local_snapshot_path)
        return

    print("=== STEP 4: CREATE INGESTION JOBS ===")
    controller = call_edge("mplads-controller", {"run_id": run_id})
    expected_jobs = int(controller.get("total_jobs", 0))
    if expected_jobs <= 0:
        raise RuntimeError("Comparator found changes, but controller created 0 jobs")

    print("Expected jobs:", expected_jobs)

    print("=== STEP 5: RUN WORKER ===")
    while True:
        result = call_edge("mplads-worker", {"run_id": run_id})
        print("Worker:", result)
        if result.get("success") and (
            result.get("message") == "All ingestion jobs completed."
            or result.get("progress", {}).get("remaining", 1) == 0
        ):
            break

    print("=== STEP 6: VERIFY ===")
    if not verify_run(run_id, expected_jobs):
        raise RuntimeError("Ingestion verification failed")

    print("=== STEP 7: CLEANUP ===")
    delete_delta_run(run_id)
    delete_old_raw_snapshots(snapshots[-2:])

    _cleanup_local_snapshot(local_snapshot_path)

    print("=== PIPELINE COMPLETE ===")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"PIPELINE FAILED: {exc}", file=sys.stderr)
        print("OLD SNAPSHOT HAS NOT BEEN DELETED", file=sys.stderr)
        sys.exit(1)
