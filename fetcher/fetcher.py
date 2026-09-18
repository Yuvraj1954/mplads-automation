import json
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from supabase import create_client



load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SECRET_KEY = os.getenv("SUPABASE_SECRET_KEY")

if not SUPABASE_URL:
    raise RuntimeError("SUPABASE_URL is missing from .env")

if not SUPABASE_SECRET_KEY:
    raise RuntimeError("SUPABASE_SECRET_KEY is missing from .env")

def create_supabase_client():
    """Create a new Supabase client. Each worker must call this to get
    its own independent client — supabase-py wraps httpx.Client which is
    NOT thread-safe, so a shared global client would corrupt under
    concurrent uploads."""
    return create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)

DASHBOARD_URL = (
    "https://mplads.mospi.gov.in/digigov/dashboard.html"
)

API_URL = (
    "https://mplads.mospi.gov.in/rest/"
    "PreLoginDashboardData/getTilesReportData"
)

BUCKET = "mplads-raw"

# Combo values for MP and MLA data
MP_COMBO = "0,0,0,2"
MLA_COMBO = "0,0,0,1"

# Number of records per NDJSON file
CHUNK_SIZE = 2000

# A failed dataset is retried only AFTER all 6 datasets
# have had their first attempt.
MAX_RETRIES = 2

# Delay between retry attempts, in seconds.
RETRY_DELAY_SECONDS = 5

# MP datasets
MP_DATASETS = {
    "works_recommended": "Works Recommended",
    "works_sanctioned": "Works Sanctioned",
    "works_completed": "Works Completed",
    "expenditure": "Expenditure on Completed and On-going Works as on Date",
    "calamity": "Amount consented for Calamity",
    "allocated_limit": "Allocated Limit for Hon'ble MPs",
}

# MLA datasets
MLA_DATASETS = {
    "works_recommended": "Works Recommended",
    "works_sanctioned": "Works Sanctioned",
    "works_completed": "Works Completed",
    "expenditure": "Expenditure on Completed and On-going Works as on Date",
    "calamity": "Amount consented for Calamity",
    "allocated_limit": "Allocated Limit for Hon'ble MLAs",
}

# Combined datasets for backward compatibility
DATASETS = MP_DATASETS



def create_session():
    session = requests.Session()

    session.headers.update({
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/json; charset=UTF-8",
        "Origin": "https://mplads.mospi.gov.in",
        "Referer": DASHBOARD_URL,
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": (
            "Mozilla/5.0 "
            "(Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 "
            "(KHTML, like Gecko) "
            "Chrome/152.0.0.0 Safari/537.36"
        ),
    })

    return session



def establish_session(session):
    dashboard = session.get(
        DASHBOARD_URL,
        timeout=(30, 60)
    )

    dashboard.raise_for_status()

    print("✓ MPLADS session established")



def fetch_dataset(session, dataset_name, dataset_key, combo):
    print()
    print("=" * 70)
    print(f"Fetching: {dataset_name}")
    print(f"Key:      {dataset_key}")
    print(f"Combo:    {combo}")
    print("=" * 70)

    payload = {
        "combo": combo,
        "key": dataset_key
    }

    response = session.post(
        API_URL,
        json=payload,
        timeout=(30, 300)
    )

    response.raise_for_status()

    size_mb = len(response.content) / (1024 * 1024)

    print(f"HTTP: {response.status_code}")
    print(f"Response size: {size_mb:.2f} MB")

    try:
        data = response.json()
    except ValueError as exc:
        raise RuntimeError(
            f"{dataset_name}: response is not valid JSON"
        ) from exc

    if not isinstance(data, dict):
        raise RuntimeError(
            f"{dataset_name}: unexpected JSON structure"
        )

    if not data:
        raise RuntimeError(
            f"{dataset_name}: empty response"
        )

    print(f"JSON keys: {list(data.keys())}")

    return data



def extract_records(data, dataset_name):
    if not data:
        raise RuntimeError(
            f"{dataset_name}: empty dataset"
        )

    dataset_key = next(iter(data))
    raw_records = data[dataset_key]

    if isinstance(raw_records, str):
        try:
            records = json.loads(raw_records)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"{dataset_name}: dataset value "
                f"is not valid nested JSON"
            ) from exc

    elif isinstance(raw_records, list):
        records = raw_records

    else:
        raise RuntimeError(
            f"{dataset_name}: unexpected dataset "
            f"value type: {type(raw_records).__name__}"
        )

    if not isinstance(records, list):
        raise RuntimeError(
            f"{dataset_name}: extracted data "
            f"is not a list"
        )

    return dataset_key, records



def upload_chunk(
    dataset_name,
    timestamp,
    chunk_number,
    records,
    local_snapshot_dir,
    local_only=False,
    supabase_client=None,
):
    folder = f"{timestamp}/{dataset_name}"
    filename = f"part_{chunk_number:04d}.ndjson"
    cloud_path = f"{folder}/{filename}"

    ndjson = "\n".join(
        json.dumps(
            record,
            ensure_ascii=False,
            separators=(",", ":")
        )
        for record in records
    )

    ndjson += "\n"
    content = ndjson.encode("utf-8")

    local_dir = local_snapshot_dir / dataset_name
    local_dir.mkdir(parents=True, exist_ok=True)
    local_path = local_dir / filename
    local_path.write_bytes(content)

    size_mb = len(content) / (1024 * 1024)

    if local_only:
        print(
            f"  Wrote {filename} locally "
            f"({len(records)} records, "
            f"{size_mb:.2f} MB)"
        )
        return None

    print(
        f"  Uploading {filename} "
        f"({len(records)} records, "
        f"{size_mb:.2f} MB)"
    )

    client = supabase_client or create_supabase_client()
    result = (
        client
        .storage
        .from_(BUCKET)
        .upload(
            path=cloud_path,
            file=content,
            file_options={
                "content-type": "application/x-ndjson",
                "cache-control": "3600",
                "upsert": "true",
            }
        )
    )

    print(f"  ✓ {cloud_path}")

    return result



def upload_dataset(
    dataset_name,
    data,
    timestamp,
    local_snapshot_dir,
    local_only=False,
    supabase_client=None,
):
    dataset_key, records = extract_records(
        data,
        dataset_name
    )

    print()
    print(f"Dataset key: {dataset_key}")
    print(f"Actual records: {len(records)}")

    # --------------------------------------
    # Remove summary/non-record rows
    # --------------------------------------

    # Check both MP and MLA dataset names
    base_name = dataset_name.replace("mla_", "")
    if base_name in {
        "works_recommended",
        "works_sanctioned",
        "expenditure",
    }:
        before = len(records)

        records = [
            record
            for record in records
            if isinstance(record, dict)
            and record.get(
                "WORK_RECOMMENDATION_DTL_ID"
            ) is not None
        ]

        removed = before - len(records)

        if removed:
            print(
                f"Removed {removed} "
                f"non-work records"
            )

    elif base_name == "works_completed":
        before = len(records)

        records = [
            record
            for record in records
            if isinstance(record, dict)
            and (
                record.get(
                    "WORK_RECOMMENDATION_DTL_ID"
                ) is not None
                or record.get("WORK_ID") is not None
            )
        ]

        removed = before - len(records)

        if removed:
            print(
                f"Removed {removed} "
                f"non-work records"
            )

    print(f"Records to upload: {len(records)}")

    if not records and base_name in {
        "works_recommended",
        "works_sanctioned",
        "expenditure",
    }:
        raise RuntimeError(
            f"{dataset_name}: API returned 0 valid work records "
            f"after filtering (likely returned only a summary row). "
            f"Treating as failure so it will be retried."
        )

    # --------------------------------------
    # Create chunks
    # --------------------------------------

    total_chunks = (
        len(records) + CHUNK_SIZE - 1
    ) // CHUNK_SIZE

    print(f"Chunk size: {CHUNK_SIZE}")
    print(f"Total chunks: {total_chunks}")

    uploaded_records = 0

    for start in range(
        0,
        len(records),
        CHUNK_SIZE
    ):
        chunk_number = (start // CHUNK_SIZE) + 1

        chunk = records[
            start:start + CHUNK_SIZE
        ]

        upload_chunk(
            dataset_name=dataset_name,
            timestamp=timestamp,
            chunk_number=chunk_number,
            records=chunk,
            local_snapshot_dir=local_snapshot_dir,
            local_only=local_only,
            supabase_client=supabase_client,
        )

        uploaded_records += len(chunk)

    print()
    print(f"✓ {dataset_name} complete")
    print(f"  Records uploaded: {uploaded_records}")
    print(f"  Chunks uploaded: {total_chunks}")

    return {
        "dataset_key": dataset_key,
        "records": uploaded_records,
        "chunks": total_chunks,
    }



def upload_manifest(
    dataset_name,
    timestamp,
    result,
    supabase_client=None,
):
    manifest = {
        "dataset": dataset_name,
        "dataset_key": result["dataset_key"],
        "records": result["records"],
        "chunk_size": CHUNK_SIZE,
        "chunks": result["chunks"],
        "created_at": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    path = (
        f"{timestamp}/"
        f"{dataset_name}/manifest.json"
    )

    content = json.dumps(
        manifest,
        indent=2,
        ensure_ascii=False
    ).encode("utf-8")

    client = supabase_client or create_supabase_client()
    (
        client
        .storage
        .from_(BUCKET)
        .upload(
            path=path,
            file=content,
            file_options={
                "content-type": "application/json",
                "cache-control": "3600",
                "upsert": "true",
            }
        )
    )

    print("  ✓ Manifest uploaded")



def build_completion_marker(timestamp, results):
    """Build and validate the canonical _COMPLETE.json marker.

    Validates that all expected datasets are present with valid metadata.
    Returns a single marker dict used for both local write and cloud upload.

    Raises RuntimeError if validation fails.
    """
    expected_mp = set(MP_DATASETS.keys())
    expected_mla = {f"mla_{k}" for k in MLA_DATASETS.keys()}
    expected = expected_mp | expected_mla
    actual = set(results.keys())

    if actual != expected:
        missing = expected - actual
        raise RuntimeError(
            "Cannot create completion marker: "
            f"missing datasets: {sorted(missing)}"
        )

    for name in expected:
        r = results[name]

        if not isinstance(r, dict):
            raise RuntimeError(
                "Cannot create completion marker: "
                f"{name} has invalid result"
            )

        records = r.get("records")
        chunks = r.get("chunks")

        if not isinstance(records, int):
            raise RuntimeError(
                "Cannot create completion marker: "
                f"{name} has invalid record count"
            )

        if not isinstance(chunks, int):
            raise RuntimeError(
                "Cannot create completion marker: "
                f"{name} has invalid chunk count"
            )

    datasets_meta = {}

    for name in expected:
        r = results[name]

        datasets_meta[name] = {
            "records": r["records"],
            "chunks": r["chunks"],
        }

    return {
        "timestamp": timestamp,
        "completed_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "status": "complete",
        "datasets": datasets_meta,
    }


def upload_completion_marker(marker, supabase_client=None):
    """Upload a pre-built completion marker to Supabase Storage.

    The marker dict is serialized and uploaded as _COMPLETE.json
    under the marker's timestamp path. Raises on upload failure.
    """
    path = f"{marker['timestamp']}/_COMPLETE.json"

    content = json.dumps(
        marker,
        indent=2,
        ensure_ascii=False
    ).encode("utf-8")

    client = supabase_client or create_supabase_client()
    (
        client
        .storage
        .from_(BUCKET)
        .upload(
            path=path,
            file=content,
            file_options={
                "content-type": "application/json",
                "cache-control": "3600",
                "upsert": "true",
            }
        )
    )

    print("  ✓ Completion marker uploaded")



def process_dataset(
    session,
    dataset_name,
    dataset_key,
    timestamp,
    local_snapshot_dir,
    combo,
    local_only=False,
    supabase_client=None,
):
    data = fetch_dataset(
        session,
        dataset_name,
        dataset_key,
        combo
    )

    result = upload_dataset(
        dataset_name,
        data,
        timestamp,
        local_snapshot_dir,
        local_only=local_only,
        supabase_client=supabase_client,
    )

    if not local_only:
        upload_manifest(
            dataset_name,
            timestamp,
            result,
            supabase_client=supabase_client,
        )

    return result



def fetch_member_type(
    session,
    member_type,
    datasets,
    combo,
    timestamp,
    local_snapshot_dir,
    local_only=False,
):
    """Fetch all datasets for a member type (MP or MLA)."""
    print()
    print("=" * 70)
    print(f"FETCHING {member_type} DATASETS")
    print(f"Combo: {combo}")
    print("=" * 70)

    successful = 0
    failed = 0
    results = {}
    failed_datasets = []

    print()
    print("=" * 70)
    print(f"PHASE 1 — INITIAL FETCH ({member_type})")
    print("=" * 70)

    for dataset_name, dataset_key in datasets.items():
        # Prefix dataset name for MLA to avoid conflicts
        if member_type == "MLA":
            storage_name = f"mla_{dataset_name}"
        else:
            storage_name = dataset_name

        try:
            result = process_dataset(
                session,
                storage_name,
                dataset_key,
                timestamp,
                local_snapshot_dir,
                combo,
                local_only=local_only,
            )

            results[storage_name] = result
            successful += 1

        except Exception as error:
            print()
            print(f"✗ {storage_name} FAILED")
            print(f"ERROR: {error}")

            failed_datasets.append({
                "name": storage_name,
                "key": dataset_key,
                "error": str(error),
            })

    if failed_datasets:
        print()
        print("=" * 70)
        print(f"PHASE 2 — RETRY FAILED DATASETS ({member_type})")
        print("=" * 70)
        print(
            f"Datasets requiring retry: "
            f"{len(failed_datasets)}"
        )

        for retry_number in range(
            1,
            MAX_RETRIES + 1
        ):
            if not failed_datasets:
                break

            print()
            print(
                f"--- Retry attempt "
                f"{retry_number}/{MAX_RETRIES} ---"
            )

            # Fresh session before each retry round.
            try:
                session.close()
            except Exception:
                pass

            session = create_session()

            try:
                print("Creating fresh MPLADS session...")
                establish_session(session)
            except Exception as error:
                print(
                    f"✗ Could not establish fresh "
                    f"session: {error}"
                )

                if retry_number < MAX_RETRIES:
                    time.sleep(RETRY_DELAY_SECONDS)

                continue

            next_failed = []

            for item in failed_datasets:
                dataset_name = item["name"]
                dataset_key = item["key"]

                print()
                print(
                    f"Retrying: {dataset_name}"
                )

                try:
                    result = process_dataset(
                        session,
                        dataset_name,
                        dataset_key,
                        timestamp,
                        local_snapshot_dir,
                        combo,
                        local_only=local_only,
                    )

                    results[dataset_name] = result
                    successful += 1

                    print(
                        f"✓ {dataset_name} "
                        f"succeeded on retry "
                        f"{retry_number}"
                    )

                except Exception as error:
                    print(
                        f"✗ {dataset_name} "
                        f"retry {retry_number} failed"
                    )
                    print(f"ERROR: {error}")

                    next_failed.append({
                        "name": dataset_name,
                        "key": dataset_key,
                        "error": str(error),
                    })

            failed_datasets = next_failed

            if failed_datasets and retry_number < MAX_RETRIES:
                print()
                print(
                    f"Waiting {RETRY_DELAY_SECONDS} "
                    f"seconds before next retry..."
                )
                time.sleep(RETRY_DELAY_SECONDS)

    failed = len(failed_datasets)

    return results, successful, failed, failed_datasets


def fetch_all_datasets_concurrently(
    timestamp,
    local_snapshot_dir,
    concurrency=5,
    local_only=False,
):
    """Fetch all 12 MP and MLA datasets concurrently using bounded worker pool."""
    import queue
    import threading

    jobs = []
    for name, key in MP_DATASETS.items():
        jobs.append({
            "storage_name": name,
            "dataset_key": key,
            "combo": MP_COMBO,
            "member_type": "MP",
        })
    for name, key in MLA_DATASETS.items():
        jobs.append({
            "storage_name": f"mla_{name}",
            "dataset_key": key,
            "combo": MLA_COMBO,
            "member_type": "MLA",
        })

    print()
    print("Fetch scheduler:")
    print(f"  jobs={len(jobs)}")
    print(f"  concurrency={concurrency}")

    all_results = {}
    failed_datasets = []
    results_lock = threading.Lock()
    failures_lock = threading.Lock()

    def _execute_phase(job_list, is_retry=False, retry_num=0):
        work_queue = queue.Queue()
        for j in job_list:
            work_queue.put(j)

        def _worker():
            session = create_session()
            worker_supabase = create_supabase_client()
            try:
                establish_session(session)
            except Exception as e:
                print(f"  ✗ Worker session warning: {e}")

            while True:
                try:
                    job = work_queue.get_nowait()
                except queue.Empty:
                    break

                storage_name = job.get("storage_name") or job.get("name")
                dataset_key = job.get("dataset_key") or job.get("key")
                combo = job.get("combo")

                try:
                    res = process_dataset(
                        session,
                        storage_name,
                        dataset_key,
                        timestamp,
                        local_snapshot_dir,
                        combo,
                        local_only=local_only,
                        supabase_client=worker_supabase,
                    )
                    with results_lock:
                        all_results[storage_name] = res
                    if is_retry:
                        print(f"✓ {storage_name} succeeded on retry {retry_num}")
                except Exception as error:
                    print(f"✗ {storage_name} FAILED: {error}")
                    with failures_lock:
                        failed_datasets.append({
                            "name": storage_name,
                            "key": dataset_key,
                            "storage_name": storage_name,
                            "dataset_key": dataset_key,
                            "combo": combo,
                            "member_type": job.get("member_type"),
                            "error": str(error),
                        })
                finally:
                    work_queue.task_done()

            try:
                session.close()
            except Exception:
                pass

        threads = []
        num_workers = min(concurrency, len(job_list))
        for _ in range(num_workers):
            t = threading.Thread(target=_worker)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

    # PHASE 1: Initial concurrent fetch
    print()
    print("=" * 70)
    print("PHASE 1 — INITIAL CONCURRENT FETCH (12 datasets)")
    print("=" * 70)
    _execute_phase(jobs)

    # PHASE 2: Retry failed datasets
    if failed_datasets:
        print()
        print("=" * 70)
        print("PHASE 2 — RETRY FAILED DATASETS")
        print("=" * 70)

        for retry_number in range(1, MAX_RETRIES + 1):
            with failures_lock:
                to_retry = list(failed_datasets)
                failed_datasets.clear()

            if not to_retry:
                break

            print()
            print(f"--- Retry attempt {retry_number}/{MAX_RETRIES} ({len(to_retry)} datasets) ---")
            time.sleep(RETRY_DELAY_SECONDS)
            _execute_phase(to_retry, is_retry=True, retry_num=retry_number)

            expected_datasets = {j["storage_name"] for j in jobs}
            missing = expected_datasets - set(all_results.keys())
            if not missing:
                break

    total_successful = len(all_results)
    total_failed = len(jobs) - total_successful

    return all_results, total_successful, total_failed, failed_datasets


def main():
    import argparse
    parser = argparse.ArgumentParser(description="MPLADS Fetcher")
    parser.add_argument("--local-only", action="store_true",
                        help="Save locally only, skip Supabase Storage uploads")
    args = parser.parse_args()

    local_only = args.local_only

    timestamp = datetime.now(
        timezone.utc
    ).strftime(
        "%Y-%m-%dT%H-%M-%SZ"
    )

    print()
    print("MPLADS FETCH" + (" (LOCAL ONLY)" if local_only else " + CHUNK UPLOAD"))
    print("=" * 70)
    print(f"Timestamp: {timestamp}")
    print(f"MP Datasets: {len(MP_DATASETS)}")
    print(f"MLA Datasets: {len(MLA_DATASETS)}")
    print(f"Chunk size: {CHUNK_SIZE}")
    print(f"Retries after first pass: {MAX_RETRIES}")
    print(f"Local only: {local_only}")
    print("=" * 70)

    local_snapshot_dir = Path(
        tempfile.mkdtemp(prefix=f"mplads_local_{timestamp}_")
    )
    print(f"Local snapshot: {local_snapshot_dir}")

    fetch_concurrency = int(os.environ.get("FETCH_CONCURRENCY", "5"))

    all_results, total_successful, total_failed, all_failed_datasets = fetch_all_datasets_concurrently(
        timestamp=timestamp,
        local_snapshot_dir=local_snapshot_dir,
        concurrency=fetch_concurrency,
        local_only=local_only,
    )

    print()
    print("=" * 70)
    print("FETCH COMPLETE")
    print("=" * 70)

    print(f"Successful: {total_successful}")
    print(f"Failed:     {total_failed}")

    print()
    print("Cloud structure:")
    print(f"{timestamp}/")

    for dataset_name in all_results:
        result = all_results[dataset_name]

        print(f"├── {dataset_name}/")
        print("│   ├── manifest.json")
        print("│   └── part_0001.ndjson ...")

    if all_failed_datasets:
        print()
        print("Failed datasets after all retries:")

        for item in all_failed_datasets:
            print(
                f"  ✗ {item['name']}: "
                f"{item['error']}"
            )

    print()
    print(f"Timestamp folder: {timestamp}")

    if total_failed:
        print()
        print(
            "Run failed because one or more "
            "datasets could not be fetched "
            "after all retry attempts."
        )
        raise SystemExit(1)

    print()
    print("Writing completion marker...")

    try:
        marker = build_completion_marker(timestamp, all_results)
    except Exception as error:
        print()
        print(
            f"✗ FAILED to build "
            f"completion marker: {error}"
        )
        raise SystemExit(1)

    marker_path = local_snapshot_dir / "_COMPLETE.json"
    marker_path.write_text(
        json.dumps(marker, indent=2, ensure_ascii=False)
    )
    print(f"  ✓ Local completion marker written: {marker_path}")

    if not local_only:
        try:
            upload_completion_marker(marker, supabase_client=create_supabase_client())
        except Exception as error:
            print()
            print(
                f"✗ FAILED to upload "
                f"completion marker: {error}"
            )
            print(
                "Run treated as FAILED because "
                "_COMPLETE.json could not be "
                "uploaded."
            )
            raise SystemExit(1)

    print()
    print("✓ ALL DATASETS SUCCESSFUL")
    print("✓ COMPLETION MARKER WRITTEN")
    if not local_only:
        print("✓ COMPLETION MARKER UPLOADED")
    print("✓ SNAPSHOT IS READY FOR INGESTION")
    print(f"✓ Local snapshot: {local_snapshot_dir}")
    print(f"LOCAL_SNAPSHOT_PATH={local_snapshot_dir}")



if __name__ == "__main__":
    main()
