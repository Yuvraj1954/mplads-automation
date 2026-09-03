import json
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from supabase import create_client


# ==========================================
# Configuration
# ==========================================

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SECRET_KEY = os.getenv("SUPABASE_SECRET_KEY")

if not SUPABASE_URL:
    raise RuntimeError("SUPABASE_URL is missing from .env")

if not SUPABASE_SECRET_KEY:
    raise RuntimeError("SUPABASE_SECRET_KEY is missing from .env")

supabase = create_client(
    SUPABASE_URL,
    SUPABASE_SECRET_KEY
)

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


# ==========================================
# Create MPLADS session
# ==========================================

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


# ==========================================
# Establish MPLADS session
# ==========================================

def establish_session(session):
    dashboard = session.get(
        DASHBOARD_URL,
        timeout=(30, 60)
    )

    dashboard.raise_for_status()

    print("✓ MPLADS session established")


# ==========================================
# Fetch one dataset
# ==========================================

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


# ==========================================
# Extract actual records
# ==========================================

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


# ==========================================
# Upload one NDJSON chunk
# ==========================================

def upload_chunk(
    dataset_name,
    timestamp,
    chunk_number,
    records,
    local_snapshot_dir,
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

    print(
        f"  Uploading {filename} "
        f"({len(records)} records, "
        f"{size_mb:.2f} MB)"
    )

    result = (
        supabase
        .storage
        .from_(BUCKET)
        .upload(
            path=cloud_path,
            file=content,
            file_options={
                "content-type": "application/x-ndjson",
                "cache-control": "3600",
                "upsert": "false",
            }
        )
    )

    print(f"  ✓ {cloud_path}")

    return result


# ==========================================
# Upload dataset as NDJSON chunks
# ==========================================

def upload_dataset(
    dataset_name,
    data,
    timestamp,
    local_snapshot_dir,
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


# ==========================================
# Upload manifest
# ==========================================

def upload_manifest(
    dataset_name,
    timestamp,
    result
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

    (
        supabase
        .storage
        .from_(BUCKET)
        .upload(
            path=path,
            file=content,
            file_options={
                "content-type": "application/json",
                "cache-control": "3600",
                "upsert": "false",
            }
        )
    )

    print("  ✓ Manifest uploaded")


# ==========================================
# Upload completion marker
# ==========================================

def upload_completion_marker(
    timestamp,
    results
):
    """
    Validate all datasets and upload
    _COMPLETE.json.

    If validation fails or the upload fails,
    raise an exception so the caller can
    treat the entire run as failed.
    """

    # Build expected dataset names from MP and MLA datasets
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

    marker = {
        "timestamp": timestamp,
        "completed_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "status": "complete",
        "datasets": datasets_meta,
    }

    path = f"{timestamp}/_COMPLETE.json"

    content = json.dumps(
        marker,
        indent=2,
        ensure_ascii=False
    ).encode("utf-8")

    (
        supabase
        .storage
        .from_(BUCKET)
        .upload(
            path=path,
            file=content,
            file_options={
                "content-type": "application/json",
                "cache-control": "3600",
                "upsert": "false",
            }
        )
    )

    print("  ✓ Completion marker uploaded")


# ==========================================
# Process one dataset
# ==========================================

def process_dataset(
    session,
    dataset_name,
    dataset_key,
    timestamp,
    local_snapshot_dir,
    combo,
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
    )

    upload_manifest(
        dataset_name,
        timestamp,
        result
    )

    return result


# ==========================================
# Main
# ==========================================

def fetch_member_type(
    session,
    member_type,
    datasets,
    combo,
    timestamp,
    local_snapshot_dir,
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

    # ==================================
    # PHASE 1: Attempt ALL datasets once
    # ==================================

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

    # ==================================
    # PHASE 2: Retry only failures
    # ==================================

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


def main():
    timestamp = datetime.now(
        timezone.utc
    ).strftime(
        "%Y-%m-%dT%H-%M-%SZ"
    )

    print()
    print("MPLADS FETCH + CHUNK UPLOAD")
    print("=" * 70)
    print(f"Timestamp: {timestamp}")
    print(f"MP Datasets: {len(MP_DATASETS)}")
    print(f"MLA Datasets: {len(MLA_DATASETS)}")
    print(f"Chunk size: {CHUNK_SIZE}")
    print(f"Retries after first pass: {MAX_RETRIES}")
    print("=" * 70)

    local_snapshot_dir = Path(
        tempfile.mkdtemp(prefix=f"mplads_local_{timestamp}_")
    )
    print(f"Local snapshot: {local_snapshot_dir}")

    # --------------------------------------
    # Initial session
    # --------------------------------------

    session = create_session()

    all_results = {}
    total_successful = 0
    total_failed = 0
    all_failed_datasets = []

    try:
        print()
        print("Connecting to MPLADS...")
        establish_session(session)

        # ==================================
        # Fetch MP datasets
        # ==================================

        mp_results, mp_successful, mp_failed, mp_failed_datasets = fetch_member_type(
            session,
            "MP",
            MP_DATASETS,
            MP_COMBO,
            timestamp,
            local_snapshot_dir,
        )

        all_results.update(mp_results)
        total_successful += mp_successful
        total_failed += mp_failed
        all_failed_datasets.extend(mp_failed_datasets)

        # ==================================
        # Fetch MLA datasets
        # ==================================

        mla_results, mla_successful, mla_failed, mla_failed_datasets = fetch_member_type(
            session,
            "MLA",
            MLA_DATASETS,
            MLA_COMBO,
            timestamp,
            local_snapshot_dir,
        )

        all_results.update(mla_results)
        total_successful += mla_successful
        total_failed += mla_failed
        all_failed_datasets.extend(mla_failed_datasets)

        # ==================================
        # Final summary
        # ==================================

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

        # ==================================
        # COMPLETION MARKER
        # ==================================

        if total_failed:
            print()
            print(
                "Run failed because one or more "
                "datasets could not be fetched "
                "after all retry attempts."
            )
            raise SystemExit(1)

        print()
        print("Uploading completion marker...")

        try:
            upload_completion_marker(
                timestamp,
                all_results
            )
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
        print("✓ COMPLETION MARKER UPLOADED")
        print("✓ SNAPSHOT IS READY FOR INGESTION")
        print(f"✓ Local snapshot: {local_snapshot_dir}")
        print(f"LOCAL_SNAPSHOT_PATH={local_snapshot_dir}")

    finally:
        session.close()


if __name__ == "__main__":
    main()
