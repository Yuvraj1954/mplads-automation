import json
import os
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

COMBO = "0,0,0,2"

# Number of records per NDJSON file
CHUNK_SIZE = 2000


DATASETS = {
    "works_recommended": "Works Recommended",
    "works_sanctioned": "Works Sanctioned",
    "works_completed": "Works Completed",
    "expenditure": "Expenditure on Completed and On-going Works as on Date",
    "calamity": "Amount consented for Calamity",
    "allocated_limit": "Allocated Limit for Hon'ble MPs",
}


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
# Fetch one dataset
# ==========================================

def fetch_dataset(session, dataset_name, dataset_key):

    print()
    print("=" * 70)
    print(f"Fetching: {dataset_name}")
    print(f"Key:      {dataset_key}")
    print("=" * 70)

    payload = {
        "combo": COMBO,
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

    # Some MPLADS responses store the actual
    # array as a JSON string.
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
    records
):

    folder = (
        f"{timestamp}/"
        f"{dataset_name}"
    )

    filename = (
        f"part_{chunk_number:04d}.ndjson"
    )

    cloud_path = (
        f"{folder}/{filename}"
    )

    # Convert records to NDJSON
    ndjson = "\n".join(
        json.dumps(
            record,
            ensure_ascii=False,
            separators=(",", ":")
        )
        for record in records
    )

    # Always end file with newline
    ndjson += "\n"

    content = ndjson.encode("utf-8")

    size_mb = (
        len(content)
        / (1024 * 1024)
    )

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

    print(
        f"  ✓ {cloud_path}"
    )

    return result


# ==========================================
# Upload dataset as NDJSON chunks
# ==========================================

def upload_dataset(
    dataset_name,
    data,
    timestamp
):

    dataset_key, records = extract_records(
        data,
        dataset_name
    )

    total_records = len(records)

    print()
    print(
        f"Dataset key: {dataset_key}"
    )

    print(
        f"Actual records: {total_records}"
    )

    # --------------------------------------
    # Remove summary/non-record rows
    # --------------------------------------

    # We only remove a row if it clearly isn't
    # an actual dataset record.
    #
    # For works datasets the consistent ID is:
    # WORK_RECOMMENDATION_DTL_ID
    #
    # For allocated_limit/calamity we keep
    # all actual rows.
    #
    # We don't blindly remove the first row.

    if dataset_name in {
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

        removed = (
            before - len(records)
        )

        if removed:
            print(
                f"Removed {removed} "
                f"non-work records"
            )

    elif dataset_name == "works_completed":

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

        removed = (
            before - len(records)
        )

        if removed:
            print(
                f"Removed {removed} "
                f"non-work records"
            )

    print(
        f"Records to upload: {len(records)}"
    )

    # --------------------------------------
    # Create chunks
    # --------------------------------------

    total_chunks = (
        len(records) + CHUNK_SIZE - 1
    ) // CHUNK_SIZE

    print(
        f"Chunk size: {CHUNK_SIZE}"
    )

    print(
        f"Total chunks: {total_chunks}"
    )

    uploaded_records = 0

    for start in range(
        0,
        len(records),
        CHUNK_SIZE
    ):

        chunk_number = (
            start // CHUNK_SIZE
        ) + 1

        chunk = records[
            start:start + CHUNK_SIZE
        ]

        upload_chunk(
            dataset_name=dataset_name,
            timestamp=timestamp,
            chunk_number=chunk_number,
            records=chunk
        )

        uploaded_records += len(chunk)

    print()
    print(
        f"✓ {dataset_name} complete"
    )

    print(
        f"  Records uploaded: "
        f"{uploaded_records}"
    )

    print(
        f"  Chunks uploaded: "
        f"{total_chunks}"
    )

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

    supabase.storage \
        .from_(BUCKET) \
        .upload(
            path=path,
            file=content,
            file_options={
                "content-type":
                    "application/json",
                "cache-control": "3600",
                "upsert": "false",
            }
        )

    print(
        f"  ✓ Manifest uploaded"
    )


# ==========================================
# Main
# ==========================================

def main():

    timestamp = datetime.now(
        timezone.utc
    ).strftime(
        "%Y-%m-%dT%H-%M-%SZ"
    )

    print()
    print(
        "MPLADS FETCH + CHUNK UPLOAD"
    )

    print("=" * 70)

    print(
        f"Timestamp: {timestamp}"
    )

    print(
        f"Datasets: {len(DATASETS)}"
    )

    print(
        f"Chunk size: {CHUNK_SIZE}"
    )

    print("=" * 70)

    session = create_session()

    try:

        # ----------------------------------
        # Establish MPLADS session
        # ----------------------------------

        print()
        print(
            "Connecting to MPLADS..."
        )

        dashboard = session.get(
            DASHBOARD_URL,
            timeout=(30, 60)
        )

        dashboard.raise_for_status()

        print(
            "✓ MPLADS session established"
        )

        successful = 0
        failed = 0

        results = {}

        # ----------------------------------
        # Process datasets
        # ----------------------------------

        for (
            dataset_name,
            dataset_key
        ) in DATASETS.items():

            try:

                data = fetch_dataset(
                    session,
                    dataset_name,
                    dataset_key
                )

                result = upload_dataset(
                    dataset_name,
                    data,
                    timestamp
                )

                upload_manifest(
                    dataset_name,
                    timestamp,
                    result
                )

                results[
                    dataset_name
                ] = result

                successful += 1

            except Exception as error:

                failed += 1

                print()
                print(
                    f"✗ {dataset_name} FAILED"
                )

                print(
                    f"ERROR: {error}"
                )

        # ----------------------------------
        # Summary
        # ----------------------------------

        print()
        print("=" * 70)
        print("FETCH COMPLETE")
        print("=" * 70)

        print(
            f"Successful: {successful}"
        )

        print(
            f"Failed:     {failed}"
        )

        print()
        print(
            "Cloud structure:"
        )

        print(
            f"{timestamp}/"
        )

        for dataset_name in results:

            result = results[
                dataset_name
            ]

            print(
                f"├── {dataset_name}/"
            )

            print(
                f"│   ├── manifest.json"
            )

            print(
                f"│   └── "
                f"part_0001.ndjson ..."
            )

        print()
        print(
            f"Timestamp folder: {timestamp}"
        )

        if failed:
            raise SystemExit(1)

    finally:

        session.close()


if __name__ == "__main__":
    main()