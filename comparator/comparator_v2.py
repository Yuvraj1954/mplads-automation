#!/usr/bin/env python3
"""
MPLADS Snapshot Comparator v3

Compares two complete MPLADS snapshots stored in Supabase Storage.
Uses SQLite as a temporary local database for memory-efficient comparison.

Usage:
    python3 comparator_v2.py [--old TIMESTAMP] [--new TIMESTAMP] [--new-local PATH]
    python3 comparator_v2.py --old 2026-08-31T18-22-06Z --new 2026-09-01T16-50-37Z
    python3 comparator_v2.py --old 2026-08-31T18-22-06Z --new 2026-09-01T16-50-37Z --new-local /path/to/local/snapshot

If --old is omitted, reports bootstrap (no delta generated).
If --new is omitted (and --new-local not given), auto-discovers the two most recent complete snapshots.
If --new-local is supplied, uses the local directory as the new snapshot instead of downloading from Supabase.
  The --new timestamp is still used for manifest labeling and output naming.

Output structure:
    delta_<new_timestamp>/
        allocated_limit/
            append_part_0001.ndjson
            update_part_0001.ndjson
        works_recommended/
            append_part_0001.ndjson
            update_part_0001.ndjson
        works_sanctioned/
            append_part_0001.ndjson
            update_part_0001.ndjson
        works_completed/
            append_part_0001.ndjson
            update_part_0001.ndjson
        expenditure/
            append_part_0001.ndjson
        calamity/
            append_part_0001.ndjson
            update_part_0001.ndjson
        manifest.json
"""

import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
import tempfile
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from dotenv import load_dotenv
from supabase import create_client



load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SECRET_KEY = os.getenv("SUPABASE_SECRET_KEY")
BUCKET = "mplads-raw"
DELTA_CHUNK_SIZE = 2000
DOWNLOAD_RETRIES = 3
DOWNLOAD_BACKOFF = 5

if not SUPABASE_URL:
    raise RuntimeError("SUPABASE_URL is missing from .env")
if not SUPABASE_SECRET_KEY:
    raise RuntimeError("SUPABASE_SECRET_KEY is missing from .env")

supabase = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)

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



IDENTITY_FIELDS = {
    "allocated_limit": ["MP_NAME", "STATE_NAME", "CONSTITUENCY", "TENURE"],
    "works_recommended": ["WORK_RECOMMENDATION_DTL_ID"],
    "works_sanctioned": ["WORK_RECOMMENDATION_DTL_ID"],
    "works_completed": ["WORK_RECOMMENDATION_DTL_ID"],
    "expenditure": [
        "WORK_RECOMMENDATION_DTL_ID",
        "WORK_ID",
        "EXPENDITURE_DATE",
        "VENDOR_NAME",
        "WORK_STATUS",
        "FUND_DISBURSED_AMT",
        "STATE_NAME",
        "CONSTITUENCY",
        "MP_NAME",
    ],
    "calamity": ["MP_NAME", "CRT_DT", "CALAMITY_NAME"],
    # MLA datasets use the same identity fields
    "mla_allocated_limit": ["MP_NAME", "STATE_NAME", "CONSTITUENCY", "TENURE"],
    "mla_works_recommended": [
    "STATE_NAME",
    "MP_NAME",
    "WORK_RECOMMENDATION_DTL_ID"
    ],
    "mla_works_sanctioned": ["WORK_RECOMMENDATION_DTL_ID"],
    "mla_works_completed": ["WORK_RECOMMENDATION_DTL_ID"],
    "mla_expenditure": [
        "WORK_RECOMMENDATION_DTL_ID",
        "WORK_ID",
        "EXPENDITURE_DATE",
        "VENDOR_NAME",
        "WORK_STATUS",
        "FUND_DISBURSED_AMT",
        "STATE_NAME",
        "CONSTITUENCY",
        "MP_NAME",
    ],
    "mla_calamity": ["MP_NAME", "CRT_DT", "CALAMITY_NAME"],
}

# Content fields that actually cause UPDATE in the injector.
# These match the exact fields persisted by ingest-mplads-part.
CONTENT_FIELDS = {
    "allocated_limit": [
        "ALLOCATED_AMT",
        "HOUSE_OF_PARLIAMENT",
        "TENURE_START_DATE",
        "TENURE_END_DATE",
    ],
    "works_recommended": [
        "ACTIVITY_NAME",
        "WORK_CATEGORY",
        "WORK_DESCRIPTION",
        "RECOMMENDATION_DATE",
        "RECOMMENDED_AMOUNT",
        "SANCTION_DATE",
        "SANCTION_AMOUNT",
        "WORK_STAGE",
        "LETTER_NO",
        "FLAG",
        "FILE_STATUS",
        "ATTACH_ID",
    ],
    "works_sanctioned": [
        "ACTIVITY_NAME",
        "WORK_CATEGORY",
        "WORK_DESCRIPTION",
        "RECOMMENDATION_DATE",
        "SANCTION_DATE",
        "SANCTION_AMOUNT",
        "WORK_STAGE",
        "FILE_STATUS",
        "ATTACH_ID",
    ],
    "works_completed": [
        "ACTUAL_END_DATE",
        "ACTUAL_AMOUNT",
    ],
    "expenditure": [],  # append-only, no content hash
    "calamity": [
        "TYPE",
        "CONSENTED_AMOUNT",
    ],
    # MLA datasets use the same content fields
    "mla_allocated_limit": [
        "ALLOCATED_AMT",
        "HOUSE_OF_PARLIAMENT",
        "TENURE_START_DATE",
        "TENURE_END_DATE",
    ],
    "mla_works_recommended": [
        "ACTIVITY_NAME",
        "WORK_CATEGORY",
        "WORK_DESCRIPTION",
        "RECOMMENDATION_DATE",
        "RECOMMENDED_AMOUNT",
        "SANCTION_DATE",
        "SANCTION_AMOUNT",
        "WORK_STAGE",
        "LETTER_NO",
        "FLAG",
        "FILE_STATUS",
        "ATTACH_ID",
    ],
    "mla_works_sanctioned": [
        "ACTIVITY_NAME",
        "WORK_CATEGORY",
        "WORK_DESCRIPTION",
        "RECOMMENDATION_DATE",
        "SANCTION_DATE",
        "SANCTION_AMOUNT",
        "WORK_STAGE",
        "FILE_STATUS",
        "ATTACH_ID",
    ],
    "mla_works_completed": [
        "ACTUAL_END_DATE",
        "ACTUAL_AMOUNT",
    ],
    "mla_expenditure": [],  # append-only, no content hash
    "mla_calamity": [
        "TYPE",
        "CONSENTED_AMOUNT",
    ],
}

DATE_FIELDS = {
    "RECOMMENDATION_DATE",
    "SANCTION_DATE",
    "ACTUAL_END_DATE",
    "EXPENDITURE_DATE",
    "CRT_DT",
    "TENURE_START_DATE",
    "TENURE_END_DATE",
}

NUMBER_FIELDS = {
    "ALLOCATED_AMT",
    "RECOMMENDED_AMOUNT",
    "SANCTION_AMOUNT",
    "FUND_DISBURSED_AMT",
    "CONSENTED_AMOUNT",
    "ACTUAL_AMOUNT",
    "Sno",
    "HOUSE_OF_PARLIAMENT",
    "WORK_RECOMMENDATION_DTL_ID",
    "WORK_ID",
    "VENDOR_ID",
    "CONSTITUENCY_ID",
    "ATTACH_ID",
    "FLAG",
}

MONTHS = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12",
}



def normalize_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    v = str(value).replace("\t", " ")
    v = re.sub(r"\s+", " ", v).strip()
    return v if v else None


def normalize_number(value: Any) -> Optional[str]:
    if value is None or value == "":
        return None
    try:
        d = Decimal(str(value))
        s = format(d, "f")
        if "." in s:
            s = s.rstrip("0").rstrip(".")
        return s or "0"
    except (InvalidOperation, ValueError):
        return normalize_text(value)


def normalize_date(value: Any) -> Optional[str]:
    if value is None or value == "":
        return None
    raw = str(value).strip()

    # Already YYYY-MM-DD
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        return raw

    # DD-Mon-YYYY (e.g. 08-Jul-2024)
    match = re.fullmatch(r"(\d{1,2})-([A-Za-z]{3})-(\d{4})", raw)
    if match:
        month = MONTHS.get(match.group(2).lower())
        if month:
            return f"{match.group(3)}-{month}-{int(match.group(1)):02d}"

    # "Mon DD, YYYY ..." (e.g. "Jun 4, 2024 12:00:00 AM")
    match = re.fullmatch(r"([A-Za-z]{3})\s+(\d{1,2}),\s+(\d{4}).*", raw)
    if match:
        month = MONTHS.get(match.group(1).lower())
        if month:
            return f"{match.group(3)}-{month}-{int(match.group(2)):02d}"

    # Fallback: try generic parsing
    return normalize_text(raw)


def normalize_value(field: str, value: Any) -> Any:
    if field in DATE_FIELDS:
        return normalize_date(value)
    if field in NUMBER_FIELDS:
        return normalize_number(value)
    if isinstance(value, bool):
        return value
    return normalize_text(value)


def is_total_row(record: Dict[str, Any]) -> bool:
    return "Total_Amt" in record



def identity_key(dataset: str, record: Dict[str, Any]) -> Optional[str]:
    values = []
    for field in IDENTITY_FIELDS[dataset]:
        v = normalize_value(field, record.get(field))
        if v is None:
            return None
        values.append(v)
    return json.dumps(values, ensure_ascii=False, separators=(",", ":"))



def content_hash(dataset: str, record: Dict[str, Any]) -> str:
    fields = CONTENT_FIELDS[dataset]
    if not fields:
        return ""
    meaningful = {
        field: normalize_value(field, record.get(field))
        for field in fields
    }
    payload = json.dumps(
        meaningful, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()



def fingerprint_part(value: Any) -> str:
    if value is None:
        return "<NULL>"
    return str(value)


def fingerprint_amount(value: Any) -> str:
    if value is None:
        return "<NULL>"
    try:
        n = float(value)
        return f"{n:.2f}".rstrip("0").rstrip(".")
    except (ValueError, TypeError):
        return "<NULL>"


def expenditure_fingerprint(record: Dict[str, Any]) -> str:
    """
    SHA-256 fingerprint from 9 raw source fields.
    Matches the injector's expenditureFingerprint() exactly.
    """
    canonical = "|".join([
        fingerprint_part(normalize_number(record.get("WORK_RECOMMENDATION_DTL_ID"))),
        fingerprint_part(normalize_text(record.get("WORK_ID"))),
        fingerprint_part(normalize_date(record.get("EXPENDITURE_DATE"))),
        fingerprint_part(normalize_text(record.get("VENDOR_NAME"))),
        fingerprint_part(normalize_text(record.get("WORK_STATUS"))),
        fingerprint_amount(normalize_number(record.get("FUND_DISBURSED_AMT"))),
        fingerprint_part(normalize_text(record.get("STATE_NAME"))),
        fingerprint_part(normalize_text(record.get("CONSTITUENCY"))),
        fingerprint_part(normalize_text(record.get("MP_NAME"))),
    ])
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()



def list_complete_snapshots() -> List[str]:
    """List all timestamps that have _COMPLETE.json with status 'complete'."""
    top_entries = _list_folder("")
    timestamps = []
    for entry in top_entries:
        name = entry.get("name", "")
        if name and not name.startswith("."):
            timestamps.append(name)

    complete = []
    for ts in timestamps:
        try:
            data = supabase.storage.from_(BUCKET).download(f"{ts}/_COMPLETE.json")
            marker = json.loads(data)
            if isinstance(marker, dict) and marker.get("status") == "complete":
                complete.append(ts)
        except (json.JSONDecodeError, ValueError):
            continue
        except Exception:
            continue

    complete.sort()
    return complete


def _list_folder(path: str, limit: int = 1000) -> List[Dict[str, Any]]:
    """
    List entries in a Supabase Storage folder with pagination.
    Returns all entries up to `limit`.
    """
    offset = 0
    all_entries: List[Dict[str, Any]] = []
    while True:
        try:
            batch = supabase.storage.from_(BUCKET).list(
                path, {"limit": limit, "offset": offset}
            )
        except Exception as exc:
            raise RuntimeError(f"Failed to list {path}: {exc}")
        if not batch:
            break
        all_entries.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
    return all_entries


def download_snapshot_files(timestamp: str) -> Dict[str, Dict[str, Any]]:
    """
    Download all NDJSON files for a snapshot into local temp directory.
    Returns {dataset: {local_dir, file_count}}.

    Validates against _COMPLETE.json:
    - Every dataset listed in the marker must exist with part files,
      unless the marker explicitly records 0 records for that dataset.
    - Hard fails if marker says records > 0 but no part files found.
    """
    tmpdir = Path(tempfile.mkdtemp(prefix=f"mplads_{timestamp}_"))

    # ----------------------------------------------------------
    # 1. Read completion marker
    # ----------------------------------------------------------
    marker_path = f"{timestamp}/_COMPLETE.json"
    try:
        marker_data = supabase.storage.from_(BUCKET).download(marker_path)
    except Exception as exc:
        raise RuntimeError(
            f"FATAL: Cannot download completion marker {marker_path}: {exc}"
        )

    try:
        marker = json.loads(marker_data)
    except (json.JSONDecodeError, ValueError) as exc:
        raise RuntimeError(
            f"FATAL: Completion marker {marker_path} is malformed JSON: {exc}"
        )

    if not isinstance(marker, dict):
        raise RuntimeError(
            f"FATAL: Completion marker {marker_path} is not a JSON object."
        )

    if marker.get("status") != "complete":
        raise RuntimeError(
            f"FATAL: Completion marker {marker_path} has status "
            f"{marker.get('status')!r} (expected 'complete')."
        )

    marker_datasets = marker.get("datasets", {})

    # ----------------------------------------------------------
    # 2. List immediate children of <timestamp>/
    #    These are dataset folder names (e.g. "allocated_limit").
    # ----------------------------------------------------------
    top_entries = _list_folder(f"{timestamp}/")
    found_folders = {
        e.get("name", "") for e in top_entries if e.get("name")
    }

    # ----------------------------------------------------------
    # 3. For each expected dataset, list its contents
    #    and collect part_*.ndjson files.
    # ----------------------------------------------------------
    result: Dict[str, Dict[str, Any]] = {}

    for dataset in DATASETS:
        expected_records = marker_datasets.get(dataset, {}).get("records", None)

        # Dataset folder must exist
        if dataset not in found_folders:
            if expected_records is not None and expected_records > 0:
                raise RuntimeError(
                    f"FATAL: Dataset '{dataset}' folder not found in "
                    f"snapshot {timestamp}, but completion marker says "
                    f"{expected_records} records."
                )
            # Zero-record dataset is acceptable
            continue

        # List contents of <timestamp>/<dataset>/
        dataset_entries = _list_folder(f"{timestamp}/{dataset}/")
        part_files = sorted(
            e.get("name", "")
            for e in dataset_entries
            if e.get("name", "").startswith("part_")
            and e.get("name", "").endswith(".ndjson")
        )

        if not part_files:
            if expected_records is not None and expected_records > 0:
                raise RuntimeError(
                    f"FATAL: Dataset '{dataset}' has no part_*.ndjson files "
                    f"in snapshot {timestamp}, but completion marker says "
                    f"{expected_records} records."
                )
            # Zero-record dataset with no files is acceptable
            continue

        # Download all part files
        local_dir = tmpdir / dataset
        local_dir.mkdir(parents=True, exist_ok=True)

        for filename in part_files:
            cloud_path = f"{timestamp}/{dataset}/{filename}"
            local_path = local_dir / filename
            last_exc = None
            for attempt in range(1, DOWNLOAD_RETRIES + 1):
                try:
                    data = supabase.storage.from_(BUCKET).download(cloud_path)
                    local_path.write_bytes(data)
                    last_exc = None
                    break
                except Exception as exc:
                    last_exc = exc
                    if attempt < DOWNLOAD_RETRIES:
                        wait = DOWNLOAD_BACKOFF * attempt
                        print(f"    Download {cloud_path} attempt {attempt}/{DOWNLOAD_RETRIES} failed: {exc}")
                        print(f"    Retrying in {wait}s...")
                        time.sleep(wait)
            if last_exc is not None:
                raise RuntimeError(
                    f"Failed to download {cloud_path} after {DOWNLOAD_RETRIES} attempts: {last_exc}"
                )

        result[dataset] = {
            "local_dir": local_dir,
            "file_count": len(part_files),
        }

    # ----------------------------------------------------------
    # 4. Final validation: every dataset with records > 0 must
    #    have been downloaded.
    # ----------------------------------------------------------
    for dataset in DATASETS:
        expected_records = marker_datasets.get(dataset, {}).get("records", None)
        if expected_records is not None and expected_records > 0:
            if dataset not in result:
                raise RuntimeError(
                    f"FATAL: Dataset '{dataset}' has {expected_records} "
                    f"records per completion marker but no part files "
                    f"were downloaded from snapshot {timestamp}."
                )

    return result


def load_local_snapshot_files(snapshot_dir: Path) -> Dict[str, Dict[str, Any]]:
    """
    Validate and load a local snapshot directory as the new snapshot.

    The directory is expected to follow the same structure as a Supabase
    snapshot (as produced by the government fetcher):

        <snapshot_dir>/
            allocated_limit/
                part_0001.ndjson
                part_0002.ndjson
            works_recommended/
                part_0001.ndjson
            ...

    Returns {dataset: {local_dir, file_count}} for each dataset that has
    part files, matching the format returned by download_snapshot_files().

    Validates that:
    - The path exists and is a directory.
    - At least one expected dataset folder contains part_*.ndjson files.
    - No dataset folder exists without valid part_*.ndjson files (structural
      integrity check).
    """
    if not snapshot_dir.exists():
        raise RuntimeError(
            f"FATAL: Local snapshot path does not exist: {snapshot_dir}"
        )
    if not snapshot_dir.is_dir():
        raise RuntimeError(
            f"FATAL: Local snapshot path is not a directory: {snapshot_dir}"
        )

    result: Dict[str, Dict[str, Any]] = {}

    for dataset in DATASETS:
        dataset_dir = snapshot_dir / dataset
        if not dataset_dir.is_dir():
            continue

        part_files = sorted(
            f.name for f in dataset_dir.iterdir()
            if f.name.startswith("part_") and f.name.endswith(".ndjson")
        )

        if not part_files:
            raise RuntimeError(
                f"FATAL: Dataset folder '{dataset}' exists in local snapshot "
                f"at {snapshot_dir} but contains no part_*.ndjson files."
            )

        result[dataset] = {
            "local_dir": dataset_dir,
            "file_count": len(part_files),
        }

    if not result:
        raise RuntimeError(
            f"FATAL: Local snapshot at {snapshot_dir} contains no valid "
            f"dataset folders with part_*.ndjson files. "
            f"Expected at least one of: {DATASETS}"
        )

    return result



def iter_records(folder: Path) -> Iterator[Dict[str, Any]]:
    """
    Iterate all records across part_*.ndjson files in a folder.
    Raises ValueError on malformed JSON, missing identity fields,
    or non-dict records.
    """
    files = sorted(folder.glob("part_*.ndjson"))
    if not files:
        return
    for fp in files:
        with fp.open("r", encoding="utf-8") as fh:
            for line_num, line in enumerate(fh, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(
                        f"Malformed JSON in {fp.name}:{line_num}: {exc}\n"
                        f"  Line: {line[:200]}"
                    ) from exc
                if not isinstance(record, dict):
                    raise ValueError(
                        f"Expected JSON object in {fp.name}:{line_num}, "
                        f"got {type(record).__name__}"
                    )
                yield record



class ChunkWriter:
    """
    Writes records directly into chunked NDJSON files.
    No temporary append.ndjson/update.ndjson files.
    Final output: append_part_0001.ndjson, update_part_0001.ndjson, etc.
    """

    def __init__(self, output_dir: Path, op: str, chunk_size: int = DELTA_CHUNK_SIZE):
        self.output_dir = output_dir
        self.op = op
        self.chunk_size = chunk_size
        self.chunk_num = 0
        self.records_in_chunk = 0
        self.chunk_file = None
        self.total_records = 0

    def write(self, record: Dict[str, Any]) -> None:
        if self.records_in_chunk == 0:
            self.chunk_num += 1
            filename = f"{self.op}_part_{self.chunk_num:04d}.ndjson"
            self.chunk_file = (self.output_dir / filename).open("w", encoding="utf-8")

        line = json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
        self.chunk_file.write(line)
        self.records_in_chunk += 1
        self.total_records += 1

        if self.records_in_chunk >= self.chunk_size:
            self.chunk_file.close()
            self.chunk_file = None
            self.records_in_chunk = 0

    def close(self) -> None:
        if self.chunk_file and not self.chunk_file.closed:
            self.chunk_file.close()
            self.chunk_file = None

    def finalize(self) -> int:
        self.close()
        return self.chunk_num



def create_table_sql(dataset: str) -> str:
    if dataset in ("expenditure", "mla_expenditure"):
        table_name = dataset.replace("-", "_")
        return f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                fingerprint TEXT PRIMARY KEY,
                raw_json TEXT NOT NULL
            )
        """
    else:
        return """
            CREATE TABLE IF NOT EXISTS records (
                identity TEXT PRIMARY KEY,
                content_hash TEXT NOT NULL,
                raw_json TEXT NOT NULL
            )
        """


def load_snapshot_to_sqlite(
    dataset: str,
    folder: Path,
    conn: sqlite3.Connection,
) -> Dict[str, int]:
    """
    Load a snapshot into SQLite.
    For non-expenditure: hard fails if same identity has different content_hash,
    or if identity fields are missing (except empty calamity rows).
    For expenditure: hard fails if same fingerprint has different raw content.
    Returns stats dict.
    """
    conn.execute(create_table_sql(dataset))
    conn.commit()

    total = 0
    skipped = 0
    safe_dedup = 0

    if dataset in ("expenditure", "mla_expenditure"):
        for record in iter_records(folder):
            total += 1
            if is_total_row(record):
                continue
            fp = expenditure_fingerprint(record)
            raw = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
            existing = conn.execute(
                f"SELECT raw_json FROM {dataset} WHERE fingerprint = ?", (fp,)
            ).fetchone()
            if existing is None:
                conn.execute(
                    f"INSERT INTO {dataset} (fingerprint, raw_json) VALUES (?, ?)",
                    (fp, raw),
                )
            else:
                # Same fingerprint = same 9 identity fields.
                # Compare after excluding Sno and VENDOR_ID (unstable metadata).
                existing_rec = json.loads(existing[0])
                new_rec = json.loads(raw)
                _skip = {"Sno", "VENDOR_ID"}
                existing_no_meta = {k: v for k, v in existing_rec.items() if k not in _skip}
                new_no_meta = {k: v for k, v in new_rec.items() if k not in _skip}

                if existing_no_meta == new_no_meta:
                    # Only Sno/VENDOR_ID differ — safe duplicate
                    safe_dedup += 1
                else:
                    # A field other than Sno/VENDOR_ID differs — hard fail
                    conn.close()
                    fp_fields = [
                        "WORK_RECOMMENDATION_DTL_ID", "WORK_ID",
                        "EXPENDITURE_DATE", "VENDOR_NAME", "WORK_STATUS",
                        "FUND_DISBURSED_AMT", "STATE_NAME", "CONSTITUENCY",
                        "MP_NAME",
                    ]

                    all_keys = sorted(set(existing_rec) | set(new_rec))
                    diffs = []
                    only_existing = []
                    only_new = []
                    for k in all_keys:
                        ev = existing_rec.get(k, "<MISSING>")
                        nv = new_rec.get(k, "<MISSING>")
                        if k not in existing_rec:
                            only_existing.append(k)
                        elif k not in new_rec:
                            only_new.append(k)
                        elif ev != nv:
                            diffs.append((k, ev, nv))

                    lines = [
                        f"CONFLICT in {dataset}: same fingerprint but different content.",
                        f"Fingerprint: {fp}",
                        "",
                        "--- Fingerprint source fields (existing) ---",
                    ]
                    for f in fp_fields:
                        lines.append(f"  {f}: {existing_rec.get(f)}")
                    lines.append("")
                    lines.append("--- Fingerprint source fields (new) ---")
                    for f in fp_fields:
                        lines.append(f"  {f}: {new_rec.get(f)}")
                    lines.append("")
                    if only_existing:
                        lines.append(f"Fields only in existing: {only_existing}")
                    if only_new:
                        lines.append(f"Fields only in new: {only_new}")
                    if diffs:
                        lines.append("")
                        lines.append(f"Differing fields ({len(diffs)}):")
                        for k, ev, nv in diffs:
                            lines.append(f"  {k}:")
                            lines.append(f"    Existing: {ev}")
                            lines.append(f"    New:      {nv}")
                    else:
                        lines.append("No field-level diffs found (possible key ordering difference).")

                    raise ValueError("\n".join(lines))
                safe_dedup += 1
        conn.commit()
    else:
        for record in iter_records(folder):
            total += 1
            if is_total_row(record):
                continue
            ident = identity_key(dataset, record)
            if ident is None:
                # For calamity: skip only when ALL THREE identity fields
                # (MP_NAME, CRT_DT, CALAMITY_NAME) are null/blank.
                # If one or two are present and others missing, hard-fail.
                if dataset == "calamity":
                    missing = [
                        f for f in IDENTITY_FIELDS["calamity"]
                        if record.get(f) is None
                        or (isinstance(record.get(f), str) and record.get(f).strip() == "")
                    ]
                    if len(missing) < len(IDENTITY_FIELDS["calamity"]):
                        # At least one identity field present → partial record
                        raise ValueError(
                            f"MISSING IDENTITY in calamity: record has partial "
                            f"identity fields (missing {missing}).\n"
                            f"  Record: {json.dumps(record, ensure_ascii=False)[:300]}"
                        )
                    # All three identity fields null/blank → legitimately empty
                    skipped += 1
                    continue
                # For all other datasets: hard fail on missing identity fields.
                missing = [
                    f for f in IDENTITY_FIELDS[dataset]
                    if record.get(f) is None
                    or (isinstance(record.get(f), str) and record.get(f).strip() == "")
                ]
                raise ValueError(
                    f"MISSING IDENTITY in {dataset}: record is missing "
                    f"required identity fields {missing}.\n"
                    f"  Record: {json.dumps(record, ensure_ascii=False)[:300]}"
                )
            ch = content_hash(dataset, record)
            raw = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
            existing = conn.execute(
                "SELECT content_hash, raw_json FROM records WHERE identity = ?",
                (ident,),
            ).fetchone()
            if existing is None:
                conn.execute(
                    "INSERT INTO records (identity, content_hash, raw_json) VALUES (?, ?, ?)",
                    (ident, ch, raw),
                )
            else:
                existing_hash = existing[0]
                if existing_hash != ch:
                    conn.close()
                    raise ValueError(
                        f"CONFLICT in {dataset}: identity {ident} "
                        f"has different content_hash.\n"
                        f"  Existing hash: {existing_hash}\n"
                        f"  New hash:      {ch}\n"
                        f"  Existing raw:  {existing[1][:300]}\n"
                        f"  New raw:       {raw[:300]}"
                    )
                safe_dedup += 1
        conn.commit()

    return {
        "total": total,
        "skipped": skipped,
        "safe_dedup": safe_dedup,
    }



def quick_compare(
    old_dir: Path,
    new_dir: Path,
) -> Dict[str, Any]:
    """
    Fast file-level comparison before SQLite loading.

    Compares dataset structure, file sizes, and SHA-256 hashes.
    Returns {"changed": bool, "changed_datasets": list, "stats": dict}.

    If all files match, returns changed=False without loading SQLite.
    If any file differs, returns changed=True with the list of affected datasets.
    Falls back safely on any error (returns changed=True to trigger full comparison).
    """
    result = {"changed": False, "changed_datasets": [], "stats": {}}

    for dataset in DATASETS:
        old_ds = old_dir / dataset
        new_ds = new_dir / dataset

        has_old = old_ds.is_dir()
        has_new = new_ds.is_dir()

        if not has_new:
            continue

        if not has_old:
            result["changed"] = True
            result["changed_datasets"].append(dataset)
            result["stats"][dataset] = {"reason": "new_dataset"}
            continue

        old_files = sorted(
            f.name for f in old_ds.iterdir()
            if f.name.startswith("part_") and f.name.endswith(".ndjson")
        )
        new_files = sorted(
            f.name for f in new_ds.iterdir()
            if f.name.startswith("part_") and f.name.endswith(".ndjson")
        )

        if old_files != new_files:
            result["changed"] = True
            result["changed_datasets"].append(dataset)
            result["stats"][dataset] = {"reason": "file_list_diff"}
            continue

        for fname in old_files:
            old_path = old_ds / fname
            new_path = new_ds / fname

            if old_path.stat().st_size != new_path.stat().st_size:
                result["changed"] = True
                result["changed_datasets"].append(dataset)
                result["stats"][dataset] = {"reason": "size_diff", "file": fname}
                break

            old_hash = hashlib.sha256(old_path.read_bytes()).hexdigest()
            new_hash = hashlib.sha256(new_path.read_bytes()).hexdigest()

            if old_hash != new_hash:
                result["changed"] = True
                result["changed_datasets"].append(dataset)
                result["stats"][dataset] = {"reason": "hash_diff", "file": fname}
                break
        else:
            result["stats"][dataset] = {"reason": "identical", "files": len(old_files)}

    return result



def compare_dataset(
    dataset: str,
    old_conn: sqlite3.Connection,
    new_conn: sqlite3.Connection,
    output_dir: Path,
) -> Dict[str, Any]:
    """Compare two SQLite databases for a dataset. Write chunked delta files."""
    output_dir.mkdir(parents=True, exist_ok=True)

    append_writer = ChunkWriter(output_dir, "append")
    update_writer = ChunkWriter(output_dir, "update")

    append_count = 0
    update_count = 0
    unchanged_count = 0

    if dataset in ("expenditure", "mla_expenditure"):
        old_fps = {
            r[0] for r in old_conn.execute(f"SELECT fingerprint FROM {dataset}")
        }
        new_rows = new_conn.execute(
            f"SELECT fingerprint, raw_json FROM {dataset}"
        ).fetchall()

        new_fps = set()
        for fp, raw in new_rows:
            new_fps.add(fp)
            if fp not in old_fps:
                record = json.loads(raw)
                append_writer.write(record)
                append_count += 1

        removed_groups = len(old_fps - new_fps)

        append_writer.finalize()
        update_writer.finalize()

        return {
            "previous_records": len(old_fps),
            "new_records": len(new_fps),
            "append": append_count,
            "update": 0,
            "unchanged": 0,
            "removed": removed_groups,
            "previous_skipped": 0,
            "new_skipped": 0,
            "previous_safe_dedup": 0,
            "new_safe_dedup": 0,
            "append_chunks": append_writer.chunk_num,
            "update_chunks": 0,
        }

    else:
        old_rows = {
            r[0]: r[1]
            for r in old_conn.execute("SELECT identity, content_hash FROM records")
        }
        new_rows = new_conn.execute(
            "SELECT identity, content_hash, raw_json FROM records"
        ).fetchall()

        seen = set()
        for ident, ch, raw in new_rows:
            seen.add(ident)
            if ident not in old_rows:
                record = json.loads(raw)
                append_writer.write(record)
                append_count += 1
            elif old_rows[ident] != ch:
                record = json.loads(raw)
                update_writer.write(record)
                update_count += 1
            else:
                unchanged_count += 1

        removed = len(set(old_rows) - seen)

        append_chunks = append_writer.finalize()
        update_chunks = update_writer.finalize()

        return {
            "previous_logical": len(old_rows),
            "new_logical": len(seen),
            "append": append_count,
            "update": update_count,
            "unchanged": unchanged_count,
            "removed": removed,
            "previous_skipped": 0,
            "new_skipped": 0,
            "previous_safe_dedup": 0,
            "new_safe_dedup": 0,
            "append_chunks": append_chunks,
            "update_chunks": update_chunks,
        }



def generate_manifest(
    old_timestamp: Optional[str],
    new_timestamp: str,
    results: Dict[str, Dict[str, Any]],
    delta_dir: Path,
) -> Dict[str, Any]:
    total_append = sum(r.get("append", 0) for r in results.values())
    total_update = sum(r.get("update", 0) for r in results.values())
    total_unchanged = sum(r.get("unchanged", 0) for r in results.values())

    manifest = {
        "old_snapshot": old_timestamp,
        "new_snapshot": new_timestamp,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "delta_dir": str(delta_dir),
        "summary": {
            "total_append": total_append,
            "total_update": total_update,
            "total_unchanged": total_unchanged,
            "is_bootstrap": old_timestamp is None,
        },
        "datasets": results,
    }

    return manifest



def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare two MPLADS snapshots from Supabase Storage."
    )
    parser.add_argument("--old", type=str, default=None, help="Previous snapshot timestamp")
    parser.add_argument("--new", type=str, default=None, help="New snapshot timestamp")
    parser.add_argument("--new-local", type=str, default=None,
                        help="Path to local directory containing the new snapshot "
                             "(avoids downloading from Supabase; --new still required "
                             "for timestamp label)")
    parser.add_argument("--old-local", type=str, default=None,
                        help="Path to local directory containing the old snapshot "
                             "(avoids downloading from Supabase)")
    parser.add_argument("--output", type=str, default=None, help="Output directory for delta files")
    args = parser.parse_args()

    print("MPLADS Snapshot Comparator v3")
    print("=" * 60)

    # ----------------------------------------------------------
    # Discover snapshots
    # ----------------------------------------------------------

    if args.new:
        new_timestamp = args.new
    else:
        print("\nDiscovering complete snapshots in Supabase Storage...")
        snapshots = list_complete_snapshots()
        if not snapshots:
            print("ERROR: No complete snapshots found.")
            return 1
        print(f"Found {len(snapshots)} complete snapshots: {snapshots}")
        new_timestamp = snapshots[-1]

    if args.old:
        old_timestamp = args.old
    else:
        print("\nDiscovering previous snapshot...")
        snapshots = list_complete_snapshots()
        old_timestamp = None
        for ts in reversed(snapshots):
            if ts != new_timestamp:
                old_timestamp = ts
                break

    if old_timestamp is None:
        print(f"\nNo previous snapshot found. Bootstrap mode for {new_timestamp}.")
    else:
        print(f"\nComparing: {old_timestamp} -> {new_timestamp}")

    # ----------------------------------------------------------
    # Determine output directory
    # ----------------------------------------------------------

    if args.output:
        delta_dir = Path(args.output)
    else:
        delta_dir = Path(f"./delta_{new_timestamp}")

    delta_dir.mkdir(parents=True, exist_ok=True)
    print(f"Output: {delta_dir}")

    # ----------------------------------------------------------
    # Download snapshots
    # ----------------------------------------------------------

    if args.new_local and not args.new:
        parser.error("--new is required when --new-local is supplied (used for timestamp label)")

    if args.new_local:
        print(f"\nUsing local snapshot: {args.new_local}")
        new_files = load_local_snapshot_files(Path(args.new_local))
        print(f"  Datasets: {list(new_files.keys())}")
    else:
        print(f"\nDownloading new snapshot {new_timestamp}...")
        new_files = download_snapshot_files(new_timestamp)
        print(f"  Datasets: {list(new_files.keys())}")

    old_files = {}
    if old_timestamp:
        if args.old_local:
            print(f"\nUsing local old snapshot: {args.old_local}")
            old_files = load_local_snapshot_files(Path(args.old_local))
            print(f"  Datasets: {list(old_files.keys())}")
        else:
            print(f"\nDownloading old snapshot {old_timestamp}...")
            old_files = download_snapshot_files(old_timestamp)
            print(f"  Datasets: {list(old_files.keys())}")

    # ----------------------------------------------------------
    # Quick comparison (file-level hash optimization)
    # ----------------------------------------------------------

    if args.old_local and args.new_local:
        print("\n--- Quick comparison (file-level hashes) ---")
        qc = quick_compare(Path(args.old_local), Path(args.new_local))
        if not qc["changed"]:
            print("Quick comparison: ALL files identical — 0 changes")
            manifest = generate_manifest(old_timestamp, new_timestamp, {}, delta_dir)
            manifest_path = delta_dir / "manifest.json"
            with manifest_path.open("w", encoding="utf-8") as fh:
                json.dump(manifest, fh, ensure_ascii=False, indent=2)
                fh.write("\n")
            print(f"\nManifest: {manifest_path}")
            print(f"\n{'=' * 60}")
            print("COMPARISON COMPLETE (quick path)")
            print(f"{'=' * 60}")
            print(f"Total append: 0")
            print(f"Total update: 0")
            print(f"Total unchanged: 0")
            return 0
        else:
            print(f"Quick comparison: changes detected in {len(qc['changed_datasets'])} dataset(s)")
            for ds, stats in qc["stats"].items():
                if stats.get("reason") != "identical":
                    print(f"  {ds}: {stats}")

    # ----------------------------------------------------------
    # Compare each dataset
    # ----------------------------------------------------------

    results = {}
    all_datasets = DATASETS

    for dataset in all_datasets:
        print(f"\n{'=' * 60}")
        print(f"Comparing: {dataset}")
        print(f"{'=' * 60}")

        has_old = dataset in old_files
        has_new = dataset in new_files

        if not has_new:
            print(f"  SKIP: dataset not in new snapshot")
            results[dataset] = {"status": "missing_in_new"}
            continue

        if not has_old:
            print(f"  BOOTSTRAP: dataset not in old snapshot (no delta)")
            results[dataset] = {
                "status": "bootstrap",
                "new_records": new_files[dataset]["file_count"],
            }
            continue

        # Create file-backed SQLite databases (bounded memory)
        old_db_file = tempfile.NamedTemporaryFile(
            suffix=f"_old_{dataset}.db", delete=False
        )
        new_db_file = tempfile.NamedTemporaryFile(
            suffix=f"_new_{dataset}.db", delete=False
        )
        old_db_path = Path(old_db_file.name)
        new_db_path = Path(new_db_file.name)
        old_db_file.close()
        new_db_file.close()
        old_db = sqlite3.connect(str(old_db_path))
        new_db = sqlite3.connect(str(new_db_path))

        try:
            print(f"  Loading old snapshot into SQLite...")
            old_stats = load_snapshot_to_sqlite(
                dataset, old_files[dataset]["local_dir"], old_db
            )
            print(f"  Old: {old_stats['total']} total, {old_stats['skipped']} skipped, {old_stats['safe_dedup']} safe dedup")

            print(f"  Loading new snapshot into SQLite...")
            new_stats = load_snapshot_to_sqlite(
                dataset, new_files[dataset]["local_dir"], new_db
            )
            print(f"  New: {new_stats['total']} total, {new_stats['skipped']} skipped, {new_stats['safe_dedup']} safe dedup")
        except ValueError as exc:
            print(f"\n  FATAL ERROR: {exc}")
            old_db.close()
            new_db.close()
            old_db_path.unlink(missing_ok=True)
            new_db_path.unlink(missing_ok=True)
            return 1

        # Compare
        print(f"  Comparing...")
        try:
            stats = compare_dataset(
                dataset, old_db, new_db, delta_dir / dataset
            )
        except Exception as exc:
            print(f"\n  FATAL ERROR during comparison: {exc}")
            old_db.close()
            new_db.close()
            old_db_path.unlink(missing_ok=True)
            new_db_path.unlink(missing_ok=True)
            return 1

        stats["previous_skipped"] = old_stats["skipped"]
        stats["new_skipped"] = new_stats["skipped"]
        stats["previous_safe_dedup"] = old_stats["safe_dedup"]
        stats["new_safe_dedup"] = new_stats["safe_dedup"]

        results[dataset] = stats

        print(f"  Results:")
        for key, val in stats.items():
            print(f"    {key}: {val}")

        old_db.close()
        new_db.close()
        old_db_path.unlink(missing_ok=True)
        new_db_path.unlink(missing_ok=True)

    # ----------------------------------------------------------
    # Generate manifest
    # ----------------------------------------------------------

    manifest = generate_manifest(old_timestamp, new_timestamp, results, delta_dir)
    manifest_path = delta_dir / "manifest.json"
    with manifest_path.open("w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2)
        fh.write("\n")

    # ----------------------------------------------------------
    # Summary
    # ----------------------------------------------------------

    print(f"\n{'=' * 60}")
    print("COMPARISON COMPLETE")
    print(f"{'=' * 60}")

    total_append = manifest["summary"]["total_append"]
    total_update = manifest["summary"]["total_update"]
    total_unchanged = manifest["summary"]["total_unchanged"]

    print(f"Total append: {total_append}")
    print(f"Total update: {total_update}")
    print(f"Total unchanged: {total_unchanged}")
    print(f"Manifest: {manifest_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
