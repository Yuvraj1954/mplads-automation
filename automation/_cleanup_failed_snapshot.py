"""Remove a failed snapshot from Supabase Storage.

Used by the GitHub Actions workflow when a pipeline run fails.
Deletes all files under the given timestamp prefix.

Usage:
    python automation/_cleanup_failed_snapshot.py <timestamp>
"""
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env", override=False)

from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SECRET_KEY = os.environ.get("SUPABASE_SECRET_KEY", "")

if not SUPABASE_URL or not SUPABASE_SECRET_KEY:
    print("ERROR: SUPABASE_URL and SUPABASE_SECRET_KEY required", file=sys.stderr)
    sys.exit(1)

BUCKET = "mplads-raw"
DATASETS = [
    "allocated_limit", "works_recommended", "works_sanctioned",
    "works_completed", "expenditure", "calamity",
    "mla_allocated_limit", "mla_works_recommended", "mla_works_sanctioned",
    "mla_works_completed", "mla_expenditure", "mla_calamity",
]


def main():
    if len(sys.argv) < 2:
        print("Usage: python _cleanup_failed_snapshot.py <timestamp>", file=sys.stderr)
        sys.exit(1)

    timestamp = sys.argv[1]
    sb = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)
    storage = sb.storage.from_(BUCKET)
    all_paths = []

    # Collect all files under this snapshot timestamp
    for ds in DATASETS:
        try:
            rows = storage.list(f"{timestamp}/{ds}", {"limit": 1000, "offset": 0})
            for row in rows or []:
                name = row.get("name")
                if name:
                    all_paths.append(f"{timestamp}/{ds}/{name}")
        except Exception:
            pass

    # Also collect root-level files
    try:
        rows = storage.list(timestamp, {"limit": 1000, "offset": 0})
        for row in rows or []:
            name = row.get("name")
            if name and name not in DATASETS:
                all_paths.append(f"{timestamp}/{name}")
    except Exception:
        pass

    # Collect and delete delta files if they exist
    delta_id = f"delta_{timestamp}"
    for ds in DATASETS:
        try:
            rows = storage.list(f"{delta_id}/{ds}", {"limit": 1000, "offset": 0})
            for row in rows or []:
                name = row.get("name")
                if name:
                    all_paths.append(f"{delta_id}/{ds}/{name}")
        except Exception:
            pass
    try:
        rows = storage.list(delta_id, {"limit": 1000, "offset": 0})
        for row in rows or []:
            name = row.get("name")
            if name and name not in DATASETS:
                all_paths.append(f"{delta_id}/{name}")
    except Exception:
        pass

    if all_paths:
        try:
            storage.remove(all_paths)
            print(f"Deleted {len(all_paths)} files for failed snapshot: {timestamp}")
        except Exception as exc:
            print(f"WARNING: Partial cleanup failure: {exc}", file=sys.stderr)
            print(f"Deleted some but not all of {len(all_paths)} files")
    else:
        print(f"No remote files found for snapshot: {timestamp}")


if __name__ == "__main__":
    main()
