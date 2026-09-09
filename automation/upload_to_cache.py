#!/usr/bin/env python3
"""Upload a local snapshot directory to GitHub Actions cache and Supabase Storage.

Usage:
    python3 automation/upload_to_cache.py /path/to/snapshot_dir

This reads the timestamp from _COMPLETE.json (not the directory name),
validates the snapshot, and uploads to:
  1. GitHub Actions cache (via local cache directory for next workflow)
  2. Supabase Storage (mplads-raw bucket)
"""
import json
import os
import shutil
import sys
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SECRET_KEY", "")
BUCKET = "mplads-raw"
CACHE_DIR = Path("/tmp/mplads-work")


def read_timestamp(snap_dir: Path) -> str:
    complete = snap_dir / "_COMPLETE.json"
    if not complete.exists():
        raise RuntimeError(f"_COMPLETE.json missing in {snap_dir}")
    data = json.loads(complete.read_text(encoding="utf-8"))
    ts = data.get("timestamp", "")
    if not ts:
        raise RuntimeError(f"timestamp field missing in _COMPLETE.json")
    if data.get("status") != "complete":
        raise RuntimeError(f"_COMPLETE.json status is '{data.get('status')}', expected 'complete'")
    return ts


def stage_for_cache(snap_dir: Path, timestamp: str):
    curr = CACHE_DIR / "current" / timestamp
    if curr.exists():
        print(f"Cache target already exists: {curr}")
        return

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    shutil.copytree(str(snap_dir), str(curr))

    (CACHE_DIR / ".new_timestamp").write_text(timestamp)
    meta = {"previous_ts": "none", "current_ts": timestamp}
    (CACHE_DIR / ".cache-metadata.json").write_text(json.dumps(meta, indent=2))

    complete = curr / "_COMPLETE.json"
    if not complete.exists():
        raise RuntimeError(f"_COMPLETE.json missing in staged copy: {curr}")
    print(f"Staged to cache: {curr}")


def upload_to_supabase(snap_dir: Path, timestamp: str):
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Skipping Supabase upload: credentials not set")
        return

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    storage = sb.storage.from_(BUCKET)

    # Upload _COMPLETE.json
    complete = snap_dir / "_COMPLETE.json"
    storage.upload(
        f"{timestamp}/_COMPLETE.json",
        complete.read_bytes(),
        {"upsert": "true", "content-type": "application/json"},
    )
    print(f"Uploaded _COMPLETE.json to Supabase")

    # Upload ndjson files
    files = [p for p in snap_dir.rglob("*.ndjson")]
    uploaded = 0
    for p in files:
        rel = p.relative_to(snap_dir).as_posix()
        storage.upload(
            f"{timestamp}/{rel}",
            p.read_bytes(),
            {"upsert": "true", "content-type": "application/x-ndjson"},
        )
        uploaded += 1
        if uploaded % 50 == 0:
            print(f"  Uploaded {uploaded}/{len(files)}")

    print(f"Uploaded {uploaded}/{len(files)} ndjson files to Supabase")


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 automation/upload_to_cache.py /path/to/snapshot_dir")
        sys.exit(1)

    snap_dir = Path(sys.argv[1])
    if not snap_dir.is_dir():
        print(f"FATAL: {snap_dir} is not a directory")
        sys.exit(1)

    timestamp = read_timestamp(snap_dir)
    print(f"Snapshot timestamp: {timestamp}")
    print(f"Source: {snap_dir}")

    stage_for_cache(snap_dir, timestamp)
    upload_to_supabase(snap_dir, timestamp)

    print(f"\nDone.")
    print(f"Cache key: mplads-snapshot-state-{timestamp}")
    print(f"Supabase: {BUCKET}/{timestamp}/")


if __name__ == "__main__":
    main()
