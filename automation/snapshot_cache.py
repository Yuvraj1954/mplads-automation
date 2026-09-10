"""
GitHub Actions Rolling Snapshot Cache for MPLADS Pipeline.

Manages previous/current snapshot directories on the runner filesystem.
The GitHub Actions workflow handles cache restore/save via actions/cache.
This module handles validation, rotation, tarballing, and metadata.

Cache layout on disk (after restore):
    $WORK_DIR/
        previous/
            <timestamp>/
                _COMPLETE.json
                manifests/
                allocated_limit/part_*.ndjson
                ...
        current/
            <timestamp>/
                _COMPLETE.json
                manifests/
                allocated_limit/part_*.ndjson
                ...
        .cache-metadata.json
"""

import json
import shutil
import tarfile
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

METADATA_FILE = ".cache-metadata.json"
PREVIOUS_DIR = "previous"
CURRENT_DIR = "current"
TIMESTAMP_FILE = ".new_timestamp"

EXPECTED_PART_FILES = 4  # Minimum expected across all datasets


def _get_work_dir() -> Path:
    """Return the cache work directory from env or default."""
    import os
    return Path(os.environ.get("MPLADS_CACHE_WORK_DIR", "/tmp/mplads-work"))


def _find_snapshot_ts(base: Path) -> Optional[str]:
    """Find the NEWEST timestamp directory inside base/ (e.g., previous/ or current/).

    Returns the newest (lexicographically last) timestamp string or None if not found.
    ISO 8601 timestamps sort chronologically, so last = newest.
    """
    if not base.exists() or not base.is_dir():
        return None
    candidates = [
        d.name for d in sorted(base.iterdir())
        if d.is_dir() and not d.name.startswith(".") and not d.name.startswith("_")
    ]
    if not candidates:
        return None
    if len(candidates) > 1:
        print(f"SNAPSHOT CACHE: Found {len(candidates)} snapshots in {base.name}/: {candidates}")
        print(f"SNAPSHOT CACHE: Selecting newest: {candidates[-1]}")
    return candidates[-1]


def _cleanup_stale_snapshots(base: Path, keep_ts: str) -> int:
    """Remove all snapshot directories in base/ except keep_ts.

    Returns the number of directories removed.
    """
    if not base.exists() or not base.is_dir():
        return 0
    removed = 0
    for d in sorted(base.iterdir()):
        if d.is_dir() and not d.name.startswith(".") and not d.name.startswith("_"):
            if d.name != keep_ts:
                print(f"SNAPSHOT CACHE: Removing stale snapshot: {base.name}/{d.name}")
                shutil.rmtree(d)
                removed += 1
    return removed


def validate_snapshot_structure(snapshot_dir: Path) -> Tuple[bool, str]:
    """Validate that a snapshot directory has the expected structure.

    Checks for:
    - _COMPLETE.json with status 'complete'
    - At least one dataset folder with part_*.ndjson files
    - manifests/ directory

    Returns (is_valid, error_message).
    """
    if not snapshot_dir.exists():
        return False, f"Snapshot directory does not exist: {snapshot_dir}"
    if not snapshot_dir.is_dir():
        return False, f"Snapshot path is not a directory: {snapshot_dir}"

    # Check _COMPLETE.json
    complete_file = snapshot_dir / "_COMPLETE.json"
    if not complete_file.exists():
        return False, f"Missing _COMPLETE.json in {snapshot_dir}"
    try:
        data = json.loads(complete_file.read_text(encoding="utf-8"))
        if data.get("status") != "complete":
            return False, f"_COMPLETE.json status is not 'complete': {data.get('status')}"
    except (json.JSONDecodeError, ValueError) as exc:
        return False, f"Invalid _COMPLETE.json: {exc}"

    # Check for datasets with part files
    total_parts = 0
    for child in snapshot_dir.iterdir():
        if child.is_dir() and not child.name.startswith(".") and child.name != "manifests":
            parts = list(child.glob("part_*.ndjson"))
            total_parts += len(parts)

    if total_parts < EXPECTED_PART_FILES:
        return False, (
            f"Expected at least {EXPECTED_PART_FILES} part_*.ndjson files, "
            f"found {total_parts}"
        )

    return True, ""


def _find_valid_snapshot_ts(base: Path) -> Optional[str]:
    """Find the newest valid snapshot timestamp in base/.

    Iterates from newest to oldest, returning the first that passes
    validate_snapshot_structure. Cleans up any stale invalid snapshots found.
    """
    if not base.exists() or not base.is_dir():
        return None
    candidates = sorted(
        [
            d.name for d in base.iterdir()
            if d.is_dir() and not d.name.startswith(".") and not d.name.startswith("_")
        ],
        reverse=True,
    )
    for ts in candidates:
        ok, err = validate_snapshot_structure(base / ts)
        if ok:
            # Clean up any older stale snapshots
            _cleanup_stale_snapshots(base, ts)
            return ts
        print(f"SNAPSHOT CACHE: {base.name}/{ts} invalid: {err}")
    return None


def validate_cache(work_dir: Optional[Path] = None) -> Tuple[bool, str]:
    """Validate that the cache contains both previous and current snapshots.

    Selects the NEWEST valid snapshot for previous (not the oldest).
    Falls back to older snapshots if the newest is invalid.

    Returns (is_valid, error_message).
    """
    work_dir = work_dir or _get_work_dir()

    if not work_dir.exists():
        return False, f"Cache work directory does not exist: {work_dir}"

    # Find newest valid previous snapshot (tries newest first, falls back)
    prev_ts = _find_valid_snapshot_ts(work_dir / PREVIOUS_DIR)
    if not prev_ts:
        return False, "No valid previous snapshot found in cache"

    # Find newest valid current snapshot
    curr_ts = _find_valid_snapshot_ts(work_dir / CURRENT_DIR)
    if not curr_ts:
        return False, "No valid current snapshot found in cache"

    return True, ""


def validate_bootstrap_cache(work_dir: Optional[Path] = None) -> Tuple[bool, str]:
    """Validate that the cache has a valid current snapshot for bootstrap.

    Unlike validate_cache(), this does NOT require a valid previous snapshot.
    Bootstrap only needs the current snapshot because it processes the full
    snapshot without comparing against a previous one.

    Returns (is_valid, error_message).
    """
    work_dir = work_dir or _get_work_dir()

    if not work_dir.exists():
        return False, f"Cache work directory does not exist: {work_dir}"

    # Find current snapshot
    curr_ts = _find_snapshot_ts(work_dir / CURRENT_DIR)
    if not curr_ts:
        return False, "No current snapshot found in cache"

    curr_dir = work_dir / CURRENT_DIR / curr_ts
    ok, err = validate_snapshot_structure(curr_dir)
    if not ok:
        return False, f"Current snapshot invalid: {err}"

    return True, ""


def get_previous_local_path(work_dir: Optional[Path] = None) -> Optional[Path]:
    """Return the local path to the newest valid previous snapshot directory.

    Returns None if no valid previous snapshot exists.
    """
    work_dir = work_dir or _get_work_dir()
    prev_ts = _find_valid_snapshot_ts(work_dir / PREVIOUS_DIR)
    if not prev_ts:
        return None
    return work_dir / PREVIOUS_DIR / prev_ts


def get_current_local_path(work_dir: Optional[Path] = None) -> Optional[Path]:
    """Return the local path to the newest valid current snapshot directory.

    Returns None if no valid current snapshot exists.
    """
    work_dir = work_dir or _get_work_dir()
    curr_ts = _find_valid_snapshot_ts(work_dir / CURRENT_DIR)
    if not curr_ts:
        return None
    return work_dir / CURRENT_DIR / curr_ts


def get_previous_timestamp(work_dir: Optional[Path] = None) -> Optional[str]:
    """Return the newest valid previous snapshot timestamp string."""
    work_dir = work_dir or _get_work_dir()
    return _find_valid_snapshot_ts(work_dir / PREVIOUS_DIR)


def get_current_timestamp(work_dir: Optional[Path] = None) -> Optional[str]:
    """Return the newest valid current snapshot timestamp string."""
    work_dir = work_dir or _get_work_dir()
    return _find_valid_snapshot_ts(work_dir / CURRENT_DIR)


def rotate_snapshots(
    new_snapshot_ts: str,
    new_snapshot_path: Path,
    work_dir: Optional[Path] = None,
) -> None:
    """Rotate snapshots after a successful pipeline run.

    Before: previous=<old>, current=<old_fetched>
    After:  previous=<old_fetched>, current=<new_snapshot_ts>

    The caller must ensure the new snapshot is valid before calling this.
    """
    work_dir = work_dir or _get_work_dir()

    prev_dir = work_dir / PREVIOUS_DIR
    curr_dir = work_dir / CURRENT_DIR

    # Find the OLD current timestamp (the one that was there before the new
    # snapshot was added). Exclude new_snapshot_ts since it may already exist
    # in current/ from _preserve_fetched_snapshot.
    old_curr_ts = None
    if curr_dir.exists():
        for d in sorted(curr_dir.iterdir()):
            if (d.is_dir() and not d.name.startswith(".")
                    and not d.name.startswith("_") and d.name != new_snapshot_ts):
                old_curr_ts = d.name

    # Clean up old previous
    if prev_dir.exists():
        shutil.rmtree(prev_dir)

    # Move current → previous
    if old_curr_ts and (curr_dir / old_curr_ts).exists():
        prev_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(str(curr_dir / old_curr_ts), str(prev_dir / old_curr_ts))

    # Clean up current and place new snapshot
    if curr_dir.exists():
        shutil.rmtree(curr_dir)
    curr_dir.mkdir(parents=True, exist_ok=True)
    shutil.copytree(str(new_snapshot_path), str(curr_dir / new_snapshot_ts))

    print(f"Rotated: previous={old_curr_ts}, current={new_snapshot_ts}")


def write_metadata(
    previous_ts: str,
    current_ts: str,
    work_dir: Optional[Path] = None,
) -> None:
    """Write cache metadata JSON."""
    work_dir = work_dir or _get_work_dir()
    meta = {
        "previous_ts": previous_ts,
        "current_ts": current_ts,
    }
    path = work_dir / METADATA_FILE
    path.write_text(json.dumps(meta, indent=2), encoding="utf-8")


def read_metadata(work_dir: Optional[Path] = None) -> Optional[Dict]:
    """Read cache metadata JSON."""
    work_dir = work_dir or _get_work_dir()
    path = work_dir / METADATA_FILE
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, ValueError):
        return None


def write_new_timestamp(ts: str, work_dir: Optional[Path] = None) -> None:
    """Write the new timestamp file for the workflow to read."""
    work_dir = work_dir or _get_work_dir()
    path = work_dir / TIMESTAMP_FILE
    path.write_text(ts, encoding="utf-8")


def read_new_timestamp(work_dir: Optional[Path] = None) -> Optional[str]:
    """Read the new timestamp file written by the pipeline."""
    work_dir = work_dir or _get_work_dir()
    path = work_dir / TIMESTAMP_FILE
    if not path.exists():
        return None
    return path.read_text(encoding="utf-8").strip()


def create_tarball(
    src_dir: Path,
    output_path: Path,
) -> None:
    """Create a tar.gz archive of a snapshot directory."""
    with tarfile.open(str(output_path), "w:gz") as tar:
        tar.add(str(src_dir), arcname=src_dir.name)


def extract_tarball(
    tarball_path: Path,
    dest_dir: Path,
) -> None:
    """Extract a tar.gz archive into dest_dir."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    with tarfile.open(str(tarball_path), "r:gz") as tar:
        tar.extractall(path=str(dest_dir))


def prepare_local_state(
    current_snapshot_path: Path,
    work_dir: Optional[Path] = None,
) -> Dict:
    """Validate and prepare local state for cache save.

    This is called by the workflow after the pipeline succeeds.
    It does NOT modify previous/current — the workflow handles rotation.

    Returns metadata dict with timestamps.
    """
    work_dir = work_dir or _get_work_dir()

    ok, err = validate_cache(work_dir)
    if not ok:
        raise RuntimeError(f"Cache validation failed: {err}")

    meta = read_metadata(work_dir)
    if not meta:
        raise RuntimeError("No cache metadata found after rotation")

    return meta


def ensure_cache_dirs(work_dir: Optional[Path] = None) -> None:
    """Create the cache directory structure if it doesn't exist."""
    work_dir = work_dir or _get_work_dir()
    (work_dir / PREVIOUS_DIR).mkdir(parents=True, exist_ok=True)
    (work_dir / CURRENT_DIR).mkdir(parents=True, exist_ok=True)
