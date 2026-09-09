#!/usr/bin/env python3
"""Seed a local snapshot into GitHub Actions cache.

Usage:
    python3 automation/seed_cache.py /path/to/snapshot_dir

This script:
1. Validates the local snapshot (_COMPLETE.json, structure)
2. Packages it into a temporary tarball
3. Uploads the tarball as a GitHub release asset (pre-release, draft)
4. Triggers the seed-mplads-cache workflow
5. The seed workflow downloads, validates, and saves to GitHub cache
6. Clean up: delete the draft release after seed completes

ZERO Supabase egress — snapshot stays local until uploaded to GitHub.

Requires: gh CLI authenticated, Python 3.12+
"""
import json
import subprocess
import sys
import tempfile
import time
import tarfile
from pathlib import Path


def run(cmd, check=True, capture=True):
    result = subprocess.run(
        cmd if isinstance(cmd, list) else cmd,
        shell=isinstance(cmd, str),
        capture_output=capture, text=True
    )
    if check and result.returncode != 0:
        print(f"ERROR: {cmd}", file=sys.stderr)
        print(f"stderr: {result.stderr}", file=sys.stderr)
        sys.exit(1)
    return result


def validate_snapshot(snap_dir: Path):
    """Validate snapshot structure and _COMPLETE.json."""
    print(f"\n--- Validating snapshot: {snap_dir} ---")

    if not snap_dir.is_dir():
        print(f"FATAL: {snap_dir} is not a directory")
        sys.exit(1)

    complete = snap_dir / "_COMPLETE.json"
    if not complete.exists():
        print("FATAL: _COMPLETE.json missing")
        sys.exit(1)

    try:
        data = json.loads(complete.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, ValueError) as exc:
        print(f"FATAL: _COMPLETE.json is invalid JSON: {exc}")
        sys.exit(1)

    if data.get("status") != "complete":
        print(f"FATAL: status is '{data.get('status')}', expected 'complete'")
        sys.exit(1)

    ts = data.get("timestamp", "")
    if not ts:
        print("FATAL: timestamp field missing in _COMPLETE.json")
        sys.exit(1)

    datasets = data.get("datasets", {})
    print(f"  Timestamp: {ts}")
    print(f"  Status: complete")
    print(f"  Datasets: {len(datasets)}")

    total_chunks = 0
    for ds_name, ds_info in datasets.items():
        expected = ds_info.get("chunks", 0)
        total_chunks += expected
        ds_dir = snap_dir / ds_name
        if not ds_dir.is_dir():
            print(f"FATAL: Dataset directory missing: {ds_name}")
            sys.exit(1)
        actual = len(list(ds_dir.glob("part_*.ndjson")))
        if actual < expected:
            print(f"FATAL: {ds_name} has {actual} parts, expected {expected}")
            sys.exit(1)

    print(f"  Total chunks: {total_chunks}")
    print("  Validation: PASS")
    return ts


def create_tarball(snap_dir: Path, output: Path):
    """Create a tar.gz of the snapshot directory."""
    print(f"\n--- Creating tarball ---")
    with tarfile.open(str(output), "w:gz") as tar:
        tar.add(str(snap_dir), arcname=snap_dir.name)
    size_mb = output.stat().st_size / (1024 * 1024)
    print(f"  Created: {output.name} ({size_mb:.1f} MB)")
    return output


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 automation/seed_cache.py /path/to/snapshot_dir")
        sys.exit(1)

    snap_dir = Path(sys.argv[1]).resolve()
    ts = validate_snapshot(snap_dir)

    release_tag = f"seed-cache-{ts}"
    artifact_name = f"snapshot-seed-{ts}.tar.gz"

    # Create tarball
    with tempfile.TemporaryDirectory() as tmpdir:
        tarball = Path(tmpdir) / artifact_name
        create_tarball(snap_dir, tarball)

        # Create draft pre-release with the tarball
        print(f"\n--- Uploading to GitHub release: {release_tag} ---")
        run([
            "gh", "release", "create", release_tag,
            "--title", f"Cache Seed: {ts}",
            "--notes", f"Temporary release for cache seeding. Will be deleted after seed completes.",
            "--draft",
            "--prerelease",
            str(tarball),
        ])
        print(f"  Release created: {release_tag}")

    # Trigger the seed workflow
    print(f"\n--- Triggering seed workflow ---")
    run([
        "gh", "workflow", "run", "seed-mplads-cache.yml",
        "--ref", "main",
        "-f", f"release_tag={release_tag}",
    ])
    print(f"  Workflow triggered")

    # Wait for workflow to appear
    time.sleep(5)

    result = run([
        "gh", "run", "list",
        "--workflow=seed-mplads-cache.yml",
        "--limit=1",
        "--json", "databaseId,status",
    ], check=False)

    if result.returncode == 0:
        runs = json.loads(result.stdout)
        if runs:
            run_id = runs[0]["databaseId"]
            print(f"\n  Seed workflow run: {run_id}")
            print(f"  URL: https://github.com/Yuvraj1954/mplads-automation/actions/runs/{run_id}")

    print(f"\n{'='*50}")
    print(f"  SEED INITIATED")
    print(f"{'='*50}")
    print(f"  Snapshot:  {ts}")
    print(f"  Release:   {release_tag}")
    print(f"  Cache key: mplads-snapshot-state-{ts}")
    print(f"\n  After the seed workflow completes:")
    print(f"  1. The cache will be available for the daily pipeline")
    print(f"  2. Clean up the draft release:")
    print(f"     gh release delete {release_tag} --yes --cleanup-tag")


if __name__ == "__main__":
    main()
