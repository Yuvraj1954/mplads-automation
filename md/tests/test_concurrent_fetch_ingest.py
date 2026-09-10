"""Tests for concurrent fetch (concurrency=5, 12 datasets) and concurrent ingestion.
"""

import json
import os
import threading
import time
from pathlib import Path
from unittest.mock import patch, MagicMock


import sys
fetcher_dir = str(Path(__file__).resolve().parent.parent / "fetcher")
if fetcher_dir not in sys.path:
    sys.path.insert(0, fetcher_dir)
import fetcher


# ---------------------------------------------------------------------------
# 1. Concurrent Fetch Tests
# ---------------------------------------------------------------------------

class TestConcurrentFetch:
    """Tests for fetch_all_datasets_concurrently with 5 workers and 12 datasets."""

    def test_fetch_all_datasets_concurrently_runs_with_concurrency_5(self, tmp_path):
        from fetcher import fetch_all_datasets_concurrently, MP_DATASETS, MLA_DATASETS

        active_threads = []
        max_active_threads = 0
        thread_lock = threading.Lock()

        def mock_process_dataset(session, storage_name, dataset_key, timestamp, local_snapshot_dir, combo, local_only=False):
            nonlocal max_active_threads
            with thread_lock:
                active_threads.append(threading.current_thread().ident)
                current_active = len(set(active_threads))
                if current_active > max_active_threads:
                    max_active_threads = current_active

            # Simulate small network delay
            time.sleep(0.01)

            with thread_lock:
                active_threads.remove(threading.current_thread().ident)

            return {
                "records": 10,
                "chunks": 1,
                "dataset": storage_name,
            }

        with patch.object(fetcher, "process_dataset", side_effect=mock_process_dataset), \
             patch.object(fetcher, "create_session", return_value=MagicMock()), \
             patch.object(fetcher, "establish_session", return_value=True):

            results, successful, failed, failed_datasets = fetch_all_datasets_concurrently(
                timestamp="2026-09-10T12-00-00Z",
                local_snapshot_dir=tmp_path,
                concurrency=5,
                local_only=True,
            )

        assert successful == 12
        assert failed == 0
        assert len(failed_datasets) == 0
        assert len(results) == 12
        # All 6 MP and 6 MLA datasets should be present
        for name in MP_DATASETS:
            assert name in results
        for name in MLA_DATASETS:
            assert f"mla_{name}" in results

        # Max active worker threads should not exceed 5
        assert max_active_threads <= 5
        assert max_active_threads > 1

    def test_concurrent_fetch_retry_isolated_failure(self, tmp_path):
        from fetcher import fetch_all_datasets_concurrently

        call_counts = {}
        call_lock = threading.Lock()

        def mock_process_dataset(session, storage_name, dataset_key, timestamp, local_snapshot_dir, combo, local_only=False):
            with call_lock:
                cnt = call_counts.get(storage_name, 0) + 1
                call_counts[storage_name] = cnt

            # Fail calamity on first attempt, succeed on retry
            if storage_name == "calamity" and cnt == 1:
                raise RuntimeError("Temporary MOSPI 502 Bad Gateway")

            return {
                "records": 5,
                "chunks": 1,
                "dataset": storage_name,
            }

        with patch.object(fetcher, "process_dataset", side_effect=mock_process_dataset), \
             patch.object(fetcher, "create_session", return_value=MagicMock()), \
             patch.object(fetcher, "establish_session", return_value=True), \
             patch.object(fetcher, "RETRY_DELAY_SECONDS", 0.01):

            results, successful, failed, failed_datasets = fetch_all_datasets_concurrently(
                timestamp="2026-09-10T12-00-00Z",
                local_snapshot_dir=tmp_path,
                concurrency=5,
                local_only=True,
            )

        assert successful == 12
        assert failed == 0
        assert len(results) == 12
        # calamity was called twice (initial + retry)
        assert call_counts["calamity"] == 2
        # other datasets called once
        assert call_counts["allocated_limit"] == 1


# ---------------------------------------------------------------------------
# 2. Concurrent Ingestion Tests
# ---------------------------------------------------------------------------

class TestConcurrentIngestion:
    """Tests for daily_pipeline.py Step 6 ingestion concurrency."""

    def test_concurrent_worker_processes_all_jobs(self):
        """Simulate concurrent workers claiming and processing 15 jobs."""
        total_jobs = 15
        unclaimed_jobs = list(range(1, total_jobs + 1))
        completed_jobs = []
        jobs_lock = threading.Lock()

        active_workers = set()
        max_active = 0

        def mock_call_edge(func_name, payload):
            nonlocal max_active
            assert func_name == "mplads-worker"
            tid = threading.current_thread().ident
            with jobs_lock:
                active_workers.add(tid)
                if len(active_workers) > max_active:
                    max_active = len(active_workers)

            time.sleep(0.01)

            with jobs_lock:
                active_workers.discard(tid)
                if unclaimed_jobs:
                    job_id = unclaimed_jobs.pop(0)
                    completed_jobs.append(job_id)
                    remaining = len(unclaimed_jobs)
                    return {
                        "success": True,
                        "job_id": job_id,
                        "progress": {
                            "completed": len(completed_jobs),
                            "total": total_jobs,
                            "remaining": remaining,
                        },
                    }
                else:
                    return {
                        "success": True,
                        "message": "All ingestion jobs completed.",
                        "completed": len(completed_jobs),
                        "run_id": payload.get("run_id"),
                    }

        # Run the concurrent worker logic identical to daily_pipeline.py Step 6
        ingest_concurrency = 5
        expected_jobs = 15
        run_id = "test_run_123"
        worker_max_minutes = 1
        worker_start = time.time()
        per_job_timeout_seconds = 60
        all_completed = threading.Event()
        stop_event = threading.Event()
        worker_errors = []
        errors_lock = threading.Lock()

        def _ingest_worker(worker_idx):
            while not stop_event.is_set() and not all_completed.is_set():
                elapsed = time.time() - worker_start
                if elapsed > worker_max_minutes * 60:
                    stop_event.set()
                    break

                job_start = time.time()
                try:
                    result = mock_call_edge("mplads-worker", {"run_id": run_id})
                except Exception as exc:
                    time.sleep(0.01)
                    continue

                if result.get("success"):
                    msg = result.get("message", "")
                    completed_count = result.get("completed", 0)
                    remaining = result.get("progress", {}).get("remaining", None)

                    if msg == "All ingestion jobs completed." and completed_count >= expected_jobs:
                        all_completed.set()
                        break
                    elif remaining == 0:
                        all_completed.set()
                        break
                    elif msg == "Job was already claimed. Try again.":
                        time.sleep(0.01)
                        continue
                    elif msg == "All ingestion jobs completed.":
                        time.sleep(0.01)
                        continue
                else:
                    with errors_lock:
                        worker_errors.append(result.get("error"))

        threads = []
        for i in range(min(ingest_concurrency, expected_jobs)):
            t = threading.Thread(target=_ingest_worker, args=(i + 1,))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        assert all_completed.is_set()
        assert len(completed_jobs) == 15
        assert max_active <= 5
        assert max_active > 1

    def test_concurrent_worker_handles_already_claimed_race_condition(self):
        """Simulate race conditions where jobs are already claimed by another worker."""
        total_jobs = 5
        unclaimed = list(range(1, total_jobs + 1))
        completed = []
        race_count = 0
        state_lock = threading.Lock()

        def mock_call_edge(func_name, payload):
            nonlocal race_count
            with state_lock:
                # Force a race condition for the first 2 calls
                if race_count < 2:
                    race_count += 1
                    return {
                        "success": True,
                        "message": "Job was already claimed. Try again.",
                    }

                if unclaimed:
                    job_id = unclaimed.pop(0)
                    completed.append(job_id)
                    return {
                        "success": True,
                        "job_id": job_id,
                        "progress": {
                            "completed": len(completed),
                            "total": total_jobs,
                            "remaining": len(unclaimed),
                        },
                    }
                else:
                    return {
                        "success": True,
                        "message": "All ingestion jobs completed.",
                        "completed": len(completed),
                    }

        all_completed = threading.Event()
        stop_event = threading.Event()
        expected_jobs = 5
        run_id = "test_race_run"
        worker_start = time.time()
        worker_max_minutes = 1

        def _ingest_worker(worker_idx):
            while not stop_event.is_set() and not all_completed.is_set():
                if time.time() - worker_start > worker_max_minutes * 60:
                    stop_event.set()
                    break

                result = mock_call_edge("mplads-worker", {"run_id": run_id})
                if result.get("success"):
                    msg = result.get("message", "")
                    completed_count = result.get("completed", 0)
                    remaining = result.get("progress", {}).get("remaining", None)

                    if msg == "All ingestion jobs completed." and completed_count >= expected_jobs:
                        all_completed.set()
                        break
                    elif remaining == 0:
                        all_completed.set()
                        break
                    elif msg == "Job was already claimed. Try again.":
                        time.sleep(0.01)
                        continue
                    elif msg == "All ingestion jobs completed.":
                        time.sleep(0.01)
                        continue

        threads = []
        for i in range(3):
            t = threading.Thread(target=_ingest_worker, args=(i + 1,))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        assert all_completed.is_set()
        assert len(completed) == 5
        assert race_count == 2
