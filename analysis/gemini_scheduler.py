"""Bounded parallel Gemini scheduler with rate limiting, key rotation, packing, and retries.

Architecture:
    Evidence Queue → Central Scheduler → 10 Worker Threads → 8 Lanes (2 Models × 4 Keys)
         ↓
    Gemini API → JSON Array Response → Python Validation → Immediate Persistence (Valid)
         ↓
    Re-queue Only Missing/Failed Records

Does NOT own:
- Evidence persistence (pipeline_controller)
"""

import os
import threading
import time
import queue
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Set

from analysis.gemini_client import GeminiClient, GeminiError, ErrorClass
from analysis.gemini_processor import (
    build_packed_prompt,
    check_token_safety,
    validate_packed_output,
    GeminiResult,
)


@dataclass
class WorkerLane:
    """A single (model, api_key) processing lane."""
    model: str
    api_key: str
    lane_id: str
    safe_key_identifier: str = ""
    rpm_limit: int = 15
    rpd_limit: int = 500

    # Runtime state (managed by scheduler)
    request_times: list = field(default_factory=list)
    cooldown_until: float = 0.0
    rpd_exhausted: bool = False  # In-memory flag for current run only
    fail_count: int = 0
    success_count: int = 0

    def __post_init__(self):
        if not self.safe_key_identifier:
            if len(self.api_key) > 8:
                self.safe_key_identifier = self.api_key[:4] + "..." + self.api_key[-4:]
            else:
                self.safe_key_identifier = "****"

    def __repr__(self):
        return (
            f"WorkerLane(model={self.model!r}, key={self.safe_key_identifier}, "
            f"lane_id={self.lane_id!r}, rpm_limit={self.rpm_limit}, rpd_exhausted={self.rpd_exhausted})"
        )

    def available(self, now=None):
        """Check if this lane can accept a new request."""
        if self.rpd_exhausted:
            return False
        now = now or time.monotonic()
        if now < self.cooldown_until:
            return False
        cutoff = now - 60.0
        self.request_times = [t for t in self.request_times if t > cutoff]
        return len(self.request_times) < self.rpm_limit

    def record_request(self, now=None):
        now = now or time.monotonic()
        self.request_times.append(now)

    def apply_cooldown(self, seconds, now=None):
        now = now or time.monotonic()
        self.cooldown_until = max(self.cooldown_until, now + seconds)


@dataclass
class PackedQueueItem:
    """A packed request item in the processing queue."""
    evidence_rows: list
    attempt: int = 1
    max_attempts: int = 3
    tried_lanes: set = field(default_factory=set)


# Alias for backwards compatibility with test imports if needed
QueueItem = PackedQueueItem


class GeminiScheduler:
    """Bounded parallel scheduler for Gemini API processing.

    - Packs up to 5 evidence items into 1 Gemini API request.
    - True concurrency with up to 10 worker threads.
    - Up to 8 MODEL + KEY lanes (e.g., 2 models × 4 keys).
    - Distinguishes RPM rate limits vs RPD daily quota limits.
    - In-memory RPD blacklist for current run only.
    - Partial response handling: valid items persist immediately, only failed items retried.
    """

    def __init__(self, api_keys, models=None, rpm_per_lane=15,
                 max_workers=None, items_per_request=None,
                 max_attempts=3, daily_quota=None):
        if models is None:
            models = ["gemini-3.1-flash-lite", "gemini-3.5-flash-lite"]

        env_concurrency = int(os.environ.get("GEMINI_CONCURRENCY", "10"))
        env_items_per_req = int(os.environ.get("GEMINI_ITEMS_PER_REQUEST", "5"))

        self.max_workers = max_workers if max_workers is not None else env_concurrency
        self.items_per_request = items_per_request if items_per_request is not None else env_items_per_req
        self.max_attempts = max_attempts
        self.daily_quota = daily_quota
        self.rpm_per_lane = rpm_per_lane

        # Build lanes: one per (model, key) combination
        self.lanes = []
        for model in models:
            for k_idx, key in enumerate(api_keys, 1):
                if not key or key.startswith("PASTE_"):
                    continue
                safe_id = f"key-{k_idx}"
                lane = WorkerLane(
                    model=model,
                    api_key=key,
                    lane_id=f"{model}/{safe_id}",
                    safe_key_identifier=safe_id,
                    rpm_limit=rpm_per_lane,
                )
                self.lanes.append(lane)

        if self.lanes and self.max_workers > 0:
            self.max_workers = max(1, min(self.max_workers, 64))
        else:
            self.max_workers = 0

        # Clients per model
        self._clients = {}
        for model in models:
            self._clients[model] = GeminiClient(model=model)

        self._daily_count = 0
        self._total_success = 0
        self._total_failure = 0
        self._total_retries = 0
        self._total_api_requests = 0
        self._lock = threading.Lock()
        self._log_lock = threading.Lock()
        self._last_all_rpm_wait = 0.0

    def _pick_lane_with_status(self, exclude_lanes=None):
        """Select an available lane or return system status.

        Returns:
            (lane_or_none, status_code)
            status_code can be: 'AVAILABLE', 'ALL_RPD_EXHAUSTED', 'ALL_RPM_LIMITED', 'EXCLUDED'
        """
        exclude = exclude_lanes or set()
        now = time.monotonic()

        eligible = [l for l in self.lanes if not l.rpd_exhausted]
        if not eligible:
            return None, "ALL_RPD_EXHAUSTED"

        # Check available lanes
        best = None
        best_oldest = float("inf")
        for lane in eligible:
            if lane.lane_id in exclude:
                continue
            if not lane.available(now):
                continue
            oldest = min(lane.request_times) if lane.request_times else 0
            if oldest < best_oldest:
                best = lane
                best_oldest = oldest

        if best is not None:
            return best, "AVAILABLE"

        # Check if all eligible are RPM limited / on cooldown
        unavailable_count = sum(1 for l in eligible if not l.available(now))
        if unavailable_count == len(eligible):
            return None, "ALL_RPM_LIMITED"

        return None, "EXCLUDED"

    def _pick_lane(self, exclude_lanes=None):
        """Helper for test compatibility."""
        lane, _ = self._pick_lane_with_status(exclude_lanes)
        return lane

    def process(self, evidence_rows, on_success=None):
        """Process evidence rows in parallel with packed requests, rate limiting, and retries."""
        if not evidence_rows or not self.lanes:
            return {
                "results": [],
                "success_count": 0,
                "failure_count": len(evidence_rows),
                "failures": [{"entity_type": r["entity_type"],
                              "entity_id": r["entity_id"],
                              "evidence_id": r.get("evidence_id"),
                              "reason": "no_lanes_available"}
                             for r in evidence_rows],
                "retries": 0,
                "api_requests": 0,
            }

        start_time = time.monotonic()
        total_records = len(evidence_rows)
        estimated_requests = (total_records + self.items_per_request - 1) // self.items_per_request

        print("Gemini scheduler starting")
        print(f"  records={total_records}")
        print(f"  items_per_request={self.items_per_request}")
        print(f"  estimated_requests={estimated_requests}")
        print(f"  concurrency={self.max_workers}")
        print(f"  lanes={len(self.lanes)}")

        # Build initial work queue using packing + token safety
        work_queue = queue.Queue()

        # Step 1: Pack into chunks of items_per_request
        raw_chunks = []
        for i in range(0, total_records, self.items_per_request):
            raw_chunks.append(evidence_rows[i:i + self.items_per_request])

        # Step 2: Apply token safety check to each chunk
        for chunk in raw_chunks:
            safe_sub_batches = check_token_safety(chunk)
            for sub_batch in safe_sub_batches:
                work_queue.put(PackedQueueItem(evidence_rows=sub_batch, attempt=1, max_attempts=self.max_attempts))

        results = []
        failures = []
        results_lock = threading.Lock()
        failure_lock = threading.Lock()

        completed_count = [0]
        progress_lock = threading.Lock()

        def _log_progress(increment=1):
            with progress_lock:
                completed_count[0] += increment
                c = completed_count[0]
                total = total_records
            if c % 50 == 0 or c >= total:
                print(f"    Gemini progress: {c}/{total} "
                      f"successful={self._total_success} failed={self._total_failure} "
                      f"retries={self._total_retries} requests={self._total_api_requests}")

        stop_signal = threading.Event()

        def _worker():
            while not stop_signal.is_set():
                try:
                    item = work_queue.get(timeout=0.2)
                except queue.Empty:
                    if work_queue.unfinished_tasks == 0 or stop_signal.is_set():
                        return
                    continue

                try:
                    self._process_packed_item(
                        item, work_queue, on_success, results, results_lock,
                        failures, failure_lock, _log_progress, stop_signal
                    )
                finally:
                    work_queue.task_done()

        threads = []
        for _ in range(self.max_workers):
            t = threading.Thread(target=_worker, daemon=True)
            t.start()
            threads.append(t)

        work_queue.join()
        stop_signal.set()

        for t in threads:
            t.join(timeout=1.0)

        duration = time.monotonic() - start_time

        lane_stats = {}
        for lane in self.lanes:
            lane_stats[lane.lane_id] = {
                "success": lane.success_count,
                "failed": lane.fail_count,
                "rpd_exhausted": lane.rpd_exhausted,
            }

        print("Gemini complete")
        print(f"  processed={total_records}")
        print(f"  successful={self._total_success}")
        print(f"  failed={self._total_failure}")
        print(f"  retries={self._total_retries}")
        print(f"  api_requests={self._total_api_requests}")
        print(f"  duration={duration:.2f}s")

        return {
            "results": results,
            "success_count": self._total_success,
            "failure_count": self._total_failure,
            "failures": failures,
            "retries": self._total_retries,
            "api_requests": self._total_api_requests,
            "lane_stats": lane_stats,
            "duration": duration,
        }

    def _process_packed_item(self, item, work_queue, on_success, results, results_lock,
                             failures, failure_lock, log_progress, stop_signal):
        """Process a single packed request item with lane selection, retry, and partial response persistence."""
        while item.attempt <= item.max_attempts and not stop_signal.is_set():
            # Check global daily request quota
            if self.daily_quota and self._daily_count >= self.daily_quota:
                with self._lock:
                    self._total_failure += len(item.evidence_rows)
                with failure_lock:
                    for r in item.evidence_rows:
                        failures.append({
                            "entity_type": r["entity_type"],
                            "entity_id": r["entity_id"],
                            "evidence_id": r.get("evidence_id"),
                            "reason": "daily_quota_exceeded",
                        })
                log_progress(len(item.evidence_rows))
                return

            lane, status = self._pick_lane_with_status(exclude_lanes=item.tried_lanes)

            if status == "ALL_RPD_EXHAUSTED":
                with self._log_lock:
                    if not stop_signal.is_set():
                        print("    All eligible lanes daily quota exhausted; stopping Gemini")
                        stop_signal.set()
                # Fail remaining rows
                with self._lock:
                    self._total_failure += len(item.evidence_rows)
                with failure_lock:
                    for r in item.evidence_rows:
                        failures.append({
                            "entity_type": r["entity_type"],
                            "entity_id": r["entity_id"],
                            "evidence_id": r.get("evidence_id"),
                            "reason": "all_lanes_rpd_exhausted",
                        })
                log_progress(len(item.evidence_rows))
                return

            if status == "ALL_RPM_LIMITED":
                with self._log_lock:
                    now = time.monotonic()
                    if now - self._last_all_rpm_wait > 5.0:
                        print("    All eligible lanes RPM-limited; waiting 60 seconds")
                        self._last_all_rpm_wait = now
                time.sleep(1.0)
                item.tried_lanes.clear()
                continue

            if status == "EXCLUDED" or lane is None:
                time.sleep(0.2)
                item.tried_lanes.clear()
                continue

            item.tried_lanes.add(lane.lane_id)

            # Record API call
            lane.record_request()
            with self._lock:
                self._daily_count += 1
                self._total_api_requests += 1

            client = self._clients[lane.model]
            prompt = build_packed_prompt(item.evidence_rows)

            try:
                raw_response = client.call(prompt, lane.api_key)

                # Validate and parse response
                valid_results, failed_entries = validate_packed_output(raw_response, item.evidence_rows)

                # 1. Handle valid results (Persist immediately)
                if valid_results:
                    lane.success_count += len(valid_results)
                    with self._lock:
                        self._total_success += len(valid_results)
                    with results_lock:
                        results.extend(valid_results)

                    if on_success:
                        for res in valid_results:
                            try:
                                on_success(res)
                            except Exception as exc:
                                print(f"    Error in on_success callback for {res.entity_type} {res.entity_id}: {exc}")

                    log_progress(len(valid_results))

                # 2. Handle failed entries (Partial failure / Requeue ONLY failed records)
                if failed_entries:
                    request_map = {str(r["entity_id"]): r for r in item.evidence_rows}
                    for f_entry in failed_entries:
                        ent_id = str(f_entry["entity_id"])
                        row = request_map.get(ent_id)
                        if not row:
                            continue

                        if item.attempt < item.max_attempts:
                            with self._lock:
                                self._total_retries += 1
                            # Re-queue ONLY this specific failed evidence item for retry
                            retry_item = PackedQueueItem(
                                evidence_rows=[row],
                                attempt=item.attempt + 1,
                                max_attempts=item.max_attempts,
                            )
                            work_queue.put(retry_item)
                        else:
                            lane.fail_count += 1
                            with self._lock:
                                self._total_failure += 1
                            with failure_lock:
                                failures.append({
                                    "entity_type": row["entity_type"],
                                    "entity_id": row["entity_id"],
                                    "evidence_id": row.get("evidence_id"),
                                    "reason": f_entry["reason"],
                                })
                            log_progress(1)

                return  # Call succeeded (partial or full)

            except GeminiError as gemini_err:
                if gemini_err.rpd_exhausted:
                    lane.rpd_exhausted = True
                    print(f"    lane={lane.model}/{lane.safe_key_identifier} status=RPD_EXHAUSTED")
                    # Re-queue item to try on remaining eligible lanes
                    continue

                if gemini_err.rpm_limited:
                    wait = gemini_err.retry_after or 10.0
                    lane.apply_cooldown(wait)
                    print(f"    lane={lane.model}/{lane.safe_key_identifier} status=RPM_LIMITED")
                    with self._lock:
                        self._total_retries += 1
                    # Re-queue item for another lane
                    continue

                if gemini_err.permanent:
                    # Permanent error (401 Auth, 404 Model Not Found, 400 Invalid) — do not retry
                    lane.fail_count += len(item.evidence_rows)
                    with self._lock:
                        self._total_failure += len(item.evidence_rows)
                    with failure_lock:
                        for r in item.evidence_rows:
                            failures.append({
                                "entity_type": r["entity_type"],
                                "entity_id": r["entity_id"],
                                "evidence_id": r.get("evidence_id"),
                                "reason": f"permanent_{gemini_err.status_code or 'error'}",
                            })
                    log_progress(len(item.evidence_rows))
                    return

                # Other retryable error (5xx server error, timeout, transient)
                lane.fail_count += 1
                with self._lock:
                    self._total_retries += 1

                item.attempt += 1
                if item.attempt > item.max_attempts:
                    with self._lock:
                        self._total_failure += len(item.evidence_rows)
                    with failure_lock:
                        for r in item.evidence_rows:
                            failures.append({
                                "entity_type": r["entity_type"],
                                "entity_id": r["entity_id"],
                                "evidence_id": r.get("evidence_id"),
                                "reason": "max_attempts_exceeded",
                            })
                    log_progress(len(item.evidence_rows))
                    return
                time.sleep(0.5)

            except Exception as exc:
                lane.fail_count += 1
                with self._lock:
                    self._total_retries += 1
                item.attempt += 1
                if item.attempt > item.max_attempts:
                    with self._lock:
                        self._total_failure += len(item.evidence_rows)
                    with failure_lock:
                        for r in item.evidence_rows:
                            failures.append({
                                "entity_type": r["entity_type"],
                                "entity_id": r["entity_id"],
                                "evidence_id": r.get("evidence_id"),
                                "reason": f"unexpected_error: {exc}",
                            })
                    log_progress(len(item.evidence_rows))
                    return
                time.sleep(0.5)

