"""Bounded parallel Gemini scheduler with rate limiting, key rotation, and retries.

Architecture:
    Evidence queue → Scheduler → Worker lanes → API → Retry queue → Persistence

Each worker lane = (model, api_key) pair with independent RPM tracking.
Completed workers immediately receive the next queued item.
Failed items rotate to another available lane for retry.

Does NOT own:
- Prompt building (gemini_processor)
- Output validation (gemini_processor)
- Evidence persistence (pipeline_controller)
"""

import threading
import time
import queue
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

from analysis.gemini_client import GeminiClient, GeminiError, ErrorClass
from analysis.gemini_processor import build_prompt, validate_output, GeminiResult


@dataclass
class WorkerLane:
    """A single (model, api_key) processing lane."""
    model: str
    api_key: str
    lane_id: str
    rpm_limit: int = 15

    # Runtime state (managed by scheduler)
    request_times: list = field(default_factory=list)
    cooldown_until: float = 0.0
    fail_count: int = 0
    success_count: int = 0

    def __repr__(self):
        masked = self.api_key[:4] + "..." + self.api_key[-4:] if len(self.api_key) > 8 else "****"
        return (
            f"WorkerLane(model={self.model!r}, api_key={masked}, "
            f"lane_id={self.lane_id!r}, rpm_limit={self.rpm_limit})"
        )

    def available(self, now=None):
        """Check if this lane can accept a new request."""
        now = now or time.monotonic()
        if now < self.cooldown_until:
            return False
        # Remove request timestamps older than 60 seconds
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
class QueueItem:
    """An evidence item in the processing queue."""
    evidence_row: dict
    attempt: int = 0
    max_attempts: int = 3
    last_error: Optional[GeminiError] = None
    tried_lanes: set = field(default_factory=set)


class GeminiScheduler:
    """Bounded parallel scheduler for Gemini API processing.

    Uses a thread pool with per-lane RPM limiting.
    Failed items rotate to another available lane.
    Global daily quota enforced.
    """

    def __init__(self, api_keys, models=None, rpm_per_lane=15,
                 max_workers=8, max_attempts=3, daily_quota=None):
        """
        Args:
            api_keys: list of API key strings
            models: list of model name strings. Defaults to [DEFAULT_MODEL]
            rpm_per_lane: max requests per minute per (model, key) lane
            max_workers: max concurrent worker threads
            max_attempts: max retry attempts per evidence item
            daily_quota: max total requests for the day (None = unlimited)
        """
        from analysis.gemini_client import DEFAULT_MODEL
        if models is None:
            models = [DEFAULT_MODEL]

        self.max_attempts = max_attempts
        self.daily_quota = daily_quota
        self.rpm_per_lane = rpm_per_lane

        # Build lanes: one per (model, key) combination
        self.lanes = []
        lane_id = 0
        for model in models:
            for key in api_keys:
                if not key or key.startswith("PASTE_"):
                    continue
                lane = WorkerLane(
                    model=model,
                    api_key=key,
                    lane_id=f"lane_{lane_id}",
                    rpm_limit=rpm_per_lane,
                )
                self.lanes.append(lane)
                lane_id += 1

        self.max_workers = min(max_workers, len(self.lanes)) if self.lanes else 0

        # Clients per model (stateless, reusable)
        self._clients = {}
        for model in models:
            self._clients[model] = GeminiClient(model=model)

        # Counters
        self._daily_count = 0
        self._total_success = 0
        self._total_failure = 0
        self._total_retries = 0
        self._lock = threading.Lock()

    def _pick_lane(self, exclude_lanes=None):
        """Select an available lane, preferring least-recently-used."""
        exclude = exclude_lanes or set()
        now = time.monotonic()

        # First pass: find any available lane not in exclude set
        best = None
        best_oldest = float("inf")
        for lane in self.lanes:
            if lane.lane_id in exclude:
                continue
            if not lane.available(now):
                continue
            # Prefer lane with oldest last request (spreading load)
            oldest = min(lane.request_times) if lane.request_times else 0
            if oldest < best_oldest:
                best = lane
                best_oldest = oldest

        return best

    def _check_model_available(self, model):
        """Check if at least one lane for this model is available."""
        now = time.monotonic()
        for lane in self.lanes:
            if lane.model == model and lane.available(now):
                return True
        return False

    def _cooldown_all_lanes_for_model(self, model, seconds=60):
        """Apply cooldown to all lanes of a model."""
        for lane in self.lanes:
            if lane.model == model:
                lane.apply_cooldown(seconds)

    def _other_model(self, model):
        """Return the other configured model, or None."""
        models = list(self._clients.keys())
        if len(models) < 2:
            return None
        return next((m for m in models if m != model), None)

    def process(self, evidence_rows, on_success=None):
        """Process evidence rows in parallel with rate limiting and retries.

        Args:
            evidence_rows: list of evidence record dicts
            on_success: callback(GeminiResult) called after each success

        Returns:
            dict with results, success_count, failure_count, etc.
        """
        if not self.lanes:
            return {
                "results": [],
                "success_count": 0,
                "failure_count": len(evidence_rows),
                "failures": [{"entity_type": r["entity_type"],
                              "entity_id": r["entity_id"],
                              "evidence_id": r.get("evidence_id")}
                             for r in evidence_rows],
                "retries": 0,
            }

        # Build work queue
        work_queue = queue.Queue()
        for row in evidence_rows:
            work_queue.put(QueueItem(evidence_row=row, max_attempts=self.max_attempts))

        results = []
        failures = []
        results_lock = threading.Lock()
        failure_lock = threading.Lock()

        # Progress tracking
        total = len(evidence_rows)
        completed = [0]
        progress_lock = threading.Lock()

        def _log_progress():
            with progress_lock:
                completed[0] += 1
                c = completed[0]
            if c % 50 == 0 or c == total:
                print(f"    Gemini progress: {c}/{total}")

        def _worker():
            while True:
                try:
                    item = work_queue.get(timeout=0.5)
                except queue.Empty:
                    return

                try:
                    self._process_item(item, on_success, results,
                                       results_lock, failures, failure_lock,
                                       _log_progress)
                finally:
                    work_queue.task_done()

        # Log configuration
        models = list(self._clients.keys())
        print(f"    Concurrency: {self.max_workers} workers, "
              f"{len(self.lanes)} lanes, "
              f"{self.rpm_per_lane} RPM/lane")
        print(f"    Models: {', '.join(models)}")

        # Start workers
        threads = []
        for _ in range(self.max_workers):
            t = threading.Thread(target=_worker, daemon=True)
            t.start()
            threads.append(t)

        # Wait for all work to complete
        work_queue.join()

        # Compute per-lane stats
        lane_stats = {}
        for lane in self.lanes:
            if lane.success_count or lane.fail_count:
                lane_stats[f"{lane.model}/{lane.lane_id}"] = {
                    "success": lane.success_count,
                    "failed": lane.fail_count,
                }

        return {
            "results": results,
            "success_count": self._total_success,
            "failure_count": self._total_failure,
            "failures": failures,
            "retries": self._total_retries,
            "lane_stats": lane_stats,
        }

    def _process_item(self, item, on_success, results, results_lock,
                      failures, failure_lock, log_progress):
        """Process a single evidence item with retry and lane rotation."""
        while item.attempt < item.max_attempts:
            # Check daily quota
            if self.daily_quota and self._daily_count >= self.daily_quota:
                with self._lock:
                    self._total_failure += 1
                with failure_lock:
                    failures.append({
                        "entity_type": item.evidence_row["entity_type"],
                        "entity_id": item.evidence_row["entity_id"],
                        "evidence_id": item.evidence_row.get("evidence_id"),
                        "reason": "daily_quota_exceeded",
                    })
                return

            # Pick a lane (exclude previously tried lanes)
            lane = self._pick_lane(exclude_lanes=item.tried_lanes)

            if lane is None:
                # All lanes exhausted or on cooldown — check if any model is fully unavailable
                available_models = set()
                for m in self._clients:
                    if self._check_model_available(m):
                        available_models.add(m)

                if not available_models:
                    # All models unavailable — global cooldown
                    print(f"    All lanes unavailable, cooling down 60s...")
                    self._cooldown_all_lanes_for_model(
                        list(self._clients.keys())[0], 60
                    )
                    # Also cooldown other models
                    for m in self._clients:
                        self._cooldown_all_lanes_for_model(m, 60)
                    time.sleep(60)
                    item.tried_lanes.clear()  # Allow retrying all lanes
                    continue
                else:
                    # Some models available but all their lanes excluded
                    # Wait briefly then allow retrying any lane
                    time.sleep(0.5)
                    item.tried_lanes.clear()
                    continue

            item.attempt += 1
            item.tried_lanes.add(lane.lane_id)

            # Check quota again after waiting
            if self.daily_quota and self._daily_count >= self.daily_quota:
                with self._lock:
                    self._total_failure += 1
                with failure_lock:
                    failures.append({
                        "entity_type": item.evidence_row["entity_type"],
                        "entity_id": item.evidence_row["entity_id"],
                        "evidence_id": item.evidence_row.get("evidence_id"),
                        "reason": "daily_quota_exceeded",
                    })
                return

            # Make the API call
            try:
                lane.record_request()
                with self._lock:
                    self._daily_count += 1

                client = self._clients[lane.model]

                prompt = build_prompt(item.evidence_row)
                obj = client.call(prompt, lane.api_key)

                # Validate output
                validation_errors = validate_output(obj, item.evidence_row)
                if validation_errors:
                    raise GeminiError(
                        ErrorClass.RETRYABLE,
                        message=f"Validation: {'; '.join(validation_errors)}"
                    )

                result = GeminiResult(
                    entity_type=item.evidence_row["entity_type"],
                    entity_id=item.evidence_row["entity_id"],
                    summary=obj["summary"],
                    highlights=obj["highlights"],
                    cautions=obj["cautions"],
                    evidence_hash=item.evidence_row.get("evidence_hash", ""),
                    model=lane.model,
                    prompt_version=item.evidence_row.get("prompt_version", ""),
                )

                # Success
                lane.success_count += 1
                with self._lock:
                    self._total_success += 1
                with results_lock:
                    results.append(result)
                if on_success:
                    on_success(result)
                log_progress()
                return

            except GeminiError as gemini_err:
                lane.fail_count += 1

                if gemini_err.permanent:
                    # Permanent failure — do not retry
                    with self._lock:
                        self._total_failure += 1
                    with failure_lock:
                        failures.append({
                            "entity_type": item.evidence_row["entity_type"],
                            "entity_id": item.evidence_row["entity_id"],
                            "evidence_id": item.evidence_row.get("evidence_id"),
                            "reason": f"permanent_{gemini_err.status_code}",
                        })
                    log_progress()
                    return

                # Retryable failure
                item.last_error = gemini_err
                with self._lock:
                    self._total_retries += 1

                # Apply retry-after if provided
                if gemini_err.retry_after:
                    wait = min(gemini_err.retry_after, 30)
                    lane.apply_cooldown(wait)
                    time.sleep(wait)
                elif gemini_err.status_code == 429:
                    # Rate limited — cooldown this lane
                    lane.apply_cooldown(10)
                    time.sleep(1)
                else:
                    # Other retryable error — brief backoff
                    backoff = min(5, 2 ** (item.attempt - 1))
                    time.sleep(backoff)

            except Exception as exc:
                # Unexpected error — treat as retryable
                lane.fail_count += 1
                with self._lock:
                    self._total_retries += 1
                item.last_error = GeminiError(
                    ErrorClass.RETRYABLE, message=str(exc)
                )
                time.sleep(1)

        # All attempts exhausted
        with self._lock:
            self._total_failure += 1
        with failure_lock:
            failures.append({
                "entity_type": item.evidence_row["entity_type"],
                "entity_id": item.evidence_row["entity_id"],
                "evidence_id": item.evidence_row.get("evidence_id"),
                "reason": "max_attempts_exceeded",
            })
        log_progress()
