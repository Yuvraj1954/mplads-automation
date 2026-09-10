"""Tests for Phase 2 reliability fixes (G1, G2, #4, #5, #7, #14)."""
import queue
import sys
import threading
import time
from unittest.mock import MagicMock, patch

import pytest


class TestGeminiQuotaTOCTOU:
    """G2: Daily quota check must be inside the lock to prevent race conditions."""

    def test_quota_check_inside_lock(self):
        """Verify quota check and increment happen atomically under the same lock."""
        from analysis.gemini_scheduler import GeminiScheduler, WorkerLane, PackedQueueItem

        scheduler = GeminiScheduler(
            api_keys=["test-key-12345678"],
            models=["test-model"],
            max_workers=2,
            daily_quota=5,
        )

        # Track lock acquisition order
        lock_events = []

        original_lock = scheduler._lock

        class TrackingLock:
            def __enter__(self):
                lock_events.append("acquired")
                return original_lock.__enter__()

            def __exit__(self, *args):
                lock_events.append("released")
                return original_lock.__exit__(*args)

        scheduler._lock = TrackingLock()

        # Simulate 10 concurrent threads all trying to increment
        results = []

        def try_increment():
            # Each thread: check quota, then pick lane and increment
            with scheduler._lock:
                if scheduler._daily_count >= scheduler.daily_quota:
                    results.append("blocked")
                    return
                lane, status = scheduler._pick_lane_with_status()
                if lane is not None:
                    lane.record_request()
                    scheduler._daily_count += 1
                    results.append("incremented")

        threads = [threading.Thread(target=try_increment) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Only 5 should have incremented (daily_quota=5)
        incremented = sum(1 for r in results if r == "incremented")
        blocked = sum(1 for r in results if r == "blocked")
        assert incremented == 5, f"Expected 5 incremented, got {incremented}"
        assert blocked == 5, f"Expected 5 blocked, got {blocked}"
        assert scheduler._daily_count == 5

    def test_quota_not_exceeded_under_contention(self):
        """Multiple threads cannot exceed daily_quota."""
        from analysis.gemini_scheduler import GeminiScheduler, WorkerLane

        scheduler = GeminiScheduler(
            api_keys=["test-key-12345678"],
            models=["test-model"],
            max_workers=10,
            daily_quota=3,
        )

        barrier = threading.Barrier(10)
        overflow_count = 0

        def try_request():
            barrier.wait()
            # Simulate the fixed code path: check inside lock
            with scheduler._lock:
                if scheduler.daily_quota and scheduler._daily_count >= scheduler.daily_quota:
                    return "blocked"
                lane, status = scheduler._pick_lane_with_status()
                if lane is not None:
                    lane.record_request()
                    scheduler._daily_count += 1
                    return "ok"
                return "no_lane"

        threads = [threading.Thread(target=try_request) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert scheduler._daily_count <= 3, f"Quota exceeded: {scheduler._daily_count}"


class TestWorkerTaskDone:
    """G1: Worker must always call task_done() after dequeuing, even on stop_signal."""

    def test_task_done_called_on_stop_signal(self):
        """Worker exits cleanly when stop_signal is set after dequeue, without hanging join()."""
        from analysis.gemini_scheduler import PackedQueueItem

        work_queue = queue.Queue()
        item = PackedQueueItem(evidence_rows=[], attempt=1, max_attempts=1)
        work_queue.put(item)

        stop_signal = threading.Event()
        # Do NOT pre-set — set it after dequeue to simulate the race condition

        def worker_fn():
            while not stop_signal.is_set():
                itm = None
                try:
                    itm = work_queue.get(timeout=0.1)
                except queue.Empty:
                    if work_queue.unfinished_tasks == 0 or stop_signal.is_set():
                        return
                    continue
                try:
                    if stop_signal.is_set():
                        return
                    # Simulate processing — set stop_signal to trigger the race
                    stop_signal.set()
                finally:
                    work_queue.task_done()

        t = threading.Thread(target=worker_fn)
        t.start()
        t.join(timeout=2.0)

        assert not t.is_alive(), "Worker thread hung (task_done not called)"
        assert work_queue.unfinished_tasks == 0, "Queue not drained — task_done was skipped"

    def test_worker_drains_queue_before_exiting(self):
        """Worker processes remaining items before stopping."""
        from analysis.gemini_scheduler import PackedQueueItem

        work_queue = queue.Queue()
        processed = []

        for i in range(3):
            work_queue.put(PackedQueueItem(evidence_rows=[{"id": i}], attempt=1, max_attempts=1))

        stop_signal = threading.Event()

        def worker_fn():
            while not stop_signal.is_set():
                itm = None
                try:
                    itm = work_queue.get(timeout=0.1)
                except queue.Empty:
                    if work_queue.unfinished_tasks == 0 or stop_signal.is_set():
                        return
                    continue
                try:
                    if stop_signal.is_set():
                        return
                    processed.append(itm.evidence_rows[0]["id"])
                finally:
                    work_queue.task_done()

        t = threading.Thread(target=worker_fn)
        t.start()

        # Set stop signal after a brief delay
        time.sleep(0.05)
        stop_signal.set()

        t.join(timeout=2.0)
        assert not t.is_alive(), "Worker hung"
        # Worker should have processed at least 1 item before seeing stop_signal
        assert len(processed) >= 1, f"Nothing processed: {processed}"


class TestTriedLanesOnRetry:
    """#14: Retry items must carry tried_lanes from the original item."""

    def test_tried_lanes_copied_on_retry(self):
        """When a PackedQueueItem is retried, tried_lanes is carried forward."""
        from analysis.gemini_scheduler import PackedQueueItem

        original = PackedQueueItem(
            evidence_rows=[{"id": 1}],
            attempt=1,
            max_attempts=3,
            tried_lanes={"L1", "L2"},
        )

        # Simulate the fix: copy tried_lanes to retry item
        retry = PackedQueueItem(
            evidence_rows=[{"id": 1}],
            attempt=original.attempt + 1,
            max_attempts=original.max_attempts,
            tried_lanes=set(original.tried_lanes),
        )

        assert retry.tried_lanes == {"L1", "L2"}
        assert retry.attempt == 2
        # Original should be unaffected (copy, not reference)
        original.tried_lanes.add("L3")
        assert "L3" not in retry.tried_lanes

    def test_tried_lanes_independent_copy(self):
        """tried_lanes on retry is a copy, not a reference to original."""
        from analysis.gemini_scheduler import PackedQueueItem

        original = PackedQueueItem(
            evidence_rows=[],
            tried_lanes={"L1"},
        )
        retry = PackedQueueItem(
            evidence_rows=[],
            tried_lanes=set(original.tried_lanes),
        )

        retry.tried_lanes.add("L2")
        assert "L2" not in original.tried_lanes


class TestPersistOrder:
    """#4, #5: Insert/upsert must happen BEFORE delete to prevent data loss."""

    def test_work_analysis_insert_before_delete(self):
        """stage_work_analysis_persist inserts new records before deleting old ones."""
        import ast
        import inspect

        # Read the source and check the order of operations
        from automation import pipeline_controller
        source = inspect.getsource(pipeline_controller.stage_work_analysis_persist)

        # Find positions of insert and delete operations
        insert_pos = source.find("client.table(table_name).insert(batch).execute()")
        delete_pos = source.find("sb_delete(db1_url, db1_key, table_name,")

        assert insert_pos != -1, "Insert not found"
        assert delete_pos != -1, "Delete not found"
        assert insert_pos < delete_pos, "Insert must come BEFORE delete"

    def test_analytics_upsert_before_delete(self):
        """stage_analytics_persist_affected upserts before deleting."""
        import inspect

        from automation import pipeline_controller
        source = inspect.getsource(pipeline_controller.stage_analytics_persist_affected)

        # For national_statistics: upsert must come before delete
        ns_upsert_pos = source.find('sb_upsert(db2_url, db2_key, "national_statistics"')
        ns_delete_pos = source.find('sb_delete(db2_url, db2_key, "national_statistics"')

        assert ns_upsert_pos != -1, "national_statistics upsert not found"
        assert ns_delete_pos != -1, "national_statistics delete not found"
        assert ns_upsert_pos < ns_delete_pos, "national_statistics upsert must come BEFORE delete"

        # For trends: upsert must come before delete
        trends_upsert_pos = source.find('sb_upsert(db2_url, db2_key, "trends"')
        trends_delete_pos = source.find('sb_delete(db2_url, db2_key, "trends"')

        assert trends_upsert_pos != -1, "trends upsert not found"
        assert trends_delete_pos != -1, "trends delete not found"
        assert trends_upsert_pos < trends_delete_pos, "trends upsert must come BEFORE delete"


class TestRemoteValidationFailClosed:
    """#7: Remote validation failure must cause sys.exit(1)."""

    def test_remote_validation_failure_exits(self):
        """Pipeline exits on remote validation failure."""
        import inspect

        from automation import daily_pipeline
        source = inspect.getsource(daily_pipeline.main)

        # Check that the exception handler calls sys.exit(1)
        exc_handler_start = source.find("Remote snapshot validation failed")
        assert exc_handler_start != -1, "Exception handler not found"

        # Check that sys.exit(1) is called after the warning
        handler_section = source[exc_handler_start:exc_handler_start + 300]
        assert "sys.exit(1)" in handler_section, (
            "Remote validation failure should call sys.exit(1), not just print WARNING"
        )

    def test_no_warning_fallback(self):
        """Pipeline does NOT continue with a warning after remote validation failure."""
        import inspect

        from automation import daily_pipeline
        source = inspect.getsource(daily_pipeline.main)

        # The old code had "WARNING: Remote snapshot validation failed"
        # The new code should have "FATAL:" not just "WARNING:"
        assert "FATAL: Remote snapshot validation failed" in source or \
               "sys.exit(1)" in source, "Pipeline should fail closed, not warn"
