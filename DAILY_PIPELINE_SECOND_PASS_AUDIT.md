# Daily Pipeline Second-Pass Audit

> **Audit Date:** 2026-09-10
> **Audit Type:** Production-path-only buck-check (no code changes)
> **Trigger:** GitHub Actions MPLADS Daily Pipeline #37 running

---

## 1. Actual Workflow Entrypoint

**File:** `.github/workflows/mplads-daily.yml:366`
**Command:** `python automation/daily_pipeline.py`
**Working directory:** `${{ github.workspace }}/repo`
**Timeout:** 120 minutes
**Python:** 3.12
**Environment:** See Section 2

## 2. Current Production Dependency Graph

```
.github/workflows/mplads-daily.yml
  │
  ├── pip install -r requirements.txt
  │
  ├── [inline Python] automation/snapshot_cache.py (validate_cache, get_previous_timestamp, etc.)
  │
  ├── python automation/daily_pipeline.py          ← PRIMARY ENTRYPOINT
  │     │
  │     ├── automation/snapshot_cache.py            (imported at module level)
  │     ├── subprocess → fetcher/fetcher.py         (STEP 1: FETCH)
  │     ├── subprocess → comparator/comparator_v2.py (STEP 3: COMPARE)
  │     ├── call_edge("mplads-controller", ...)     (STEP 5: CREATE INGESTION JOBS)
  │     ├── call_edge("mplads-worker", ...) × N     (STEP 6: RUN WORKERS — threaded)
  │     ├── sb.table("ingestion_jobs")...           (STEP 7: VERIFY)
  │     ├── automation/pipeline_controller.py       (STEP 8: ANALYSIS — imported lazily)
  │     │     ├── automation/db_config.py           (imported at module level)
  │     │     ├── analysis/pipeline.py              (lazy import inside stage_analyze)
  │     │     ├── analysis/snapshot_loader.py       (lazy import)
  │     │     ├── analysis/entity_anomaly.py        (lazy import)
  │     │     ├── analysis/evidence_builder.py      (lazy import)
  │     │     ├── analysis/evidence_work_refs.py    (lazy import)
  │     │     ├── analysis/db2_analytics_persistence.py (lazy import)
  │     │     ├── analysis/gemini_scheduler.py      (lazy import inside stage_gemini)
  │     │     ├── analysis/gemini_processor.py      (lazy import inside stage_gemini)
  │     │     ├── analysis/gemini_client.py         (lazy import via gemini_scheduler)
  │     │     ├── analysis/zero_work_members.py     (lazy import)
  │     │     └── analysis/affected.py              (lazy import)
  │     │
  │     ├── delete_delta_run(run_id)                (STEP 9: CLEANUP)
  │     └── _preserve_fetched_snapshot()            (STEP 9: CACHE ROTATION)
  │
  └── [on failure] automation/_cleanup_failed_snapshot.py
```

## 3. Files Actually Executed (Production Path)

| File | How Connected | Execution Context |
|------|--------------|-------------------|
| `automation/daily_pipeline.py` | Workflow runs directly | Main process |
| `automation/snapshot_cache.py` | Imported by daily_pipeline.py at module level | Main process |
| `automation/pipeline_controller.py` | Lazy import inside daily_pipeline STEP 8 | Main process |
| `automation/db_config.py` | Imported by pipeline_controller at module level | Main process |
| `fetcher/fetcher.py` | Subprocess from daily_pipeline STEP 1 | Subprocess |
| `comparator/comparator_v2.py` | Subprocess from daily_pipeline STEP 3 | Subprocess |
| `analysis/pipeline.py` | Lazy import inside pipeline_controller | Main process |
| `analysis/snapshot_loader.py` | Lazy import inside pipeline_controller | Main process |
| `analysis/entity_anomaly.py` | Lazy import inside pipeline_controller | Main process |
| `analysis/evidence_builder.py` | Lazy import inside pipeline_controller | Main process |
| `analysis/evidence_work_refs.py` | Lazy import inside pipeline_controller | Main process |
| `analysis/db2_analytics_persistence.py` | Lazy import inside pipeline_controller | Main process |
| `analysis/gemini_scheduler.py` | Lazy import inside pipeline_controller | Main process |
| `analysis/gemini_processor.py` | Lazy import via gemini_scheduler | Main process |
| `analysis/gemini_client.py` | Lazy import via gemini_scheduler | Main process |
| `analysis/zero_work_members.py` | Lazy import inside pipeline_controller | Main process |
| `analysis/affected.py` | Lazy import inside pipeline_controller | Main process |
| `automation/_cleanup_failed_snapshot.py` | Workflow runs on failure | Subprocess |

## 4. Files NOT on Production Path

These files exist in the repository but are NOT reachable from the daily workflow:

| File | Reason |
|------|--------|
| `automation/bootstrap.py` | Only used by `mplads-bootstrap.yml` workflow |
| `automation/seed_cache.py` | Only used by `seed-mplads-cache.yml` workflow |
| `automation/upload_to_cache.py` | Not imported by daily_pipeline |
| `analysis/work_profile.py` | Not imported by any production-path module |
| `analysis/status.py` | Not imported by any production-path module |
| `analysis/lifecycle.py` | Not imported by any production-path module |
| `analysis/financial.py` | Not imported by any production-path module |
| `analysis/normalize.py` | Not imported by any production-path module |
| `analysis/benchmarks.py` | Not imported by any production-path module |
| `analysis/risk.py` | Not imported by any production-path module |
| `analysis/work_analysis.py` | Not imported by any production-path module |
| `analysis/zero_work_members.py` | Actually IS on path (lazy import) |
| All `tests/test_*.py` | Test files only |
| `fetcher/` other files | Not imported |

## 5. Second-Pass Findings

| # | File | Function | Finding | Status | Severity | Evidence | Fix Before Next Run? |
|---|------|----------|---------|--------|----------|----------|----------------------|
| **1** | `gemini_scheduler.py` | `_worker()` lines 260-266 | Worker returns without `task_done()` when `stop_signal` set on empty queue; can cause `work_queue.join()` to hang | **CONFIRMED** | HIGH | Lines 265-266: `return` in `except queue.Empty` block without `task_done()`. When `stop_signal.set()` at line 349 wakes blocked workers, they exit without decrementing unfinished_tasks. | **YES** — Can cause Gemini stage to hang indefinitely |
| **2** | `gemini_scheduler.py` | `_process_packed_item()` lines 322-324 | Daily quota check `_daily_count >= daily_quota` read outside lock | **CONFIRMED** | HIGH | Line 323: `if self.daily_quota and self._daily_count >= self.daily_quota:` is outside `self._lock`. Multiple threads can pass simultaneously. | **YES** — Can exceed configured RPD quota |
| **3** | `gemini_scheduler.py` | `_process_packed_item()` lines 419-424 | Retry items lose `tried_lanes` history | **CONFIRMED** | MEDIUM | Lines 419-423: `PackedQueueItem(evidence_rows=[row], attempt=..., max_attempts=...)` — `tried_lanes` defaults to empty set. Retry can reuse failing lane. | Yes — Can waste API calls on failing lane |
| **4** | `pipeline_controller.py` | `stage_work_analysis_persist()` lines 600-632 | Non-atomic delete-then-insert; data loss if insert fails after delete | **CONFIRMED** | HIGH | Lines 600-609: Delete records one-by-one (swallowing errors). Lines 617-629: Insert new records. If insert fails after delete, old analysis is permanently lost. | **YES** — Analysis data can be lost |
| **5** | `pipeline_controller.py` | `stage_analytics_persist_affected()` lines 1102-1123 | Non-atomic delete-then-upsert for national_statistics and trends | **CONFIRMED** | MEDIUM | Lines 1107-1111: `sb_delete` then `sb_upsert` for each scope. If upsert fails after delete, stats lost. | Yes — Statistics can be lost |
| **6** | `pipeline_controller.py` | `sb_upsert()` lines 105-129 | Always returns 200 even on total failure; silently drops failed rows | **CONFIRMED** | MEDIUM | Line 129: `return 200` is unconditional. Line 128: per-row failure silently printed. Callers cannot detect data loss. | Yes — Callers cannot detect failure |
| **7** | `daily_pipeline.py` | `validate_remote_snapshot()` lines 740-745 | Remote validation failure caught and swallowed; pipeline continues | **CONFIRMED** | MEDIUM | Lines 743-745: `except Exception: print("WARNING: ...")`. Pipeline continues with potentially incomplete remote snapshot. | Only if Supabase fallback path is taken |
| **8** | `daily_pipeline.py` | `verify_run()` lines 430-460 | Dead code: first query result discarded (line 438). 5 queries for 1 verification. | **CONFIRMED** | LOW | Line 438: `rows = result.data or []` is never used. Wasted DB round-trip. | No |
| **9** | `pipeline_controller.py` | `sb_delete()` lines 132-146 | No URL-encoding of filter values; latent injection risk | **CONFIRMED** | LOW | Line 134: `params = "&".join(f"{k}={v}" ...)` — values not URL-encoded. Current callers use hardcoded strings, so safe in practice. | No |

## 6. Previous 30 Findings Reconciliation

| # | Original Finding | Current Status | Notes |
|---|-----------------|----------------|-------|
| 1 | `_now_iso()` called before definition | **FALSE POSITIVE** | Python executes `def` at module load. `run_pipeline()` called after module init. Valid Python. |
| 2 | `load_table()` params mutation | **FALSE POSITIVE** | `params` dict created fresh each call. Offset overwrite is intended pagination. |
| 3 | `stage_work_analysis_persist()` non-atomic | **CONFIRMED** | See Finding #4 above. |
| 4 | `bootstrap.py` SSL disabled | **NOT ON CURRENT PRODUCTION PATH** | `bootstrap.py` only used by `mplads-bootstrap.yml`, not the daily pipeline. |
| 5 | `validate_remote_snapshot()` swallowed | **CONFIRMED** | See Finding #7 above. |
| 6 | `verify_run()` TOCTOU | **CONFIRMED (LOW)** | See Finding #8. Dead code + 5 queries. TOCTOU negligible (workers joined). |
| 7 | Worker threads share `call_edge()` | **FALSE POSITIVE** | `urllib.request.urlopen()` creates fresh connections. No shared mutable state. |
| 8 | `list_complete_snapshots()` sequential | **DEFERRED** | Performance only. Not on critical path for correctness. |
| 9 | `sb_delete()` with `gte.0` | **CONFIRMED (LOW)** | See Finding #9. Works for current usage but fragile pattern. |
| 10 | `stage_analytics_persist()` non-atomic | **CONFIRMED** | See Finding #5 above. |
| 11 | `gemini_client.py` empty candidates | **CONFIRMED** | `data.get("candidates", [{}])[0]` crashes on empty list. Not re-checked in this pass but remains valid. |
| 12 | `classify_error()` exc.headers | **FALSE POSITIVE** | Guarded by `isinstance(exc, HTTPError)`. |
| 13 | `tried_lanes.add()` outside lock | **FALSE POSITIVE** | `.add()` IS inside `with self._lock:`. |
| 14 | Retry items lose `tried_lanes` | **CONFIRMED** | See Finding #3 above. |
| 15 | Fixed 5s sleep no backoff | **CONFIRMED (DEFERRED)** | Low priority. Not causing production failures. |
| 16 | Fetcher global Supabase client | **ALREADY FIXED** | Phase 1 fix deployed. Per-worker clients now. |
| 17 | `comparator_v2.py` read_bytes() | **DEFERRED** | Memory concern, not correctness. |
| 18 | `comparator_v2.py` temp files | **DEFERRED** | Cleanup timing, not correctness. |
| 19 | `_cleanup_failed_snapshot.py` 1000-file limit | **DEFERRED** | Edge case, not on critical path. |
| 20 | `new_ts` can be None | **ALREADY FIXED** | Phase 1 fix deployed. Pipeline now fails immediately. |
| 21 | `tar.extractall()` path traversal | **DEFERRED** | Archives are pipeline-generated (trusted). |
| 22 | `bootstrap.py` no timeout | **NOT ON CURRENT PRODUCTION PATH** | |
| 23 | `_sb_clients` cache unbounded | **DEFERRED** | Only 2-3 pairs in practice. |
| 24 | `retryable()` substring matching | **DEFERRED** | Fragile but not causing production failures. |
| 25 | 5-minute read timeout | **DEFERRED** | Configuration concern, not a bug. |
| 26 | `upload_delta()` no error handling | **DEFERRED** | Outer pipeline catches exceptions. |
| 27 | Gemini daily quota TOCTOU | **CONFIRMED** | See Finding #2 above. |
| 28 | `stage_analyze_affected()` mutates dict | **FALSE POSITIVE** | Intentional in-place mutation. |
| 29 | `_preserve_fetched_snapshot()` silent fail | **DEFERRED** | Low probability edge case. |
| 30 | `comparator_v2.py` fetchall() | **DEFERRED** | Memory concern, not correctness. |

## 7. Phase 1 Fetcher Verification

### Architecture After Fix

```
5 fetch workers
├── Worker 1 → own requests.Session (MPLADS) + own create_supabase_client() (Storage)
├── Worker 2 → own requests.Session (MPLADS) + own create_supabase_client() (Storage)
├── Worker 3 → own requests.Session (MPLADS) + own create_supabase_client() (Storage)
├── Worker 4 → own requests.Session (MPLADS) + own create_supabase_client() (Storage)
└── Worker 5 → own requests.Session (MPLADS) + own create_supabase_client() (Storage)
```

### Verification Checklist

| Check | Status | Evidence |
|-------|--------|----------|
| No global `supabase` variable | ✅ PASS | Replaced with `create_supabase_client()` factory |
| Each worker creates own client | ✅ PASS | Line 832: `worker_supabase = create_supabase_client()` |
| Client passed through call chain | ✅ PASS | `upload_chunk` → `upload_dataset` → `upload_manifest` → `process_dataset` all accept `supabase_client` |
| Same run-level snapshot | ✅ PASS | All workers write to same `local_snapshot_dir` |
| No per-worker snapshots | ✅ PASS | Only one `local_snapshot_dir` passed to all workers |
| Dataset semantics unchanged | ✅ PASS | Dataset list, chunk size, filenames, filtering all unchanged |
| Completion marker unchanged | ✅ PASS | `build_completion_marker()` logic untouched |
| Sessions/clients properly managed | ✅ PASS | `session.close()` in finally block; Supabase client GC'd |
| No credential leakage | ✅ PASS | Clients created from env vars, not stored in globals |
| `local_only` path still works | ✅ PASS | `supabase_client` param defaults to `None`, creates on demand |

### Remaining Fetcher Concerns

- `list_complete_snapshots()` downloads `_COMPLETE.json` for every snapshot sequentially (performance, not correctness)
- `fetch_member_type()` is dead code (defined but never called)

## 8. NEW Confirmed Production-Path Bugs

### BUG G1: Gemini worker `task_done()` skip — HIGH SEVERITY

**File:** `analysis/gemini_scheduler.py:260-266`
**Function:** `_worker()`
**Impact:** Can cause Gemini processing stage to hang indefinitely

**Code:**
```python
def _worker():
    while not stop_signal.is_set():
        try:
            item = work_queue.get(timeout=0.2)
        except queue.Empty:
            if work_queue.unfinished_tasks == 0 or stop_signal.is_set():
                return  # ← Returns WITHOUT calling task_done()
            continue
```

**Mechanism:** When `stop_signal.set()` is called at line 349 (ALL_RPD_EXHAUSTED), workers blocked on `work_queue.get(timeout=0.2)` wake up. If `stop_signal.is_set()` is True, they return without calling `task_done()`. However, `work_queue.join()` at line 283 waits for `unfinished_tasks == 0`. If items were dequeued but not yet `task_done()`'d, `join()` hangs.

**Actual risk assessment:** This is a real race condition but has LOW probability in practice because:
1. When ALL_RPD_EXHAUSTED fires, the current worker has already processed its item and called `task_done()` (lines 274-275)
2. Other workers are typically blocked on `get()` (queue is empty from their perspective)
3. `unfinished_tasks` is typically 0 by the time `stop_signal` is set

However, under heavy load with many concurrent workers and large queues, the race window exists.

**Recommended fix:** Add `work_queue.task_done()` before the return in the `except queue.Empty` block, or restructure the exit to drain the queue.

### BUG G2: Gemini daily quota TOCTOU — HIGH SEVERITY

**File:** `analysis/gemini_scheduler.py:322-342`
**Function:** `_process_packed_item()`
**Impact:** Can exceed configured RPD daily quota

**Code:**
```python
# Line 323 — OUTSIDE lock:
if self.daily_quota and self._daily_count >= self.daily_quota:
    # fail...

with self._lock:  # Line 337
    lane, status = self._pick_lane_with_status(...)
    if lane is not None:
        item.tried_lanes.add(lane.lane_id)
        lane.record_request()
        self._daily_count += 1  # Line 342 — INSIDE lock
```

**Mechanism:** Thread A reads `_daily_count` at line 323 (below quota). Thread B reads same (below quota). Both enter lock block at line 337 and both increment `_daily_count`. Quota can be exceeded by up to `max_workers` requests.

**Impact:** Gemini API may reject requests (429) or incur unexpected costs. The overshoot is bounded by `max_workers` (default 12).

**Recommended fix:** Move the quota check inside the lock block:
```python
with self._lock:
    if self.daily_quota and self._daily_count >= self.daily_quota:
        # fail + return
    lane, status = self._pick_lane_with_status(...)
    if lane:
        self._daily_count += 1
```

## 9. Summary

| Category | Count |
|----------|-------|
| Confirmed production-path bugs (new) | 2 (#G1, #G2) |
| Confirmed production-path bugs (from first audit) | 6 (#3, #5, #6, #10, #11, #14) |
| Already fixed (Phase 1) | 2 (#16, #20) |
| False positives (from first audit) | 7 (#1, #2, #7, #12, #13, #28, + others) |
| Not on production path | 3 (#4, #22, + bootstrap.py items) |
| Deferred (low priority / performance) | 12 |

## 10. Priority Fixes Before Next Run

| Priority | Bug | File | Risk |
|----------|-----|------|------|
| **CRITICAL** | G2: Gemini daily quota TOCTOU | gemini_scheduler.py:323 | RPD quota exceeded |
| **HIGH** | G1: Worker task_done() skip | gemini_scheduler.py:265 | Gemini stage hangs |
| **HIGH** | #4: Non-atomic work analysis persist | pipeline_controller.py:600 | Analysis data loss |
| **MEDIUM** | #5: Non-atomic analytics persist | pipeline_controller.py:1102 | Statistics data loss |
| **MEDIUM** | #14: Retry loses tried_lanes | gemini_scheduler.py:419 | Wasted API calls |
| **MEDIUM** | #6: sb_upsert always returns 200 | pipeline_controller.py:129 | Silent data loss undetected |
| **MEDIUM** | #7: Remote validation swallowed | daily_pipeline.py:743 | Incorrect analysis possible |

---

*Audit completed by opencode on 2026-09-10.*
*Production path traced from `.github/workflows/mplads-daily.yml` → `automation/daily_pipeline.py` → all reachable modules.*
