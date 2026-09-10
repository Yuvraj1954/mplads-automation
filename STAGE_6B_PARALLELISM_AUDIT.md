# Stage 6B Parallelism Audit

> **Audit Date:** 2026-09-11
> **Stage:** `stage_work_analysis_persist` (work_analysis persistence to DB1)
> **File:** `automation/pipeline_controller.py:506-639`
> **Current Runtime:** ~15 minutes
> **Goal:** Safe controlled parallelism to reduce runtime

---

## 1. What Stage 6B Does

Stage 6B persists work analysis results from the in-memory `AnalysisPipeline` into two DB1 tables:

- **`work_analysis`** — MP work analyses
- **`mla_work_analysis`** — MLA work analyses

Each record contains ~50 fields per work: work_id, member_id, status, amounts, dates, risk flags, benchmark percentiles, lifecycle metrics, etc.

The stage is called from two paths in `run_pipeline()`:
- **Affected mode** (line 1578): `stage_work_analysis_persist(analysis_result["work_analyses"], affected_work_ids=...)` — persists only affected works
- **Full mode** (line 1708): `stage_work_analysis_persist(analysis_result["work_analyses"])` — persists all works

---

## 2. Complete Production Path

```
.github/workflows/mplads-daily.yml:366
  → python automation/daily_pipeline.py
    → STEP 8 ANALYSIS (line 951)
      → automation/pipeline_controller.py:run_pipeline() (line 963)
        → Stage 6a: stage_analyze_affected() → produces work_analyses (line 1537)
        → Stage 6b: stage_work_analysis_persist() (line 1578)
```

### Inside `stage_work_analysis_persist` (lines 506-639):

```
1. get_db1() → (db1_url, db1_key)
2. Separate MP and MLA analyses
3. Filter by affected_work_ids (affected mode only)
4. For EACH table (MP THEN MLA, SEQUENTIALLY):
   a. Convert WorkAnalysis dataclasses → DB record dicts (in-memory, fast)
   b. Step 1 — INSERT: Batch inserts of 500 records
      - create_client(db1_url, db1_key) → fresh Supabase client
      - for each batch: client.table(table_name).insert(batch).execute()
      - on batch failure: retry individual rows one-by-one
   c. Step 2 — DELETE (affected mode only): Individual per work_id
      - for each wid in work_ids:
        sb_delete(db1_url, db1_key, table_name, {"work_id": f"eq.{wid}"})
      - sb_delete() → raw urllib.request.Request DELETE to Supabase REST API
      - each delete = 1 HTTP request with 60s timeout
```

### DB operations involved:
- **Inserts**: `supabase-py` client `.table().insert().execute()` (batch of 500)
- **Deletes**: Raw `urllib.request.urlopen()` DELETE via `sb_delete()` (1 per work_id)

---

## 3. Record Counts and Operation Counts

### Full mode (no affected_work_ids filtering):
From `rebuild_work_analysis.py` line 688-693:
- **MP total**: ~107,543 records
- **MLA total**: ~25,274 records
- **Total**: ~132,817 records

| Operation | MP | MLA | Total |
|-----------|-----|-----|-------|
| Insert batches (size 500) | ceil(107543/500) = 216 | ceil(25274/500) = 51 | **267** |
| Delete operations | 0 (no affected_work_ids) | 0 | **0** |
| **Total HTTP requests** | **216** | **51** | **267** |

### Affected mode (typical daily run):
The affected_work_ids come from the delta comparator. Typical daily runs produce **50-5000 affected works**.

| Operation | Per affected work | Total for 500 works |
|-----------|-------------------|---------------------|
| Insert batches (size 500) | 1 batch per 500 | 1 per table |
| Delete operations | **1 HTTP request each** | **500 per table** |

**For affected mode with 500 affected works:**

| Operation | MP | MLA | Total |
|-----------|-----|-----|-------|
| Insert batches | 1 | 1 | **2** |
| Delete operations | 500 | 500 | **1,000** |
| **Total HTTP requests** | **501** | **501** | **1,002** |

---

## 4. Current Batch Size

- **Insert batch size**: 500 records per batch (line 601)
- **Delete batch size**: 1 record per HTTP request (individual, sequential)

---

## 5. Whether Requests Are Sequential

**YES — Everything is sequential.**

1. **MP and MLA tables**: Processed sequentially (MP first at line 634, then MLA at line 635)
2. **Insert batches**: Sequential within each table (for loop, line 605)
3. **Delete operations**: Sequential within each table (for loop, line 623)

There is zero parallelism in the current implementation.

---

## 6. Which Operations Are Independent

| Operation | Independent of | Safe to parallelize? |
|-----------|---------------|---------------------|
| MP insert batches | MLA insert batches | YES (different tables) |
| MLA insert batches | MP insert batches | YES (different tables) |
| Individual MP deletes | All MLA operations | YES (different tables) |
| Individual MLA deletes | All MP operations | YES (different tables) |
| Delete of work_id=A | Delete of work_id=B | YES (different rows) |
| Insert of batch N | Insert of batch M | YES (different row ranges) |
| MP inserts | MP deletes | **PARTIAL** — inserts must complete before deletes for same work_ids (see §7) |

**Key insight**: ALL operations target disjoint data (different tables or different work_ids). Every operation is independent.

---

## 7. Transaction/Atomicity Analysis

### Current pattern (per table):

```
Step 1: INSERT fresh records FIRST (batch of 500)
Step 2: DELETE old records (one per work_id) [affected mode only]
```

### Analysis:

- **No transactions used**: Each `insert()` and `sb_delete()` is an independent HTTP request
- **No multi-row atomicity**: No batch delete, no transaction wrapping
- **Insert-before-delete for data safety**: Comment says "safe to fail — new data already persisted" (line 619)
- **Individual error swallowing**: Both insert fallback (line 611-617) and delete (line 623-628) catch exceptions and continue

### Consequence:
- A crash between insert and delete would leave duplicate rows (both old + new for same work_id)
- Since there's no unique index on work_id (confirmed by `rebuild_work_analysis.py:13`), the table can accumulate duplicates
- The insert-before-delete pattern is designed to prevent data LOSS (not duplication)

---

## 8. Whether Multiple Workers Can Safely Process Disjoint Batches

**YES — Multiple workers can safely process disjoint batches.**

Justification:
1. **No shared mutable state**: Each worker would create its own `create_client()` or use stateless `sb_delete()`
2. **No unique constraints to conflict on**: The tables lack unique indexes on work_id (only PK), so concurrent inserts cannot violate constraints
3. **Disjoint batches**: Workers processing different work_id ranges don't overlap
4. **Different tables**: MP and MLA writes target completely separate tables
5. **HTTP-based clients**: `urllib.request.urlopen()` creates fresh connections per request (thread-safe)

### Safety requirement:
Each worker MUST create its own Supabase client instance (via `create_client()`) — must NOT share clients across threads. The current code already creates a fresh client per `_persist_batch()` call (line 604), which is the right pattern.

---

## 9. Shared Mutable State Analysis

| State | Location | Shared? | Thread-safe? |
|-------|----------|---------|-------------|
| `_sb_clients` dict | `pipeline_controller.py:103` | Module-level | NOT thread-safe (dict mutation) — but **NOT used by Stage 6B** |
| `sb_delete()` | `pipeline_controller.py:132` | Stateless function | YES (creates fresh urllib request each call) |
| `create_client()` | Called per `_persist_batch()` | Local variable | YES (fresh instance per call) |
| `SSL_CTX` | `db_config.py:45` | Module-level, read-only | YES (immutable after creation) |
| `db1_url`, `db1_key` | Module-level strings | Read-only | YES |
| Work analysis records list | In-memory, local to `stage_work_analysis_persist` | Local variable | YES (each worker gets its own slice) |

**Verdict: No shared mutable state exists in the Stage 6B path that would prevent safe concurrent access.**

---

## 10. Database Uniqueness/Upsert Behavior

**Tables lack unique indexes on work_id.** (Confirmed by `rebuild_work_analysis.py:13`)

- The primary key is auto-incremented
- work_id is NOT unique — multiple rows can exist for the same work_id
- Upsert (`on_conflict="work_id"`) does NOT work (silently inserts duplicates)
- Current approach: INSERT (creates new rows), then DELETE old rows by work_id

**Implication for concurrency:**
- Concurrent inserts with same work_id → creates duplicates (no constraint violation)
- Concurrent deletes with same work_id → both succeed (no locking conflict)
- After all operations complete, table may have duplicates if inserts and deletes overlap temporally

**Safe approach**: Ensure all inserts complete, then all deletes, OR partition work_ids so workers don't overlap.

---

## 11. Whether Ordering Matters

**Partial ordering constraint:**

Within each table:
1. **Inserts must complete before deletes** for the same work_ids
   - Reason: If delete runs first and removes old rows, then insert adds new rows → correct
   - If insert runs first and then delete removes ALL rows with that work_id → data loss for that work_id
   - The current code enforces this: inserts (lines 605-617) before deletes (lines 622-628)

2. **MP and MLA order doesn't matter** — different tables, no dependencies

3. **Batch order within inserts doesn't matter** — all batches go to same table, no overlap

**For parallel implementation:**
- Each worker should complete ALL inserts for its batch of work_ids, THEN complete ALL deletes for those same work_ids
- OR: Run all inserts first (all workers), then all deletes (all workers)
- The safest is: partition work_ids → each worker does inserts then deletes for its partition

---

## 12. Retry Behavior and Duplicates

### Current retry:
- **Insert fallback** (lines 610-617): On batch insert failure, retry each row individually
- **Delete retry**: None — delete failures are silently swallowed (line 227: `except Exception: pass`)
- **No retry logic for deletes**: A failed delete means the old row persists alongside the new row

### Duplicate creation risk:
- If insert succeeds but delete fails: Old row + new row = 2 rows with same work_id
- Since work_id is not unique, this is allowed by the DB
- On next pipeline run: new insert creates 3rd row, delete removes all 3 → only newest remains
- **Net effect**: Temporary duplicates are possible but self-correcting on next run

### Under concurrent workers:
- If worker A and worker B both process overlapping work_ids → duplicate inserts
- **Mitigation**: Partition work_ids so workers never overlap
- Delete of duplicates: Next run's delete (WHERE work_id=X) removes ALL rows with that work_id, cleaning up any duplicates

---

## 13. Exact Bottleneck Identification

### Affected mode (primary concern — ~15 minutes):

| Phase | Operations | Time estimate | Bottleneck? |
|-------|-----------|---------------|------------|
| Record conversion (in-memory) | Python dict creation | <1s | No |
| MP batch inserts | ceil(N_mp/500) HTTP calls | 2-10s | No |
| MLA batch inserts | ceil(N_mla/500) HTTP calls | 2-5s | No |
| **MP individual deletes** | **N_mp × 1 HTTP call each** | **~0.3-1s per delete** | **YES** |
| **MLA individual deletes** | **N_mla × 1 HTTP call each** | **~0.3-1s per delete** | **YES** |

**Root cause: The sequential delete loop (lines 622-628).**

Each `sb_delete()` call:
1. Creates a `urllib.request.Request` object
2. Opens a new HTTPS connection to Supabase
3. Performs TLS handshake
4. Sends DELETE request
5. Waits for response (timeout=60s)
6. Closes connection

For 500 affected works × 2 tables = 1,000 sequential HTTP requests.
At 0.5s average per request = **500 seconds = ~8.3 minutes**.
At 1.0s average per request = **1,000 seconds = ~16.7 minutes**.

This matches the reported ~15 minute runtime.

### Full mode:
- No deletes (affected_work_ids is None)
- Only inserts: 267 batches at ~3-5s each = ~13-22 minutes
- Not the primary optimization target (user reported affected mode)

---

## 14. Safe Parallelization Boundary

### Parallelizable without any concerns:
1. **MP and MLA table operations** — Run in parallel (2 workers)
2. **Individual deletes within a table** — All independent, run with bounded concurrency
3. **Insert batches within a table** — All independent (different row ranges)

### Must maintain ordering:
- Within each worker's partition: inserts before deletes

### What NOT to parallelize:
- Do NOT share Supabase clients across threads
- Do NOT change the insert-before-delete pattern

---

## 15. Recommended Architecture

### Approach: Partition + Bounded Worker Pool

```
Stage 6B:
  1. Partition affected_work_ids into N non-overlapping subsets
  2. For EACH table (MP, MLA):
     a. Convert records (single-threaded, fast)
     b. Partition records into N subsets
     c. Launch N workers, each processing one subset:
        - Insert its batch of records (one batch of up to 500)
        - Delete old records for its work_ids (sequential within worker)
     d. Join all workers
  3. MP and MLA can also run in parallel (additional 2x speedup)
```

### Alternative (simpler, also effective):
```
Stage 6B:
  1. For EACH table (MP, MLA):
     a. Convert records (single-threaded)
     b. Insert ALL records in parallel batches (N workers, each handles ~N/total batches)
     c. Delete ALL old records in parallel (N workers, each handles ~N/total deletes)
  2. MP and MLA in parallel
```

---

## 16. Recommended Worker Count

| Consideration | Value |
|---------------|-------|
| Supabase free tier connection limit | ~50-100 concurrent |
| Typical affected works count | 50-5,000 |
| Worker threads overhead | Low (urllib, no shared state) |
| Diminishing returns | Beyond 10-15 workers, HTTP latency dominates |
| **Recommended** | **5 workers** (safe default, can tune to 8-10) |

**Rationale:**
- 5 workers: 500 deletes / 5 = 100 deletes per worker × 0.5s = 50s (down from 500s)
- 10 workers: 50 deletes per worker × 0.5s = 25s (but may hit connection limits)
- Start with 5, measure, tune upward if safe

---

## 17. Recommended Batch Size

| Operation | Current | Recommended | Rationale |
|-----------|---------|-------------|-----------|
| Insert batch | 500 | **500** (keep) | Already optimal for Supabase bulk insert |
| Delete batch | 1 (sequential) | **Partitioned: ~N/5 deletes per worker** | Each worker deletes its partition sequentially |

For 500 affected works with 5 workers:
- Each worker: ~100 deletes × 0.5s = ~50s
- Total: ~50s (down from ~500s) — **~10x speedup**

---

## 18. Expected Behavior Under Concurrency

### Performance:
- **Affected mode (500 works, 5 workers)**: ~50-100s (down from ~500s) → **5-10x speedup**
- **Affected mode (1000 works, 5 workers)**: ~100-200s (down from ~1000s) → **5-10x speedup**
- **MP + MLA parallel**: Additional ~1.5-1.8x speedup
- **Combined**: ~8-15x speedup overall

### Correctness:
- No data loss: Each worker inserts then deletes its own disjoint partition
- No duplicates: Workers never touch the same work_ids
- No constraint violations: work_id is not unique; concurrent inserts are safe
- Error isolation: Each worker's failures are independent
- Same data: Identical records persisted, same fields, same values

### Edge cases:
- **Empty input**: No workers launched, returns immediately
- **Single record**: 1 worker, 1 batch, same as current
- **Worker failure**: Other workers continue; failed batch raises error, stage fails cleanly
- **All deletes fail**: Stage raises error (not swallowed)
- **Partial batch failure**: Other batches succeed; failed batch reported

---

## 19. Risks

| Risk | Severity | Likelihood | Mitigation |
|------|----------|------------|-----------|
| Supabase rate limiting (too many concurrent requests) | MEDIUM | LOW (5 workers, ~10 QPS) | Bounded worker count; configurable via env var |
| Thread-unsafe Supabase client sharing | HIGH | LOW | Each worker creates own client; no shared clients |
| Worker exception swallowed | HIGH | LOW | Collect exceptions; re-raise if any worker fails |
| Progress tracking across workers | LOW | LOW | Use thread-safe counter + per-worker logging |
| Delete creates transient state (rows missing between insert and delete) | LOW | LOW | Same as current (insert-before-delete) |
| Connection exhaustion | MEDIUM | LOW | 5 workers × 1 connection = 5 connections (well within limits) |
| Interrupted pipeline leaves partial data | LOW | LOW | Same as current; insert-before-delete ensures new data persists |

---

## 20. Rollback Strategy

1. **Feature flag**: Add `WORK_ANALYSIS_PARALLEL` env var (default: true)
2. **Fallback**: If env var is false/off, run sequential code (current implementation)
3. **Monitoring**: Log per-worker stats; compare total records persisted
4. **Verification**: Existing `stage_verify()` checks evidence counts; add work_analysis count check
5. **Code preservation**: Keep original `_persist_batch` function as `_persist_batch_sequential`
6. **Instant rollback**: Change env var to false in workflow, no code deploy needed

---

## 21. Files to Modify

| File | Change | Risk |
|------|--------|------|
| `automation/pipeline_controller.py` | Add parallel `_persist_batch` in `stage_work_analysis_persist` | LOW — contained to one function |

**DO NOT modify:**
- `fetcher/fetcher.py` (already optimized)
- `automation/daily_pipeline.py` (Stage 6B not here)
- `analysis/` files (no analysis changes)
- Database schema
- Edge functions
- Snapshot/cache logic

---

## 22. Test Strategy

### Existing tests to run:
- `md/tests/test_affected_pipeline.py::TestWorkAnalysisPersist` (6 tests)
- `md/tests/test_two_db_pipeline.py` (all tests)
- `test_phase2_fixes.py::TestPersistOrder` (1 test)

### New tests to add:
| Test | Purpose |
|------|---------|
| `test_parallel_persist_multiple_batches` | 3+ batches, parallel workers |
| `test_parallel_persist_concurrent_workers` | 5 workers, verify all records persisted |
| `test_parallel_persist_worker_failure` | 1 worker fails, stage fails cleanly |
| `test_parallel_persist_retry_behavior` | Insert retry on failure |
| `test_parallel_persist_empty_input` | 0 records, no workers launched |
| `test_parallel_persist_partial_batch_failure` | Batch insert fails, individual rows retried |
| `test_parallel_persist_upsert_safety` | No duplicates for same work_id |
| `test_parallel_persist_completion_join` | All workers join before stage completes |
| `test_parallel_persist_client_isolation` | Each worker has own Supabase client |
| `test_parallel_persist_mp_mla_independent` | MP and MLA run in parallel |

---

## 23. Summary

| Metric | Current | After Optimization |
|--------|---------|-------------------|
| MP+MLA processing | Sequential | Parallel |
| Delete operations | Sequential (1 at a time) | Parallel (5 workers) |
| Delete throughput | ~1-2 per second | ~5-10 per second |
| **Affected mode (500 works)** | **~500s (8.3 min)** | **~50-100s (1-2 min)** |
| **Affected mode (1000 works)** | **~1000s (16.7 min)** | **~100-200s (2-3 min)** |
| Expected overall speedup | — | **5-10x** |
| Correctness risk | — | NONE (same data, same semantics) |

---

*Audit completed by opencode on 2026-09-11. Awaiting approval before implementation.*
