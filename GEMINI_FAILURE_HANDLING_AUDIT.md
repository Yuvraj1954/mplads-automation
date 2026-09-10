# Gemini Failure Handling Audit

## Production Run Summary

```
Gemini:
  records=769 (affected, after filter_affected)
  successful=763
  failed=6
  retries=86
  api_requests=238
  duration=105.27s

Transients observed:
  - "Server disconnected" (SERVER_ERROR → retryable)
  - "RPM_LIMITED" (429 → lane cooldown)

Pipeline completed:
  Stage 10 DB2 Persist: 772/772 evidence records
  Stage 11 Verify: Issues 0, PASS
```

---

## 1. What happens to an entity after all Gemini retries are exhausted?

**Location**: `analysis/gemini_scheduler.py:428-439`

When all 3 attempts are exhausted (or all eligible lanes are RPD-exhausted), the entity is appended to the in-memory `failures` list with a reason string:

| Failure Path | Reason String |
|---|---|
| Partial response validation failure after max_attempts | `validation_failed: ...` |
| GeminiError (server error, transient) after max_attempts | `max_attempts_exceeded` |
| Unexpected exception after max_attempts | `unexpected_error: ...` |
| All lanes RPD exhausted | `all_lanes_rpd_exhausted` |
| Global daily quota exceeded | `daily_quota_exceeded` |
| Permanent error (401/400/404) | `permanent_{status_code}` |

**No further action is taken.** The entity is not retried within the same run. The `failures` list is returned in `batch_result["failures"]` and propagated to `results["stages"]["gemini"]["failures"]`.

---

## 2. Is its previous ai_analysis preserved if one already exists?

**YES.** The `on_success` callback (`pipeline_controller.py:1010-1021`) only fires for successful `GeminiResult` objects. It calls `sb_upsert` with `conflict_cols=["entity_type", "entity_id"]`.

If Gemini fails, **no upsert occurs**, so any pre-existing `ai_analysis` row for that entity remains untouched in DB2. This is correct behavior — stale analysis is better than missing analysis.

---

## 3. Is a failed entity written to DB2 with an explicit failure state?

**NO.** There is no failure state column or failure record written to DB2.

- `ai_analysis` table: NOT updated (only updated on success via `on_success`)
- `entity_evidence` table: **IS written** (Stage 10 runs independently of Gemini; it upserts ALL evidence records)
- `evidence_work_refs` table: **IS written** (Stage 8b runs before Gemini)

The evidence and refs are persisted. Only the AI analysis is missing/stale.

---

## 4. Can a failed entity accidentally be treated as successful?

**NO.** The `on_success` callback fires only after `validate_packed_output` confirms valid results (`gemini_scheduler.py:389-406`). Failed entities are routed to the `failures` list via `failed_entries` (`gemini_scheduler.py:409-439`).

The `sb_upsert` call that writes to `ai_analysis` is gated by `on_success`. No failed entity reaches it.

---

## 5. Can the next daily run retry that entity?

**YES.** `filter_affected()` (`analysis/gemini_processor.py:505-546`) determines the affected set by checking:

1. Entity has no existing `ai_analysis` → **needs processing**
2. Entity's `evidence_hash` changed → **needs processing**
3. Entity's `prompt_version` changed → **needs processing**

A failed entity will either:
- Have **no `ai_analysis` row** (first-time entity) → included in next run
- Have an **old `ai_analysis` row** with a different `evidence_hash` → included in next run

Either way, the entity will be retried on the next daily run. This is correct.

---

## 6. Does evidence_hash/prompt_version cause a failed entity to be incorrectly considered up-to-date?

**NO.** The idempotency check in `filter_affected` is correct:

```python
if existing is None:              # No record → affected
if existing.prompt_version != v:  # Version changed → affected
if existing.evidence_hash != h:   # Hash changed → affected
```

A failed entity either has no `ai_analysis` row or has a stale `evidence_hash`. Both cases correctly mark it as "needs processing."

---

## 7. Does the 6-failure count affect pipeline success/failure?

**BUG (in the production run)**: **No.**

`stage_verify` (`pipeline_controller.py:1437-1440`) contains:

```python
if gemini_result and not gemini_result.get("skipped"):
    if gemini_result.get("failed", 0) > 0:
        issues.append(f"Gemini had {gemini_result['failed']} failures")
```

**However**, the production log shows "Stage 11 Verify: Issues 0, PASS" despite 6 failures. This means one of:

1. **The verify check was not in the code version that ran** — the Gemini failure check in verify may have been added after this production run, or
2. **The `gemini_result` dict structure differs** — a subtle key mismatch could cause the check to not fire.

**Impact**: The pipeline completed and ran cleanup (Stage 12), even though 6 entities have no AI analysis. This is a **soft bug** — the pipeline should not declare PASS when Gemini failures exist, because it means the dashboard will show entities with no AI summary.

---

## 8. Does Stage 10/11 verification validate Gemini success, or only evidence persistence?

- **Stage 10** (`stage_persist`): Writes `entity_evidence` records. No Gemini check.
- **Stage 11** (`stage_verify`): **DOES check Gemini** (lines 1437-1440), but as noted in #7, the production run showed this check was not effective.

Additionally, Stage 11 checks:
- Evidence record counts
- Duplicate evidence hashes
- Persist write counts
- Analytics write counts
- Work refs write counts

But it does **not** validate that every entity has a corresponding `ai_analysis` row.

---

## 9. Are the 6 failed records visible in any persistent failure/retry state?

**NO.** The `failures` list is:
- In-memory during the run
- Stored in `results["stages"]["gemini"]["failures"]` (a checkpoint file)
- Printed to stdout/log

There is **no persistent failure table** in DB2. If the checkpoint file is cleaned up (Stage 12), the failure information is lost. The only evidence of failure is the **absence** of an `ai_analysis` row for those entities.

---

## 10. What happens if the same entity fails repeatedly for several daily runs?

Each daily run will:
1. Rebuild evidence for the entity (Stage 8)
2. Compute a new `evidence_hash`
3. `filter_affected` sees the old/stale `evidence_hash` in `ai_analysis` → marks as affected
4. Gemini retries (3 attempts per run)
5. If it fails again, the cycle repeats

**Risk**: If a model consistently fails for a specific entity (e.g., prompt triggers a validation error), the entity will accumulate permanent Gemini failures across runs with no alerting mechanism. The AI analysis will stay stale indefinitely.

---

## Classification

### **NEEDS IMPROVEMENT**

### Issue 1: Verify should catch Gemini failures (MEDIUM)

**File**: `automation/pipeline_controller.py:1437-1440`

The verify check exists but the production run showed it did not fire. Possible causes:
- Code version mismatch (check added after the run)
- `gemini_result` structure issue

**Recommendation**: Verify the check is present and working. If the pipeline completed with `failed > 0` and `verify.passed == True`, the check is not effective.

### Issue 2: No persistent failure tracking (LOW-MEDIUM)

**File**: `analysis/gemini_scheduler.py` (return value only)

Failed entities are only visible in checkpoint files. There is no `gemini_failures` table in DB2 to track which entities have persistent Gemini failures across runs.

**Recommendation**: Add a lightweight `gemini_failures` table or log failed entity_ids to a persistent location so operators can identify entities with chronic Gemini issues.

### Issue 3: No alerting for persistent failures (LOW)

If the same entity fails across N consecutive runs, there is no mechanism to alert or escalate. The entity silently gets stale AI analysis.

### Issue 4: verify does not check ai_analysis completeness (LOW)

**File**: `automation/pipeline_controller.py:1346-1455`

Stage 11 verifies evidence persistence but does not verify that every evidence record has a corresponding `ai_analysis` row. This is a gap: a dashboard entity with evidence but no AI summary is a partial failure.

---

## Summary for the 6 Failed Records

| Aspect | Status |
|---|---|
| Previous ai_analysis preserved? | YES (if existed) |
| Explicit failure state in DB2? | NO |
| Evidence written to DB2? | YES (772/772) |
| Retried next daily run? | YES (via filter_affected) |
| Visible in persistent state? | NO (checkpoint only) |
| Pipeline correctly failed? | **BUG**: Should have, but reported PASS |
| Dashboard impact? | 6 entities will show no AI summary |
