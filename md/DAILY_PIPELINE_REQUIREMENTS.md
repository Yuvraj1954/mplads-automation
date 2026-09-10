# DAILY_PIPELINE_REQUIREMENTS.md

> Complete trace of the daily pipeline from source ingestion to final database state.
> Based on actual pipeline code audit and database inspection.

---

## Table of Contents

1. [Pipeline Architecture](#pipeline-architecture)
2. [Stage-by-Stage Trace](#stage-by-stage-trace)
3. [Failure & Recovery Behavior](#failure--recovery-behavior)
4. [Pipeline Gaps for Frontend Integration](#pipeline-gaps-for-frontend-integration)
5. [Required Pipeline Changes](#required-pipeline-changes)

---

## Pipeline Architecture

### Trigger
- **GitHub Actions cron**: `30 18 * * *` (daily at 18:30 UTC)
- **Manual trigger**: workflow_dispatch with optional `FORCE_FULL_INGESTION`

### Execution Flow
```
┌─────────────────────────────────────────────────────────────┐
│                    GITHUB ACTIONS                            │
│                                                             │
│  ┌──────────┐   ┌───────────┐   ┌──────────┐               │
│  │  CACHE   │──→│  FETCHER  │──→│COMPARATOR│               │
│  │ RESTORE  │   │           │   │ (SQLite) │               │
│  └──────────┘   └───────────┘   └────┬─────┘               │
│                                      │                      │
│                              ┌───────▼────────┐             │
│                              │   EDGE FN:     │             │
│                              │   INGEST       │             │
│                              └───────┬────────┘             │
│                                      │                      │
│  ┌───────────────────────────────────▼──────────────────┐  │
│  │              PYTHON PIPELINE                          │  │
│  │  affected → analyze → anomaly → evidence → gemini    │  │
│  │  → db2_persist → verify → cleanup                    │  │
│  └──────────────────────────────────────────────────────┘  │
│                                      │                      │
│  ┌───────────────────────────────────▼──────────────────┐  │
│  │              CACHE SAVE + SNAPSHOT ROTATION            │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

---

## Stage-by-Stage Trace

### Stage 1: Cache Restore

**Input**: GitHub Actions cache keys (`mplads-snapshot-state-*`)
**Output**: Snapshot directory with NDJSON files

| Behavior | Detail |
|----------|--------|
| Attempts | 3 retries |
| Cache keys tried | `mplads-snapshot-state-latest`, then timestamp-based |
| On failure | Sets `USE_SUPABASE_FALLBACK=true`, tries Supabase Storage |
| Duration | 5-30 seconds |

**Data state**: Local filesystem has `previous/` and `current/` snapshot directories.

### Stage 2: Fetcher

**Input**: Government API at `mplads.mospi.gov.in`
**Output**: NDJSON files in Supabase Storage (`mplads-raw` bucket)

| Behavior | Detail |
|----------|--------|
| API | `https://mplads.mospi.gov.in/rest/PreLoginDashboardData/getTilesReportData` |
| Auth | Session cookies from dashboard page |
| Datasets | 12 (6 MP + 6 MLA) |
| Chunk size | 2000 records per NDJSON file |
| Filtering | Removes summary rows (NULL `WORK_RECOMMENDATION_DTL_ID`) |
| Duration | 2-10 minutes |

**Files produced**:
```
{timestamp}/
├── _COMPLETE.json
├── allocated_limit/part_0001.ndjson [... part_NNNN.ndjson]
├── works_recommended/part_0001.ndjson [...]
├── works_sanctioned/part_0001.ndjson [...]
├── works_completed/part_0001.ndjson [...]
├── expenditure/part_0001.ndjson [...]
├── calamity/part_0001.ndjson [...]
├── mla_allocated_limit/part_0001.ndjson [...]
├── mla_works_recommended/part_0001.ndjson [...]
├── mla_works_sanctioned/part_0001.ndjson [...]
├── mla_works_completed/part_0001.ndjson [...]
├── mla_expenditure/part_0001.ndjson [...]
└── mla_calamity/part_0001.ndjson [...]
```

**Failure behavior**: Retries 3 times. If all fail, pipeline aborts. Previous snapshot preserved.

### Stage 3: Comparator

**Input**: Previous snapshot + current snapshot
**Output**: Delta NDJSON (append + update files)

| Behavior | Detail |
|----------|--------|
| Storage | SQLite temp database |
| Identity keys | Per dataset (see below) |
| Content hash | SHA-256 of content fields |
| Expenditure | Fingerprint-based (SHA-256 of 9 fields) |
| Duration | 1-5 minutes |

**Identity fields per dataset**:
| Dataset | Identity Fields |
|---------|----------------|
| `allocated_limit` | MP_NAME, STATE_NAME, CONSTITUENCY, TENURE |
| `works_recommended` | WORK_RECOMMENDATION_DTL_ID |
| `works_sanctioned` | WORK_RECOMMENDATION_DTL_ID |
| `works_completed` | WORK_RECOMMENDATION_DTL_ID |
| `expenditure` | WORK_RECOMMENDATION_DTL_ID + WORK_ID + EXPENDITURE_DATE + VENDOR_NAME + WORK_STATUS + FUND_DISBURSED_AMT + STATE_NAME + CONSTITUENCY + MP_NAME |
| `calamity` | MP_NAME + CRT_DT + CALAMITY_NAME |

**Output structure**:
```
{timestamp}/
├── {dataset}/append_part_NNNN.ndjson  (new records)
└── {dataset}/update_part_NNNN.ndjson  (changed records)
```

**Failure behavior**: If comparator fails, pipeline aborts. No data modified.

### Stage 4: Edge Function Ingestion

**Input**: Delta NDJSON from Storage
**Output**: DB1 tables upserted

| Behavior | Detail |
|----------|--------|
| Worker | `ingest-mplads-part` (Deno/TypeScript) |
| Batch size | 5 parts per orchestrator call |
| DB batch | 500 rows per upsert |
| Entity resolution | states → constituencies → MPs/MLAs → works |
| Idempotency | `source_record_key` (SHA-256) for expenditure |
| Duration | 5-20 minutes |

**Tables written**:
| Source Dataset | Target Tables |
|---------------|---------------|
| `works_recommended` | `works`, `work_recommendations`, `work_sanctions` |
| `works_sanctioned` | `works`, `work_sanctions` |
| `works_completed` | `work_completions` |
| `expenditure` | `work_expenditures` |
| `allocated_limit` | `mp_allocations` |
| `calamity` | `calamities` |
| MLA equivalents | `mla_*` tables (same pattern) |

**MLA work_id convention**: `work_recommendation_dtl_id + 1,000,000`

**Failure behavior**: Per-part error handling. Failed parts marked in `ingestion_jobs`. Successful parts persisted. Pipeline continues with partial ingestion.

### Stage 5: Affected Processing

**Input**: Delta work IDs from comparator
**Output**: Expanded set of affected work IDs and member keys

| Behavior | Detail |
|----------|--------|
| Input | Delta work_ids from Stage 3 |
| Expansion | Group by `(normalized_activity, state_id)` → find peer works |
| Time-sensitive | Identify In Progress, Recommended, recent expenditure works |
| Output | `affected_work_ids`, `affected_members` |
| Duration | 10-30 seconds |

**Purpose**: Ensures that when a work changes, all benchmark peers are re-analyzed (percentiles may shift).

### Stage 6: Full Analysis

**Input**: ALL work records from snapshot (not just affected)
**Output**: Work analysis records with risk flags, percentiles, statuses

| Behavior | Detail |
|----------|--------|
| Input | Snapshot loader reads ALL NDJSON datasets |
| Joins | recommendations + sanctions + completions + expenditures by WORK_RECOMMENDATION_DTL_ID |
| Status | Derived from dates (Completed/In Progress/Recommended/Unknown) |
| Lifecycle | sanction_delay, execution_days, project_age, pending_days |
| Financial | expenditure_percentage, completion_percentage |
| Benchmarks | Per (activity, state) group, fallback to national |
| Risk flags | 10 flag types based on percentiles and thresholds |
| Risk level | Derived from flag count and overexpenditure |
| Profiles | financial, timeline, payment_activity |
| Duration | 5-15 minutes |

**Key**: This always runs on ALL works, not just affected. The persistence stage (6b) filters to affected members.

### Stage 6b: Work Analysis Persistence

**Input**: Work analysis records
**Output**: DB1 `work_analysis` and `mla_work_analysis` tables

| Behavior | Detail |
|----------|--------|
| Table | `work_analysis` (MP), `mla_work_analysis` (MLA) |
| Conflict key | `work_id` |
| Filter | Only affected members (or all if first run) |
| Duration | 1-5 minutes |

### Stage 7: Member + State Metrics

**Input**: Work analysis records
**Output**: Member-level and state-level aggregates

| Behavior | Detail |
|----------|--------|
| Member metrics | Aggregate per (member_type, member_id) |
| State metrics | Aggregate per state_id |
| Zero-work injection | From allocation snapshot |
| Duration | 1-3 minutes |

### Stage 7b: Anomaly Detection

**Input**: Member metrics
**Output**: Anomaly scores, levels, confidence, classification, ranking

| Behavior | Detail |
|----------|--------|
| Method | Robust Z-score (MAD-based) |
| Features | 12 per-member metrics |
| Composite | Mean of directional contributions |
| Thresholds | HIGH ≥ 95th, MEDIUM ≥ 80th percentile |
| Classification | 6-tier based on anomaly + completion + utilization |
| Rank | Ordered by anomaly_score DESC |
| Duration | 30-60 seconds |

### Stage 7c: DB2 Analytics Persistence

**Input**: Member metrics, state metrics, statistics, trends
**Output**: DB2 tables

| Table | Rows | Conflict Key | Method |
|-------|------|-------------|--------|
| `overall_metrics` | 3 | `scope` | Upsert |
| `member_metrics` | ~1,011 | `member_id, member_type` | Upsert |
| `state_metrics` | 36 | `state_id` | Upsert |
| `national_statistics` | ~129 | (delete+insert per scope) | Delete all for scope, then insert |
| `trends` | ~7 | (delete+insert per member_type) | Delete all for type, then insert |

**Duration**: 1-5 minutes

### Stage 8: Evidence Generation

**Input**: Member metrics + state metrics + statistics + anomaly results
**Output**: Evidence records with structured JSON

| Behavior | Detail |
|----------|--------|
| Entity types | MP, MLA, STATE |
| Evidence sections | portfolio, financial, execution, risk, anomalies, quality, national_context, state_context, entity_anomaly |
| Hash | SHA-256 of evidence JSON |
| Version | Schema version tracked |
| Work refs | Supporting work references with roles |
| Duration | 1-3 minutes |

### Stage 8b: Evidence Work Refs

**Input**: Evidence records + work analysis
**Output**: DB2 `evidence_work_refs`

| Role | Description | Count |
|------|-------------|-------|
| `risk` | Works contributing to risk flags | 3,567 |
| `execution` | Works with execution anomalies | 2,123 |
| `financial` | Works with financial anomalies | 2,077 |
| `representative` | Representative works for portfolio | 2,291 |
| `positive_signal` | Works with positive performance | 1,916 |

### Stage 9: Gemini AI Analysis

**Input**: Evidence records (affected only)
**Output**: DB2 `ai_analysis` with generated text

| Behavior | Detail |
|----------|--------|
| Model | `gemini-3.1-flash-lite`, `gemini-3.5-flash-lite` |
| Batch size | 5 items per request |
| Concurrency | 12 workers, 8 lanes (2 models × 4 keys) |
| RPM | 15 per lane |
| Temperature | 0.15 |
| Output | `{ summary, highlights[3-6], cautions[0-2] }` |
| Validation | Non-empty, entity name present, no forbidden terms |
| Idempotency | Skip if same evidence_hash + prompt_version exists |
| Duration | 5-30 minutes (depends on affected count) |

**Prompt structure**:
```
SYSTEM_INSTRUCTION (15 rules)
+ context_line (entity identity)
+ "ENTITY EVIDENCE:" + JSON payload
```

**Key rules**:
- Must use actual entity name (never "the entity")
- Currency format: ₹11.92 crore
- Risk flags = signals, never proof
- 27 forbidden phrases

### Stage 10: Evidence Persistence

**Input**: Evidence records
**Output**: DB2 `entity_evidence`

| Table | Conflict Key | Rows |
|-------|-------------|------|
| `entity_evidence` | `entity_type, entity_id` | ~810 |

### Stage 11: Verification

**Input**: DB1 + DB2
**Output**: Structural integrity checks

| Check | Description |
|-------|------------|
| Row counts | Verify expected counts per table |
| NULL checks | Critical fields not NULL |
| Consistency | member_metrics count matches expected |
| Duration | 10-30 seconds |

### Stage 12: Cleanup

| Action | Detail |
|--------|--------|
| Temp files | Delete local working directory |
| Failed snapshot | Delete incomplete Supabase Storage upload |
| Cache save | Immutable timestamped key |
| Snapshot rotation | `current/old` → `previous/`, `current/new` stays |

---

## Failure & Recovery Behavior

### Failure Stages

| Stage | Failure Mode | Recovery |
|-------|-------------|----------|
| Cache restore | All 3 attempts fail | Set `USE_SUPABASE_FALLBACK`, try Storage |
| Fetcher | API unreachable/auth fail | Abort pipeline, preserve previous state |
| Comparator | Diff computation fails | Abort, no data modified |
| Edge Function | Partial ingestion | Successful parts persist, failed parts retried |
| Python Analysis | Calculation error | Abort, DB1 data intact |
| Gemini | API rate limits/keys exhausted | Partial results persist, retry on next run |
| DB2 Persist | Write failure | Retry, DB1 data intact |

### Consecutive Failure Tracking

```
Failure counter in GitHub cache:
  < 3 failures → normal retry next day
  ≥ 3 failures → emergency Supabase fallback mode
```

### Snapshot Rotation

```
Before: previous/ = old snapshot, current/ = in-progress
After success: previous/ = current/old, current/ = new
On failure: current/ rolled back, previous/ unchanged
```

---

## Pipeline Gaps for Frontend Integration

### Gap 1: Allocated Amount Not in DB2

**Current**: `overall_metrics.allocated_amount = 0.00` for all scopes
**Impact**: Dashboard cannot show allocated amount in fund flow
**Fix**: Add allocation aggregation in Stage 7c:
```python
# From mp_allocations
total_mp_allocated = sum(mp_allocations.allocated_amount)
# From mla_allocations
total_mla_allocated = sum(mla_allocations.allocated_amount)
# Write to overall_metrics
```

### Gap 2: Member Allocated Amount Not Populated

**Current**: `member_metrics.allocated_amount = 0` for all members
**Impact**: MP detail page cannot show allocation
**Fix**: Join `mp_allocations`/`mla_allocations` during member metrics calculation

### Gap 3: State Allocated Amount Not Aggregated

**Current**: `state_metrics` has no `allocated_amount` column
**Impact**: State cards cannot show allocated amount
**Fix**: Add `allocated_amount` to `state_metrics` and aggregate from member-level

### Gap 4: MLA state_name NULL

**Current**: 196 MLA member_metrics rows have `state_name = NULL`
**Impact**: State filter and MLA detail pages show blank state
**Fix**: Ensure MLA state resolution in Stage 7 uses `mlas.constituency_id → constituencies.state_id → states.state_name`

### Gap 5: No Constituency-Level Aggregation

**Current**: No pre-computed constituency metrics
**Impact**: State detail page constituency matrix requires new SQL
**Fix**: Either add a pipeline stage or compute on-demand in FastAPI

### Gap 6: No Quarterly Trends

**Current**: `trends` table has yearly data only
**Impact**: AI Risk Center and State Detail quarterly charts
**Fix**: Either add quarterly aggregation to pipeline or compute on-demand from `work_expenditures.expenditure_date`

### Gap 7: No Sector Breakdown

**Current**: No aggregation by `work_category`
**Impact**: MP detail expenditure by sector chart
**Fix**: Compute on-demand in FastAPI from `phase_a_work_unified GROUP BY work_category`

### Gap 8: Risk Signal Composition Not Pre-computed

**Current**: Venn diagram data (cost-only, duration-only, dual) not stored
**Impact**: AI Risk Center Venn chart
**Fix**: Compute on-demand in FastAPI or add to pipeline

---

## Required Pipeline Changes

### Priority 1: Allocation Data (Critical)

**Files to modify**: `analysis/phase_a_member.py`, `analysis/db2_analytics_persistence.py`

**Change**: During member metrics calculation, join allocation tables:
```python
# In phase_a_member.py
def compute_member_metrics(member_id, member_type, works, allocations):
    allocated_amount = sum(a['allocated_amount'] for a in allocations)
    # ... existing calculations ...
    return MemberMetrics(allocated_amount=allocated_amount, ...)

# In db2_analytics_persistence.py
def persist_overall_metrics(member_metrics_list):
    total_allocated = sum(m.allocated_amount for m in member_metrics_list)
    # Write to overall_metrics
```

### Priority 2: MLA State Resolution (Medium)

**File to modify**: `analysis/phase_a_member.py`

**Change**: Ensure MLA state_name is populated from constituency → state join:
```python
# When building member metrics for MLAs
state_name = lookup_state_name(mla.constituency_id)  # via constituencies → states
```

### Priority 3: State-Level Allocation (Medium)

**File to modify**: `analysis/phase_a_state.py`, `analysis/db2_analytics_persistence.py`

**Change**: Add `allocated_amount` column to `state_metrics` table and aggregate:
```python
# In state metrics
state_allocated = sum(
    m.allocated_amount for m in member_metrics 
    if m.state_id == state_id
)
```

### Priority 4: Evidence Work Refs Cleanup (Low)

**File to modify**: `analysis/evidence_builder.py`

**Change**: Ensure work refs are correctly linked to evidence_id (currently evidence_id may be NULL in some refs).

### No Changes Required

- Fetcher: Working correctly
- Comparator: Working correctly
- Edge Functions: Working correctly
- Work Analysis: Working correctly (107,636 MP + 25,297 MLA works analyzed)
- Anomaly Detection: Working correctly
- Gemini Processing: Working correctly (1,037 analyses generated)
- Cache/Snapshot Management: Working correctly
