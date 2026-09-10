# BACKEND_SQL_REQUIREMENTS.md

> Complete backend data inventory, metric definitions, SQL requirements, and FastAPI endpoint specifications.
> Based on actual PostgreSQL database inspection (DB1 + DB2) and pipeline code audit.

---

## Table of Contents

1. [Database Architecture](#database-architecture)
2. [DB1 Complete Table Inventory](#db1-complete-table-inventory)
3. [DB2 Complete Table Inventory](#db2-complete-table-inventory)
4. [Metric Definitions — Authoritative Reference](#metric-definitions--authoritative-reference)
5. [Data Quality Audit](#data-quality-audit)
6. [Missing Calculations Required](#missing-calculations-required)
7. [FastAPI Endpoint Plan](#fastapi-endpoint-plan)

---

## Database Architecture

```
DB1 (CORE/SOURCE)                    DB2 (INTELLIGENCE/RESULTS)
├── Source records                    ├── overall_metrics (3 rows)
├── Entity resolution                 ├── member_metrics (1,011 rows)
├── Work lifecycle tables             ├── state_metrics (36 rows)
├── Analysis (work + member)          ├── national_statistics (129 rows)
├── Unified views                     ├── trends (7 rows)
└── Ingestion state                   ├── entity_evidence (810 rows)
                                      ├── ai_analysis (1,037 rows)
                                      ├── evidence_work_refs (11,974 rows)
                                      └── evidence_pipeline_metadata (2 rows)
```

**Data Flow**: Government API → Fetcher → Comparator → DB1 Ingestion → Python Analysis → Evidence → Gemini → DB2

---

## DB1 Complete Table Inventory

### Core Identity Tables

| Table | Rows | Purpose | Key Fields |
|-------|------|---------|------------|
| `states` | 36 | State/UT master | state_id (PK), state_name |
| `constituencies` | 582 | Constituency master | constituency_id (PK), constituency_name, state_id (FK) |
| `mps` | 543 | MP master | mp_id (PK), mp_name, house_name, constituency_id (FK), tenure, tenure_start_date, tenure_end_date |
| `mlas` | 232 | MLA master | mla_id (PK), mla_name, house_name, constituency_id (FK), tenure |

### MP Work Lifecycle

| Table | Rows | Purpose | Key Fields |
|-------|------|---------|------------|
| `works` | 108,204 | MP work master | work_id (PK), source_work_id, work_recommendation_dtl_id, mp_id (FK), constituency_id (FK), work_category, activity_name, work_description |
| `work_recommendations` | 107,829 | Recommendation records | recommendation_id (PK), work_id (FK), recommendation_date, recommended_amount, letter_no, sanction_date |
| `work_sanctions` | 108,204 | Sanction records | sanction_id (PK), work_id (FK), sanction_date, sanction_amount, work_stage, file_status |
| `work_completions` | 35,000 | Completion records | completion_id (PK), work_id (FK), completion_date, amount_disbursed |
| `work_expenditures` | 85,113 | Payment records | expenditure_id (PK), work_id (FK), expenditure_date, vendor_name, fund_disbursed_amount, source_record_key (SHA-256) |
| `mp_allocations` | 543 | MP fund allocation | allocation_id (PK), mp_id (FK), allocated_amount, source_tenure |
| `calamities` | 12 | Calamity funds | calamity_id (PK), mp_id (FK), calamity_type, consent_amount |

### MLA Work Lifecycle

| Table | Rows | Purpose | Key Fields |
|-------|------|---------|------------|
| `mla_works` | 25,587 | MLA work master | work_id (PK, offset +1M), mla_id (FK) |
| `mla_work_recommendations` | 25,358 | MLA recommendations | Same schema as MP |
| `mla_work_sanctions` | 25,587 | MLA sanctions | Same schema as MP |
| `mla_work_completions` | 10,040 | MLA completions | Same schema as MP |
| `mla_work_expenditures` | 24,877 | MLA payments | Same schema as MP |
| `mla_allocations` | 232 | MLA fund allocation | mla_id (FK), allocated_amount |
| `mla_calamities` | 20 | MLA calamity funds | Same schema as MP |

### Analysis Tables (Pipeline-Generated)

| Table | Rows | Purpose | Key Fields |
|-------|------|---------|------------|
| `work_analysis` | 107,636 | MP work analysis | 49 columns: work_id, risk_flags[], risk_level, cost_percentile, duration_percentile, etc. |
| `mla_work_analysis` | 25,297 | MLA work analysis | Same 49 columns as work_analysis |
| `ml_work_anomaly` | 132,070 | ML anomaly scores | work_id, ml_anomaly_score, ml_anomaly_flag, model_name |

### Aggregated Analytics (Pipeline-Generated)

| Table | Rows | Purpose | Key Fields |
|-------|------|---------|------------|
| `phase_a_member_metrics` | 774 | Per-member aggregates | member_id, 58 columns of aggregated metrics |
| `phase_a_state_metrics` | 36 | Per-state aggregates | state_id, 32 columns |
| `phase_a_statistics` | 8 | Distribution stats | metric_name, mean, median, p25-p95 |
| `phase_a_trends` | 7 | Yearly trends | year, member_type, work/financial aggregates |
| `phase_a_evidence` | 774 | Evidence records | entity_id, entity_type, evidence (JSONB) |

### Views

| View | Rows | Purpose |
|------|------|---------|
| `phase_a_work_unified` | 132,933 | UNION ALL of work_analysis + mla_work_analysis |
| `work_analysis_all` | 132,933 | Same as phase_a_work_unified (identical) |

### Operational

| Table | Rows | Purpose |
|-------|------|---------|
| `ingestion_jobs` | 1,284 | Pipeline job tracking |

---

## DB2 Complete Table Inventory

### Dashboard/Aggregation Tables

| Table | Rows | Conflict Key | Purpose |
|-------|------|-------------|---------|
| `overall_metrics` | 3 | `scope` | National KPIs for scope: BOTH, MP, MLA |
| `member_metrics` | 1,011 | `member_id, member_type` | Per-member KPIs + anomaly + classification + rank |
| `state_metrics` | 36 | `state_id` | Per-state KPIs + anomaly + rank |
| `national_statistics` | 129 | (delete+insert per scope) | Distribution statistics for 50+ metrics |
| `trends` | 7 | (delete+insert per member_type) | Yearly trend data |

### Evidence/AI Tables

| Table | Rows | Conflict Key | Purpose |
|-------|------|-------------|---------|
| `entity_evidence` | 810 | `entity_type, entity_id` | Structured evidence JSONB per entity |
| `ai_analysis` | 1,037 | `entity_type, entity_id` | Gemini-generated text analysis |
| `evidence_work_refs` | 11,974 | (delete+insert) | Maps evidence to supporting works |
| `evidence_pipeline_metadata` | 2 | `metadata_id` | Pipeline version tracking |

### overall_metrics Actual Values (scope=BOTH)

```
total_members: 735          work_bearing: 735
total_works: 132,880        recommended_works: 33,815
sanctioned_works: 99,065    completed_works: 44,840
ongoing_works: 54,225       pending_works: 33,815
completion_rate_pct: 33.74  sanction_rate_pct: 74.55
sanction_conversion_pct: 45.26
recommended_amount: ₹80,126 Cr   sanctioned_amount: ₹79,411 Cr
expenditure_amount: ₹40,197 Cr   completion_amount: ₹24,468 Cr
unspent_amount: ₹39,213 Cr
fund_utilization_pct: 50.62       expenditure_rate_pct: 50.17
avg_sanction_delay_days: 117.69   avg_execution_days: 176.02
overdue_over_1_year: 30,448       overdue_over_2_years: 1,648
flagged_works: 47,650             high_risk_works: 1,249
cost_anomaly_works: 2,800         duration_anomaly_works: 4,066
```

---

## Metric Definitions — Authoritative Reference

### Financial Metrics

| Metric | Formula | Numerator Source | Denominator Source | NULL Handling |
|--------|---------|-----------------|-------------------|---------------|
| **Fund Utilization %** | `expenditure / sanctioned × 100` | `work_expenditures.sum(fund_disbursed_amount)` | `work_sanctions.sum(sanction_amount)` | If sanctioned=0 or NULL → NULL |
| **Expenditure Rate %** | `expenditure / recommended × 100` | Same | `work_recommendations.sum(recommended_amount)` | If recommended=0 or NULL → NULL |
| **Sanction Conversion %** | `completed_works / sanctioned_works × 100` | Count of completed works | Count of works with sanction_date | If sanctioned=0 → NULL |
| **Completion Rate %** | `completed_works / total_works × 100` | Count where status='Completed' | Count of all works | If total=0 → NULL |
| **Expenditure Percentage** (per work) | `expenditure_amount / sanction_amount × 100` | Per-work expenditure sum | Per-work sanction_amount | If sanction=0 or NULL → NULL |
| **Completion Percentage** (per work) | `completion_amount / sanction_amount × 100` | Per-work completion_amount | Per-work sanction_amount | If sanction=0 or NULL → NULL |

**CRITICAL**: `allocated_amount` is currently 0.00 in `overall_metrics`. Allocation data exists in `mp_allocations` (₹83,336 Cr total) and `mla_allocations` (₹33,687 Cr total) but is NOT aggregated into DB2's `overall_metrics` or `state_metrics`.

### Work Status Definitions

| Status | Definition | Source Fields |
|--------|-----------|---------------|
| **Completed** | `completion_date IS NOT NULL` | `work_completions.completion_date` |
| **In Progress** | `sanction_date IS NOT NULL AND completion_date IS NULL` | `work_sanctions.sanction_date` |
| **Recommended** | `recommendation_date IS NOT NULL AND sanction_date IS NULL` | `work_recommendations.recommendation_date` |
| **Unknown** | None of the above | Fallback |

### Lifecycle Metrics (Per Work)

| Metric | Formula | NULL Handling |
|--------|---------|---------------|
| **Sanction Delay Days** | `sanction_date - recommendation_date` | If either date NULL → NULL |
| **Execution Days** | `completion_date - sanction_date` | If not completed → NULL |
| **Project Age Days** | `reference_date - recommendation_date` | If recommendation_date NULL → NULL |
| **Pending Days** | `reference_date - sanction_date` | If completed → NULL |

### Risk Classification (Per Work)

| Risk Level | Condition |
|------------|-----------|
| **HIGH** | over_expenditure OR non_defect_flags ≥ 3 |
| **MEDIUM** | non_defect_flags == 2 |
| **LOW** | non_defect_flags == 1 |
| **NORMAL** | non_defect_flags == 0 |

### Risk Flags (Per Work)

| Flag | Condition |
|------|-----------|
| `SANCTION_DELAY_VERY_HIGH` | sanction_delay > duration_p95 |
| `SANCTION_DELAY_HIGH` | sanction_delay > duration_p90 |
| `SANCTION_DELAY_ELEVATED` | sanction_delay > duration_p75 |
| `LONG_PENDING` | NOT Completed AND pending_days > duration_p90 |
| `LOW_EXPENDITURE` | NOT Completed AND sanctioned > 0 AND expenditure == 0 AND pending > 180 days |
| `PAYMENT_STAGNATION` | NOT Completed AND days_since_last_expenditure > 180 |
| `COST_ANOMALY` | cost_status IN ('VERY_HIGH', 'HIGH') — i.e. cost_percentile ≥ 90 |
| `DURATION_ANOMALY` | Completed AND duration_status IN ('VERY_LONG', 'LONG') — i.e. duration_percentile ≥ 90 |
| `OVER_EXPENDITURE` | expenditure_amount > sanction_amount × 1.05 |
| `POST_COMPLETION_PAYMENT` | last_expenditure_date > completion_date |
| `SOURCE_DATA_DEFECT_NEGATIVE_DELAY` | sanction_delay < 0 |

### Cost/Duration Status Thresholds

| Status | Percentile Range |
|--------|-----------------|
| VERY_HIGH / VERY_LONG | ≥ 95th |
| HIGH / LONG | ≥ 90th and < 95th |
| ABOVE_NORMAL / — | ≥ 75th and < 90th |
| NORMAL | ≥ 25th and < 75th |
| BELOW_NORMAL / — | ≥ 10th and < 25th |
| VERY_LOW / VERY_SHORT | < 10th |

### Entity Anomaly Score

**Method**: Robust Z-Score (MAD-based)

```python
MAD = median(|value - median|)
z = 0.6745 × (value - median) / MAD
z = clip(z, -5.0, 5.0)
```

**12 Member Features**:
1. flagged_rate_pct (direction: HIGH)
2. high_risk_rate_pct (direction: HIGH)
3. completion_rate_pct (direction: LOW)
4. expenditure_utilization_pct (direction: LOW)
5. avg_sanction_delay_days (direction: HIGH)
6. overdue_rate (direction: HIGH)
7. cost_anomaly_rate (direction: HIGH)
8. duration_anomaly_rate (direction: HIGH)
9. avg_cost_percentile (direction: HIGH)
10. expenditure_over_sanction_rate (direction: HIGH)
11. negative_delay_rate (direction: HIGH)
12. avg_project_age_days (direction: BOTH)

**Composite**: `mean(contributions)` where contributions = max(0, z) for HIGH, max(0, -z) for LOW, abs(z) for BOTH.

**Thresholds**: HIGH ≥ 95th percentile, MEDIUM ≥ 80th percentile.

**Confidence**: HIGH ≥ 50 works, MEDIUM ≥ 20 works, LOW < 20 works.

### Performance Classification

| Classification | Condition |
|---------------|-----------|
| `NO_DATA` | zero_work_member = true |
| `INSUFFICIENT_DATA` | low_sample_member = true (total_works < 5) |
| `UNDERPERFORMER` | anomaly_level = 'HIGH' |
| `NEEDS_ATTENTION` | anomaly_level = 'MEDIUM' |
| `PERFORMER` | ranking_qualified AND completion_rate ≥ 60% AND utilization ≥ 60% |
| `AVERAGE` | ranking_qualified AND completion_rate ≥ 40% |
| `UNCLASSIFIED` | All others |

### Ranking

- `rank`: Ordered by `anomaly_score DESC` (1 = highest anomaly score)
- `ranking_qualified`: `total_works >= 5`

---

## Data Quality Audit

### Verified Clean

| Check | Result |
|-------|--------|
| Duplicate work_ids in work_analysis | 0 |
| Works without member_id | 0 |
| Works without state_id | 0 |
| expenditure > sanction (with 5% tolerance) | 0 |
| NULL sanction with expenditure > 0 | 0 |
| Negative sanction delays | 0 |
| Negative execution days | 0 |

### Known Issues

| Issue | Severity | Details | Fix Required |
|-------|----------|---------|-------------|
| **allocated_amount = 0 in overall_metrics** | HIGH | `mp_allocations` has ₹83,336 Cr but not aggregated to DB2 | Add allocation aggregation to pipeline |
| **196 MLAs with NULL state_name in member_metrics** | MEDIUM | MLA member_metrics rows have NULL state_name | Fix state resolution in pipeline |
| **Zero-work members in DB2: 70 MLA + 10 MP** | INFO | Injected from allocation snapshot, valid behavior | No fix needed |
| **Two identical views** | LOW | `phase_a_work_unified` and `work_analysis_all` are identical | Drop one, use single view |
| **MP allocated_amount not in member_metrics** | HIGH | `member_metrics.allocated_amount` = 0 for all members | Need to populate from `mp_allocations`/`mla_allocations` |
| **State-level allocation not aggregated** | MEDIUM | No `state_metrics.allocated_amount` | Sum member allocations per state |
| **MLA member count discrepancy** | LOW | phase_a_member_metrics has 231 MLAs, mlas table has 232 | One MLA may have no works AND no allocation |
| **Risk flag: LOW_EXPENDITURE never fires** | MEDIUM | 0 works have this flag; condition may be too strict or works always have some expenditure | Review threshold |

---

## Missing Calculations Required

These are metrics the frontend needs but the backend does NOT currently provide:

### 1. Classification Distribution (Dashboard)

```sql
-- Needed for dashboard classification matrix
SELECT 
  performance_classification,
  COUNT(*) as count
FROM member_metrics
WHERE zero_work_member = false
GROUP BY performance_classification;
```

**Status**: Data exists in `member_metrics.performance_classification` — just needs API aggregation.

### 2. Constituency-Level Aggregation (State Detail)

```sql
-- Needed for state detail constituency matrix
SELECT 
  c.constituency_id,
  c.constituency_name,
  s.state_name,
  COUNT(wa.work_id) as total_works,
  COUNT(CASE WHEN wa.status = 'Completed' THEN 1 END) as completed_works,
  ROUND(COUNT(CASE WHEN wa.status = 'Completed' THEN 1 END)::numeric / NULLIF(COUNT(wa.work_id), 0) * 100, 2) as completion_rate_pct,
  SUM(wa.expenditure_amount) as expenditure_amount,
  SUM(wa.sanction_amount) as sanctioned_amount,
  ROUND(SUM(wa.expenditure_amount) / NULLIF(SUM(wa.sanction_amount), 0) * 100, 2) as fund_utilization_pct,
  COUNT(CASE WHEN wa.risk_flags IS NOT NULL AND array_length(wa.risk_flags, 1) > 0 THEN 1 END) as flagged_works
FROM constituencies c
JOIN phase_a_work_unified wa ON c.constituency_id = wa.constituency_id
WHERE c.state_id = $1
GROUP BY c.constituency_id, c.constituency_name, s.state_name
ORDER BY total_works DESC;
```

**Status**: **NEEDS NEW SQL** — not pre-computed in any table.

### 3. Expenditure by Sector/Category (MP Detail)

```sql
-- Needed for MP detail expenditure by sector chart
SELECT 
  wa.work_category,
  SUM(wa.expenditure_amount) as total_expenditure,
  ROUND(SUM(wa.expenditure_amount)::numeric / NULLIF(SUM(SUM(wa.expenditure_amount)) OVER(), 0) * 100, 1) as percentage
FROM phase_a_work_unified wa
WHERE wa.member_id = $1 AND wa.member_type = 'MP'
  AND wa.expenditure_amount > 0
GROUP BY wa.work_category
ORDER BY total_expenditure DESC;
```

**Status**: **NEEDS NEW SQL** — not pre-computed.

### 4. Risk Signal Composition / Venn Diagram (AI Risk Center)

```sql
-- Needed for risk signal composition Venn diagram
SELECT
  COUNT(CASE WHEN cost_status IN ('VERY_HIGH', 'HIGH') 
        AND (duration_status IS NULL OR duration_status NOT IN ('VERY_LONG', 'LONG')) 
        THEN 1 END) as cost_only,
  COUNT(CASE WHEN duration_status IN ('VERY_LONG', 'LONG') 
        AND (cost_status IS NULL OR cost_status NOT IN ('VERY_HIGH', 'HIGH')) 
        THEN 1 END) as duration_only,
  COUNT(CASE WHEN cost_status IN ('VERY_HIGH', 'HIGH') 
        AND duration_status IN ('VERY_LONG', 'LONG') 
        THEN 1 END) as dual_flagged,
  COUNT(CASE WHEN cost_status NOT IN ('VERY_HIGH', 'HIGH') 
        AND duration_status NOT IN ('VERY_LONG', 'LONG') 
        THEN 1 END) as no_anomaly
FROM phase_a_work_unified;
```

**Status**: **NEEDS NEW SQL** — not pre-computed.

### 5. Allocated Amount Aggregation (All Levels)

```sql
-- MP level: already exists in mp_allocations
SELECT mp_id, SUM(allocated_amount) as allocated_amount
FROM mp_allocations GROUP BY mp_id;

-- State level: NEW aggregation needed
SELECT 
  m.state_id,
  SUM(ma.allocated_amount) as state_allocated_amount
FROM mp_allocations ma
JOIN mps m ON ma.mp_id = m.mp_id
GROUP BY m.state_id;

-- National level: NEW aggregation
SELECT SUM(allocated_amount) as total_allocated FROM mp_allocations;
```

**Status**: **NEEDS NEW PIPELINE STEP** to write allocated_amount to member_metrics and state_metrics.

### 6. Quarterly Trends (AI Risk Center, State Detail)

```sql
-- Needed for quarterly trend charts
SELECT
  EXTRACT(YEAR FROM we.expenditure_date) as year,
  EXTRACT(QUARTER FROM we.expenditure_date) as quarter,
  wa.member_type,
  COUNT(DISTINCT wa.work_id) as works_with_expenditure,
  SUM(we.fund_disbursed_amount) as quarterly_expenditure
FROM work_expenditures we
JOIN works w ON we.work_id = w.work_id
JOIN work_analysis wa ON w.work_id = wa.work_id
WHERE we.expenditure_date IS NOT NULL
GROUP BY year, quarter, wa.member_type
ORDER BY year, quarter;
```

**Status**: **NEEDS NEW SQL** — yearly trends exist in `trends` table but quarterly do not.

### 7. Search Autocomplete Data

```sql
-- For global search dropdown
SELECT 'MP' as type, mp_id as id, mp_name as name, NULL as state_name FROM mps
UNION ALL
SELECT 'MLA' as type, mla_id as id, mla_name as name, NULL FROM mlas
UNION ALL
SELECT 'STATE' as type, state_id as id, state_name as name, NULL FROM states
ORDER BY name;
```

**Status**: **NEEDS NEW SQL** — simple but not pre-computed.

---

## FastAPI Endpoint Plan

### Endpoint List

| Method | Endpoint | Purpose | Primary Source |
|--------|----------|---------|---------------|
| GET | `/api/dashboard/overview` | National dashboard KPIs | `overall_metrics` + derived |
| GET | `/api/dashboard/trends` | Yearly trend data | `trends` |
| GET | `/api/states` | All states list with metrics | `state_metrics` |
| GET | `/api/states/{state_id}` | Single state detail | `state_metrics` + member + work queries |
| GET | `/api/states/{state_id}/constituencies` | Constituency breakdown | NEW SQL aggregation |
| GET | `/api/representatives` | All members list | `member_metrics` |
| GET | `/api/representatives/{member_type}/{id}` | Single member detail | `member_metrics` + evidence + analysis |
| GET | `/api/works` | Work list with filters | `phase_a_work_unified` |
| GET | `/api/works/{work_id}` | Single work detail | `phase_a_work_unified` + expenditures |
| GET | `/api/risk/overview` | Risk center KPIs | `overall_metrics` + derived |
| GET | `/api/risk/entities` | Risk-ranked entity list | `member_metrics` + `state_metrics` |
| GET | `/api/risk/composition` | Venn diagram data | NEW SQL on work_analysis |
| GET | `/api/search` | Global search autocomplete | NEW SQL |
| GET | `/api/ai/{entity_type}/{entity_id}` | AI analysis for entity | `ai_analysis` + `entity_evidence` |

### Response Shapes

#### GET /api/dashboard/overview

```json
{
  "total_members": 735,
  "work_bearing_members": 735,
  "zero_work_members": 80,
  "total_works": 132880,
  "recommended_works": 33815,
  "sanctioned_works": 99065,
  "completed_works": 44840,
  "ongoing_works": 54225,
  "pending_works": 33815,
  "completion_rate_pct": 33.74,
  "sanction_rate_pct": 74.55,
  "sanction_conversion_pct": 45.26,
  "allocated_amount": 83336673298.01,
  "recommended_amount": 80126275294.32,
  "sanctioned_amount": 79411464060.32,
  "expenditure_amount": 40197730903.14,
  "completion_amount": 24468395237.61,
  "unspent_amount": 39213733157.18,
  "fund_utilization_pct": 50.62,
  "expenditure_rate_pct": 50.17,
  "avg_sanction_delay_days": 117.69,
  "median_sanction_delay_days": 96.35,
  "avg_execution_days": 176.02,
  "median_execution_days": 165.18,
  "avg_project_age_days": 357.67,
  "overdue_over_1_year": 30448,
  "overdue_over_2_years": 1648,
  "flagged_works": 47650,
  "high_risk_works": 1249,
  "medium_risk_works": 9496,
  "cost_anomaly_works": 2800,
  "duration_anomaly_works": 4066,
  "classification_distribution": {
    "PERFORMER": 200,
    "AVERAGE": 300,
    "UNDERPERFORMER": 50,
    "NEEDS_ATTENTION": 100,
    "NO_DATA": 80,
    "INSUFFICIENT_DATA": 16
  },
  "calculated_at": "2026-09-09T22:50:22Z"
}
```

#### GET /api/states

```json
{
  "states": [
    {
      "state_id": 1,
      "state_name": "Maharashtra",
      "total_works": 5000,
      "sanctioned_works": 4000,
      "completed_works": 2000,
      "ongoing_works": 1500,
      "pending_works": 500,
      "allocated_amount": 15000000000,
      "recommended_amount": 14000000000,
      "sanctioned_amount": 13000000000,
      "expenditure_amount": 7000000000,
      "fund_utilization_pct": 53.85,
      "completion_rate_pct": 50.00,
      "flagged_works": 1500,
      "high_risk_works": 50,
      "risk_rate_pct": 3.00,
      "anomaly_score": 0.45,
      "anomaly_level": "NORMAL",
      "rank": 15,
      "active_members": 80,
      "mp_count": 48,
      "mla_count": 32,
      "avg_sanction_delay_days": 110,
      "avg_execution_days": 180,
      "avg_project_age_days": 360,
      "overdue_over_1_year": 3000,
      "overdue_over_2_years": 200
    }
  ]
}
```

#### GET /api/states/{state_id}

```json
{
  "state_id": 1,
  "state_name": "Maharashtra",
  "total_members": 80,
  "mp_count": 48,
  "mla_count": 32,
  "total_works": 5000,
  "recommended_works": 1200,
  "sanctioned_works": 4000,
  "completed_works": 2000,
  "ongoing_works": 1500,
  "pending_works": 500,
  "completion_rate_pct": 50.00,
  "sanction_rate_pct": 80.00,
  "allocated_amount": 15000000000,
  "recommended_amount": 14000000000,
  "sanctioned_amount": 13000000000,
  "expenditure_amount": 7000000000,
  "completion_amount": 4000000000,
  "unspent_amount": 6000000000,
  "fund_utilization_pct": 53.85,
  "flagged_works": 1500,
  "high_risk_works": 50,
  "risk_rate_pct": 3.00,
  "anomaly_score": 0.45,
  "anomaly_level": "NORMAL",
  "avg_sanction_delay_days": 110,
  "avg_execution_days": 180,
  "overdue_over_1_year": 3000,
  "overdue_over_2_years": 200,
  "representatives": [
    {
      "member_id": 1,
      "member_type": "MP",
      "member_name": "...",
      "total_works": 50,
      "completed_works": 25,
      "completion_rate_pct": 50.0,
      "fund_utilization_pct": 60.0,
      "anomaly_score": 0.8,
      "anomaly_level": "NORMAL",
      "performance_classification": "AVERAGE",
      "rank": 50
    }
  ],
  "evidence": { "... evidence JSON ..." },
  "ai_analysis": {
    "summary": "...",
    "highlights": ["...", "..."],
    "cautions": ["..."]
  }
}
```

#### GET /api/representatives/{member_type}/{id}

```json
{
  "member_id": 1,
  "member_type": "MP",
  "member_name": "Pralhad Venkatesh Joshi",
  "state_name": "Karnataka",
  "constituency_id": 100,
  "house_name": "...",
  "tenure": "18th Lok Sabha",
  "allocated_amount": 160491894.11,
  "sanctioned_amount": 120000000.00,
  "expenditure_amount": 80000000.00,
  "completion_amount": 50000000.00,
  "unspent_amount": 40000000.00,
  "fund_utilization_pct": 66.67,
  "completion_rate_pct": 40.00,
  "sanction_conversion_pct": 50.00,
  "total_works": 100,
  "recommended_works": 30,
  "sanctioned_works": 80,
  "completed_works": 40,
  "ongoing_works": 40,
  "pending_works": 20,
  "avg_sanction_delay_days": 120,
  "avg_execution_days": 180,
  "flagged_works": 30,
  "high_risk_works": 5,
  "medium_risk_works": 15,
  "anomaly_score": 0.85,
  "anomaly_level": "NORMAL",
  "confidence_level": "HIGH",
  "performance_classification": "AVERAGE",
  "rank": 50,
  "ai_analysis": {
    "summary": "...",
    "highlights": ["...", "..."],
    "cautions": ["..."]
  },
  "evidence": {
    "portfolio": { "..." },
    "financial": { "..." },
    "execution": { "..." },
    "risk": { "..." },
    "anomalies": { "..." },
    "quality": { "..." },
    "national_context": { "..." },
    "state_context": { "..." },
    "entity_anomaly": { "..." }
  },
  "sector_breakdown": [
    { "category": "Education", "expenditure_amount": 30000000, "percentage": 37.5 },
    { "category": "Health", "expenditure_amount": 20000000, "percentage": 25.0 }
  ],
  "works": [
    {
      "work_id": 133166,
      "activity_name": "...",
      "work_category": "Education",
      "status": "Completed",
      "sanction_amount": 497185,
      "expenditure_amount": 497185,
      "completion_percentage": 100.0,
      "recommendation_date": "2024-01-15",
      "sanction_date": "2024-02-01",
      "completion_date": "2024-06-15",
      "sanction_delay_days": 17,
      "execution_days": 135,
      "risk_level": "NORMAL",
      "risk_flags": []
    }
  ]
}
```

#### GET /api/works/{work_id}

```json
{
  "work_id": 133166,
  "activity_name": "...",
  "work_description": "...",
  "work_category": "Education",
  "status": "Completed",
  "member_id": 1,
  "member_type": "MP",
  "member_name": "...",
  "constituency_name": "...",
  "state_name": "Karnataka",
  "recommended_amount": 497185,
  "sanction_amount": 497185,
  "expenditure_amount": 497185,
  "completion_amount": 497185,
  "expenditure_percentage": 100.0,
  "completion_percentage": 100.0,
  "recommendation_date": "2024-01-15",
  "sanction_date": "2024-02-01",
  "first_expenditure_date": "2024-03-10",
  "last_expenditure_date": "2024-06-10",
  "completion_date": "2024-06-15",
  "sanction_delay_days": 17,
  "execution_days": 135,
  "project_age_days": 793,
  "pending_days": null,
  "risk_level": "NORMAL",
  "risk_flags": ["POST_COMPLETION_PAYMENT"],
  "flag_count": 1,
  "cost_percentile": 25,
  "duration_percentile": 10,
  "cost_status": "NORMAL",
  "duration_status": "VERY_SHORT",
  "benchmark_peer_group": "education_karnataka",
  "benchmark_sample_size": 45,
  "expenditure_timeline": [
    { "date": "2024-03-10", "amount": 200000, "vendor_name": "..." }
  ]
}
```

#### GET /api/risk/overview

```json
{
  "total_entities": 775,
  "total_works": 132880,
  "flagged_works": 47650,
  "high_risk_works": 1249,
  "medium_risk_works": 9496,
  "cost_anomaly_works": 2800,
  "duration_anomaly_works": 4066,
  "overdue_over_1_year": 30448,
  "overdue_over_2_years": 1648,
  "risk_distribution": {
    "HIGH": 50,
    "MEDIUM": 100,
    "LOW": 200,
    "NORMAL": 425
  },
  "risk_signal_composition": {
    "cost_only": 800,
    "duration_only": 1200,
    "dual_flagged": 500,
    "no_anomaly": 130380
  },
  "avg_anomaly_score": 0.15,
  "median_anomaly_score": 0.08
}
```

#### GET /api/risk/entities

Query parameters: `type` (MP|MLA|STATE), `risk_level`, `state_id`, `sort` (score|flagged|utilization|alpha), `limit`, `offset`

```json
{
  "entities": [
    {
      "entity_id": 26,
      "entity_type": "MP",
      "member_name": "SK NURUL ISLAM",
      "state_name": "West Bengal",
      "total_works": 29,
      "sanctioned_works": 18,
      "completed_works": 8,
      "fund_utilization_pct": 68.04,
      "completion_rate_pct": 27.59,
      "sanctioned_amount": 45177769,
      "expenditure_amount": 30740105,
      "flagged_works": 18,
      "high_risk_works": 1,
      "anomaly_score": 1.7901,
      "anomaly_level": "HIGH",
      "confidence_level": "MEDIUM",
      "performance_classification": "UNDERPERFORMER",
      "rank": 1
    }
  ],
  "total": 775
}
```

#### GET /api/ai/{entity_type}/{entity_id}

```json
{
  "entity_type": "MP",
  "entity_id": 1,
  "analysis": {
    "summary": "...",
    "highlights": ["...", "..."],
    "cautions": ["..."]
  },
  "model": "gemini-3.1-flash-lite",
  "generated_at": "2026-09-09T21:40:23Z",
  "evidence_version": 2,
  "evidence": {
    "portfolio": { "..." },
    "financial": { "..." },
    "execution": { "..." },
    "risk": { "..." },
    "anomalies": { "..." },
    "quality": { "..." },
    "national_context": { "..." },
    "state_context": { "..." },
    "entity_anomaly": { "..." }
  },
  "supporting_works": [
    {
      "work_id": 133166,
      "activity_name": "...",
      "evidence_role": "risk",
      "reason": "flag_count=3, risk_level=HIGH"
    }
  ]
}
```
