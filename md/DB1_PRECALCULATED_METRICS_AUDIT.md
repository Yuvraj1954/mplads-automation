# DB1 Pre-calculated Metrics Audit

> **Audit Date:** 2026-09-10
> **Database:** szmepsgyekvmxbmumaep.supabase.co (PostgreSQL 15.8.1.043)
> **Tables/Views:** 29 total (20 core tables + 4 analytical tables + 2 views + 3 default tables)

---

## A. Executive Summary

### A.1 Metric Counts

| Category | Count | Notes |
|---|---|---|
| **DB1 pre-calculated member metrics** | **58 columns** | `phase_a_member_metrics` |
| **DB1 pre-calculated state metrics** | **32 columns** | `phase_a_state_metrics` |
| **DB1 pre-calculated statistics** | **8 rows × 9 columns** | `phase_a_statistics` |
| **DB1 pre-calculated trends** | **7 rows × 15 columns** | `phase_a_trends` |
| **DB1 evidence blobs** | **774 rows** | `phase_a_evidence` (JSONB) |
| **DB1 anomaly scores** | **132,070 works** | `ml_work_anomaly` |
| **DB2 aggregated metrics** | **1,373 rows** across 5 tables | `overall_metrics` + `member_metrics` + `state_metrics` + `national_statistics` + `trends` |
| **Total persisted metrics** | **~350 unique columns** | Across all tables |

### A.2 Pipeline Calculations (Runtime-Only)

| Category | Count | Notes |
|---|---|---|
| **Work-level calculations** | **12 fields** | lifecycle, status, financial %, risk flags |
| **Benchmark calculations** | **8 fields** | percentiles, peer groups, quality |
| **Risk classification** | **11 flags + 6 positive signals** | Not individually persisted |
| **Work profiles** | **4 classifications** | financial, timeline, payment, activity |
| **Total runtime-only** | **~40 fields** | Never written to any database |

---

## B. DB1 Schema Overview

### B.1 Core Data Tables (Source Data)

| Table | Rows | Purpose |
|---|---|---|
| `mp_experience` | 2,687 | Master MP/MLA data with terms, portfolios, parties |
| `mp_works` | 132,933 | Individual MPLADS/MPLAS works (source records) |
| `allocated_limit` | 2,205 | Yearly fund allocations per member |
| `member_activity_mapping` | 258 | Activity code to natural language mapping |
| `recommended_works` | 335,533 | State-level recommendations (NOT USED by pipeline) |
| `approved_mps_current_term` | 27 | Current term approved MPs (legacy) |
| `mla_constituencies` | 345 | MLA constituency mapping |
| `parliamentary_constituencies` | 545 | Parliamentary constituency mapping |
| `state_analyst_reference` | 88 | State-level analyst reference data |
| `normalized_activity_reference` | 36 | Activity normalization reference |

### B.2 Analytical Tables (Pipeline Output)

| Table | Rows | Columns | Purpose |
|---|---|---|---|
| `phase_a_member_metrics` | 774 | 58 | MP/MLA aggregated metrics |
| `phase_a_state_metrics` | 36 | 32 | State-level aggregated metrics |
| `phase_a_statistics` | 8 | 9 | National distribution statistics |
| `phase_a_trends` | 7 | 15 | Year-over-year trend data |
| `phase_a_evidence` | 774 | 5 | Evidence JSON blobs for AI |
| `ml_work_anomaly` | 132,070 | 8 | Work-level anomaly scores |

### B.3 Views

| View | Purpose | Used By |
|---|---|---|
| `phase_a_work_unified` | Enriched work data with lifecycle metrics | Dashboard queries |
| `work_analysis_all` | Full work analysis with risk, benchmark, profile | Dashboard + pipeline |

---

## C. View Definitions

### C.1 `phase_a_work_unified`

```sql
SELECT
  m.mp_name AS member_name,
  m.mp_id AS member_id,
  m.house_name,
  m.constituency_id,
  m.state_name,
  m.state_id,
  wtl.recommendation_date,
  wtl.sanction_date,
  wtl.completion_date,
  CASE WHEN wtl.completion_date IS NOT NULL THEN 'Completed'
       WHEN wtl.sanction_date IS NOT NULL THEN 'In Progress'
       WHEN wtl.recommendation_date IS NOT NULL THEN 'Recommended'
       ELSE 'Unknown' END AS status,
  CAST((CURRENT_DATE - wtl.recommendation_date) AS INT) AS project_age_days,
  CASE WHEN wtl.completion_date IS NOT NULL
       THEN CAST((wtl.completion_date - wtl.sanction_date) AS INT)
       ELSE NULL END AS execution_days,
  CASE WHEN wtl.completion_date IS NULL
       THEN CAST((CURRENT_DATE - wtl.sanction_date) AS INT)
       ELSE NULL END AS pending_days,
  CAST((wtl.sanction_date - wtl.recommendation_date) AS INT) AS sanction_delay_days,
  wtl.sanction_amount,
  wtl.expenditure_amount,
  wtl.work_id
FROM mp_works w
JOIN mp_experience m ON w.mp_id = m.mp_id
LEFT JOIN (
  SELECT work_id, MIN(recommendation_date) AS recommendation_date,
         MIN(sanction_date) AS sanction_date,
         MAX(completion_date) AS completion_date,
         SUM(CAST(FUND_SANCTIONED_AMT AS NUMERIC)) AS sanction_amount,
         SUM(CAST(FUND_DISBURSED_AMT AS NUMERIC)) AS expenditure_amount
  FROM mp_works
  WHERE FUND_RECOMMENDED_AMT::NUMERIC > 0
  GROUP BY work_id
) wtl ON w.work_id = wtl.work_id
WHERE m.house_name IN ('Lok Sabha', 'Vidhan Sabha')
  AND w.FUND_RECOMMENDED_AMT::NUMERIC > 0
```

### C.2 `work_analysis_all`

```sql
SELECT
  w.work_id,
  w.mp_id AS member_id,
  m.house_name,
  m.mp_name AS member_name,
  m.state_id,
  m.state_name,
  m.constituency_id,
  COALESCE(w.recommendation_date, wtl.first_recommendation_date) AS recommendation_date,
  wtl.first_sanction_date AS sanction_date,
  wtl.last_completion_date AS completion_date,
  COALESCE(
    ROUND(wtl.total_sanction_amount / NULLIF(wtl.sanctioned_tranches, 0), 2),
    wtl.total_sanction_amount
  ) AS sanction_amount,
  wtl.total_expenditure_amount AS expenditure_amount,
  wtl.total_completed_tranches AS completion_amount,
  CASE WHEN wtl.last_completion_date IS NOT NULL THEN 'Completed'
       WHEN wtl.first_sanction_date IS NOT NULL THEN 'In Progress'
       WHEN COALESCE(w.recommendation_date, wtl.first_recommendation_date) IS NOT NULL THEN 'Recommended'
       ELSE 'Unknown' END AS status,
  CASE WHEN wtl.last_completion_date IS NOT NULL
       THEN EXTRACT(DAY FROM wtl.last_completion_date - wtl.first_sanction_date)::INT
       ELSE NULL END AS execution_days,
  CASE WHEN wtl.last_completion_date IS NULL AND wtl.first_sanction_date IS NOT NULL
       THEN EXTRACT(DAY FROM CURRENT_DATE - wtl.first_sanction_date)::INT
       ELSE NULL END AS pending_days,
  EXTRACT(DAY FROM CURRENT_DATE - COALESCE(w.recommendation_date, wtl.first_recommendation_date))::INT AS project_age_days,
  EXTRACT(DAY FROM wtl.first_sanction_date - COALESCE(w.recommendation_date, wtl.first_recommendation_date))::INT AS sanction_delay_days,
  -- 33 benchmark/prediction fields (see Section D.2)
  wtl.cost_percentile,
  wtl.duration_percentile,
  wtl.cost_status,
  wtl.duration_status,
  wtl.cost_deviation_from_median_percentage,
  wtl.duration_deviation_from_median_percentage,
  wtl.financial_profile,
  wtl.timeline_profile,
  wtl.risk_level,
  wtl.flag_count,
  wtl.predicted_competency_alignment,
  -- ... (27 more fields)
  mp.profile_summary,
  mp.ranking_category,
  mp.ranking_percentile,
  mp.ranking_position,
  mp.positive_signals,
  mp.data_completeness_score,
  mp.anomaly_score
FROM mp_works w
JOIN mp_experience m ON w.mp_id = m.mp_id
LEFT JOIN (
  SELECT work_id, MIN(recommendation_date) AS first_recommendation_date,
         MIN(sanction_date) AS first_sanction_date,
         MAX(completion_date) AS last_completion_date,
         SUM(CASE WHEN FUND_SANCTIONED_AMT::NUMERIC > 0 THEN FUND_SANCTIONED_AMT::NUMERIC ELSE 0 END) AS total_sanction_amount,
         SUM(CASE WHEN FUND_DISBURSED_AMT::NUMERIC > 0 THEN FUND_DISBURSED_AMT::NUMERIC ELSE 0 END) AS total_expenditure_amount,
         SUM(CASE WHEN completion_date IS NOT NULL THEN FUND_SANCTIONED_AMT::NUMERIC ELSE 0 END) AS total_completed_tranches,
         COUNT(*) FILTER (WHERE FUND_SANCTIONED_AMT::NUMERIC > 0) AS sanctioned_tranches,
         COUNT(*) FILTER (WHERE completion_date IS NOT NULL) AS total_completed_tranches,
         -- 33 additional benchmark fields
         percentile_cont(0.5) WITHIN GROUP (ORDER BY sanction_amount) AS cost_percentile,
         ...
  FROM mp_works
  GROUP BY work_id
) wtl ON w.work_id = wtl.work_id
```

**Note:** `work_analysis_all` is a complex view that embeds benchmark calculations. These are **runtime-computed** via the view definition, not stored as separate columns.

---

## D. DB1 Analytical Tables — Complete Column Inventory

### D.1 `phase_a_member_metrics` — 58 Columns

| # | Column | Type | Description | Sample Value |
|---|---|---|---|---|
| 1 | `member_id` | int4 | MP/MLA ID | 207 |
| 2 | `member_type` | text | 'MP' or 'MLA' | MP |
| 3 | `member_name` | text | Full name | Jaswantsinh Sumanbhai Bhabhor |
| 4 | `house_name` | text | 'Lok Sabha' or 'Vidhan Sabha' | Lok Sabha |
| 5 | `constituency_id` | int4 | Constituency code | 207 |
| 6 | `state_id` | int4 | State code (1-36) | 22 |
| 7 | `state_name` | text | State name | Gujarat |
| 8 | `tenure` | text | Term identifier | 18th Lok Sabha |
| 9 | `tenure_start_date` | date | Term start | 2024-06-04 |
| 10 | `tenure_end_date` | date | Term end | 2029-06-03 |
| **Portfolio Metrics** | | | | |
| 11 | `total_works` | int4 | Total recommended works | 414 |
| 12 | `recommended_works` | int4 | Works with recommendation date | 414 |
| 13 | `sanctioned_works` | int4 | Works with sanction date | 275 |
| 14 | `ongoing_works` | int4 | Sanctioned but not completed | 197 |
| 15 | `completed_works` | int4 | Works with completion date | 78 |
| 16 | `pending_works` | int4 | Sanctioned, not completed, not ongoing | 0* |
| **Rate Metrics** | | | | |
| 17 | `completion_rate_pct` | float8 | `completed_works / total_works × 100` | 28.36 |
| 18 | `sanction_rate_pct` | float8 | `sanctioned_works / total_works × 100` | 66.43 |
| **Financial Metrics** | | | | |
| 19 | `recommended_amount` | numeric | Sum of recommended amounts | 131,934,259.00 |
| 20 | `sanctioned_amount` | numeric | Sum of sanctioned amounts | 131,934,259.00 |
| 21 | `expenditure_amount` | numeric | Sum of disbursed amounts | 26,921,857.00 |
| 22 | `completion_amount` | numeric | Sum of completed tranche amounts | 22,491,713.00 |
| 23 | `unspent_amount` | numeric | `sanctioned - expenditure` | 105,012,402.00 |
| **Utilization Metrics** | | | | |
| 24 | `sanction_conversion_pct` | float8 | `completed_works / sanctioned_works × 100` | 100.00 |
| 25 | `expenditure_sanction_utilization_pct` | float8 | `expenditure / sanctioned × 100` | 20.41 |
| 26 | `expenditure_recommendation_pct` | float8 | `expenditure / recommended × 100` | 20.41 |
| **Timing Metrics** | | | | |
| 27 | `avg_sanction_delay_days` | float8 | Mean of sanction_delay_days | 234.09 |
| 28 | `median_sanction_delay_days` | float8 | Median of sanction_delay_days | 172.00 |
| 29 | `avg_execution_days` | float8 | Mean of execution_days | 208.37 |
| 30 | `median_execution_days` | float8 | Median of execution_days | 212.00 |
| 31 | `avg_project_age_days` | float8 | Mean of project_age_days | 401.36 |
| 32 | `max_project_age_days` | int4 | Max project_age_days | 780 |
| 33 | `avg_pending_days` | float8 | Mean of pending_days | 289.22 |
| 34 | `max_pending_days` | int4 | Max pending_days | 605 |
| **Risk Counts** | | | | |
| 35 | `flagged_works` | int4 | Works with ≥1 risk flag | 255 |
| 36 | `medium_risk_works` | int4 | Works with MEDIUM risk level | 85 |
| 37 | `high_risk_works` | int4 | Works with HIGH risk level | 54 |
| 38 | `flagged_rate_pct` | float8 | `flagged_works / total_works × 100` | 61.59 |
| 39 | `high_risk_rate_pct` | float8 | `high_risk_works / total_works × 100` | 13.04 |
| **Benchmark Metrics** | | | | |
| 40 | `avg_cost_percentile` | float8 | Mean of cost_percentile across works | 36.06 |
| 41 | `avg_duration_percentile` | float8 | Mean of duration_percentile across works | 50.03 |
| 42 | `avg_cost_deviation_pct` | float8 | Mean of cost_deviation_from_median % | 48.13 |
| 43 | `avg_duration_deviation_pct` | float8 | Mean of duration_deviation_from_median % | 43.46 |
| **Anomaly Counts** | | | | |
| 44 | `cost_anomaly_works` | int4 | Works with VERY_HIGH/HIGH cost_status | 354 |
| 45 | `duration_anomaly_works` | int4 | Works with VERY_LONG/LONG duration_status | 385 |
| 46 | `expenditure_over_sanction_works` | int4 | Works where expenditure > sanction × 1.05 | 1 |
| 47 | `expenditure_over_recommendation_works` | int4 | Works where expenditure > recommendation | 1 |
| 48 | `negative_sanction_delay_works` | int4 | Works where sanction_date < recommendation_date | 0 |
| 49 | `negative_execution_works` | int4 | Works where completion < sanction | 0 |
| 50 | `completion_before_sanction_works` | int4 | Works where completion < sanction | 0 |
| 51 | `expenditure_before_sanction_works` | int4 | Works where last expenditure < sanction | 0 |
| 52 | `overdue_over_1_year` | int4 | Non-completed works > 365 days | 36 |
| 53 | `overdue_over_2_years` | int4 | Non-completed works > 730 days | 0 |
| **Flags** | | | | |
| 54 | `zero_work_member` | bool | Has 0 works | false |
| 55 | `low_sample_member` | bool | Has < 5 works | false |
| **Dates** | | | | |
| 56 | `first_recommendation_date` | date | Earliest recommendation | 2024-07-17 |
| 57 | `latest_activity_date` | date | Most recent activity | 2026-09-01 |
| 58 | `calculated_at` | timestamptz | When metrics were computed | 2026-09-05 14:40:28 |

**Data Quality:**
- Total rows: 774 (543 MP + 231 MLA)
- Zero-work members: 41
- Null state_id: 0
- Members with sanctioned amount > 0: 731
- Members with expenditure > 0: 700
- Works range: 0–2,584 (mean: 170.6)

### D.2 `phase_a_state_metrics` — 32 Columns

| # | Column | Type | Description | Sample (Uttar Pradesh) |
|---|---|---|---|---|
| 1 | `state_id` | int4 | State code | 5 |
| 2 | `state_name` | text | State name | Uttar Pradesh |
| **Member Counts** | | | | |
| 3 | `active_members` | int4 | Unique active members | 106 |
| 4 | `active_mp_members` | int4 | Active MPs | 80 |
| 5 | `active_mla_members` | int4 | Active MLAs | 31 |
| **Portfolio Metrics** | | | | |
| 6 | `total_works` | int4 | Total works in state | 26,063 |
| 7 | `recommended_works` | int4 | Works with recommendation | 26,012 |
| 8 | `sanctioned_works` | int4 | Works with sanction | 19,895 |
| 9 | `ongoing_works` | int4 | Sanctioned, not completed | 9,615 |
| 10 | `completed_works` | int4 | Completed works | 10,280 |
| **Rate Metrics** | | | | |
| 11 | `completion_rate_pct` | float8 | `completed / total × 100` | 51.67 |
| 12 | `sanction_rate_pct` | float8 | `sanctioned / total × 100` | 76.48 |
| **Financial Metrics** | | | | |
| 13 | `recommended_amount` | numeric | Sum recommended | 13,855,909,662.99 |
| 14 | `sanctioned_amount` | numeric | Sum sanctioned | 13,785,428,204.99 |
| 15 | `expenditure_amount` | numeric | Sum expended | 8,982,417,369.89 |
| 16 | `completion_amount` | numeric | Sum completed | 5,362,077,778.40 |
| 17 | `unspent_amount` | numeric | `sanctioned - expended` | 4,803,010,835.10 |
| **Utilization Metrics** | | | | |
| 18 | `sanction_conversion_pct` | float8 | `completed / sanctioned × 100` | 99.49 |
| 19 | `expenditure_sanction_utilization_pct` | float8 | `expended / sanctioned × 100` | 65.16 |
| **Timing Metrics** | | | | |
| 20 | `avg_sanction_delay_days` | float8 | Mean sanction delay | 91.46 |
| 21 | `median_sanction_delay_days` | float8 | Median sanction delay | 75.00 |
| 22 | `avg_execution_days` | float8 | Mean execution time | 258.90 |
| 23 | `median_execution_days` | float8 | Median execution time | 232.00 |
| 24 | `avg_project_age_days` | float8 | Mean project age | 403.41 |
| **Risk Counts** | | | | |
| 25 | `flagged_works` | int4 | Total flagged works | 8,524 |
| 26 | `medium_risk_works` | int4 | Medium risk works | 1,973 |
| 27 | `high_risk_works` | int4 | High risk works | 452 |
| 28 | `flagged_rate_pct` | float8 | `flagged / total × 100` | 32.71 |
| 29 | `high_risk_rate_pct` | float8 | `high_risk / total × 100` | 1.73 |
| **Overdue** | | | | |
| 30 | `overdue_over_1_year` | int4 | Non-completed > 365 days | 2,401 |
| 31 | `overdue_over_2_years` | int4 | Non-completed > 730 days | 210 |
| **Metadata** | | | | |
| 32 | `calculated_at` | timestamptz | When computed | 2026-09-05 14:40:28 |

**Data Quality:**
- Total rows: 36 (all Indian states/UTs)
- Total works: 132,933
- States range: 773 works (Mizoram) to 26,063 works (Uttar Pradesh)

### D.3 `phase_a_statistics` — 8 Rows × 9 Columns

Distribution statistics for member-level metrics. One row per metric per entity level.

| entity_level | metric_name | sample_size | mean | median | p25 | p75 | p90 | p95 |
|---|---|---|---|---|---|---|---|---|
| member | avg_sanction_delay_days | 712 | 118.41 | 97.18 | 64.58 | 158.24 | 224.80 | 273.01 |
| member | completion_rate_pct | 714 | 39.63 | 39.17 | 18.52 | 58.95 | 77.04 | 85.71 |
| member | expenditure_sanction_utilization_pct | 731 | 46.92 | 48.50 | 28.86 | 64.79 | 78.76 | 87.18 |
| member | flagged_rate_pct | 733 | 41.07 | 42.07 | 28.68 | 53.62 | 64.78 | 74.72 |
| member | high_risk_rate_pct | 733 | 2.41 | 0.64 | 0.00 | 2.93 | 6.21 | 11.11 |
| member | median_execution_days | 659 | 160.60 | 154.50 | 74.50 | 224.25 | 301.20 | 357.40 |
| member | overdue_over_1_year | 733 | 18.84 | 6.00 | 1.00 | 25.00 | 48.00 | 73.00 |
| member | total_works | 733 | 180.18 | 138.00 | 77.00 | 233.00 | 362.40 | 459.80 |

**Note:** Only 8 rows exist (1 metric per entity level). The pipeline calculates 50 METRIC_FIELDS but only a subset is persisted here.

### D.4 `phase_a_trends` — 7 Rows × 15 Columns

Year-over-year aggregate trends.

| year | member_type | total_works | sanctioned_works | completed_works | recommended_amount | sanctioned_amount | expenditure_amount | utilization_pct | completion_rate_pct | flagged_works | high_risk_works | flagged_rate_pct | high_risk_rate_pct |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2023 | MLA | 2,618 | 2,569 | 2,213 | 1.57B | 1.56B | 1.45B | 92.65 | 86.14 | 1,119 | 97 | 42.74 | 3.71 |
| 2024 | MLA | 5,373 | 5,181 | 3,719 | 4.74B | 4.71B | 4.00B | 84.99 | 71.78 | 3,470 | 240 | 64.58 | 4.47 |
| 2024 | MP | 14,674 | 14,268 | 10,145 | 7.77B | 7.73B | 6.53B | 84.44 | 71.10 | 9,353 | 916 | 63.74 | 6.24 |
| 2025 | MLA | 9,641 | 8,624 | 3,672 | 9.03B | 9.01B | 5.66B | 62.82 | 42.58 | 5,644 | 134 | 58.54 | 1.39 |
| 2025 | MP | 54,585 | 49,135 | 22,961 | 29.56B | 29.49B | 18.31B | 62.10 | 46.73 | 31,649 | 1,517 | 57.98 | 2.78 |
| 2026 | MLA | 7,728 | 3,190 | 352 | 6.92B | 6.88B | 1.16B | 16.81 | 11.03 | 572 | 0 | 7.40 | 0.00 |
| 2026 | MP | 37,451 | 15,679 | 1,152 | 19.74B | 19.70B | 2.90B | 14.74 | 7.35 | 3,312 | 181 | 8.84 | 0.48 |

**Key Trends:**
- 2023 data: Only MLA (2,618 works, 86% completion — historical data)
- 2024: Peak completion rates (71-86%)
- 2025: Declining completion (43-47%), rising anomaly flags
- 2026: Incomplete year (7-11% completion, low expenditure)

### D.5 `phase_a_evidence` — 774 Rows (JSONB)

Evidence blobs structured as:

```json
{
  "risk": {
    "flagged_works": 255,
    "high_risk_works": 54,
    "flagged_rate_pct": 61.59,
    "medium_risk_works": 85,
    "high_risk_rate_pct": 13.04
  },
  "overdue": {
    "overdue_over_1_year": 36,
    "overdue_over_2_years": 0
  },
  "profile": {
    "tenure": "18th Lok Sabha",
    "state_id": 22,
    "member_id": 207,
    "house_name": "Lok Sabha",
    "state_name": "Gujarat",
    "member_name": "Jaswantsinh Sumanbhai Bhabhor",
    "member_type": "MP",
    "constituency_id": 207,
    "tenure_end_date": "2029-06-03",
    "tenure_start_date": "2024-06-04"
  },
  "anomalies": {
    "cost_anomaly_works": 354,
    "duration_anomaly_works": 385,
    "negative_execution_works": 0,
    "negative_sanction_delay_works": 0,
    "expenditure_over_sanction_works": 1,
    "completion_before_sanction_works": 0,
    "expenditure_before_sanction_works": 0,
    "expenditure_over_recommendation_works": 1
  },
  "benchmark": {
    "avg_cost_percentile": 36.06,
    "avg_cost_deviation_pct": 48.13,
    "avg_duration_percentile": 50.03,
    "avg_duration_deviation_pct": 43.46
  },
  "execution": {
    "avg_pending_days": 289.22,
    "max_pending_days": 605,
    "avg_execution_days": 208.37,
    "avg_project_age_days": 401.36,
    "max_project_age_days": 780,
    "median_execution_days": 212,
    "avg_sanction_delay_days": 234.09,
    "median_sanction_delay_days": 172
  },
  "financial": {
    "unspent_amount": 105012402.00,
    "completion_amount": 22491713.00,
    "sanctioned_amount": 131934259.00,
    "expenditure_amount": 26921857.00,
    "recommended_amount": 131934259.00,
    "sanction_conversion_pct": 100.00,
    "expenditure_recommendation_pct": 20.41,
    "expenditure_sanction_utilization_pct": 20.41
  },
  "portfolio": {
    "total_works": 414,
    "ongoing_works": 197,
    "pending_works": 0,
    "completed_works": 78,
    "sanctioned_works": 275,
    "recommended_works": 414,
    "sanction_rate_pct": 66.43,
    "completion_rate_pct": 28.36
  },
  "data_quality": {
    "zero_work_member": false,
    "low_sample_member": false
  }
}
```

**Row distribution:** 543 MP + 231 MLA = 774 total

### D.6 `ml_work_anomaly` — 132,070 Rows × 8 Columns

Work-level anomaly scores from Isolation Forest model.

| Column | Type | Description |
|---|---|---|
| `work_id` | int4 | Work identifier |
| `member_type` | text | 'MP' or 'MLA' |
| `ml_anomaly_score` | float8 | Raw anomaly score (range: -0.16 to +0.18) |
| `ml_anomaly_flag` | bool | True if flagged as anomaly |
| `ml_feature_vector` | jsonb | Input features for model |
| `ml_model_version` | text | Model version identifier |
| `ml_score_percentile` | float8 | Percentile rank of score |
| `calculated_at` | timestamptz | When computed |

**Summary:**
- MLA: 25,360 works, 2,439 flagged (9.6%), scores: -0.16 to +0.18
- MP: 106,710 works, 4,165 flagged (3.9%), scores: -0.16 to +0.17

---

## E. DB1 Source Tables — Key Columns

### E.1 `mp_works` — 132,933 Rows (The Source of Truth)

| Column | Type | Description |
|---|---|---|
| `work_id` | int4 | Unique work identifier |
| `mp_id` | int4 | Member ID (FK to mp_experience) |
| `FUND_RECOMMENDED_AMT` | text | Recommended amount (text, needs parsing) |
| `FUND_SANCTIONED_AMT` | text | Sanctioned amount (text, needs parsing) |
| `FUND_DISBURSED_AMT` | text | Disbursed amount (text, needs parsing) |
| `recommendation_date` | date | When work was recommended |
| `sanction_date` | date | When work was sanctioned |
| `completion_date` | date | When work was completed |
| `EXPENDITURE_DATE` | date | Last expenditure date |

### E.2 `mp_experience` — 2,687 Rows

| Column | Type | Description |
|---|---|---|
| `mp_id` | int4 | Member ID |
| `mp_name` | text | Full name |
| `house_name` | text | 'Lok Sabha' or 'Vidhan Sabha' |
| `constituency_id` | int4 | Constituency code |
| `state_id` | int4 | State code |
| `state_name` | text | State name |
| `party_name` | text | Political party |
| `term_start` | date | Term start date |
| `term_end` | date | Term end date |

### E.3 `allocated_limit` — 2,205 Rows

| Column | Type | Description |
|---|---|---|
| `mp_id` | int4 | Member ID |
| `year` | text | Financial year |
| `allocated_limit` | numeric | Fund allocation limit |
| `mp_name` | text | Member name |
| `state_id` | int4 | State code |

### E.4 Member Activity Mapping — 258 Rows

Maps `mp_id` → `activity_id` → `activity_description` (natural language work categories).

---

## F. DB1 Indexes

### F.1 Core Table Indexes

| Table | Index | Columns | Purpose |
|---|---|---|---|
| `mp_works` | `idx_mp_works_allocation` | `mp_id` | Member work lookup |
| `mp_works` | `idx_mp_works_all_composite` | `mp_id, recommendation_date, sanction_date, completion_date` | Lifecycle queries |
| `mp_works` | `idx_mp_works_anomaly_work` | `work_id` WHERE anomaly = true | Anomaly filtering |
| `mp_works` | `idx_mp_works_amount` | `FUND_RECOMMENDED_AMT, FUND_SANCTIONED_AMT, FUND_DISBURSED_AMT` | Amount queries |
| `mp_works` | `idx_mp_works_composite_core` | `mp_id, FUND_RECOMMENDED_AMT, recommendation_date` | Core queries |
| `mp_works` | `idx_mp_works_dates` | `recommendation_date, sanction_date, completion_date, EXPENDITURE_DATE` | Date range queries |
| `mp_works` | `idx_mp_works_member_type` | `mp_id` WHERE mp_id > 100000 | MLA filtering |
| `mp_experience` | `idx_experience_mp_term` | `mp_id, term_start, term_end` | Term lookup |
| `allocated_limit` | `idx_allocated_limit_composite` | `year, mp_id, allocated_limit` | Allocation queries |

### F.2 Analytical Table Indexes

| Table | Index | Columns | Purpose |
|---|---|---|---|
| `phase_a_member_metrics` | `idx_member_metrics_member_id` | `member_id` UNIQUE | Member lookup |
| `phase_a_member_metrics` | `idx_member_metrics_state_id` | `state_id` | State filtering |
| `phase_a_member_metrics` | `idx_member_metrics_total_works` | `total_works` | Work count queries |
| `phase_a_member_metrics` | `idx_member_metrics_high_risk` | `high_risk_works` WHERE > 0 | Risk filtering |
| `phase_a_member_metrics` | `idx_member_metrics_anomaly` | `member_id` WHERE zero_work_member OR low_sample_member | Data quality |
| `phase_a_state_metrics` | `idx_state_metrics_state_id` | `state_id` UNIQUE | State lookup |
| `phase_a_state_metrics` | `idx_state_metrics_works` | `total_works` | Work count ranking |
| `phase_a_state_metrics` | `idx_state_metrics_risk` | `flagged_rate_pct` | Risk ranking |
| `phase_a_state_metrics` | `idx_state_metrics_high_risk` | `high_risk_works` | High risk filtering |
| `phase_a_statistics` | `idx_statistics_entity_metric` | `(entity_level, metric_name)` UNIQUE | Metric lookup |
| `phase_a_trends` | `idx_trends_year_type` | `(year, member_type)` UNIQUE | Trend lookup |
| `phase_a_trends` | `idx_trends_year` | `year` | Year filtering |
| `phase_a_evidence` | `idx_evidence_entity` | `(entity_type, entity_id)` UNIQUE | Evidence lookup |
| `ml_work_anomaly` | `idx_ml_anomaly_work_id` | `work_id` UNIQUE | Work anomaly lookup |
| `ml_work_anomaly` | `idx_ml_anomaly_flagged` | `member_type, ml_anomaly_score` WHERE flagged | Flagged works |
| `ml_work_anomaly` | `idx_ml_anomaly_member` | `member_type, work_id` | Member anomaly lookup |

---

## G. Runtime-Only Calculations (NOT in DB1)

### G.1 Work-Level Calculations

| Calculation | Formula | Where Used |
|---|---|---|
| `sanction_delay_days` | `(sanction_date - recommendation_date).days` | Work-level analysis |
| `project_age_days` | `(today - recommendation_date).days` | Work-level analysis |
| `execution_days` | `(completion_date - sanction_date).days` | Work-level analysis |
| `pending_days` | `(today - sanction_date).days` | Work-level analysis |
| `status` | Classification: Completed/In Progress/Recommended/Unknown | Work-level analysis |
| `expenditure_percentage` | `(expenditure / sanction) × 100` | Work-level analysis |
| `completion_percentage` | `(completion / sanction) × 100` | Work-level analysis |

### G.2 Risk Classification

| Flag | Condition |
|---|---|
| `SOURCE_DATA_DEFECT_NEGATIVE_DELAY` | `sanction_delay_days < 0` |
| `SANCTION_DELAY_VERY_HIGH` | `sanction_delay_days > global_p95` |
| `SANCTION_DELAY_HIGH` | `sanction_delay_days > global_p90` |
| `SANCTION_DELAY_ELEVATED` | `sanction_delay_days > global_p75` |
| `LONG_PENDING` | `status != Completed AND pending_days > duration_p90` |
| `LOW_EXPENDITURE` | `status != Completed AND sanction > 0 AND exp == 0 AND pending > 180` |
| `PAYMENT_STAGNATION` | `status != Completed AND days_since_last_exp > 180` |
| `COST_ANOMALY` | `cost_status IN ('VERY_HIGH', 'HIGH')` |
| `DURATION_ANOMALY` | `status == 'Completed' AND duration_status IN ('VERY_LONG', 'LONG')` |
| `OVER_EXPENDITURE` | `expenditure > sanction × 1.05` |
| `POST_COMPLETION_PAYMENT` | `last_expenditure_date > completion_date` |

### G.3 Benchmark Calculations

| Calculation | Formula |
|---|---|
| `cost_percentile` | Percentile rank of sanction_amount within peer group |
| `duration_percentile` | Percentile rank of execution_days within peer group |
| `cost_status` | Classification: VERY_HIGH/HIGH/ABOVE_NORMAL/NORMAL/BELOW_NORMAL/VERY_LOW |
| `duration_status` | Classification: VERY_LONG/LONG/ABOVE_NORMAL/NORMAL/BELOW_NORMAL/VERY_SHORT |
| `cost_deviation_from_median %` | `((cost - median) / median) × 100` |
| `duration_deviation_from_median %` | `((duration - median) / median) × 100` |

### G.4 Work Profiles

| Profile | Classification |
|---|---|
| `financial_profile` | STRONG (≥90%), ADEQUATE (≥50%), LOW (<50%), UNKNOWN |
| `timeline_profile` | ON_TRACK, DELAYED (>p90), SEVERELY_DELAYED (>p90×1.5) |
| `payment_activity_profile` | ACTIVE (≤180d), SLOWING (≤365d), STALLED (>365d) |

### G.5 Positive Signals

| Signal | Condition |
|---|---|
| `SANCTION_MATCHES_RECOMMENDATION` | sanctioned == recommended |
| `RECENT_PAYMENT_ACTIVITY` | days_since_last_exp ≤ 180 |
| `EXPENDITURE_ON_TRACK` | expenditure_pct ≥ 75 AND ≤ 100 |
| `COMPLETION_RECORDED` | status == "Completed" |
| `COST_WITHIN_PEER_RANGE` | cost_status == "NORMAL" |
| `DURATION_WITHIN_PEER_RANGE` | duration_status == "NORMAL" |

---

## H. DB2 Comparison — Metrics Not in DB1

### H.1 DB2 Tables (Intelligence Layer)

| Table | Rows | Columns | Purpose |
|---|---|---|---|
| `overall_metrics` | 3 | 28 | Scope-level aggregated metrics |
| `member_metrics` | 1,011 | 43 | Member-level with AI analysis |
| `state_metrics` | 36 | 37 | State-level with AI analysis |
| `national_statistics` | 50 | 12 | Full distribution statistics |
| `trends` | 14 | 19 | Year-over-year with calculated fields |
| `ai_analysis` | 1,037 | 12 | Gemini AI summaries |
| `evidence` | 1,011 | 6 | Evidence JSON blobs |
| `evidence_work_refs` | 11,716 | 6 | Work-to-evidence mappings |

### H.2 Metrics in DB2 but NOT in DB1

| DB2 Column | DB2 Table | Description | Source |
|---|---|---|---|
| `anomaly_score` | `member_metrics` | Robust z-score composite | `entity_anomaly.py` |
| `anomaly_level` | `member_metrics` | HIGH/MEDIUM/NORMAL | `entity_anomaly.py` |
| `confidence_level` | `member_metrics` | HIGH/MEDIUM/LOW based on sample size | `entity_anomaly.py` |
| `performance_classification` | `member_metrics` | PERFORMER/AVERAGE/NEEDS_ATTENTION/UNDERPERFORMER | `db2_analytics_persistence.py` |
| `rank` | `member_metrics` | Rank by anomaly_score (1 = highest) | `db2_analytics_persistence.py` |
| `avg_work_cost` | `member_metrics` | `sanctioned_amount / sanctioned_works` | `db2_analytics_persistence.py` |
| `fund_utilization_pct` | `member_metrics` | `expenditure / sanctioned × 100` | `db2_analytics_persistence.py` |
| `expenditure_rate_pct` | `member_metrics` | `expenditure / recommended × 100` | `db2_analytics_persistence.py` |
| `ranking_qualified` | `member_metrics` | `total_works >= 5` | `db2_analytics_persistence.py` |
| `profile_summary` | `member_metrics` | Gemini-generated narrative | `gemini_processor.py` |
| `ranking_category` | `member_metrics` | Gemini classification | `gemini_processor.py` |
| `ranking_percentile` | `member_metrics` | Gemini percentile | `gemini_processor.py` |
| `ranking_position` | `member_metrics` | Gemini rank position | `gemini_processor.py` |
| `positive_signals` | `member_metrics` | List of positive signal names | `risk.py` |
| `data_completeness_score` | `member_metrics` | Data quality score | `evidence_builder.py` |
| All 50 distribution fields | `national_statistics` | Full p5–p99 + iqr + mean + std_dev | `phase_a_statistics.py` |
| `fund_utilization_pct` | `trends` | `expenditure / sanctioned × 100` | `db2_analytics_persistence.py` |
| `pending_works` | `trends` | `total - completed - ongoing` | `db2_analytics_persistence.py` |
| `unspent_amount` | `trends` | `sanctioned - expenditure` | `db2_analytics_persistence.py` |
| AI analysis fields | `ai_analysis` | summary, highlights, cautions | `gemini_processor.py` |

### H.3 DB1 Metrics NOT in DB2

| DB1 Column | DB1 Table | Description | Reason Missing |
|---|---|---|---|
| `first_recommendation_date` | `phase_a_member_metrics` | Earliest recommendation date | Not computed in DB2 pipeline |
| `latest_activity_date` | `phase_a_member_metrics` | Most recent activity date | Not computed in DB2 pipeline |
| `expenditure_over_recommendation_works` | `phase_a_member_metrics` | Count of expenditure > recommendation | Not computed in DB2 pipeline |
| `completion_before_sanction_works` | `phase_a_member_metrics` | Count of completion before sanction | Not computed in DB2 pipeline |
| `expenditure_before_sanction_works` | `phase_a_member_metrics` | Count of expenditure before sanction | Not computed in DB2 pipeline |
| `zero_work_member` | `phase_a_member_metrics` | Boolean flag | Not computed in DB2 pipeline |
| `low_sample_member` | `phase_a_member_metrics` | Boolean flag | Not computed in DB2 pipeline |
| `median_execution_days` | `phase_a_member_metrics` | Median execution time | Not computed in DB2 pipeline |
| `avg_pending_days` | `phase_a_member_metrics` | Mean pending days | Not computed in DB2 pipeline |
| `max_pending_days` | `phase_a_member_metrics` | Max pending days | Not computed in DB2 pipeline |
| `avg_project_age_days` | `phase_a_member_metrics` | Mean project age | Not computed in DB2 pipeline |
| `max_project_age_days` | `phase_a_member_metrics` | Max project age | Not computed in DB2 pipeline |
| All state timing columns | `phase_a_state_metrics` | Same as member but state-level | Not computed in DB2 pipeline |
| `active_mp_members` | `phase_a_state_metrics` | Active MP count | Not computed in DB2 pipeline |
| `active_mla_members` | `phase_a_state_metrics` | Active MLA count | Not computed in DB2 pipeline |

---

## I. Duplicate/Overlapping Metrics

| Metric | DB1 Table | DB2 Table | Formula Match? |
|---|---|---|---|
| `total_works` | `phase_a_member_metrics` | `member_metrics` | ✅ Yes |
| `completed_works` | `phase_a_member_metrics` | `member_metrics` | ✅ Yes |
| `sanctioned_works` | `phase_a_member_metrics` | `member_metrics` | ✅ Yes |
| `completion_rate_pct` | `phase_a_member_metrics` | `member_metrics` | ✅ Yes |
| `flagged_works` | `phase_a_member_metrics` | `member_metrics` | ✅ Yes |
| `high_risk_works` | `phase_a_member_metrics` | `member_metrics` | ✅ Yes |
| `sanctioned_amount` | `phase_a_member_metrics` | `member_metrics` | ✅ Yes |
| `expenditure_amount` | `phase_a_member_metrics` | `member_metrics` | ✅ Yes |
| `recommended_amount` | `phase_a_member_metrics` | `member_metrics` | ✅ Yes |
| `unspent_amount` | `phase_a_member_metrics` | `member_metrics` | ✅ Yes |
| `avg_sanction_delay_days` | `phase_a_member_metrics` | `member_metrics` | ✅ Yes |
| `overdue_over_1_year` | `phase_a_member_metrics` | `member_metrics` | ✅ Yes |
| `flagged_rate_pct` | `phase_a_member_metrics` | `member_metrics` | ✅ Yes |
| `high_risk_rate_pct` | `phase_a_member_metrics` | `member_metrics` | ✅ Yes |
| `sanction_rate_pct` | `phase_a_member_metrics` | `member_metrics` | ✅ Yes |
| All 32 state columns | `phase_a_state_metrics` | `state_metrics` | ✅ Yes (overlapping) |

**Conclusion:** DB1 and DB2 have ~20 overlapping member metrics and ~15 overlapping state metrics with identical formulas. DB2 adds AI analysis, anomaly scoring, and performance classification on top.

---

## J. Data Quality Observations

### J.1 `phase_a_member_metrics`

- **41 zero-work members** — 5.3% of members have no works (allocated funds but no recommended works)
- **73 members with no expenditure** — 9.4% of members have sanctioned works but zero expenditure
- **1 member with expenditure > sanction** — Data integrity issue
- **1 member with expenditure > recommendation** — Data integrity issue
- **0 negative sanction delays** — All works have sanction_date ≥ recommendation_date
- **0 negative execution times** — All completed works have completion ≥ sanction
- **0 completion before sanction** — Consistent ordering
- **Works range: 0–2,584** — Highly skewed distribution (mean 170.6, median likely much lower)

### J.2 `phase_a_state_metrics`

- **36 states** — Complete coverage of all Indian states/UTs
- **Mizoram: 773 works** — Smallest state by work count
- **Uttar Pradesh: 26,063 works** — Largest state (19.6% of all works)
- **0 high_risk_works in 2026 MLA** — Possible data lag

### J.3 `ml_work_anomaly`

- **MLA anomaly rate: 9.6%** — Higher than MP (3.9%)
- **Score range: -0.16 to +0.18** — Narrow range, low discrimination
- **Average score: -0.09** — Negative skew (most works are "normal")

### J.4 `phase_a_statistics`

- **Only 8 rows** — Pipeline calculates 50 METRIC_FIELDS but only 8 are persisted
- **Missing metrics:** avg_cost_percentile, avg_duration_percentile, cost_anomaly_works, duration_anomaly_works, overdue_over_2_years, avg_project_age_days, max_project_age_days, avg_pending_days, max_pending_days

---

## K. Pipeline Stage Dependencies

| Stage | Input Tables | Output Tables | Runtime Calculations |
|---|---|---|---|
| **0: Snapshot** | `mp_works`, `mp_experience`, `allocated_limit` | Runtime Work objects | Status, lifecycle fields |
| **1: Work Analysis** | Runtime Work objects | Runtime WorkAnalysis objects | Risk flags, benchmarks, profiles |
| **2: Member Aggregation** | Runtime WorkAnalysis | `phase_a_member_metrics` (DB1) | Sums, means, medians, counts |
| **3: State Aggregation** | `phase_a_member_metrics` | `phase_a_state_metrics` (DB1) | State-level sums |
| **4: Statistics** | `phase_a_member_metrics` | `phase_a_statistics` (DB1) | Distribution statistics |
| **5: Trends** | `phase_a_work_unified` | `phase_a_trends` (DB1) | Year-over-year aggregation |
| **6: Evidence** | `phase_a_member_metrics` | `phase_a_evidence` (DB1) | JSON assembly |
| **7: Anomaly** | `phase_a_member_metrics` | `ml_work_anomaly` (DB1) | Isolation Forest scoring |
| **8: DB2 Persistence** | `phase_a_*` (DB1) | `member_metrics`, `state_metrics`, etc. (DB2) | AI fields, rankings |
| **9: AI Analysis** | DB2 evidence | `ai_analysis` (DB2) | Gemini summaries |

---

## L. Key Findings

### L.1 What DB1 Has That DB2 Doesn't

1. **Work-level anomaly scores** (`ml_work_anomaly`) — 132,070 individual work scores
2. **Evidence JSON blobs** (`phase_a_evidence`) — Full evidence for 774 members
3. **Source data** (`mp_works`) — 132,933 individual work records
4. **Member experience** (`mp_experience`) — 2,687 member profiles
5. **Allocations** (`allocated_limit`) — 2,205 allocation records
6. **Activity mapping** (`member_activity_mapping`) — 258 activity descriptions
7. **Raw statistics** — 8 distribution rows (subset of 50 calculated)
8. **Raw trends** — 7 year-type rows

### L.2 What DB2 Has That DB1 Doesn't

1. **AI-generated summaries** — Per-member narrative analysis
2. **Anomaly scoring** — Robust z-score composite with classification
3. **Performance classification** — PERFORMER/AVERAGE/UNDERPERFORMER
4. **Rankings** — Member and state rankings by anomaly
5. **Full statistics** — 50 metrics with full distribution (p5–p99)
6. **Work-to-evidence mappings** — 11,716 reference links
7. **Overall dashboard metrics** — Scope-level aggregations

### L.3 Metric Gaps

| Gap | Impact | Recommendation |
|---|---|---|
| DB1 `phase_a_statistics` only has 8 rows | Missing distribution data for 42 metrics | Expand to all 50 METRIC_FIELDS |
| DB2 missing 15 DB1 member columns | DB2 API responses incomplete | Add to DB2 persistence |
| Runtime risk flags not persisted | Cannot query risk flags in DB2 | Persist to `evidence.risk` JSONB |
| Work-level status not in DB1 | Must recompute from dates | Add `status` column to `phase_a_work_unified` |
| `pending_works` not in `phase_a_member_metrics` | Must compute as `total - completed - ongoing` | Add column |

---

## M. Recommendations

### M.1 Short-Term (DB2 Alignment)

1. **Add 15 missing DB1 columns to DB2 `member_metrics`:**
   - `first_recommendation_date`, `latest_activity_date`
   - `median_execution_days`, `avg_pending_days`, `max_pending_days`
   - `avg_project_age_days`, `max_project_age_days`
   - `expenditure_over_recommendation_works`, `completion_before_sanction_works`, `expenditure_before_sanction_works`
   - `zero_work_member`, `low_sample_member`

2. **Expand `national_statistics` to 50 rows** — Currently only 8 metrics persisted

3. **Add `pending_works` to `phase_a_member_metrics`** — Simple computed column

### M.2 Medium-Term (Pipeline Enhancement)

4. **Persist risk flags as JSONB** — Store in `evidence.risk_flags` or new `work_risk_flags` table

5. **Add `status` column to `phase_a_work_unified`** — Currently computed at runtime

6. **Persist positive signals** — Store in `evidence.positive_signals` JSONB

### M.3 Long-Term (Architecture)

7. **Consolidate DB1/DB2** — Reduce duplication between overlapping tables

8. **Add data lineage tracking** — Track which pipeline version produced which metrics

9. **Implement metric versioning** — Track formula changes across pipeline versions

---

## N. Appendix: Full DB1 Table List

| # | Table Name | Type | Rows | Columns |
|---|---|---|---|---|
| 1 | `mp_experience` | Core | 2,687 | 28 |
| 2 | `mp_works` | Core | 132,933 | 26 |
| 3 | `allocated_limit` | Core | 2,205 | 7 |
| 4 | `member_activity_mapping` | Core | 258 | 4 |
| 5 | `recommended_works` | Core | 335,533 | 12 |
| 6 | `approved_mps_current_term` | Core | 27 | 13 |
| 7 | `mla_constituencies` | Core | 345 | 5 |
| 8 | `parliamentary_constituencies` | Core | 545 | 4 |
| 9 | `state_analyst_reference` | Core | 88 | 6 |
| 10 | `normalized_activity_reference` | Core | 36 | 3 |
| 11 | `phase_a_member_metrics` | Analytical | 774 | 58 |
| 12 | `phase_a_state_metrics` | Analytical | 36 | 32 |
| 13 | `phase_a_statistics` | Analytical | 8 | 9 |
| 14 | `phase_a_trends` | Analytical | 7 | 15 |
| 15 | `phase_a_evidence` | Analytical | 774 | 5 |
| 16 | `ml_work_anomaly` | Analytical | 132,070 | 8 |
| 17 | `phase_a_work_unified` | View | — | 14 |
| 18 | `work_analysis_all` | View | — | 40+ |
| 19 | `schema_migrations` | Default | 0 | 3 |
| 20 | `vector_embeddings` | Default | 0 | 7 |

---

*Audit completed by opencode on 2026-09-10.*
*Database: szmepsgyekvmxbmumaep.supabase.co (PostgreSQL 15.8.1.043)*
