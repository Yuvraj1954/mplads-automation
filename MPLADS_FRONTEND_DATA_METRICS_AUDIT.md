# MPLADS FRONTEND DATA/METRICS AUDIT REPORT

## 1. DATABASE SCHEMA INVENTORY

### DB1 (szmepsgyekvmxbmumaep) — Source Data + Phase A Analytics

**RAW DATA TABLES:**

```
states (36 rows)
├── state_id          BIGINT PK
└── state_name        TEXT

constituencies (582 rows)
├── constituency_id   BIGINT PK
├── constituency_name TEXT
└── state_id          BIGINT FK→states

mps (543 rows)
├── mp_id             BIGINT PK
├── mp_name           TEXT
├── house_name        TEXT (Lok Sabha)
├── constituency_id   BIGINT FK→constituencies
├── tenure            TEXT (18th Lok Sabha)
├── tenure_start_date DATE
└── tenure_end_date   DATE

mlas (232 rows)
├── mla_id            BIGINT PK
├── mla_name          TEXT
├── house_name        TEXT (Rajya Sabha/etc)
├── constituency_id   BIGINT FK→constituencies
├── tenure            TEXT
├── tenure_start_date DATE
└── tenure_end_date   DATE

mp_allocations (543 rows)
├── allocation_id     BIGINT PK
├── mp_id             BIGINT FK→mps
├── allocated_amount  NUMERIC
├── source_sno        INTEGER
├── source_tenure     TEXT
├── source_tenure_start_date DATE
└── source_tenure_end_date   DATE

mla_allocations (232 rows)
├── allocation_id     BIGINT PK
├── mla_id            BIGINT FK→mlas
├── allocated_amount  NUMERIC
└── [same source fields as mp_allocations]

works (108,204 rows) — MP works
├── work_id           BIGINT PK
├── source_work_id    TEXT
├── work_recommendation_dtl_id BIGINT
├── mp_id             BIGINT FK→mps
├── constituency_id   BIGINT FK→constituencies
├── work_category     TEXT
├── activity_name     TEXT
└── work_description  TEXT

mla_works (25,587 rows) — MLA works
├── work_id           BIGINT PK
├── source_work_id    TEXT
├── work_recommendation_dtl_id BIGINT
├── mla_id            BIGINT FK→mlas
├── constituency_id   BIGINT FK→constituencies
├── work_category     TEXT
├── activity_name     TEXT
└── work_description  TEXT

work_recommendations (107,829 rows)
├── recommendation_id BIGINT PK
├── work_id           BIGINT FK→works
├── recommendation_date DATE
├── recommended_amount NUMERIC
├── letter_no         TEXT
├── flag              INTEGER
└── sanction_date     DATE

mla_work_recommendations (25,358 rows)
├── recommendation_id BIGINT PK
├── work_id           BIGINT FK→mla_works
├── recommendation_date DATE
├── recommended_amount NUMERIC
├── letter_no         TEXT
├── flag              INTEGER
└── sanction_date     DATE

work_sanctions (108,204 rows)
├── sanction_id       BIGINT PK
├── work_id           BIGINT FK→works
├── sanction_date     DATE
├── sanction_amount   NUMERIC
├── work_stage        TEXT
├── file_status       BOOLEAN
└── recommended_date  DATE

mla_work_sanctions (25,587 rows)
├── sanction_id       BIGINT PK
├── work_id           BIGINT FK→mla_works
├── sanction_date     DATE
├── sanction_amount   NUMERIC
├── work_stage        TEXT
└── file_status       BOOLEAN

work_completions (35,000 rows)
├── completion_id     BIGINT PK
├── work_id           BIGINT FK→works
├── completion_date   DATE
├── amount_disbursed  NUMERIC
└── image             TEXT

mla_work_completions (10,040 rows)
├── completion_id     BIGINT PK
├── work_id           BIGINT FK→mla_works
├── completion_date   DATE
├── amount_disbursed  NUMERIC
└── image             TEXT

work_expenditures (85,113 rows)
├── expenditure_id    BIGINT PK
├── work_id           BIGINT FK→works
├── source_work_id    TEXT
├── expenditure_date  DATE
├── vendor_name       TEXT
├── payment_status    TEXT
├── fund_disbursed_amount NUMERIC
├── state_id          BIGINT FK→states
├── constituency_id   BIGINT FK→constituencies
└── mp_id             BIGINT FK→mps

mla_work_expenditures (24,877 rows)
├── expenditure_id    BIGINT PK
├── work_id           BIGINT FK→mla_works
├── source_work_id    TEXT
├── expenditure_date  DATE
├── vendor_name       TEXT
├── payment_status    TEXT
├── fund_disbursed_amount NUMERIC
├── state_id          BIGINT FK→states
├── constituency_id   BIGINT FK→constituencies
└── mla_id            BIGINT FK→mlas

calamities (12 rows)
├── calamity_id       BIGINT PK
├── mp_id             BIGINT FK→mps
├── calamity_type     TEXT
├── calamity_name     TEXT
├── consent_date      DATE
└── consent_amount    NUMERIC

mla_calamities (20 rows)
├── calamity_id       BIGINT PK
├── mla_id            BIGINT FK→mlas
├── calamity_type     TEXT
├── calamity_name     TEXT
├── consent_date      DATE
└── consent_amount    NUMERIC

ingestion_jobs (1,284 rows)
├── job_id            BIGINT PK
├── run_id            TEXT
├── dataset           TEXT
├── file_path         TEXT
├── status            TEXT (completed/pending/processing/failed)
├── attempts          INTEGER
├── records_processed INTEGER
├── error_message     TEXT
├── started_at        TIMESTAMPTZ
├── completed_at      TIMESTAMPTZ
└── part_number       INTEGER
```

**PHASE A ANALYTICS TABLES (DB1):**

```
work_analysis (107,636 rows — MP works with analysis)
├── work_id, member_id, member_type, constituency_id
├── state_id, state_name, work_category, activity_name
├── normalized_activity, work_description, status
├── recommended_amount, sanction_amount, expenditure_amount
├── completion_amount, recommendation_date, sanction_date
├── first_expenditure_date, last_expenditure_date, completion_date
├── sanction_delay_days, project_age_days, execution_days, pending_days
├── expenditure_percentage, completion_percentage
├── benchmark_peer_group, benchmark_quality, benchmark_sample_size
├── cost_p25/p50/p75/p90/p95, cost_percentile, cost_status
├── cost_deviation_from_median_percentage
├── duration_p25/p50/p75/p90/p95, duration_percentile, duration_status
├── duration_deviation_from_median_percentage
├── risk_flags ARRAY, flag_count, risk_level
└── last_calculated TIMESTAMPTZ

mla_work_analysis (25,297 rows — MLA works with analysis)
└── [same schema as work_analysis]

phase_a_member_metrics (774 rows)
├── member_id, member_type, member_name, house_name
├── constituency_id, state_id, state_name, tenure
├── tenure_start_date, tenure_end_date
├── total_works, recommended_works, sanctioned_works
├── ongoing_works, completed_works, pending_works
├── completion_rate_pct, sanction_rate_pct
├── recommended_amount, sanctioned_amount, expenditure_amount
├── completion_amount, unspent_amount
├── sanction_conversion_pct
├── expenditure_sanction_utilization_pct
├── expenditure_recommendation_pct
├── avg/median_sanction_delay_days, avg/median_execution_days
├── avg/max_project_age_days, avg/max_pending_days
├── flagged_works, medium_risk_works, high_risk_works
├── flagged_rate_pct, high_risk_rate_pct
├── avg_cost_percentile, avg_duration_percentile
├── avg_cost_deviation_pct, avg_duration_deviation_pct
├── cost_anomaly_works, duration_anomaly_works
├── expenditure_over_sanction_works
├── expenditure_over_recommendation_works
├── negative_sanction_delay_works, negative_execution_works
├── completion_before_sanction_works
├── expenditure_before_sanction_works
├── overdue_over_1_year, overdue_over_2_years
├── zero_work_member, low_sample_member
├── first_recommendation_date, latest_activity_date
└── calculated_at

phase_a_state_metrics (36 rows)
├── state_id, state_name
├── total_works, active_members, active_mp_members, active_mla_members
├── recommended_works, sanctioned_works, ongoing_works, completed_works
├── completion_rate_pct, sanction_rate_pct
├── recommended_amount, sanctioned_amount, expenditure_amount
├── completion_amount, unspent_amount
├── sanction_conversion_pct, expenditure_sanction_utilization_pct
├── avg/median_sanction_delay_days, avg/median_execution_days
├── avg_project_age_days
├── flagged_works, medium_risk_works, high_risk_works
├── flagged_rate_pct, high_risk_rate_pct
├── overdue_over_1_year, overdue_over_2_years
└── calculated_at

phase_a_statistics (8 rows)
├── entity_level, metric_name, sample_size
├── mean, standard_deviation, minimum
├── p25, median, p75, p90, p95, maximum
├── iqr, calculated_at

phase_a_trends (7 rows)
├── year, member_type
├── total_works, sanctioned_works, completed_works
├── recommended_amount, sanctioned_amount, expenditure_amount
├── utilization_pct, completion_rate_pct
├── flagged_works, high_risk_works
├── flagged_rate_pct, high_risk_rate_pct
└── calculated_at

phase_a_evidence (774 rows)
├── entity_id, entity_type, state_id, state_name
├── evidence_version, evidence_hash
├── evidence JSONB
└── calculated_at
```

**VIEWS (DB1):**

```
phase_a_work_unified — union of work_analysis + mla_work_analysis
work_analysis_all — union of all work analysis
```

---

### DB2 (nhtrvpsqfztuuiitydlh) — Application Analytics

```
overall_metrics (3 rows — scopes: BOTH, MP, MLA)
├── scope, total_members, work_bearing
├── zero_work_members, low_sample_members
├── total_works, recommended_works, sanctioned_works
├── completed_works, ongoing_works, pending_works
├── completion_rate_pct, sanction_rate_pct, sanction_conversion_pct
├── allocated_amount, recommended_amount, sanctioned_amount
├── expenditure_amount, completion_amount, unspent_amount
├── fund_utilization_pct, expenditure_rate_pct
├── expenditure_per_recommended
├── avg/median_work_cost, p25/p75/p90/p95_work_cost
├── avg/median_sanction_delay_days
├── avg/median_execution_days
├── avg/median_project_age_days
├── overdue_over_1_year, overdue_over_2_years
├── flagged_works, high_risk_works, medium_risk_works
├── cost_anomaly_works, duration_anomaly_works
├── negative_delay_count
├── benchmark_qualified, insufficient_benchmark
├── anomaly_qualified, calculated_at

member_metrics (1,011 rows)
├── member_id, member_type, member_name
├── state_id, state_name, constituency_id
├── house_name, tenure, tenure_start_date, tenure_end_date
├── total_works, recommended_works, sanctioned_works
├── completed_works, ongoing_works, pending_works
├── completion_rate_pct, sanction_rate_pct, sanction_conversion_pct
├── allocated_amount, recommended_amount, sanctioned_amount
├── expenditure_amount, completion_amount, unspent_amount
├── fund_utilization_pct, expenditure_rate_pct
├── avg/median_work_cost
├── avg/median_sanction_delay_days
├── avg/median_execution_days
├── avg_project_age_days, max_project_age_days
├── overdue_over_1_year, overdue_over_2_years
├── flagged_works, high_risk_works, medium_risk_works
├── flagged_rate_pct, high_risk_rate_pct
├── cost_anomaly_works, duration_anomaly_works
├── anomaly_score NUMERIC
├── anomaly_level TEXT (NORMAL/MEDIUM/HIGH)
├── confidence_level TEXT (LOW/MEDIUM/HIGH)
├── zero_work_member, low_sample_member, ranking_qualified
├── performance_classification TEXT
│   (PERFORMER/AVERAGE/NEEDS_ATTENTION/UNDERPERFORMER/
│    UNCLASSIFIED/NO_DATA/INSUFFICIENT_DATA)
├── rank INTEGER
└── calculated_at

state_metrics (36 rows)
├── state_id, state_name
├── total_members, mp_count, mla_count, active_members
├── total_works, recommended_works, sanctioned_works
├── completed_works, ongoing_works, pending_works
├── completion_rate_pct, sanction_rate_pct
├── allocated_amount, recommended_amount, sanctioned_amount
├── expenditure_amount, completion_amount, unspent_amount
├── fund_utilization_pct, expenditure_rate_pct
├── avg/median_work_cost
├── avg/median_sanction_delay_days
├── avg/median_execution_days
├── avg_project_age_days
├── overdue_over_1_year, overdue_over_2_years
├── flagged_works, high_risk_works, risk_rate_pct
├── cost_anomaly_works, duration_anomaly_works
├── anomaly_score, anomaly_level, confidence_level
├── performance_classification, rank, calculated_at

national_statistics (129 rows)
├── stat_id, metric_name, scope
├── sample_size, mean, std_dev
├── minimum, p25, median, p75, p90, p95, maximum
├── iqr, calculated_at

trends (7 rows)
├── trend_id, year, member_type
├── is_partial_year
├── total_works, recommended_works, sanctioned_works
├── completed_works, ongoing_works, pending_works
├── recommended_amount, sanctioned_amount, expenditure_amount
├── completion_amount, completion_rate_pct
├── fund_utilization_pct
├── avg_sanction_delay_days, avg_execution_days
└── calculated_at

entity_evidence (810 rows)
├── evidence_id, entity_type, entity_id, entity_name
├── state_id, state_name, constituency_id
├── evidence_version, source_data_calculated_at
├── generated_at, evidence_hash
├── evidence JSONB (portfolio/financial/execution/risk/
│   anomalies/quality/national_context/state_context/
│   entity_anomaly/master_context)
├── member_type

evidence_work_refs (11,974 rows)
├── ref_id, evidence_id
├── entity_type, entity_id, work_id
├── member_type, evidence_role
├── reason TEXT

ai_analysis (1,037 rows)
├── analysis_id, evidence_id
├── entity_type, entity_id
├── evidence_version, evidence_hash
├── model TEXT (gemini-3.1-flash-lite: 797, gemini-3.5-flash-lite: 240)
├── prompt_version
├── analysis_text TEXT (JSON with summary/highlights/cautions)
└── generated_at

evidence_pipeline_metadata (2 rows)
├── metadata_id, pipeline_version
├── evidence_schema_version, source_database_version
├── statistics_version, ml_model_version, ml_feature_version
├── generated_at, notes
```

---

## 2. EXISTING CALCULATED METRICS

### PERSISTED IN DB2 `overall_metrics`:

| Metric | Source | Column | Calculation | Scope | Persisted? | Safe? |
|--------|--------|--------|-------------|-------|------------|-------|
| Total Members | overall_metrics | total_members | COUNT(DISTINCT member) | BOTH/MP/MLA | YES | YES |
| Work-Bearing Members | overall_metrics | work_bearing | COUNT(total_works > 0) | BOTH/MP/MLA | YES | YES |
| Zero-Work Members | overall_metrics | zero_work_members | COUNT(total_works = 0) | BOTH/MP/MLA | YES | YES |
| Total Works | overall_metrics | total_works | SUM(member.total_works) | BOTH/MP/MLA | YES | YES |
| Recommended Works | overall_metrics | recommended_works | SUM | BOTH/MP/MLA | YES | YES |
| Sanctioned Works | overall_metrics | sanctioned_works | SUM | BOTH/MP/MLA | YES | YES |
| Completed Works | overall_metrics | completed_works | SUM | BOTH/MP/MLA | YES | YES |
| Ongoing Works | overall_metrics | ongoing_works | SUM | BOTH/MP/MLA | YES | YES |
| Pending Works | overall_metrics | pending_works | SUM | BOTH/MP/MLA | YES | YES |
| Completion Rate % | overall_metrics | completion_rate_pct | completed/total×100 | BOTH/MP/MLA | YES | YES |
| Sanction Rate % | overall_metrics | sanction_rate_pct | sanctioned/total×100 | BOTH/MP/MLA | YES | YES |
| Sanction Conversion % | overall_metrics | sanction_conversion_pct | completed/sanctioned×100 | BOTH/MP/MLA | YES | YES |
| Allocated Amount | overall_metrics | allocated_amount | SUM(allocations) | BOTH/MP/MLA | YES | YES |
| Recommended Amount | overall_metrics | recommended_amount | SUM | BOTH/MP/MLA | YES | YES |
| Sanctioned Amount | overall_metrics | sanctioned_amount | SUM | BOTH/MP/MLA | YES | YES |
| Expenditure Amount | overall_metrics | expenditure_amount | SUM | BOTH/MP/MLA | YES | YES |
| Completion Amount | overall_metrics | completion_amount | SUM | BOTH/MP/MLA | YES | YES |
| Unspent Amount | overall_metrics | unspent_amount | sanctioned - expenditure | BOTH/MP/MLA | YES | YES |
| Fund Utilization % | overall_metrics | fund_utilization_pct | expenditure/sanctioned×100 | BOTH/MP/MLA | YES | YES |
| Expenditure Rate % | overall_metrics | expenditure_rate_pct | expenditure/recommended×100 | BOTH/MP/MLA | YES | YES |
| Avg Work Cost | overall_metrics | avg_work_cost | AVG(sanctioned_amount/works) | BOTH/MP/MLA | YES | YES |
| Median Work Cost | overall_metrics | median_work_cost | MEDIAN | BOTH/MP/MLA | YES | YES |
| Avg Sanction Delay Days | overall_metrics | avg_sanction_delay_days | AVG | BOTH/MP/MLA | YES | YES |
| Median Sanction Delay Days | overall_metrics | median_sanction_delay_days | MEDIAN | BOTH/MP/MLA | YES | YES |
| Avg Execution Days | overall_metrics | avg_execution_days | AVG | BOTH/MP/MLA | YES | YES |
| Median Execution Days | overall_metrics | median_execution_days | MEDIAN | BOTH/MP/MLA | YES | YES |
| Avg Project Age Days | overall_metrics | avg_project_age_days | AVG | BOTH/MP/MLA | YES | YES |
| Overdue >1 Year | overall_metrics | overdue_over_1_year | SUM | BOTH/MP/MLA | YES | YES |
| Overdue >2 Years | overall_metrics | overdue_over_2_years | SUM | BOTH/MP/MLA | YES | YES |
| Flagged Works | overall_metrics | flagged_works | SUM | BOTH/MP/MLA | YES | YES |
| High Risk Works | overall_metrics | high_risk_works | SUM | BOTH/MP/MLA | YES | YES |
| Medium Risk Works | overall_metrics | medium_risk_works | SUM | BOTH/MP/MLA | YES | YES |
| Cost Anomaly Works | overall_metrics | cost_anomaly_works | SUM | BOTH/MP/MLA | YES | YES |
| Duration Anomaly Works | overall_metrics | duration_anomaly_works | SUM | BOTH/MP/MLA | YES | YES |

### PERSISTED IN DB2 `member_metrics`:

| Metric | Column | Calculation |
|--------|--------|-------------|
| Performance Classification | performance_classification | PERFORMER/AVERAGE/NEEDS_ATTENTION/UNDERPERFORMER/UNCLASSIFIED/NO_DATA/INSUFFICIENT_DATA |
| Anomaly Score | anomaly_score | Composite z-score (entity_anomaly.py) |
| Anomaly Level | anomaly_level | NORMAL/MEDIUM/HIGH (95th/80th percentile) |
| Confidence Level | confidence_level | HIGH(≥50 works)/MEDIUM(≥20)/LOW(<20) |
| Rank | rank | Based on anomaly_score DESC |

### PERSISTED IN DB2 `state_metrics`:

| Metric | Column |
|--------|--------|
| MP Count | mp_count |
| MLA Count | mla_count |
| Active Members | active_members |
| Performance Classification | performance_classification |
| Anomaly Score/Level | anomaly_score, anomaly_level |
| Risk Rate % | risk_rate_pct |

### PERSISTED IN DB2 `national_statistics`:

- 129 rows covering 43 metrics × 3 scopes (BOTH/MP/MLA)
- Each metric: mean, std_dev, min, p25, median, p75, p90, p95, max, iqr
- Metrics include: total_works, completion_rate_pct, avg_sanction_delay_days, expenditure_amount, sanctioned_amount, etc.

### PERSISTED IN DB2 `trends`:

- Year-wise data (2023-2026) × member_type (MP/MLA)
- Metrics: total_works, sanctioned_works, completed_works, recommended_amount, sanctioned_amount, expenditure_amount, completion_rate_pct, fund_utilization_pct, avg_sanction_delay_days, avg_execution_days

### PERSISTED IN DB2 `entity_evidence`:

- JSONB evidence for each member (774 MP + 231 MLA) and each state (36)
- Contains: portfolio, financial, execution, risk, anomalies, quality, national_context, state_context, entity_anomaly

### PERSISTED IN DB2 `evidence_work_refs`:

- 11,974 work-level evidence references
- Roles: risk (3,567), representative (2,291), execution (2,123), financial (2,077), positive_signal (1,916)

### PERSISTED IN DB2 `ai_analysis`:

- 1,037 Gemini-generated analysis texts
- Models: gemini-3.1-flash-lite (797), gemini-3.5-flash-lite (240)
- Entity types: MP (549), MLA (452), STATE (36)
- Format: JSON with summary, highlights, cautions

---

## 3. CALCULATION PIPELINE AUDIT

```
RAW TABLES (DB1)                    CALCULATION                ANALYTICS TABLE (DB1→DB2)
─────────────────                    ───────────                ─────────────────────────

works + mla_works ──────────► classify_status() ──────► work_analysis.status
  + work_recommendations      compute_lifecycle()         work_analysis (107K+25K rows)
  + work_sanctions            compute_financial()         - sanction_delay_days
  + work_completions          compute_benchmarks()        - execution_days
  + work_expenditures         compute_risk_flags()        - risk_flags[]
                              compute_work_analyses()     - risk_level
                                                          - cost/duration percentile/status
                    │
                    ▼
phase_a_member_metrics ──────► db2_analytics_persistence ──► DB2 member_metrics (1,011)
  compute_member_metrics()     build_member_metrics()       - performance_classification
  (aggregated from works)      _classify_member_performance() - anomaly_score/level
                               compute_member_ranks()      - rank

                    │
                    ▼
phase_a_state_metrics ────────► build_state_metrics() ────► DB2 state_metrics (36)
  compute_state_metrics()      _classify_state_performance()
  (aggregated from members)    compute_state_ranks()

                    │
                    ▼
phase_a_statistics ───────────► build_national_statistics() ► DB2 national_statistics (129)
  compute_statistics()         (percentiles for 43 metrics)

                    │
                    ▼
phase_a_trends ───────────────► build_trends() ───────────► DB2 trends (7)
  compute_trends()             (year × member_type)

                    │
                    ▼
build_overall_metrics() ───────► DB2 overall_metrics (3)
  (BOTH/MP/MLA aggregate)

                    │
                    ▼
entity_anomaly.py ────────────► anomaly_score/level ───────► DB2 member_metrics, state_metrics
  compute_entity_anomalies()   (robust z-score composite)

                    │
                    ▼
evidence_builder.py ──────────► evidence JSONB ────────────► DB2 entity_evidence (810)
  build_member_evidence()      build_state_evidence()

                    │
                    ▼
evidence_work_refs ───────────► work-level refs ───────────► DB2 evidence_work_refs (11,974)
  (risk/financial/execution/representative/positive_signal)

                    │
                    ▼
gemini_processor.py ──────────► AI analysis text ──────────► DB2 ai_analysis (1,037)
  Gemini API calls             (summary/highlights/cautions)
```

**Classification of metrics:**

| Category | Metrics |
|----------|---------|
| **A. Already calculated & persisted** | All overall_metrics, member_metrics, state_metrics, national_statistics, trends, entity_evidence, ai_analysis |
| **B. Easily calculable but not persisted** | State-wise expenditure_utilization (can be derived from state_metrics), works-per-member distribution, monthly breakdowns within years |
| **C. Require new raw data** | Geospatial coordinates, detailed vendor data, fund release timeline (beyond allocation), district-level aggregation |
| **D. AI-generated only** | Gemini analysis text (summary/highlights/cautions) — 1,037 records |
| **E. Unclear/unverified** | None — all persisted metrics are backed by deterministic calculation |

---

## 4. FRONTEND DASHBOARD METRIC MAP

### TOP KPIs

| Current UI KPI | Status | DB Source | Notes |
|----------------|--------|-----------|-------|
| Total Allocated | **AVAILABLE NOW** | DB2.overall_metrics.allocated_amount | ₹80.1B (BOTH scope) |
| Total Expenditure | **AVAILABLE NOW** | DB2.overall_metrics.expenditure_amount | ₹40.2B (BOTH) |
| Fund Utilization % | **AVAILABLE NOW** | DB2.overall_metrics.fund_utilization_pct | 50.62% (verified) |
| Total Works | **AVAILABLE NOW** | DB2.overall_metrics.total_works | 132,880 |
| Total Sanctioned Works | **AVAILABLE NOW** | DB2.overall_metrics.sanctioned_works | 99,065 |
| Total Completed Works | **AVAILABLE NOW** | DB2.overall_metrics.completed_works | 44,840 |
| Total In Progress Works | **AVAILABLE NOW** | DB2.overall_metrics.ongoing_works | 54,225 |
| Remaining/Unspent Funds | **AVAILABLE NOW** | DB2.overall_metrics.unspent_amount | ₹39.2B |

### PROJECT STATUS

| Metric | Status | DB Source |
|--------|--------|-----------|
| Total works | **AVAILABLE NOW** | overall_metrics.total_works |
| Completed | **AVAILABLE NOW** | overall_metrics.completed_works |
| In progress | **AVAILABLE NOW** | overall_metrics.ongoing_works |
| Pending | **AVAILABLE NOW** | overall_metrics.pending_works |
| Cancelled | **MISSING** | No cancelled status exists in data |
| Rejected | **MISSING** | No rejected status exists in data |
| Other statuses | **N/A** | Only 3 statuses: Completed, In Progress, Recommended |

### STATE ANALYSIS

| Metric | Status | DB Source |
|--------|--------|-----------|
| State-wise works | **AVAILABLE NOW** | state_metrics.total_works |
| State-wise expenditure | **AVAILABLE NOW** | state_metrics.expenditure_amount |
| State-wise utilization % | **AVAILABLE NOW** | state_metrics.fund_utilization_pct |
| State-wise completion % | **AVAILABLE NOW** | state_metrics.completion_rate_pct |
| Top states | **AVAILABLE NOW** | state_metrics ORDER BY metric DESC |
| Bottom states | **AVAILABLE NOW** | state_metrics ORDER BY metric ASC |

### TIME/TREND ANALYSIS

| Metric | Status | DB Source |
|--------|--------|-----------|
| Year-wise expenditure | **AVAILABLE NOW** | trends.expenditure_amount |
| Year-wise works | **AVAILABLE NOW** | trends.total_works |
| Year-wise completed works | **AVAILABLE NOW** | trends.completed_works |
| Utilization trend | **AVAILABLE NOW** | trends.fund_utilization_pct |
| Completion trend | **AVAILABLE NOW** | trends.completion_rate_pct |
| Sanction trend | **CALCULATABLE** | Derive from trends.sanctioned_works |

### MEMBER/MP ANALYSIS

| Metric | Status | DB Source |
|--------|--------|-----------|
| Number of MPs | **AVAILABLE NOW** | overall_metrics (MP scope: 538) |
| Works per MP | **AVAILABLE NOW** | member_metrics.total_works |
| Expenditure per MP | **AVAILABLE NOW** | member_metrics.expenditure_amount |
| Utilization per MP | **AVAILABLE NOW** | member_metrics.fund_utilization_pct |
| Completion per MP | **AVAILABLE NOW** | member_metrics.completion_rate_pct |
| Performance classification | **AVAILABLE NOW** | member_metrics.performance_classification |
| Anomaly/risk metrics | **AVAILABLE NOW** | member_metrics.anomaly_score, anomaly_level |

### RISK/AI

| Metric | Status | DB Source |
|--------|--------|-----------|
| Risk count (flagged works) | **AVAILABLE NOW** | overall_metrics.flagged_works = 47,650 |
| Critical/High/Medium/Low | **AVAILABLE NOW** | work_analysis.risk_level (HIGH: 880, MEDIUM: 7,149, LOW: 29,911, NORMAL: 69,696) |
| Anomaly score | **AVAILABLE NOW** | member_metrics.anomaly_score |
| Evidence count | **AVAILABLE NOW** | entity_evidence (810) |
| AI analysis count | **AVAILABLE NOW** | ai_analysis (1,037) |
| AI-generated insights | **AVAILABLE NOW** | ai_analysis.analysis_text (JSON) |
| Work-level evidence refs | **AVAILABLE NOW** | evidence_work_refs (11,974) |

### DATA HEALTH

| Metric | Status | DB Source |
|--------|--------|-----------|
| Last successful pipeline run | **AVAILABLE NOW** | ingestion_jobs (latest completed_at) |
| Latest source update | **AVAILABLE NOW** | works.updated_at MAX |
| Records processed | **AVAILABLE NOW** | ingestion_jobs.records_processed |
| Data freshness | **CALCULATABLE** | Compare latest timestamp to now |
| Failed/pending jobs | **AVAILABLE NOW** | ingestion_jobs (failed: 1, pending: 510) |

---

## 5. RECOMMENDED DASHBOARD VISUALIZATIONS

| Visualization | Metric | DB Source | Chart Type | Available? |
|---------------|--------|-----------|------------|------------|
| KPI: Total Allocated | ₹80.1B | overall_metrics | Number card | YES |
| KPI: Total Expenditure | ₹40.2B | overall_metrics | Number card | YES |
| KPI: Fund Utilization % | 50.62% | overall_metrics | Number card | YES |
| KPI: Total Works | 132,880 | overall_metrics | Number card | YES |
| Project Status Donut | Completed/In Progress/Pending | overall_metrics | Donut chart | YES |
| State Performance Bar | State-wise expenditure | state_metrics | Horizontal bar | YES |
| State Utilization Heatmap | Fund utilization by state | state_metrics | Heatmap | YES |
| National Trend Line | Year-wise expenditure | trends | Line chart | YES |
| Completion Trend | Year-wise completion % | trends | Line chart | YES |
| Member Performance Dist | classification breakdown | member_metrics | Bar chart | YES |
| Risk Summary | risk_level distribution | work_analysis | Stacked bar | YES |
| Data Health | pipeline status | ingestion_jobs | Status indicator | YES |
| Anomaly Score Distribution | anomaly_score | member_metrics | Histogram | YES |
| MP vs MLA Comparison | Both scopes | overall_metrics | Grouped bar | YES |

---

## 6. IDENTIFY FAKE/UNSUPPORTED CURRENT UI METRICS

| Current UI Element | Status | Reason |
|-------------------|--------|--------|
| "342 risk signals detected" | **REMOVE** | No such aggregate exists. Risk is per-work (47,650 flagged) and per-member. The number 342 is fabricated. |
| Critical/High/Medium/Low risk cards | **KEEP AFTER VERIFICATION** | Work-level risk_level exists (HIGH:880, MEDIUM:7,149, LOW:29,911). Verify the UI shows correct counts. |
| "High Utilizers / Good Utilizers / Moderate Utilizers" | **REPLACE** | Use actual performance_classification: PERFORMER(77), AVERAGE(136), NEEDS_ATTENTION(130), UNDERPERFORMER(44) |
| "Total MPs Analyzed: 774" | **REPLACE** | Correct is 1,011 (548 MP + 463 MLA) in DB2, or 735 work-bearing in overall_metrics. 774 is from DB1's phase_a_member_metrics (old snapshot). |
| "Recommended 83,968" | **REPLACE** | Actual recommended_works = 33,815 (overall_metrics). 83,968 is fabricated. |
| "Pipeline Total" | **KEEP** | ingestion_jobs.total = 1,284 jobs tracked |
| "Delivered works" | **REPLACE** | Use "Completed Works" (44,840). "Delivered" is ambiguous. |
| "Unspent outlay" | **KEEP** | Maps to unspent_amount = ₹39.2B |
| "MoSPI Geospatial Stream Synced" | **REMOVE** | No geospatial data exists in either database. Fabricated. |

---

## 7. IMPORTANT NUMERIC VALIDATION

**CONSISTENCY CHECKS (all PASS):**

| Check | Result |
|-------|--------|
| completed + ongoing + pending = total_works | ✅ PASS (132,880 = 132,880) |
| fund_utilization_pct = expenditure/sanctioned×100 | ✅ PASS (50.62% = 50.62%) |
| completion_rate_pct = completed/total×100 | ✅ PASS (33.74% = 33.74%) |

**DISCREPANCIES FOUND:**

| Issue | Detail |
|-------|--------|
| Member count mismatch | overall_metrics: 538 MP + 197 MLA = 735. member_metrics: 548 MP + 463 MLA = 1,011. The overall_metrics is from an older pipeline run or excludes zero-work/MLA members differently. |
| DB1 vs DB2 member count | DB1 phase_a_member_metrics: 543 MP + 231 MLA = 774. DB2 member_metrics: 548 MP + 463 MLA = 1,011. DB2 has been updated more recently with additional members. |
| Missing statuses | Only 3 statuses exist: Completed, In Progress, Recommended. No "Cancelled", "Rejected", "Pipeline" statuses in the database. |

---

## 8. FINAL OUTPUT

### A. WHAT WE ALREADY HAVE

- Total Allocated, Expenditure, Sanctioned Amount, Recommended Amount
- Fund Utilization %, Completion %, Sanction Rate %, Sanction Conversion %
- Total/Completed/Ongoing/Pending/Recommended/Sanctioned Works
- Unspent Amount, Completion Amount
- State-wise: works, expenditure, utilization, completion, sanction delay, risk rates
- Year-wise trends (2023-2026): works, expenditure, utilization, completion
- Per-member: all financial, execution, risk, anomaly metrics
- Performance classification (7 categories)
- Anomaly score, level, confidence (per member and state)
- National statistics (43 metrics × 3 scopes with full percentile distributions)
- Evidence records (810 entities) with structured JSONB
- Work-level evidence references (11,974) with 5 roles
- AI analysis (1,037 Gemini-generated summaries)
- Pipeline ingestion status (1,284 jobs tracked)

### B. WHAT WE CAN CALCULATE

- Monthly trends within years (from work_recommendations.recommendation_date)
- Activity-wise breakdown (from normalized_activity in work_analysis)
- District-level aggregation (via constituencies.state_id)
- Fund release timeline (from mp_allocations/mla_allocations)
- Work cost per state (from state_metrics.sanctioned_amount / total_works)
- Vendor-level expenditure patterns (from work_expenditures.vendor_name)
- Payment status distribution (from work_expenditures.payment_status)
- Work category breakdown (from works.work_category)

### C. WHAT WE NEED TO ADD

- District-level analytics table (currently only state-level)
- Monthly granularity in trends (currently only yearly)
- Fund release vs expenditure timeline
- Work completion velocity (works completed per month)
- Calamity-specific analytics (only 32 calamity records)
- Geospatial coordinates for map visualizations
- Historical comparison (year-over-year delta)

---

## RECOMMENDED DASHBOARD V2 STRUCTURE

**TOP (KPI Cards):**
- Total Allocated: ₹80.1B
- Total Expenditure: ₹40.2B
- Fund Utilization: 50.62%
- Total Works: 132,880

**MIDDLE:**
- Project Status Donut (Completed: 44,840 / In Progress: 54,225 / Pending: 33,815)
- State Performance Bar Chart (top 10 states by expenditure)

**BOTTOM:**
- Year-wise Trend Line (expenditure + completion rate dual-axis)
- Data Health Indicator (pipeline status, last run, freshness)

**SEPARATE PAGE:**
- Member Performance Table (sortable by classification, anomaly score)
- Risk/Anomaly Analysis (anomaly distribution, flagged works)
- AI Analysis (Gemini summaries per member)
- Evidence Explorer (work-level refs with roles)

---

## READ-ONLY VERIFICATION QUERIES

```sql
-- 1. Overall KPI verification
SELECT scope, total_works, completed_works, ongoing_works, pending_works,
       sanctioned_amount, expenditure_amount,
       ROUND((expenditure_amount/sanctioned_amount*100)::numeric,2) as util_pct,
       ROUND((completed_works::numeric/total_works*100),2) as comp_pct
FROM overall_metrics;

-- 2. Status consistency check
SELECT total_works, completed_works + ongoing_works + pending_works as sum_check,
       total_works - (completed_works + ongoing_works + pending_works) as diff
FROM overall_metrics WHERE scope = 'BOTH';

-- 3. Member count verification
SELECT 'overall' as src, scope, total_members FROM overall_metrics
UNION ALL
SELECT 'member_metrics' as src, member_type, count(*) FROM member_metrics GROUP BY member_type;

-- 4. State metrics sample
SELECT state_id, state_name, total_works, completed_works,
       fund_utilization_pct, completion_rate_pct, anomaly_level
FROM state_metrics ORDER BY total_works DESC LIMIT 10;

-- 5. Trends sample
SELECT year, member_type, total_works, completed_works,
       fund_utilization_pct, completion_rate_pct
FROM trends ORDER BY year, member_type;

-- 6. Performance classification distribution
SELECT performance_classification, count(*) FROM member_metrics GROUP BY performance_classification;

-- 7. Anomaly level distribution
SELECT anomaly_level, count(*) FROM member_metrics GROUP BY anomaly_level;

-- 8. Risk level distribution (from work_analysis)
SELECT risk_level, count(*) FROM work_analysis GROUP BY risk_level;

-- 9. Evidence coverage
SELECT entity_type, count(*) FROM entity_evidence GROUP BY entity_type;

-- 10. AI analysis coverage
SELECT entity_type, model, count(*) FROM ai_analysis GROUP BY entity_type, model;

-- 11. Pipeline status
SELECT status, count(*) FROM ingestion_jobs GROUP BY status;
SELECT run_id, MAX(completed_at) as last_run FROM ingestion_jobs WHERE status = 'completed' GROUP BY run_id ORDER BY last_run DESC LIMIT 3;

-- 12. Data freshness
SELECT MAX(updated_at) as latest_work_update FROM works;
SELECT MAX(calculated_at) as latest_analytics FROM member_metrics;
```
