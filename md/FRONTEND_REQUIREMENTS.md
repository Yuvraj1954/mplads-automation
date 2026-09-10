# FRONTEND_REQUIREMENTS.md

> Complete audit of every frontend page, its data needs, hardcoded values, and API requirements.
> Project: CivicLens AI — MPLADS Monitoring Dashboard

---

## Table of Contents

1. [Global Architecture](#global-architecture)
2. [index.html — Splash Page](#indexhtml--splash-page)
3. [dashboard.html — National Dashboard](#dashboardhtml--national-dashboard)
4. [state.html — State Explorer](#statehtml--state-explorer)
5. [statedetail.html — State Detail](#statedetailhtml--state-detail)
6. [mps.html — MP Explorer](#mpshtml--mp-explorer)
7. [mpdetail.html — MP Detail](#mpdetailhtml--mp-detail)
8. [project.html — Project Analysis](#projecthtml--project-analysis)
9. [workdetail.html — Work Detail](#workdetailhtml--work-detail)
10. [airiskcentre.html — AI Risk Center](#airiskcentrehtml--ai-risk-center)
11. [Cross-Cutting Concerns](#cross-cutting-concerns)

---

## Global Architecture

- **Stack**: Vanilla JS, Tailwind CSS v3 CDN, no frameworks, no charting libraries
- **Current data source**: 100% hardcoded in HTML/JS — zero `fetch()` calls exist
- **Sidebar**: Collapsible, state persisted via `localStorage('sidebarCollapsed')`
- **Shared components**: Sidebar navigation, KPI info popups, search autocomplete

---

## index.html — Splash Page

**Purpose**: Landing/loader page that redirects to `dashboard.html`

| Element | Current | API Needed |
|---------|---------|------------|
| Logo display | Static `logo.png` | None |
| Auto-redirect | 3s timer → `dashboard.html` | None |

**Verdict**: No API integration needed.

---

## dashboard.html — National Dashboard

### A. What the UI displays
National-level overview of MPLADS across all states, MPs, and MLAs. Shows fund flow funnel, key performance indicators, classification matrix, and execution analytics.

### B. KPI Cards (6 cards)

| # | KPI Label | Hardcoded Value | Backend Source | API Field |
|---|-----------|-----------------|----------------|-----------|
| 1 | Total Works | 782 works, 100% | `overall_metrics.total_works` (scope=BOTH) | `total_works` |
| 2 | Completion Ratio | 46.2% | `overall_metrics.completion_rate_pct` | `completion_rate_pct` |
| 3 | Fund Utilization | 50.2% | `overall_metrics.fund_utilization_pct` | `fund_utilization_pct` |
| 4 | Sanction Conversion | 67.4% | `overall_metrics.sanction_conversion_pct` | `sanction_conversion_pct` |
| 5 | Classification Matrix | COMPLETE/IN_PROGRESS/NO_DATA/INSUFFICIENT_DATA bars | Derived from `member_metrics.performance_classification` counts | New aggregation needed |
| 6 | Fund Flow | Allocated ₹80,140.5 Cr → Recommended ₹76,400 Cr → Sanctioned ₹68,250 Cr → Expenditure ₹40,210.8 Cr | `overall_metrics.{recommended,sanctioned,expenditure}_amount` | `recommended_amount`, `sanctioned_amount`, `expenditure_amount` |

### C. Charts/Graphs

| # | Chart | Type | Current Data | Backend Source | New SQL Needed? |
|---|-------|------|-------------|----------------|-----------------|
| 1 | Fund Flow Funnel | Horizontal bars (CSS) | Hardcoded ₹ values | `overall_metrics` | No — data exists |
| 2 | Classification Matrix | Horizontal bars | Hardcoded counts | Needs aggregation from `member_metrics.performance_classification` | **YES** — GROUP BY performance_classification, COUNT |
| 3 | Execution Analytics | Text metrics | Avg Sanction Delay 42d, Avg Execution 18.4mo, Active Overdue 12,410, Project Age >2yr 8,940 | `overall_metrics.{avg_sanction_delay_days, avg_execution_days, overdue_over_1_year, avg_project_age_days}` | No — data exists |

### D. Tables/Lists
None.

### E. Filters/Search

| Control | Type | Current | API Needed |
|---------|------|---------|------------|
| Search dropdown | Static list | 6 hardcoded search suggestions | Should dynamically populate from member/state data |
| Activity Trend toggle | Button group | Works/Expenditure/Completion | Needs trend data from `trends` table |

### F. Click Behavior
- KPI info buttons → show tooltip popup (no API)
- Sidebar links → navigate to other pages

### G. Required API Response Fields
```
GET /api/dashboard/overview
{
  total_works, completion_rate_pct, fund_utilization_pct, sanction_conversion_pct,
  recommended_amount, sanctioned_amount, expenditure_amount, completion_amount,
  avg_sanction_delay_days, median_sanction_delay_days, avg_execution_days,
  overdue_over_1_year, overdue_over_2_years, avg_project_age_days,
  flagged_works, high_risk_works, medium_risk_works,
  total_members, zero_work_members,
  classification_counts: { PERFORMER: N, AVERAGE: N, UNDERPERFORMER: N, NEEDS_ATTENTION: N, NO_DATA: N, INSUFFICIENT_DATA: N }
}
```

### H. Direct vs Derived

| Metric | Source | Direct/Derived |
|--------|--------|----------------|
| total_works | `overall_metrics` | Direct |
| completion_rate_pct | `overall_metrics` | Direct |
| fund_utilization_pct | `overall_metrics` | Direct |
| sanction_conversion_pct | `overall_metrics` | Direct |
| Fund flow amounts | `overall_metrics` | Direct |
| Classification counts | `member_metrics` | **Derived** — needs GROUP BY |
| Execution metrics | `overall_metrics` | Direct |

### I. Hardcoded/Mock Values — MUST Become Dynamic

| Element | Current Hardcoded Value | Replacement |
|---------|------------------------|-------------|
| "782 works" | Static number | `overall_metrics.total_works` |
| "46.2%" | Static percentage | `overall_metrics.completion_rate_pct` |
| "50.2%" | Static percentage | `overall_metrics.fund_utilization_pct` |
| "67.4%" | Static percentage | `overall_metrics.sanction_conversion_pct` |
| Fund Flow ₹ values | Static ₹80,140.5 Cr etc. | `overall_metrics` amounts |
| "14 works (1.8%)" | Static | Needs calculation from member_metrics |
| "Avg Sanction Delay 42 days" | Static | `overall_metrics.avg_sanction_delay_days` |
| "Avg Execution Duration 18.4 months" | Static | `overall_metrics.avg_execution_days` / 30 |
| "Active Overdue Works 12,410" | Static | `overall_metrics.overdue_over_1_year` |
| "Project Age >2yr 8,940" | Static | Needs new query or `overall_metrics.avg_project_age_days` |
| Classification matrix bars | Static percentages | Derived from `member_metrics` |

### J. Elements to Remove/Reconsider
None — all current elements are supported by backend data.

---

## state.html — State Explorer

### A. What the UI displays
Grid of 36 state/UT cards with key metrics. SVG scatter plot of state performance. Ranking bars by 4 metrics.

### B. KPI Cards (36 state cards)
Each card shows: State Name, Allocated ₹, Expenditure ₹, Fund Utilization %, Total Works, Completed Works, Completion Rate %.

### C. Charts/Graphs

| # | Chart | Current Data | Backend Source | New SQL Needed? |
|---|-------|-------------|----------------|-----------------|
| 1 | Scatter Plot (SVG circles) | 36 states with cx/cy coordinates mapped to utilization% and completion% | `state_metrics` | No |
| 2 | State Ranking Bars | 4 tabs (Utilization/Completion/Expenditure/Works) × 10 states each | `state_metrics` sorted | No |

### D. Tables/Lists
State ranking bars (top 10 per metric).

### E. Filters/Search

| Control | Type | Current | API Needed |
|---------|------|---------|------------|
| State search | Autocomplete | 36 hardcoded state names | `GET /api/states` |
| Region filter | Dropdown | Hardcoded regions | **NEEDS MAPPING** — states don't have region field; would need manual mapping or derive from state grouping |
| Performance filter | Dropdown | All/Underperformer/Needs Attention | `state_metrics.anomaly_level` |
| Sort | Dropdown | Utilization/Completion/Expenditure/Works | Sorting parameter |

### F. Click Behavior
- State card → navigate to `statedetail.html?state_id={id}`
- Scatter dot → same as card click

### G. Required API Response Fields
```
GET /api/states
{
  states: [{
    state_id, state_name,
    total_works, sanctioned_works, completed_works, ongoing_works, pending_works,
    allocated_amount, recommended_amount, sanctioned_amount, expenditure_amount,
    completion_amount, unspent_amount,
    fund_utilization_pct, completion_rate_pct,
    flagged_works, high_risk_works, risk_rate_pct,
    anomaly_score, anomaly_level, rank,
    active_members, mp_count, mla_count,
    avg_sanction_delay_days, avg_execution_days, avg_project_age_days,
    overdue_over_1_year, overdue_over_2_years
  }]
}
```

### H. Direct vs Derived

| Metric | Source | Direct/Derived |
|--------|--------|----------------|
| All card metrics | `state_metrics` | Direct |
| Scatter coordinates | `state_metrics.{fund_utilization_pct, completion_rate_pct}` | Derived (frontend mapping) |
| Ranking sort | `state_metrics` | Direct (sort parameter) |

### I. Hardcoded/Mock Values — MUST Become Dynamic

| Element | Current | Replacement |
|---------|---------|-------------|
| 36 state cards with financial data | All hardcoded in HTML markup | `GET /api/states` |
| SVG scatter coordinates | Hardcoded cx/cy per state | Frontend maps `fund_utilization_pct` → cx, `completion_rate_pct` → cy |
| Ranking datasets | 4 JS arrays with 10 states each | `state_metrics` sorted by respective field |
| State names | Hardcoded in HTML | `GET /api/states` |

### J. Elements to Remove/Reconsider
- **Region filter**: No region field exists in the database. Either add a static mapping table (North/South/East/West/Central/NE) or remove this filter.
- **Allocated Amount column**: `allocated_amount` is 0.00 in `overall_metrics` — the allocation data flows through `mp_allocations`/`mla_allocations` but is not aggregated to state level in `state_metrics`. Need to either aggregate from member_metrics or remove this column.

---

## statedetail.html — State Detail

### A. What the UI displays
Deep dive into a single state. 8 tab panels: Overview, Representatives, Projects, Financial, Risk, Trends, Constituencies, AI Analysis.

### B. KPI Cards (Overview tab)

| # | KPI | Hardcoded Value | Backend Source |
|---|-----|-----------------|----------------|
| 1 | Fund Utilization | 88.4% | `state_metrics.fund_utilization_pct` |
| 2 | Completion Rate | 46.2% | `state_metrics.completion_rate_pct` |
| 3 | Total Works | 1,482 | `state_metrics.total_works` |
| 4 | Expenditure | ₹95.8 Cr | `state_metrics.expenditure_amount` |
| 5 | Flagged Works | 18 | `state_metrics.flagged_works` |
| 6 | Active Members | 208 | `state_metrics.active_members` |
| 7 | Risk Rate | 1.2% | `state_metrics.risk_rate_pct` |
| 8 | Overdue Works | 0 | `state_metrics.overdue_over_1_year` |

### C. Charts/Graphs

| # | Chart | Current Data | Backend Source | New SQL Needed? |
|---|-------|-------------|----------------|-----------------|
| 1 | NE States Scatter | 8 hardcoded NE states | Filter `state_metrics` by NE states | No |
| 2 | Quarterly Capital Deployment | Hardcoded quarterly line chart | **NOT IN DATABASE** — needs new time-based aggregation from expenditure dates | **YES** |

### D. Tables/Lists

| Table | Current | Backend Source |
|-------|---------|----------------|
| Constituency Intelligence Matrix | 60 hardcoded constituencies | `constituencies` + aggregated work data per constituency |
| Representatives tab | Hardcoded member cards | `member_metrics` filtered by state_id |
| Projects tab | Hardcoded project list | `phase_a_work_unified` filtered by state_id |
| Evidence items | 5 hardcoded items | `entity_evidence` where entity_type='STATE' AND entity_id={state_id} |

### E. Filters/Search

| Control | Current | API Needed |
|---------|---------|------------|
| Tab navigation | 8 tabs | Client-side |
| Evidence filter | All/Risk/Financial/Execution/Positive | `entity_evidence.evidence` JSON keys |
| Rep toggle | Combined/MPs/MLAs | Filter by member_type |

### F. Click Behavior
- Representative card → `mpdetail.html?mp_id={id}` or `mpdetail.html?mla_id={id}`
- Project card → `workdetail.html?work_id={id}`

### G. Required API Response Fields
```
GET /api/states/{state_id}
{
  // From state_metrics
  state_id, state_name, total_works, ... (all state_metrics fields),
  // From member_metrics filtered
  representatives: [{
    member_id, member_type, member_name, total_works,
    completion_rate_pct, fund_utilization_pct, anomaly_score, anomaly_level,
    performance_classification, sanctioned_amount, expenditure_amount, rank
  }],
  // From phase_a_work_unified filtered
  works: [{
    work_id, member_id, member_type, activity_name, status,
    sanctioned_amount, expenditure_amount, completion_percentage,
    risk_level, risk_flags, sanction_date, completion_date
  }],
  // From entity_evidence
  evidence: { ... evidence JSON ... },
  ai_analysis: { summary, highlights, cautions },
  // From constituencies aggregated
  constituencies: [{
    constituency_id, constituency_name, total_works, completed_works,
    fund_utilization_pct, completion_rate_pct
  }]
}
```

### H. Direct vs Derived

| Metric | Source | Direct/Derived |
|--------|--------|----------------|
| State KPIs | `state_metrics` | Direct |
| Representatives | `member_metrics` WHERE state_id | Direct |
| Works | `phase_a_work_unified` WHERE state_id | Direct |
| Evidence | `entity_evidence` | Direct |
| Constituency metrics | **Needs new aggregation** | **Derived** — GROUP BY constituency |
| Quarterly trends | **Needs new calculation** | **Derived** — GROUP BY quarter from expenditure dates |

### I. Hardcoded/Mock Values — MUST Become Dynamic

Every value on this page is hardcoded. Key replacements:
- All KPI numbers → `state_metrics`
- Scatter nodes → `state_metrics` for the selected state's constituencies
- Constituency matrix → New constituency-level aggregation
- Representative cards → `member_metrics` filtered by state
- Financial funnel → `state_metrics.{recommended_amount, sanctioned_amount, expenditure_amount}`
- Evidence items → `entity_evidence`
- AI Analysis → `ai_analysis`

### J. Elements to Remove/Reconsider
- **Quarterly Capital Deployment chart**: No quarterly data exists. Either build a new SQL aggregation from `work_expenditures.expenditure_date` grouped by quarter, or replace with a simpler yearly trend from `trends` table.
- **Scatter plot of NE states**: This is a comparison view hardcoded to NE states. Make it dynamic by allowing any state comparison, or keep as a curated NE view with data from `state_metrics`.

---

## mps.html — MP Explorer

### A. What the UI displays
Grid of MP profile cards with financial and performance metrics. SVG scatter plot of MP performance.

### B. KPI Cards (6 MP cards)

Each card shows: MP Name, Constituency, State, Party, Allocated ₹, Expenditure ₹, Fund Utilization %, Works Completed/Recommended, Completion Rate %.

### C. Charts/Graphs

| # | Chart | Current Data | Backend Source |
|---|-------|-------------|----------------|
| 1 | Scatter Plot (SVG) | 6 MPs with data attributes | `member_metrics` filtered to MP type |

### D. Tables/Lists
MP profile cards in a grid.

### E. Filters/Search

| Control | Current | API Needed |
|---------|---------|------------|
| Search autocomplete | 6 hardcoded MP names | `GET /api/representatives?type=MP` |
| Filter controls | Hardcoded | Filter by state, performance classification |

### F. Click Behavior
- MP card → `mpdetail.html?mp_id={id}`

### G. Required API Response Fields
```
GET /api/representatives?type=MP
{
  members: [{
    member_id, member_name, member_type,
    state_name, constituency_id, constituency_name, house_name,
    allocated_amount, sanctioned_amount, expenditure_amount,
    fund_utilization_pct, completion_rate_pct,
    total_works, recommended_works, sanctioned_works, completed_works,
    anomaly_score, anomaly_level, performance_classification, rank,
    flagged_works, high_risk_works
  }]
}
```

### I. Hardcoded/Mock Values — MUST Become Dynamic

All 6 MP cards are fully hardcoded with names, financials, and metrics. Must be replaced with API data.

### J. Elements to Remove/Reconsider
- **Party column**: The `mps` table has no party field. Either add party data to the ingestion pipeline (if available from source API) or remove this column.

---

## mpdetail.html — MP Detail

### A. What the UI displays
Deep dive for a single MP. Profile header, financial analysis, AI analysis, works table, recommended works.

### B. KPI Cards

| # | KPI | Hardcoded Value | Backend Source |
|---|-----|-----------------|----------------|
| 1 | Initial Allocation | ₹9.8 Cr | `member_metrics.allocated_amount` or `mp_allocations` |
| 2 | Sanctioned | ₹7.14 Cr | `member_metrics.sanctioned_amount` |
| 3 | Expenditure | ₹7.14 Cr | `member_metrics.expenditure_amount` |
| 4 | Remaining | ₹2.66 Cr | `member_metrics.unspent_amount` |

### C. Charts/Graphs

| # | Chart | Current | Backend Source | New SQL Needed? |
|---|-------|---------|----------------|-----------------|
| 1 | Fund Progression Funnel | Hardcoded | `member_metrics` | No |
| 2 | Expenditure by Sector | 4 sectors (Education 44.8%, Health 25.9%, Water 17.5%, Infrastructure 11.8%) | **NOT IN DATABASE** — needs GROUP BY work_category from works | **YES** |

### D. Tables/Lists

| Table | Current | Backend Source |
|-------|---------|----------------|
| Works table | 17 pages hardcoded | `phase_a_work_unified` filtered by member_id |
| Recommended works | 143 projects, 24 pages | `work_recommendations` joined with `works` for this member |
| Evidence items | Hardcoded with categories | `entity_evidence` where entity_type='MP' |

### E. Filters/Search

| Control | Current | API Needed |
|---------|---------|------------|
| Year select | Hardcoded years | Filter by recommendation_date year |
| Cost range slider | Hardcoded | Filter by sanction_amount range |
| Search input | Text search | Filter by activity_name/work_description |

### F. Click Behavior
- Work row → `workdetail.html?work_id={id}`
- Evidence items → expand to show details

### G. Required API Response Fields
```
GET /api/representatives/{id}
{
  // From member_metrics
  member_id, member_name, member_type, state_name, constituency_id,
  allocated_amount, sanctioned_amount, expenditure_amount, completion_amount,
  unspent_amount, fund_utilization_pct, completion_rate_pct, sanction_conversion_pct,
  total_works, recommended_works, sanctioned_works, completed_works, ongoing_works, pending_works,
  avg_sanction_delay_days, avg_execution_days, avg_project_age_days,
  flagged_works, high_risk_works, medium_risk_works,
  anomaly_score, anomaly_level, confidence_level, performance_classification,
  // From ai_analysis
  ai_analysis: { summary, highlights, cautions },
  // From entity_evidence
  evidence: { portfolio, financial, execution, risk, anomalies, quality, national_context, state_context, entity_anomaly },
  // From phase_a_work_unified
  works: [{
    work_id, activity_name, work_description, work_category, status,
    sanctioned_amount, expenditure_amount, completion_percentage,
    recommendation_date, sanction_date, completion_date,
    sanction_delay_days, execution_days, risk_level, risk_flags
  }],
  // Expenditure by sector (derived)
  sector_breakdown: [{ category, expenditure_amount, percentage }]
}
```

### I. Hardcoded/Mock Values — MUST Become Dynamic

Every value is hardcoded. Key replacements:
- Financial ledger → `member_metrics`
- Works table → `phase_a_work_unified`
- Recommended works → `work_recommendations` joined
- Evidence → `entity_evidence`
- AI Analysis → `ai_analysis`
- Expenditure by sector → New aggregation

### J. Elements to Remove/Reconsider
- **Party field**: Not in database. Remove or add to pipeline.
- **Expenditure by Sector chart**: Requires new SQL: `GROUP BY work_category` from works where expenditure > 0. This is feasible.
- **Recommended works table**: The `work_recommendations` table has recommended works that may not yet be sanctioned. This is valid data.

---

## project.html — Project Analysis

### A. What the UI displays
State → Constituency → Work drill-down. Cards for works with risk flags and progress indicators.

### B. KPI Cards
None at top level — dynamic card grid.

### C. Charts/Graphs
None — card-based layout.

### D. Tables/Lists
Work cards showing: Work ID, Activity, Status, Progress %, Sanction Amount, Risk Flags.

### E. Filters/Search

| Control | Current | API Needed |
|---------|---------|------------|
| State select | 8 hardcoded states | `GET /api/states` |
| Constituency select | Dynamic per state | `GET /api/states/{id}/constituencies` |
| Work search | Text input | Filter by activity_name |
| Sort | Highest completion / Highest sanction | Sort parameter |

### F. Click Behavior
- Work card → `workdetail.html?work_id={id}`

### G. Required API Response Fields
```
GET /api/works?state_id={}&constituency_id={}&search={}&sort={}
{
  works: [{
    work_id, activity_name, work_description, work_category, status,
    member_id, member_type, member_name,
    constituency_name, state_name,
    recommended_amount, sanction_amount, expenditure_amount,
    completion_percentage, expenditure_percentage,
    recommendation_date, sanction_date, completion_date,
    risk_level, risk_flags, flag_count,
    sanction_delay_days, project_age_days
  }],
  total: N
}
```

### I. Hardcoded/Mock Values — MUST Become Dynamic

The entire `dataset` JS object is hardcoded with 8 states, sample constituencies, and 10 sample works. Must be replaced entirely.

### J. Elements to Remove/Reconsider
- **MLA works**: The `mla_works` and `mla_work_analysis` tables exist but `project.html` currently only shows MP works. Consider adding MLA support via `member_type` filter.
- **Risk flag badges**: Currently uses custom flag names like `COST_ANOMALY`, `DURATION_ANOMALY`. These match the actual `risk_flags` array values in the database.

---

## workdetail.html — Work Detail

### A. What the UI displays
Single work detail: description, status, financial summary, timeline, risk assessment.

### B. KPI/Info Display

| Field | Hardcoded Value | Backend Source |
|-------|-----------------|----------------|
| Work Name | "Construction of Multipurpose Community Hall at Mawlai Phudmuri" | `works.activity_name` or `works.work_description` |
| Status | ONGOING | `phase_a_work_unified.status` |
| Sanction Order | PLN/MPLADS/2024/782 | `work_sanctions` data (if available) |
| State | Meghalaya | `phase_a_work_unified.state_name` |
| Constituency | Shillong | `constituencies.constituency_name` |
| MP | Dr. Andrew J. Syngkon | `mps.mp_name` via member_id |
| Sanctioned Amount | ₹35.0L | `phase_a_work_unified.sanction_amount` |
| 1st Expenditure | ₹10.5L | First `work_expenditures.fund_disbursed_amount` |
| Last Tranche | ₹6.0L | Last `work_expenditures.fund_disbursed_amount` |

### C. Charts/Graphs
Timeline visualization (CSS-based, not a charting library).

### D. Timeline Milestones

| Milestone | Date | Backend Source |
|-----------|------|----------------|
| Recommendation | 02 Sep 2024 | `phase_a_work_unified.recommendation_date` |
| Sanction | 14 Oct 2024 | `phase_a_work_unified.sanction_date` |
| First Expenditure | 18 Dec 2024 | `phase_a_work_unified.first_expenditure_date` |
| Last Expenditure | 22 Jun 2025 | `phase_a_work_unified.last_expenditure_date` |
| Completion % | 65% | `phase_a_work_unified.completion_percentage` |

### E. Risk Assessment

| Field | Current | Backend Source |
|-------|---------|----------------|
| Expenditure-to-Progress Delta | +5.0% (in tolerance) | `expenditure_percentage - completion_percentage` |
| Risk Flags | Not shown | `phase_a_work_unified.risk_flags` |
| Risk Level | Not shown | `phase_a_work_unified.risk_level` |

### F. Required API Response Fields
```
GET /api/works/{work_id}
{
  work_id, activity_name, work_description, work_category,
  status, member_id, member_type, member_name,
  constituency_name, state_name,
  recommended_amount, sanction_amount, expenditure_amount, completion_amount,
  expenditure_percentage, completion_percentage,
  recommendation_date, sanction_date, first_expenditure_date,
  last_expenditure_date, completion_date,
  sanction_delay_days, execution_days, project_age_days, pending_days,
  risk_level, risk_flags, flag_count,
  cost_percentile, duration_percentile, cost_status, duration_status,
  expenditure_timeline: [{ date, amount, vendor_name }],
  benchmark: { peer_group, sample_size, cost_p50, duration_p50 }
}
```

### I. Hardcoded/Mock Values — MUST Become Dynamic

Every value is hardcoded. Must be replaced with API data from `phase_a_work_unified` + `work_expenditures`.

### J. Elements to Remove/Reconsider
- **Sanction Order number**: Not stored in the database (the `letter_no` field exists in `work_recommendations` but may be NULL). Show if available, else hide.

---

## airiskcentre.html — AI Risk Center

### A. What the UI displays
Risk intelligence hub: KPI tiles for anomaly metrics, entity-level risk rankings (MPs/MLAs/States), filterable by risk level and state.

### B. KPI Cards (8 tiles)

| # | KPI | Hardcoded Value | Backend Source |
|---|-----|-----------------|----------------|
| 1 | Anomaly Score | 59.4/100 | Derived from top anomaly score |
| 2 | AI Confidence | 87% | From `ai_analysis` or `member_metrics.confidence_level` |
| 3 | Flagged Works | 29 | `overall_metrics.flagged_works` |
| 4 | High-Risk Severity | 8 | `overall_metrics.high_risk_works` |
| 5 | Total Synthesized Entities | 808 | Count of `member_metrics` + `state_metrics` |
| 6 | Monitored Works | 132,880 | `overall_metrics.total_works` |
| 7 | Cost Anomaly | 2,918 | `overall_metrics.cost_anomaly_works` |
| 8 | Duration Anomaly | 3,494 | `overall_metrics.duration_anomaly_works` |
| 9 | Overdue Works | 1,842 | `overall_metrics.overdue_over_2_years` (or over_1_year) |

### C. Charts/Graphs

| # | Chart | Current Data | Backend Source |
|---|-------|-------------|----------------|
| 1 | Representative Risk Distribution (Donut) | Hardcoded: 774 reps, Normal 551, Medium 142, High 81 | `member_metrics` GROUP BY anomaly_level |
| 2 | Performance vs Anomaly Matrix (Scatter) | 5 hardcoded MP dots | `member_metrics` scatter: anomaly_score vs fund_utilization_pct |
| 3 | Risk Signal Composition (Venn) | Hardcoded: Cost Only 1,894, Duration Only 2,470, Dual 1,024 | Needs query: `work_analysis` WHERE cost_anomaly AND/OR duration_anomaly |
| 4 | Historical Risk Trend (Line) | Hardcoded quarterly data | `trends` table (yearly, not quarterly) |

### D. Tables/Lists

| Table | Current | Backend Source |
|-------|---------|----------------|
| Highest-Priority MPs | 6 hardcoded | `member_metrics` WHERE member_type='MP' ORDER BY anomaly_score DESC |
| Highest-Priority MLAs | 3 hardcoded | `member_metrics` WHERE member_type='MLA' ORDER BY anomaly_score DESC |
| Highest-Priority States | 4 hardcoded | `state_metrics` ORDER BY anomaly_score DESC |

### E. Filters/Search

| Control | Current | API Needed |
|---------|---------|------------|
| Risk Level checkboxes | High/Medium/Low | Filter by `anomaly_level` |
| State dropdown | 5 hardcoded states | `GET /api/states` |
| Sort By | Score/Flagged/Utilization/Alpha | Sort parameter |
| Entity toggle | MPs/MLAs/States | Entity type filter |
| Analyze + Reset buttons | Trigger filtering | Client-side filter |

### F. Click Behavior
- Entity row → `mpdetail.html?mp_id={id}` or `statedetail.html?state_id={id}`

### G. Required API Response Fields
```
GET /api/risk/overview
{
  total_entities, total_works, flagged_works, high_risk_works,
  cost_anomaly_works, duration_anomaly_works,
  overdue_over_1_year, overdue_over_2_years,
  risk_distribution: { HIGH: N, MEDIUM: N, LOW: N, NORMAL: N },
  avg_anomaly_score, median_anomaly_score
}

GET /api/risk/entities?type={MP|MLA|STATE}&risk_level={}&state_id={}&sort={}&limit={}&offset={}
{
  entities: [{
    entity_id, entity_type, entity_name, state_name,
    anomaly_score, anomaly_level, confidence_level,
    total_works, flagged_works, high_risk_works,
    fund_utilization_pct, completion_rate_pct,
    performance_classification, rank
  }],
  total: N
}
```

### H. Direct vs Derived

| Metric | Source | Direct/Derived |
|--------|--------|----------------|
| KPI tiles | `overall_metrics` | Direct |
| Entity rankings | `member_metrics` / `state_metrics` | Direct |
| Risk distribution donut | `member_metrics` GROUP BY anomaly_level | Derived |
| Scatter: Performance vs Anomaly | `member_metrics.{anomaly_score, fund_utilization_pct}` | Derived (frontend plot) |
| Risk Signal Composition (Venn) | `work_analysis` WHERE cost_status/duration_status | Derived (new SQL) |
| Historical Trend | `trends` | Direct |

### I. Hardcoded/Mock Values — MUST Become Dynamic

| Element | Current | Replacement |
|---------|---------|-------------|
| All 8 KPI tiles | Hardcoded | `GET /api/risk/overview` |
| Donut chart values | Hardcoded | `member_metrics` aggregation |
| Scatter dots | 6 MPs, 3 MLAs, 4 states | `GET /api/risk/entities` |
| Venn diagram counts | Hardcoded | New SQL on `work_analysis` |
| Trend line | Hardcoded quarterly | `trends` table (yearly) |
| Entity tables | Hardcoded | `GET /api/risk/entities` |
| State dropdown | 5 states | `GET /api/states` |

### J. Elements to Remove/Reconsider
- **Venn diagram "Overdue 1,842"**: The `overdue_over_1_year` is 30,448 in actual data (not 1,842). This is a huge discrepancy — the hardcoded value is wrong.
- **"81 High-Risk" hardcoded**: Actual `high_risk_works` in `overall_metrics` is 1,249 (MP+MLA). Need to decide which scope to show.
- **Quarterly trend**: Database has yearly trends only. Either downgrade to yearly chart or build quarterly aggregation from expenditure dates.

---

## Cross-Cutting Concerns

### Dynamic Navigation
All pages need dynamic URL parameters:
- `statedetail.html?state_id={id}`
- `mpdetail.html?mp_id={id}`
- `mpdetail.html?mla_id={id}`
- `workdetail.html?work_id={id}`
- `project.html?state_id={id}&constituency_id={id}`

Current pages use hardcoded links. JavaScript URL parameter parsing and API calls needed.

### Global Data Not Yet Available

| Data Point | Where Needed | Status |
|------------|-------------|--------|
| Party affiliation | MP cards, MP detail | **NOT IN DATABASE** — source API may have it |
| Region mapping | State explorer filter | **NOT IN DATABASE** — needs static mapping |
| Constituency-level aggregation | State detail constituency matrix | **NEEDS NEW SQL** |
| Sector/category breakdown | MP detail chart | **NEEDS NEW SQL** |
| Quarterly trends | AI Risk Center, State detail | **NEEDS NEW SQL** |
| Allocated amount at state level | State cards | **NEEDS AGGREGATION** from member-level allocations |

### Consistent Metric Definitions (Must Match Backend)

| Metric | Definition | Denominator |
|--------|-----------|-------------|
| Fund Utilization % | expenditure_amount / sanctioned_amount × 100 | Financial (₹) |
| Completion Rate % | completed_works / total_works × 100 | Work count |
| Sanction Conversion % | completed_works / sanctioned_works × 100 | Work count |
| Expenditure Rate % | expenditure_amount / recommended_amount × 100 | Financial (₹) |
| Flagged Rate % | flagged_works / total_works × 100 | Work count |
| Risk Rate % | flagged_works / total_works × 100 | Work count |
