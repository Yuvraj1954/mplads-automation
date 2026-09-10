# FINAL_IMPLEMENTATION_PLAN.md

> The master implementation plan combining all audit findings into an actionable execution guide.
> This is the single source of truth for finishing the CivicLens AI MPLADS dashboard.

---

## Table of Contents

- [A. CURRENTLY COMPLETE](#a-currently-complete)
- [B. FRONTEND MOCK DATA TO REPLACE](#b-frontend-mock-data-to-replace)
- [C. BACKEND ALREADY AVAILABLE](#c-backend-already-available)
- [D. NEW SQL/CALCULATIONS REQUIRED](#d-new-sqlcalculations-required)
- [E. PIPELINE CHANGES REQUIRED](#e-pipeline-changes-required)
- [F. FASTAPI ENDPOINT PLAN](#f-fastapi-endpoint-plan)
- [G. FRONTEND → FASTAPI BINDING PLAN](#g-frontend--fastapi-binding-plan)
- [H. AI RISK CENTER FINAL DATA PLAN](#h-ai-risk-center-final-data-plan)
- [I. DATA QUALITY PRECAUTIONS](#i-data-quality-precautions)
- [J. IMPLEMENTATION ORDER](#j-implementation-order)
- [K. FINAL GAP CHECK](#k-final-gap-check)

---

## A. CURRENTLY COMPLETE

### Database (DB1 + DB2)

| Component | Status | Evidence |
|-----------|--------|----------|
| 36 states ingested | ✅ Complete | `states` table: 36 rows |
| 582 constituencies ingested | ✅ Complete | `constituencies` table: 582 rows |
| 543 MPs ingested | ✅ Complete | `mps` table: 543 rows |
| 232 MLAs ingested | ✅ Complete | `mlas` table: 232 rows |
| 108,204 MP works ingested | ✅ Complete | `works` table: 108,204 rows |
| 25,587 MLA works ingested | ✅ Complete | `mla_works` table: 25,587 rows |
| 107,636 MP works analyzed | ✅ Complete | `work_analysis`: 107,636 rows |
| 25,297 MLA works analyzed | ✅ Complete | `mla_work_analysis`: 25,297 rows |
| 132,933 works in unified view | ✅ Complete | `phase_a_work_unified`: 132,933 rows |
| Member metrics (774+) | ✅ Complete | `phase_a_member_metrics`: 774 rows |
| State metrics (36) | ✅ Complete | `phase_a_state_metrics`: 36 rows |
| National statistics | ✅ Complete | `phase_a_statistics`: 8 rows |
| DB2 overall_metrics (3) | ✅ Complete | Scope: BOTH, MP, MLA |
| DB2 member_metrics (1,011) | ✅ Complete | Includes zero-work members |
| DB2 state_metrics (36) | ✅ Complete | All states covered |
| DB2 trends (7) | ✅ Complete | Yearly by member_type |
| DB2 ai_analysis (1,037) | ✅ Complete | 549 MP + 452 MLA + 36 State |
| DB2 entity_evidence (810) | ✅ Complete | 543 MP + 231 MLA + 36 State |
| DB2 evidence_work_refs (11,974) | ✅ Complete | 5 roles |
| ML anomaly scores (132,070) | ✅ Complete | All works scored |
| Daily pipeline automation | ✅ Complete | GitHub Actions cron |
| Gemini AI analysis | ✅ Complete | 1,037 analyses generated |

### Frontend

| Component | Status | Notes |
|-----------|--------|-------|
| All 9 HTML pages exist | ✅ Complete | dashboard, state, statedetail, mps, mpdetail, project, workdetail, airiskcentre, index |
| Sidebar with navigation | ✅ Complete | Collapsible, localStorage persistence |
| Tailwind CSS styling | ✅ Complete | Consistent design system |
| KPI info popups | ✅ Complete | i-button tooltip system |
| Search autocomplete | ✅ Complete | Client-side |
| URL parameter parsing | ⚠️ Partial | Some pages have it, some don't |

### What Works End-to-End

1. **Data ingestion**: Government API → DB1 (fully automated)
2. **Work analysis**: DB1 → risk flags, percentiles, classifications (fully automated)
3. **Member/state metrics**: Aggregated KPIs at all levels (fully automated)
4. **Anomaly detection**: Z-score based scoring (fully automated)
5. **Evidence generation**: Structured JSON per entity (fully automated)
6. **Gemini AI analysis**: Natural language summaries (fully automated)
7. **DB2 persistence**: All analytics stored (fully automated)

---

## B. FRONTEND MOCK DATA TO REPLACE

### dashboard.html

| Component | Current State | Required Change |
|-----------|--------------|-----------------|
| "782 works" KPI | Hardcoded | → `GET /api/dashboard/overview` → `total_works` |
| "46.2%" Completion | Hardcoded | → `completion_rate_pct` |
| "50.2%" Utilization | Hardcoded | → `fund_utilization_pct` |
| "67.4%" Sanction Conversion | Hardcoded | → `sanction_conversion_pct` |
| Fund Flow Funnel (4 values) | Hardcoded ₹ | → `recommended_amount`, `sanctioned_amount`, `expenditure_amount` |
| Classification Matrix (4 bars) | Hardcoded | → `classification_distribution` |
| Execution Analytics (4 values) | Hardcoded | → `avg_sanction_delay_days`, `avg_execution_days`, `overdue_over_1_year`, `avg_project_age_days` |
| Search dropdown | 6 hardcoded entries | → `GET /api/search` |

### state.html

| Component | Current State | Required Change |
|-----------|--------------|-----------------|
| 36 state cards | All hardcoded in HTML | → `GET /api/states` + dynamic render |
| SVG scatter (36 circles) | Hardcoded cx/cy | → Map `fund_utilization_pct` → cx, `completion_rate_pct` → cy |
| Ranking bars (40 entries) | Hardcoded JS arrays | → `state_metrics` sorted by field |
| State search | Hardcoded names | → `GET /api/states` |
| Region filter | Hardcoded regions | → Add static mapping or remove |

### statedetail.html

| Component | Current State | Required Change |
|-----------|--------------|-----------------|
| All 8 KPI values | Hardcoded | → `GET /api/states/{id}` |
| NE States scatter | Hardcoded 8 states | → Filter `state_metrics` by NE state_ids |
| Quarterly trend chart | Hardcoded | → Build quarterly aggregation or use yearly from `trends` |
| Constituency matrix (60 rows) | Hardcoded | → `GET /api/states/{id}/constituencies` |
| Representative cards | Hardcoded | → `GET /api/states/{id}` → `representatives` |
| Evidence items | Hardcoded | → `GET /api/ai/STATE/{id}` → `evidence` |
| AI Analysis text | Hardcoded | → `GET /api/ai/STATE/{id}` → `analysis` |

### mps.html

| Component | Current State | Required Change |
|-----------|--------------|-----------------|
| 6 MP cards | All hardcoded | → `GET /api/representatives?type=MP` |
| SVG scatter | Hardcoded 6 points | → Map `anomaly_score` vs `fund_utilization_pct` |
| Search | 6 hardcoded names | → `GET /api/search?type=MP` |

### mpdetail.html

| Component | Current State | Required Change |
|-----------|--------------|-----------------|
| Financial ledger (4 values) | Hardcoded | → `GET /api/representatives/MP/{id}` |
| Fund progression funnel | Hardcoded | → Same endpoint |
| Expenditure by sector (4 bars) | Hardcoded | → `sector_breakdown` from new SQL |
| Works table (17 pages) | Hardcoded | → `GET /api/representatives/MP/{id}` → `works` |
| Recommended works (143) | Hardcoded | → New query on `work_recommendations` |
| Evidence items | Hardcoded | → `evidence` from same endpoint |
| AI Analysis text | Hardcoded | → `ai_analysis` from same endpoint |

### project.html

| Component | Current State | Required Change |
|-----------|--------------|-----------------|
| Entire `dataset` JS object | 8 states, 10 sample works | → `GET /api/works?state_id=&constituency_id=` |
| State select options | 8 hardcoded | → `GET /api/states` |
| Constituency select | Dynamic per state | → `GET /api/states/{id}/constituencies` |

### workdetail.html

| Component | Current State | Required Change |
|-----------|--------------|-----------------|
| All work details | Hardcoded single work | → `GET /api/works/{work_id}` |
| Timeline milestones | Hardcoded dates | → Same endpoint |
| Risk assessment | Hardcoded delta | → `expenditure_percentage - completion_percentage` |
| Financial amounts | Hardcoded | → Same endpoint |

### airiskcentre.html

| Component | Current State | Required Change |
|-----------|--------------|-----------------|
| 8 KPI tiles | All hardcoded | → `GET /api/risk/overview` |
| Donut chart values | Hardcoded | → `risk_distribution` from same endpoint |
| Scatter: Performance vs Anomaly | 13 hardcoded dots | → `GET /api/risk/entities` |
| Venn diagram counts | Hardcoded (wrong values) | → `GET /api/risk/composition` |
| Historical trend line | Hardcoded quarterly | → `GET /api/dashboard/trends` (yearly) |
| MP table (6 rows) | Hardcoded | → `GET /api/risk/entities?type=MP` |
| MLA table (3 rows) | Hardcoded | → `GET /api/risk/entities?type=MLA` |
| State table (4 rows) | Hardcoded | → `GET /api/risk/entities?type=STATE` |
| State dropdown (5 states) | Hardcoded | → `GET /api/states` |

---

## C. BACKEND ALREADY AVAILABLE

### Direct-use DB2 tables (expose via FastAPI with minimal transformation)

| Table | Rows | FastAPI Use | Transformation Needed |
|-------|------|-------------|----------------------|
| `overall_metrics` | 3 | Dashboard overview, Risk overview | Fix `allocated_amount` (currently 0) |
| `member_metrics` | 1,011 | MP/MLA list, detail, risk ranking | Add `allocated_amount`, fix NULL `state_name` |
| `state_metrics` | 36 | State list, detail, risk ranking | Add `allocated_amount` column |
| `national_statistics` | 129 | Statistical context for evidence | None — expose directly |
| `trends` | 7 | Historical trend charts | None — expose directly |
| `ai_analysis` | 1,037 | AI analysis display | Parse `analysis_text` JSON |
| `entity_evidence` | 810 | Evidence display | None — expose `evidence` JSONB directly |
| `evidence_work_refs` | 11,974 | Supporting works for evidence | Join with work details |

### Direct-use DB1 tables (query via FastAPI)

| Table | Rows | FastAPI Use | Query Pattern |
|-------|------|-------------|---------------|
| `phase_a_work_unified` | 132,933 | Work list, work detail, project explorer | SELECT with WHERE filters |
| `mps` | 543 | MP identity | JOIN with member_metrics |
| `mlas` | 232 | MLA identity | JOIN with member_metrics |
| `states` | 36 | State identity | JOIN with state_metrics |
| `constituencies` | 582 | Constituency identity | JOIN with works |
| `mp_allocations` | 543 | MP allocation amounts | Aggregate per member/state |
| `mla_allocations` | 232 | MLA allocation amounts | Aggregate per member/state |
| `work_expenditures` | 85,113 | Payment timeline for works | Filter by work_id, order by date |
| `work_recommendations` | 107,829 | Recommended works list | JOIN with works |
| `ml_work_anomaly` | 132,070 | ML anomaly scores | JOIN with works |

### Derived metrics already computed

| Metric | Source | Values Available |
|--------|--------|-----------------|
| Fund Utilization % | `overall_metrics` | 50.62% (BOTH), 48.54% (MP), 56.00% (MLA) |
| Completion Rate % | `overall_metrics` | 33.74% (BOTH) |
| Sanction Conversion % | `overall_metrics` | 45.26% (BOTH) |
| Total Works | `overall_metrics` | 132,880 |
| Flagged Works | `overall_metrics` | 47,650 |
| High Risk Works | `overall_metrics` | 1,249 |
| Avg Sanction Delay | `overall_metrics` | 117.69 days |
| Avg Execution Duration | `overall_metrics` | 176.02 days |
| Overdue >1 Year | `overall_metrics` | 30,448 |
| Overdue >2 Years | `overall_metrics` | 1,648 |
| Anomaly Score per member | `member_metrics` | Range: ~0 to 1.79 |
| Performance Classification | `member_metrics` | 6 categories |
| Rank by anomaly | `member_metrics` | 1 = highest anomaly |

---

## D. NEW SQL/CALCULATIONS REQUIRED

### D1. Fix Allocated Amount in DB2

**Priority**: HIGH
**Type**: Pipeline change + DB2 update

```sql
-- MP allocations aggregated to member level
SELECT mp_id, SUM(allocated_amount) as allocated_amount
FROM mp_allocations GROUP BY mp_id;

-- MLA allocations aggregated to member level
SELECT mla_id, SUM(allocated_amount) as allocated_amount
FROM mla_allocations GROUP BY mla_id;

-- National total
SELECT SUM(allocated_amount) FROM mp_allocations  -- ₹83,336,673,298.01
UNION ALL
SELECT SUM(allocated_amount) FROM mla_allocations; -- ₹33,687,482,301.82
```

**Write to**: `member_metrics.allocated_amount`, `overall_metrics.allocated_amount`, `state_metrics.allocated_amount`

### D2. Fix MLA state_name NULL

**Priority**: MEDIUM
**Type**: Pipeline fix

```sql
-- Current: 196 MLAs have NULL state_name
-- Fix: JOIN through constituencies → states
SELECT m.member_id, s.state_name
FROM phase_a_member_metrics m
JOIN mlas ml ON m.member_id = ml.mla_id + 100000  -- or by member_id mapping
JOIN constituencies c ON ml.constituency_id = c.constituency_id
JOIN states s ON c.state_id = s.state_id
WHERE m.state_name IS NULL;
```

### D3. Constituency-Level Aggregation

**Priority**: MEDIUM
**Type**: New SQL (compute on-demand in FastAPI)

```sql
SELECT
  c.constituency_id,
  c.constituency_name,
  s.state_name,
  COUNT(wu.work_id) as total_works,
  COUNT(CASE WHEN wu.status = 'Completed' THEN 1 END) as completed_works,
  ROUND(COUNT(CASE WHEN wu.status = 'Completed' THEN 1 END)::numeric
        / NULLIF(COUNT(wu.work_id), 0) * 100, 2) as completion_rate_pct,
  SUM(wu.expenditure_amount) as expenditure_amount,
  SUM(wu.sanction_amount) as sanctioned_amount,
  ROUND(SUM(wu.expenditure_amount) / NULLIF(SUM(wu.sanction_amount), 0) * 100, 2)
        as fund_utilization_pct,
  COUNT(CASE WHEN wu.risk_flags IS NOT NULL AND array_length(wu.risk_flags, 1) > 0
        THEN 1 END) as flagged_works
FROM constituencies c
JOIN states s ON c.state_id = s.state_id
LEFT JOIN phase_a_work_unified wu ON c.constituency_id = wu.constituency_id
WHERE c.state_id = $1
GROUP BY c.constituency_id, c.constituency_name, s.state_name
ORDER BY total_works DESC;
```

### D4. Expenditure by Sector

**Priority**: MEDIUM
**Type**: New SQL (compute on-demand in FastAPI)

```sql
SELECT
  wa.work_category,
  SUM(wa.expenditure_amount) as total_expenditure,
  ROUND(SUM(wa.expenditure_amount)::numeric
        / NULLIF(SUM(SUM(wa.expenditure_amount)) OVER(), 0) * 100, 1) as percentage
FROM phase_a_work_unified wa
WHERE wa.member_id = $1 AND wa.member_type = $2
  AND wa.expenditure_amount > 0
GROUP BY wa.work_category
ORDER BY total_expenditure DESC;
```

### D5. Risk Signal Composition (Venn)

**Priority**: MEDIUM
**Type**: New SQL (compute on-demand in FastAPI)

```sql
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
  COUNT(*) as total_works
FROM phase_a_work_unified;
```

### D6. Quarterly Trends

**Priority**: LOW
**Type**: New SQL (compute on-demand in FastAPI)

```sql
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

### D7. Classification Distribution

**Priority**: HIGH
**Type**: New SQL (compute on-demand or pre-aggregate)

```sql
SELECT
  performance_classification,
  COUNT(*) as count
FROM member_metrics
WHERE zero_work_member = false
GROUP BY performance_classification;
```

### D8. Global Search Autocomplete

**Priority**: HIGH
**Type**: New SQL

```sql
SELECT 'MP' as type, mp_id as id, mp_name as name FROM mps
UNION ALL
SELECT 'MLA' as type, mla_id as id, mla_name as name FROM mlas
UNION ALL
SELECT 'STATE' as type, state_id as id, state_name as name FROM states
ORDER BY name;
```

---

## E. PIPELINE CHANGES REQUIRED

### E1. Populate Allocated Amount (CRITICAL)

**File**: `automation/analysis/phase_a_member.py`
**Change**: Join `mp_allocations`/`mla_allocations` during member metrics calculation

```python
# In compute_member_metrics():
allocations = get_allocations(member_id, member_type)
allocated_amount = sum(a['allocated_amount'] for a in allocations)
```

**File**: `automation/analysis/db2_analytics_persistence.py`
**Change**: Write `allocated_amount` to `member_metrics`, `state_metrics`, `overall_metrics`

### E2. Fix MLA state_name (MEDIUM)

**File**: `automation/analysis/phase_a_member.py`
**Change**: Ensure state_name is resolved via constituency → state join for MLAs

### E3. Add allocated_amount to state_metrics (MEDIUM)

**File**: `automation/analysis/phase_a_state.py`
**Change**: Add `allocated_amount` column, aggregate from member-level

**SQL migration**: `ALTER TABLE state_metrics ADD COLUMN allocated_amount NUMERIC DEFAULT 0;`

### E4. Drop duplicate view (LOW)

**SQL**: `DROP VIEW IF EXISTS work_analysis_all;` (identical to `phase_a_work_unified`)

---

## F. FASTAPI ENDPOINT PLAN

### Tech Stack Recommendation
- **Framework**: FastAPI (Python)
- **Database**: psycopg2 or asyncpg (direct PostgreSQL)
- **Connection**: Use `NEW_DB1_URL` and `NEW_DB2_URL` from `.env`
- **CORS**: Allow frontend origin

### Endpoint List

| # | Method | Endpoint | Purpose | Primary Source | Response Time Target |
|---|--------|----------|---------|---------------|---------------------|
| 1 | GET | `/api/dashboard/overview` | National KPIs | DB2 `overall_metrics` + derived | <100ms |
| 2 | GET | `/api/dashboard/trends` | Yearly trends | DB2 `trends` | <100ms |
| 3 | GET | `/api/states` | All states | DB2 `state_metrics` | <100ms |
| 4 | GET | `/api/states/{state_id}` | State detail | DB2 `state_metrics` + DB1 queries | <200ms |
| 5 | GET | `/api/states/{state_id}/constituencies` | Constituency matrix | DB1 new aggregation | <300ms |
| 6 | GET | `/api/representatives` | All members | DB2 `member_metrics` | <200ms |
| 7 | GET | `/api/representatives/{type}/{id}` | Member detail | DB2 `member_metrics` + DB1 | <300ms |
| 8 | GET | `/api/works` | Work list + filters | DB1 `phase_a_work_unified` | <300ms |
| 9 | GET | `/api/works/{work_id}` | Work detail | DB1 `phase_a_work_unified` + expenditures | <200ms |
| 10 | GET | `/api/risk/overview` | Risk center KPIs | DB2 `overall_metrics` + derived | <100ms |
| 11 | GET | `/api/risk/entities` | Risk-ranked entities | DB2 `member_metrics` / `state_metrics` | <200ms |
| 12 | GET | `/api/risk/composition` | Venn diagram data | DB1 new aggregation | <300ms |
| 13 | GET | `/api/search` | Global autocomplete | DB1 new query | <100ms |
| 14 | GET | `/api/ai/{type}/{id}` | AI analysis + evidence | DB2 `ai_analysis` + `entity_evidence` | <200ms |

### Caching Strategy

| Endpoint | Cache TTL | Invalidation |
|----------|-----------|-------------|
| `/api/dashboard/overview` | 5 minutes | Next pipeline run |
| `/api/states` | 5 minutes | Next pipeline run |
| `/api/states/{id}` | 5 minutes | Next pipeline run |
| `/api/representatives` | 5 minutes | Next pipeline run |
| `/api/works` | 1 minute | On-demand |
| `/api/risk/*` | 5 minutes | Next pipeline run |
| `/api/ai/*` | 1 hour | Next Gemini run |
| `/api/search` | 1 hour | Static (changes rarely) |

---

## G. FRONTEND → FASTAPI BINDING PLAN

### dashboard.html

| UI Component | API Endpoint | Response Fields | JS Change |
|-------------|-------------|-----------------|-----------|
| "Total Works" KPI | `GET /api/dashboard/overview` | `total_works` | `fetch()` + `textContent` update |
| "Completion Ratio" KPI | same | `completion_rate_pct` | Same |
| "Fund Utilization" KPI | same | `fund_utilization_pct` | Same |
| "Sanction Conversion" KPI | same | `sanction_conversion_pct` | Same |
| Fund Flow bars | same | `recommended_amount`, `sanctioned_amount`, `expenditure_amount` | Dynamic width calculation |
| Classification bars | same | `classification_distribution` | Dynamic bar width |
| Execution metrics | same | `avg_sanction_delay_days`, `avg_execution_days`, `overdue_over_1_year` | Text update |
| Search dropdown | `GET /api/search` | Array of `{type, id, name}` | Dynamic dropdown population |

### state.html

| UI Component | API Endpoint | Response Fields | JS Change |
|-------------|-------------|-----------------|-----------|
| 36 state cards | `GET /api/states` | `states[]` | Dynamic card generation |
| Scatter circles | same | `fund_utilization_pct`, `completion_rate_pct` | Map to SVG cx/cy |
| Ranking bars | same | Sort by respective field | Dynamic sort + render |
| Search autocomplete | same | `state_name` from states array | Dynamic population |

### statedetail.html

| UI Component | API Endpoint | Response Fields | JS Change |
|-------------|-------------|-----------------|-----------|
| 8 KPI values | `GET /api/states/{id}` | All state fields | Dynamic update |
| Representatives tab | same | `representatives[]` | Dynamic card generation |
| Evidence items | `GET /api/ai/STATE/{id}` | `evidence` JSON | Dynamic rendering |
| AI Analysis | same | `analysis` JSON | Text update |
| Constituency matrix | `GET /api/states/{id}/constituencies` | `constituencies[]` | Dynamic table |

### mps.html

| UI Component | API Endpoint | Response Fields | JS Change |
|-------------|-------------|-----------------|-----------|
| 6 MP cards | `GET /api/representatives?type=MP` | `members[]` | Dynamic card generation |
| Scatter points | same | `anomaly_score`, `fund_utilization_pct` | Map to SVG |

### mpdetail.html

| UI Component | API Endpoint | Response Fields | JS Change |
|-------------|-------------|-----------------|-----------|
| Financial ledger | `GET /api/representatives/MP/{id}` | All member fields | Dynamic update |
| Fund funnel | same | `sanctioned_amount`, `expenditure_amount` | Dynamic |
| Expenditure by sector | same | `sector_breakdown[]` | Dynamic bars |
| Works table | same | `works[]` | Dynamic table with pagination |
| Evidence | same | `evidence` JSON | Dynamic rendering |
| AI Analysis | same | `ai_analysis` JSON | Text update |

### project.html

| UI Component | API Endpoint | Response Fields | JS Change |
|-------------|-------------|-----------------|-----------|
| Work cards | `GET /api/works?state_id=&constituency_id=` | `works[]` | Dynamic card generation |
| State select | `GET /api/states` | `states[]` | Dynamic options |
| Constituency select | `GET /api/states/{id}/constituencies` | `constituencies[]` | Dynamic options on state change |

### workdetail.html

| UI Component | API Endpoint | Response Fields | JS Change |
|-------------|-------------|-----------------|-----------|
| All work fields | `GET /api/works/{id}` | All work fields | Dynamic update |
| Timeline | same | Date fields | Dynamic timeline |
| Risk assessment | same | `risk_flags`, `risk_level` | Dynamic display |

### airiskcentre.html

| UI Component | API Endpoint | Response Fields | JS Change |
|-------------|-------------|-----------------|-----------|
| 8 KPI tiles | `GET /api/risk/overview` | All overview fields | Dynamic update |
| Donut chart | same | `risk_distribution` | Dynamic SVG |
| Scatter: Performance vs Anomaly | `GET /api/risk/entities?type=MP` | `anomaly_score`, `fund_utilization_pct` | Dynamic SVG |
| Venn diagram | `GET /api/risk/composition` | `cost_only`, `duration_only`, `dual_flagged` | Dynamic SVG |
| Trend line | `GET /api/dashboard/trends` | `trends[]` | Dynamic line |
| MP table | `GET /api/risk/entities?type=MP&sort=score` | `entities[]` | Dynamic table |
| MLA table | `GET /api/risk/entities?type=MLA&sort=score` | `entities[]` | Dynamic table |
| State table | `GET /api/risk/entities?type=STATE&sort=score` | `entities[]` | Dynamic table |
| State dropdown | `GET /api/states` | `states[]` | Dynamic options |

---

## H. AI RISK CENTER FINAL DATA PLAN

### Overall Risk KPIs (8 tiles)

| Tile | API Source | Calculation |
|------|-----------|-------------|
| Anomaly Score | `GET /api/risk/overview` → `avg_anomaly_score` | Direct from `member_metrics` AVG |
| AI Confidence | Derived | % of entities with `confidence_level = 'HIGH'` |
| Flagged Works | `overall_metrics.flagged_works` | Direct: 47,650 |
| High-Risk Severity | `overall_metrics.high_risk_works` | Direct: 1,249 |
| Total Entities | COUNT of `member_metrics` + `state_metrics` | Direct: 775+36 = 811 |
| Monitored Works | `overall_metrics.total_works` | Direct: 132,880 |
| Cost Anomaly | `overall_metrics.cost_anomaly_works` | Direct: 2,800 |
| Duration Anomaly | `overall_metrics.duration_anomaly_works` | Direct: 4,066 |

### Representative Risk Distribution (Donut)

**Data**: `GET /api/risk/overview` → `risk_distribution`

```json
{
  "HIGH": 50,     // member_metrics WHERE anomaly_level = 'HIGH'
  "MEDIUM": 100,  // member_metrics WHERE anomaly_level = 'MEDIUM'
  "LOW": 200,     // member_metrics WHERE anomaly_level = 'LOW'
  "NORMAL": 425   // member_metrics WHERE anomaly_level = 'NORMAL'
}
```

**Total**: Sum = total members with works (excluding zero-work)

### Performance vs Anomaly Matrix (Scatter)

**Data**: `GET /api/risk/entities?type=MP&limit=50`

**X-axis**: `fund_utilization_pct` (0-100%)
**Y-axis**: `anomaly_score` (0-2.0)
**Dot size**: `total_works` (scaled)
**Dot color**: `anomaly_level` (HIGH=red, MEDIUM=orange, LOW=yellow, NORMAL=green)

### Risk Signal Composition (Venn)

**Data**: `GET /api/risk/composition`

**Calculation** (from `phase_a_work_unified`):
- **Cost Only**: `cost_status IN ('VERY_HIGH', 'HIGH') AND duration_status NOT IN ('VERY_LONG', 'LONG')`
- **Duration Only**: `duration_status IN ('VERY_LONG', 'LONG') AND cost_status NOT IN ('VERY_HIGH', 'HIGH')`
- **Dual-Flagged**: Both conditions true
- **No Anomaly**: Neither condition

### Historical Risk Trend (Line)

**Data**: `GET /api/dashboard/trends`

**Note**: Database has yearly data, not quarterly. Chart should show:
- X-axis: Year (2020-2026)
- Y-axis: `flagged_rate_pct` or `high_risk_rate_pct`
- Lines: MP, MLA, Combined

### Highest-Priority Entities (Tables)

**Data**: `GET /api/risk/entities?type={MP|MLA|STATE}&sort=score&limit=10`

Sorted by `anomaly_score DESC`. Display columns:
- Rank
- Name
- State
- Works
- Flagged
- Utilization %
- Anomaly Score
- Risk Level

### AI Assessment / Key Findings / Cautions

**Data**: `GET /api/ai/{type}/{id}` for top entities

For the overall page, show aggregate insights:
- Total entities analyzed: 1,037
- HIGH anomaly entities: ~50
- Top contributing risk factors (from evidence `entity_anomaly.contributing_features`)
- Cautions: Data quality warnings from `zero_work_member`, `low_sample_member`

### Exploration / Filtering

**Controls**:
- Risk Level checkboxes: Filter `anomaly_level` in API query
- State dropdown: Filter `state_id` in API query
- Entity type toggle: Switch between MP/MLA/STATE endpoints
- Sort: `sort=score|flagged|utilization|alpha`

---

## I. DATA QUALITY PRECAUTIONS

### Critical Safeguards

| # | Risk | Safeguard |
|---|------|-----------|
| 1 | **Double-counting MP+MLA** | Always filter by `member_type` when aggregating per-member. Use `phase_a_work_unified` which already UNION ALLs with proper member_type tagging. |
| 2 | **Financial ratio denominator mixing** | Fund Utilization uses `sanctioned_amount` (financial). Completion Rate uses `total_works` (count). Never mix. |
| 3 | **NULL denominators** | All percentage calculations must use `NULLIF(denominator, 0)` to avoid division by zero. Frontend must handle NULL as "N/A". |
| 4 | **Zero-work members** | 80 members have `total_works = 0`. Exclude from work-based ratios. Show as "NO_DATA" classification. |
| 5 | **Low sample members** | 16 members have <5 works. Mark as "INSUFFICIENT_DATA". Do not use for ranking. |
| 6 | **Stale data** | DB2 `calculated_at` timestamps must be displayed. Data updates daily via pipeline. |
| 7 | **allocated_amount = 0** | Until pipeline fix, do NOT display allocated amounts from DB2. Either fix pipeline first or query `mp_allocations` directly. |
| 8 | **MLA state_name NULL** | 196 MLAs have NULL state_name. Filter or fallback to constituency-based state lookup. |
| 9 | **Expenditure > Sanction** | Currently 0 works (good). Monitor for future data quality. |
| 10 | **Negative delays** | Currently 0 works (good). Pipeline has `SOURCE_DATA_DEFECT_NEGATIVE_DELAY` flag. |
| 11 | **Gemini analysis freshness** | AI analysis is generated when evidence changes. Some entities may have stale analysis if their data hasn't changed. |
| 12 | **Evidence version mismatch** | Evidence v2 is current. Ensure frontend reads `evidence_version = 2`. |
| 13 | **MLA work_id offset** | MLA work_ids are `original_id + 1,000,000`. Frontend must not confuse with MP work_ids. |
| 14 | **Performance classification edge cases** | UNCLASSIFIED members exist. Handle gracefully in UI. |
| 15 | **Scatter plot outliers** | Anomaly scores range 0-1.79. Some members may cluster near 0. Use log scale or jitter if needed. |

### Frontend Display Rules

| Rule | Implementation |
|------|---------------|
| Show "N/A" for NULL percentages | `value !== null ? value + '%' : 'N/A'` |
| Format currency as ₹X.XX Cr | `(value / 10000000).toFixed(2) + ' Cr'` |
| Format large numbers with commas | `Intl.NumberFormat('en-IN').format(value)` |
| Show "Updated: {date}" | Use `calculated_at` from API response |
| Handle empty states | "No data available" for zero-results |
| Disable ranking for low-sample | Show "Insufficient Data" badge |

---

## J. IMPLEMENTATION ORDER

### Phase 1: Backend Foundation (Days 1-2)

| # | Task | Est. Time | Dependencies |
|---|------|-----------|-------------|
| 1.1 | Create FastAPI project structure | 2 hours | None |
| 1.2 | Database connection pooling (psycopg2) | 1 hour | 1.1 |
| 1.3 | Implement `/api/dashboard/overview` | 2 hours | 1.2 |
| 1.4 | Implement `/api/states` | 1 hour | 1.2 |
| 1.5 | Implement `/api/representatives` | 1 hour | 1.2 |
| 1.6 | Implement `/api/works` with filters | 2 hours | 1.2 |
| 1.7 | Implement `/api/risk/overview` | 1 hour | 1.2 |
| 1.8 | Implement `/api/risk/entities` | 1 hour | 1.2 |
| 1.9 | Implement `/api/ai/{type}/{id}` | 1 hour | 1.2 |
| 1.10 | Implement `/api/search` | 1 hour | 1.2 |
| 1.11 | Implement `/api/dashboard/trends` | 1 hour | 1.2 |
| 1.12 | CORS configuration | 30 min | 1.1 |
| 1.13 | Error handling + response validation | 1 hour | All above |

### Phase 2: New SQL Calculations (Day 2)

| # | Task | Est. Time | Dependencies |
|---|------|-----------|-------------|
| 2.1 | Constituency aggregation endpoint | 2 hours | 1.2 |
| 2.2 | Expenditure by sector endpoint | 1 hour | 1.2 |
| 2.3 | Risk signal composition endpoint | 1 hour | 1.2 |
| 2.4 | Quarterly trends endpoint (if needed) | 2 hours | 1.2 |

### Phase 3: Pipeline Fixes (Days 2-3)

| # | Task | Est. Time | Dependencies |
|---|------|-----------|-------------|
| 3.1 | Populate `allocated_amount` in member_metrics | 3 hours | None |
| 3.2 | Populate `allocated_amount` in state_metrics | 2 hours | 3.1 |
| 3.3 | Populate `allocated_amount` in overall_metrics | 1 hour | 3.1 |
| 3.4 | Fix MLA state_name NULL | 2 hours | None |
| 3.5 | Add `allocated_amount` column to state_metrics table | 30 min | 3.2 |
| 3.6 | Test pipeline end-to-end | 2 hours | 3.1-3.5 |

### Phase 4: Frontend API Integration (Days 3-5)

| # | Task | Est. Time | Dependencies |
|---|------|-----------|-------------|
| 4.1 | Create shared API client utility | 2 hours | Phase 1 complete |
| 4.2 | dashboard.html: Replace all hardcoded KPIs | 3 hours | 4.1 |
| 4.3 | state.html: Replace state cards + scatter + rankings | 4 hours | 4.1 |
| 4.4 | statedetail.html: Replace all tabs | 4 hours | 4.1 |
| 4.5 | mps.html: Replace MP cards + scatter | 3 hours | 4.1 |
| 4.6 | mpdetail.html: Replace all sections | 4 hours | 4.1 |
| 4.7 | project.html: Replace dataset + filters | 3 hours | 4.1 |
| 4.8 | workdetail.html: Replace all fields | 2 hours | 4.1 |
| 4.9 | airiskcentre.html: Replace all KPIs + charts + tables | 4 hours | 4.1 |

### Phase 5: Dynamic Navigation (Day 5)

| # | Task | Est. Time | Dependencies |
|---|------|-----------|-------------|
| 5.1 | URL parameter parsing utility | 1 hour | None |
| 5.2 | State card → statedetail.html navigation | 1 hour | 5.1 |
| 5.3 | MP card → mpdetail.html navigation | 1 hour | 5.1 |
| 5.4 | Work card → workdetail.html navigation | 1 hour | 5.1 |
| 5.5 | Back navigation / breadcrumbs | 1 hour | 5.1 |

### Phase 6: Testing + Validation (Days 5-6)

| # | Task | Est. Time | Dependencies |
|---|------|-----------|-------------|
| 6.1 | API response validation against actual DB values | 2 hours | Phase 1-2 complete |
| 6.2 | Frontend rendering validation | 2 hours | Phase 4 complete |
| 6.3 | Cross-page navigation testing | 1 hour | Phase 5 complete |
| 6.4 | Data consistency checks (no double-counting) | 2 hours | All above |
| 6.5 | Performance testing (response times) | 1 hour | All above |

### Phase 7: Production Validation (Day 6)

| # | Task | Est. Time | Dependencies |
|---|------|-----------|-------------|
| 7.1 | Deploy FastAPI to hosting | 2 hours | All above |
| 7.2 | Update frontend API base URL | 30 min | 7.1 |
| 7.3 | End-to-end smoke test | 1 hour | 7.2 |
| 7.4 | Pipeline run validation | 1 hour | 7.1 |
| 7.5 | Documentation | 1 hour | All above |

**Total estimated time**: 6-7 days for one developer

---

## K. FINAL GAP CHECK

| # | Requirement | Exists? | Needs SQL? | Needs Pipeline? | Needs FastAPI? | Needs Frontend? | Priority |
|---|------------|---------|-----------|----------------|---------------|----------------|----------|
| 1 | Total works (national) | ✅ DB2 | No | No | Yes | Yes | HIGH |
| 2 | Completion rate % | ✅ DB2 | No | No | Yes | Yes | HIGH |
| 3 | Fund utilization % | ✅ DB2 | No | No | Yes | Yes | HIGH |
| 4 | Sanction conversion % | ✅ DB2 | No | No | Yes | Yes | HIGH |
| 5 | Fund flow amounts | ✅ DB2 | No | Fix alloc | Yes | Yes | HIGH |
| 6 | Classification distribution | ✅ DB2 | Yes (agg) | No | Yes | Yes | HIGH |
| 7 | Execution analytics | ✅ DB2 | No | No | Yes | Yes | HIGH |
| 8 | State list with metrics | ✅ DB2 | No | Fix alloc | Yes | Yes | HIGH |
| 9 | State detail KPIs | ✅ DB2 | No | Fix alloc | Yes | Yes | HIGH |
| 10 | Constituency matrix | ❌ | **NEW** | No | Yes | Yes | MEDIUM |
| 11 | Representative list | ✅ DB2 | No | Fix state_name | Yes | Yes | HIGH |
| 12 | Representative detail | ✅ DB2 | No | Fix alloc | Yes | Yes | HIGH |
| 13 | Expenditure by sector | ❌ | **NEW** | No | Yes | Yes | MEDIUM |
| 14 | Recommended works list | ✅ DB1 | Yes (join) | No | Yes | Yes | MEDIUM |
| 15 | Work list with filters | ✅ DB1 | No | No | Yes | Yes | HIGH |
| 16 | Work detail | ✅ DB1 | No | No | Yes | Yes | HIGH |
| 17 | Risk overview KPIs | ✅ DB2 | No | No | Yes | Yes | HIGH |
| 18 | Risk entity rankings | ✅ DB2 | No | No | Yes | Yes | HIGH |
| 19 | Risk signal composition | ❌ | **NEW** | No | Yes | Yes | MEDIUM |
| 20 | Historical trends | ✅ DB2 | No | No | Yes | Yes | MEDIUM |
| 21 | AI analysis per entity | ✅ DB2 | No | No | Yes | Yes | HIGH |
| 22 | Evidence per entity | ✅ DB2 | No | No | Yes | Yes | HIGH |
| 23 | Evidence work refs | ✅ DB2 | Yes (join) | No | Yes | Yes | LOW |
| 24 | Global search | ❌ | **NEW** | No | Yes | Yes | HIGH |
| 25 | Allocated amount (national) | ❌ DB2=0 | No | **FIX** | Yes | Yes | HIGH |
| 26 | Allocated amount (member) | ❌ DB2=0 | No | **FIX** | Yes | Yes | HIGH |
| 27 | Allocated amount (state) | ❌ | **NEW** | **FIX** | Yes | Yes | HIGH |
| 28 | MLA state_name | ⚠️ NULL | No | **FIX** | Yes | Yes | MEDIUM |
| 29 | Party affiliation | ❌ | **NEW** (source) | **FIX** | Yes | Yes | LOW |
| 30 | Region mapping | ❌ | Static | No | Yes | Yes | LOW |
| 31 | Quarterly trends | ❌ | **NEW** | Optional | Yes | Yes | LOW |
| 32 | URL parameter navigation | ⚠️ Partial | No | No | No | **FIX** | HIGH |
| 33 | Dynamic sidebar links | ✅ | No | No | No | No | DONE |

### Summary

| Category | Count |
|----------|-------|
| ✅ Already exists in DB | 18 |
| ⚠️ Exists but needs fix | 3 |
| ❌ Needs new SQL calculation | 6 |
| ❌ Needs pipeline change | 5 |
| All need FastAPI endpoint | 33 |
| All need frontend update | 33 |

### Critical Path

```
Pipeline Fix (alloc + state_name) → FastAPI Endpoints → Frontend Integration
         ↑                                    ↑                    ↑
    Day 2-3                              Day 1-2              Day 3-5
```

**The pipeline fixes (allocation data, MLA state_name) are prerequisites for accurate frontend display. FastAPI can be built in parallel using existing DB2 data (ignoring allocation fields until pipeline is fixed).**
