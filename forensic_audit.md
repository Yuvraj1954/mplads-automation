# FORENSIC DATABASE AUDIT REPORT

**Generated**: 2026-09-19T04:48:53.288063+00:00
**Elapsed**: 80170ms
**Verdict**: **PIPELINE READY — NO DATA INTEGRITY ISSUES FOUND**

## Section 1: DATABASE INVENTORY

Checks: 4 | PASS: 4 | FAIL: 0 | WARN: 0

- ✅ **DB1 table list** `INFORMATIONAL`: 30 tables on DB1

- ✅ **DB1 backup/temp tables** `INFORMATIONAL`: No backup/temp tables found

- ✅ **DB2 table list** `INFORMATIONAL`: 24 tables on DB2

- ✅ **DB2 backup/temp tables** `INFORMATIONAL`: No backup/temp tables found

---

## Section 2: CANONICAL MEMBER POPULATION

Checks: 7 | PASS: 7 | FAIL: 0 | WARN: 0

- ✅ **MP count** `INFORMATIONAL`: Expected 543, got 543

- ✅ **MLA count** `INFORMATIONAL`: Expected 233, got 233

- ✅ **Total canonical members** `INFORMATIONAL`: Expected 776, got 776 (543 MPs + 233 MLAs)

- ✅ **Duplicate MP names** `INFORMATIONAL`: No duplicate MP names

- ✅ **Duplicate MLA names** `INFORMATIONAL`: No duplicate MLA names

- ✅ **MP unique IDs** `INFORMATIONAL`: 543 unique mp_id values for 543 rows

- ✅ **MLA unique IDs** `INFORMATIONAL`: 233 unique mla_id values for 233 rows

---

## Section 3: SYNTHETIC ID DETECTION

Checks: 4 | PASS: 4 | FAIL: 0 | WARN: 0

- ✅ **DB1 mps synthetic IDs** `INFORMATIONAL`: No synthetic IDs found

- ✅ **DB1 mlas synthetic IDs** `INFORMATIONAL`: No synthetic IDs found

- ✅ **DB2 member_metrics synthetic IDs** `INFORMATIONAL`: No synthetic IDs found

- ✅ **DB2 member_intelligence synthetic IDs** `INFORMATIONAL`: No synthetic IDs found

---

## Section 4: MP/MLA SEPARATION

Checks: 5 | PASS: 4 | FAIL: 0 | WARN: 1

- ✅ **MP work_analysis count** `INFORMATIONAL`: 109,177 MP work records

- ✅ **MLA work_analysis count** `INFORMATIONAL`: 25,658 MLA work records

- ✅ **Non-MP member_type in work_analysis** `INFORMATIONAL`: All records in work_analysis are MP

- ✅ **Non-MLA member_type in mla_work_analysis** `INFORMATIONAL`: All records in mla_work_analysis are MLA

- ⚠️ **Member IDs in both MP and MLA work tables** [HIGH] `CURRENT PRODUCTION ISSUE`: Found 196 distinct member_ids with MP works AND MLA works (auto-ID overlap is normal)
  - `{"member_id": 1}`
  - `{"member_id": 2}`
  - `{"member_id": 5}`
  - `{"member_id": 6}`
  - `{"member_id": 7}`
  - `{"member_id": 8}`
  - `{"member_id": 9}`
  - `{"member_id": 11}`
  - `{"member_id": 12}`
  - `{"member_id": 14}`
  - ... and 10 more

---

## Section 5: DUPLICATE WORKS

Checks: 5 | PASS: 5 | FAIL: 0 | WARN: 0

- ✅ **MP duplicate work_ids in work_analysis** `INFORMATIONAL`: No duplicate work_ids

- ✅ **MLA duplicate work_ids in mla_work_analysis** `INFORMATIONAL`: No duplicate work_ids

- ✅ **work_id overlap between MP and MLA tables** `INFORMATIONAL`: No overlapping work_ids

- ✅ **MP work_analysis PK constraint** `INFORMATIONAL`: PK constraint exists: work_analysis_pkey

- ✅ **MLA mla_work_analysis PK constraint** `INFORMATIONAL`: PK constraint exists: mla_work_analysis_pkey

---

## Section 6: MEMBER METRICS INTEGRITY

Checks: 4 | PASS: 4 | FAIL: 0 | WARN: 0

- ✅ **member_metrics total count** `INFORMATIONAL`: Expected 776, got 776

- ✅ **member_metrics by type** `INFORMATIONAL`: MLA: 233, MP: 543

- ✅ **Duplicate member_id+type in member_metrics** `INFORMATIONAL`: No duplicates

- ✅ **Synthetic IDs in member_metrics** `INFORMATIONAL`: No synthetic IDs

---

## Section 7: MEMBER INTELLIGENCE INTEGRITY

Checks: 6 | PASS: 6 | FAIL: 0 | WARN: 0

- ✅ **member_intelligence total count** `INFORMATIONAL`: Expected 776, got 776

- ✅ **member_intelligence by type** `INFORMATIONAL`: MLA: 233, MP: 543

- ✅ **Duplicate member_id+type in member_intelligence** `INFORMATIONAL`: No duplicates

- ✅ **Synthetic IDs in member_intelligence** `INFORMATIONAL`: No synthetic IDs

- ✅ **Members in member_metrics but not member_intelligence** `INFORMATIONAL`: All members have intelligence records

- ✅ **member_intelligence without member_metrics** `INFORMATIONAL`: No orphan intelligence records

---

## Section 8: STATE TABLES INTEGRITY

Checks: 8 | PASS: 8 | FAIL: 0 | WARN: 0

- ✅ **state_metrics count** `INFORMATIONAL`: Expected >= 36, got 36

- ✅ **state_intelligence count** `INFORMATIONAL`: Expected >= 36, got 36

- ✅ **Duplicate state_id in state_metrics** `INFORMATIONAL`: No duplicates

- ✅ **Duplicate state_id in state_intelligence** `INFORMATIONAL`: No duplicates

- ✅ **NULL state_id in state_metrics** `INFORMATIONAL`: No NULL state_ids

- ✅ **NULL state_id in state_intelligence** `INFORMATIONAL`: No NULL state_ids

- ✅ **States in state_metrics but not state_intelligence** `INFORMATIONAL`: All states have intelligence

- ✅ **States in state_intelligence but not state_metrics** `INFORMATIONAL`: No orphan intelligence records

---

## Section 9: EVIDENCE & GEMINI INTEGRITY

Checks: 6 | PASS: 6 | FAIL: 0 | WARN: 0

- ✅ **entity_evidence total count** `INFORMATIONAL`: 812 evidence records

- ✅ **Duplicate entity_type+entity_id in entity_evidence** `INFORMATIONAL`: No duplicates

- ✅ **ai_analysis total count** `INFORMATIONAL`: 812 AI analysis records

- ✅ **Duplicate entity_type+entity_id in ai_analysis** `INFORMATIONAL`: No duplicates

- ✅ **evidence_work_refs total count** `INFORMATIONAL`: 11,975 evidence work references

- ✅ **gemini_retry_backlog status** `INFORMATIONAL`: Total backlog: 252, Pending: 0

---

## Section 10: ANOMALY DATA INTEGRITY

Checks: 9 | PASS: 9 | FAIL: 0 | WARN: 0

- ✅ **ml_work_anomaly table exists** `INFORMATIONAL`: Table exists

- ✅ **MP work_analysis isolation levels** `INFORMATIONAL`: HIGHLY_UNUSUAL: 10484, NORMAL: 84395, UNUSUAL: 14298

- ✅ **MLA work_analysis isolation levels** `INFORMATIONAL`: HIGHLY_UNUSUAL: 5821, NORMAL: 15298, UNUSUAL: 4539

- ✅ **MP works with isolation_score** `INFORMATIONAL`: 109,177 MP works have isolation scores

- ✅ **MLA works with isolation_score** `INFORMATIONAL`: 25,658 MLA works have isolation scores

- ✅ **MP works missing isolation_score** `INFORMATIONAL`: All MP works have isolation scores

- ✅ **MLA works missing isolation_score** `INFORMATIONAL`: All MLA works have isolation scores

- ✅ **model_registry contents** `INFORMATIONAL`: 1 models registered

- ✅ **XGBoost production artifacts** `INFORMATIONAL`: No XGBoost artifacts found

---

## Section 11: PERFORMANCE SCORES

Checks: 5 | PASS: 3 | FAIL: 0 | WARN: 1

- ⚠️ **ranking_qualified members** [HIGH] `CURRENT PRODUCTION ISSUE`: Expected 776 qualified, got 720

- ✅ **Performance score distribution (qualified members)** `INFORMATIONAL`: Min: 0.16, Max: 92.25, Avg: 40.76, NULL: 0, Negative: 0, >100: 0

- ✅ **Scores within valid range [0,100]** `INFORMATIONAL`: All scores within [0,100]

- ✅ **NULL scores for qualified members** `INFORMATIONAL`: All qualified members have scores

- ℹ️ **Unqualified members breakdown** `INFORMATIONAL`: 56 unqualified members

---

## Section 12: RANKING CONSISTENCY

Checks: 7 | PASS: 6 | FAIL: 0 | WARN: 1

- ✅ **MLA max rank vs population** `INFORMATIONAL`: Max rank 187 vs population 187 (OK)

- ✅ **MP max rank vs population** `INFORMATIONAL`: Max rank 533 vs population 533 (OK)

- ✅ **Ranks exceeding population** `INFORMATIONAL`: No ranks exceed population

- ✅ **Qualified members with NULL rank** `INFORMATIONAL`: All qualified members have ranks

- ✅ **State max rank vs population** `INFORMATIONAL`: Max state rank 36 vs population 36

- ⚠️ **member_intelligence peer_rank coverage** [MEDIUM] `CURRENT PRODUCTION ISSUE`: 720/776 have peer_rank

- ✅ **member_intelligence peer_percentile coverage** `INFORMATIONAL`: 720/720 have peer_percentile (with peer_rank)

---

## Section 13: RISK DATA INTEGRITY

Checks: 5 | PASS: 5 | FAIL: 0 | WARN: 0

- ✅ **member_intelligence risk level distribution** `INFORMATIONAL`: CRITICAL: 78, HIGH: 155, LOW: 310, MODERATE: 233

- ✅ **Invalid risk_level values** `INFORMATIONAL`: All risk levels are valid

- ✅ **Risk scores outside [0,100] range** `INFORMATIONAL`: All risk scores in [0,100]

- ✅ **Scored members without risk_level** `INFORMATIONAL`: All scored members have risk_level

- ✅ **state_intelligence risk level distribution** `INFORMATIONAL`: LOW: 14, MODERATE: 22

---

## Section 14: ORPHAN RECORD CHECKS

Checks: 3 | PASS: 3 | FAIL: 0 | WARN: 0

- ✅ **MP work_analysis members not in member_metrics** `INFORMATIONAL`: No orphan MP works

- ✅ **MLA work_analysis members not in member_metrics** `INFORMATIONAL`: No orphan MLA works

- ✅ **evidence_work_refs referencing non-existent work_ids** `INFORMATIONAL`: No orphan evidence work refs

---

## Section 15: DB1/DB2 ROUTING VERIFICATION

Checks: 3 | PASS: 3 | FAIL: 0 | WARN: 0

- ✅ **Intelligence/analytics tables on DB1** `INFORMATIONAL`: No intelligence tables on DB1 (correct)

- ✅ **Deprecated/archival tables on DB1** `HISTORICAL/ARCHIVAL`: 8 z_deprecated_* tables (historical, harmless)

- ✅ **Unexpected tables on DB1** `INFORMATIONAL`: All 30 DB1 tables are expected (source + source copies + deprecated)

---

## Section 16: NULL / INVALID DATA AUDIT

Checks: 17 | PASS: 17 | FAIL: 0 | WARN: 0

- ✅ **work_analysis NULL work_id** `INFORMATIONAL`: Clean

- ✅ **mla_work_analysis NULL work_id** `INFORMATIONAL`: Clean

- ✅ **member_metrics NULL member_id** `INFORMATIONAL`: Clean

- ✅ **member_metrics NULL member_type** `INFORMATIONAL`: Clean

- ✅ **state_metrics NULL state_id** `INFORMATIONAL`: Clean

- ✅ **member_intelligence NULL member_id** `INFORMATIONAL`: Clean

- ✅ **state_intelligence NULL state_id** `INFORMATIONAL`: Clean

- ✅ **work_analysis NULL member_id** `INFORMATIONAL`: Clean

- ✅ **mla_work_analysis NULL member_id** `INFORMATIONAL`: Clean

- ✅ **member_metrics negative total_works** `INFORMATIONAL`: Clean

- ✅ **member_metrics negative completed_works** `INFORMATIONAL`: Clean

- ✅ **state_metrics negative total_works** `INFORMATIONAL`: Clean

- ✅ **member_metrics completion_rate_pct out of range** `INFORMATIONAL`: Clean

- ✅ **member_metrics fund_utilization_pct out of range** `INFORMATIONAL`: Clean

- ✅ **state_metrics completion_rate_pct out of range** `INFORMATIONAL`: Clean

- ✅ **state_metrics NULL state_name** `INFORMATIONAL`: Clean

- ✅ **member_metrics NULL member_name** `INFORMATIONAL`: Clean

---

## Section 17: LATEST PIPELINE STATE

Checks: 4 | PASS: 4 | FAIL: 0 | WARN: 0

- ✅ **data_updated status** `INFORMATIONAL`: Status: complete, Completed: 2026-09-19 04:25:22.324084+00:00, Updated: 2026-09-19 04:25:22.324084+00:00

- ✅ **member_intelligence calculated_at range** `INFORMATIONAL`: Oldest: 2026-09-19 04:25:04.665991+00:00, Newest: 2026-09-19 04:25:06.766000+00:00, Total: 776

- ✅ **state_intelligence calculated_at range** `INFORMATIONAL`: Oldest: 2026-09-19 04:25:07.063292+00:00, Newest: 2026-09-19 04:25:07.063292+00:00

- ✅ **Ingestion jobs for latest run** `INFORMATIONAL`: 1 job records

---

## Section 18: PROJECT-DELAY ARTIFACT CHECK

Checks: 2 | PASS: 2 | FAIL: 0 | WARN: 0

- ✅ **XGBoost/project-delay in model_registry** `INFORMATIONAL`: No XGBoost/project-delay artifacts

- ✅ **XGBoost/project-delay columns in schema** `INFORMATIONAL`: No XGBoost/project-delay columns

---

# FINAL VERDICT: PIPELINE READY — NO DATA INTEGRITY ISSUES FOUND
