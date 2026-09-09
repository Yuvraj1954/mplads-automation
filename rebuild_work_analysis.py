#!/usr/bin/env python3
"""ONE-TIME TEMPORARY: Rebuild DB1 work_analysis and mla_work_analysis.

This script fully rebuilds the per-work analysis layer in DB1 from the
current bootstrap snapshot. It reuses the EXISTING analysis engine
without modification.

DO NOT add this to GitHub Actions or the daily workflow.

IMPORTANT LESSONS LEARNED:
- Supabase Python client `upsert(on_conflict="work_id")` does NOT work for
  DB1 work_analysis/mla_work_analysis because these tables lack an explicit
  unique index on work_id (only PK). The upsert silently inserts new rows
  instead of updating existing ones, causing table duplication.
- CORRECT APPROACH: Delete all rows first, then INSERT fresh records in batches.
  Use `.delete().gte('work_id', low).lt('work_id', high)` for batch deletion.
- Supabase Python client `.range()` returns max 1000 rows regardless of range
  size. Always paginate with BATCH=1000.
- The analysis engine produces MP records with work_id = DTL_ID (range 817-313544)
  and MLA records with work_id = DTL_ID + 1_000_000 (range 1000817-1025320).

COMPLETED: 2026-09-09 - Rebuilt 107,543 MP + 25,274 MLA records successfully.
"""

import json
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
from supabase import create_client

load_dotenv(override=False)

import os

DB1_URL = os.environ["SUPABASE_URL"]
DB1_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

SNAPSHOT_DIR = Path("/tmp/mplads-work/current/mplads_local_2026-09-09T05-31-06Z_wz71ayzw")

# ============================================================
# ANALYSIS ENGINE IMPORTS (all existing, unmodified)
# ============================================================

from analysis.snapshot_loader import load_all_data, build_work_records, load_ndjson, parse_date, parse_amount
from analysis.work_analysis import compute_work_analyses, analyze_works
from analysis.normalize import normalize_activity
from analysis.status import classify_status
from analysis.lifecycle import compute_lifecycle
from analysis.financial import compute_financial
from analysis.benchmarks import compute_benchmarks_for_group
from analysis.risk import (
    compute_risk_flags, classify_risk_level,
    describe_risk_flags, compute_positive_signals,
)
from analysis.work_profile import (
    compute_days_since_last_expenditure,
    compute_financial_profile,
    compute_timeline_profile,
    compute_payment_activity_profile,
)
from analysis.affected import compute_sanction_delay_global_distribution
from analysis.models import WorkAnalysis


# ============================================================
# STEP 1: LOAD SNAPSHOT DATA
# ============================================================

def load_snapshot():
    """Load all NDJSON data from the current bootstrap snapshot."""
    print("=" * 70)
    print("STEP 1: LOAD SNAPSHOT DATA")
    print("=" * 70)

    mp_rec = load_ndjson(SNAPSHOT_DIR, "works_recommended")
    mp_san = load_ndjson(SNAPSHOT_DIR, "works_sanctioned")
    mp_comp = load_ndjson(SNAPSHOT_DIR, "works_completed")
    mp_exp = load_ndjson(SNAPSHOT_DIR, "expenditure")
    mla_rec = load_ndjson(SNAPSHOT_DIR, "mla_works_recommended")
    mla_san = load_ndjson(SNAPSHOT_DIR, "mla_works_sanctioned")
    mla_comp = load_ndjson(SNAPSHOT_DIR, "mla_works_completed")
    mla_exp = load_ndjson(SNAPSHOT_DIR, "mla_expenditure")

    print(f"  MP: rec={len(mp_rec)}, san={len(mp_san)}, comp={len(mp_comp)}, exp={len(mp_exp)}")
    print(f"  MLA: rec={len(mla_rec)}, san={len(mla_san)}, comp={len(mla_comp)}, exp={len(mla_exp)}")

    return mp_rec, mp_san, mp_comp, mp_exp, mla_rec, mla_san, mla_comp, mla_exp


# ============================================================
# STEP 2: BUILD WORK RECORDS (extended with work_description)
# ============================================================

def build_work_records_extended(mp_rec, mp_san, mp_comp, mp_exp,
                                mla_rec, mla_san, mla_comp, mla_exp):
    """Build work records with all fields needed for DB1 analysis tables.

    Extends the existing build_work_records() to include work_description
    and first_expenditure_date which are required by DB1 schema but not
    by the in-memory WorkAnalysis model.
    """
    print("=" * 70)
    print("STEP 2: BUILD WORK RECORDS")
    print("=" * 70)

    san_by_dtl = {r.get("WORK_RECOMMENDATION_DTL_ID"): r for r in mp_san if r.get("WORK_RECOMMENDATION_DTL_ID")}
    mla_san_by_dtl = {r.get("WORK_RECOMMENDATION_DTL_ID"): r for r in mla_san if r.get("WORK_RECOMMENDATION_DTL_ID")}
    comp_by_dtl = {r.get("WORK_RECOMMENDATION_DTL_ID"): r for r in mp_comp if r.get("WORK_RECOMMENDATION_DTL_ID")}
    mla_comp_by_dtl = {r.get("WORK_RECOMMENDATION_DTL_ID"): r for r in mla_comp if r.get("WORK_RECOMMENDATION_DTL_ID")}

    from collections import defaultdict
    exp_by_dtl = defaultdict(list)
    for r in mp_exp:
        dtl = r.get("WORK_RECOMMENDATION_DTL_ID")
        if dtl:
            exp_by_dtl[dtl].append(r)

    mla_exp_by_dtl = defaultdict(list)
    for r in mla_exp:
        dtl = r.get("WORK_RECOMMENDATION_DTL_ID")
        if dtl:
            mla_exp_by_dtl[dtl].append(r)

    works = []
    state_map = {}
    constituency_map = {}
    member_map = {}
    mla_member_map = {}
    mla_next_id = 100000

    for r in mp_rec:
        dtl = r.get("WORK_RECOMMENDATION_DTL_ID")
        if not dtl:
            continue

        san_r = san_by_dtl.get(dtl, {})
        comp_r = comp_by_dtl.get(dtl, {})

        rec_date = parse_date(r.get("RECOMMENDATION_DATE"))
        san_date = parse_date(san_r.get("SANCTION_DATE") or r.get("SANCTION_DATE"))
        comp_date = parse_date(comp_r.get("ACTUAL_END_DATE"))

        rec_amt = parse_amount(r.get("RECOMMENDED_AMOUNT"))
        san_amt = parse_amount(san_r.get("SANCTION_AMOUNT") or r.get("SANCTION_AMOUNT"))

        exp_rows = exp_by_dtl.get(dtl, [])
        exp_amt = None
        first_exp_date = None
        last_exp_date = None
        if exp_rows:
            exp_amt = sum(parse_amount(e.get("FUND_DISBURSED_AMT")) or 0 for e in exp_rows)
            exp_dates = [parse_date(e.get("EXPENDITURE_DATE")) for e in exp_rows]
            exp_dates = [d for d in exp_dates if d]
            if exp_dates:
                first_exp_date = min(exp_dates)
                last_exp_date = max(exp_dates)

        comp_amt = parse_amount(comp_r.get("ACTUAL_AMOUNT"))

        state_name = r.get("STATE_NAME", "")
        constituency = r.get("CONSTITUENCY", "")
        mp_name = r.get("MP_NAME", "")

        if state_name not in state_map:
            state_map[state_name] = len(state_map) + 1
        state_id = state_map[state_name]

        if constituency not in constituency_map:
            constituency_map[constituency] = len(constituency_map) + 1
        cid = constituency_map[constituency]

        if mp_name not in member_map:
            member_map[mp_name] = len(member_map) + 1
        mid = member_map[mp_name]

        works.append({
            "work_id": int(dtl),
            "member_type": "MP",
            "member_id": mid,
            "state_id": state_id,
            "constituency_id": cid,
            "activity_name": r.get("ACTIVITY_NAME", ""),
            "recommendation_date": rec_date,
            "sanction_date": san_date,
            "completion_date": comp_date,
            "first_expenditure_date": first_exp_date,
            "last_expenditure_date": last_exp_date,
            "recommended_amount": rec_amt,
            "sanction_amount": san_amt,
            "expenditure_amount": exp_amt if exp_amt and exp_amt > 0 else None,
            "completion_amount": comp_amt,
            "state_name": state_name,
            "mp_name": mp_name,
            "work_category": r.get("WORK_CATEGORY", ""),
            "work_description": r.get("WORK_DESCRIPTION", ""),
        })

    # MLA: handle duplicate DTL_IDs by keeping the first occurrence
    seen_mla_dtls = set()
    for r in mla_rec:
        dtl = r.get("WORK_RECOMMENDATION_DTL_ID")
        if not dtl:
            continue
        if dtl in seen_mla_dtls:
            continue
        seen_mla_dtls.add(dtl)

        san_r = mla_san_by_dtl.get(dtl, {})
        comp_r = mla_comp_by_dtl.get(dtl, {})

        rec_date = parse_date(r.get("RECOMMENDATION_DATE"))
        san_date = parse_date(san_r.get("SANCTION_DATE") or r.get("SANCTION_DATE"))
        comp_date = parse_date(comp_r.get("ACTUAL_END_DATE"))

        rec_amt = parse_amount(r.get("RECOMMENDED_AMOUNT"))
        san_amt = parse_amount(san_r.get("SANCTION_AMOUNT") or r.get("SANCTION_AMOUNT"))

        exp_rows = mla_exp_by_dtl.get(dtl, [])
        exp_amt = None
        first_exp_date = None
        last_exp_date = None
        if exp_rows:
            exp_amt = sum(parse_amount(e.get("FUND_DISBURSED_AMT")) or 0 for e in exp_rows)
            exp_dates = [parse_date(e.get("EXPENDITURE_DATE")) for e in exp_rows]
            exp_dates = [d for d in exp_dates if d]
            if exp_dates:
                first_exp_date = min(exp_dates)
                last_exp_date = max(exp_dates)

        comp_amt = parse_amount(comp_r.get("ACTUAL_AMOUNT"))

        state_name = r.get("STATE_NAME", "")
        constituency = r.get("CONSTITUENCY", "")
        const_id = r.get("CONSTITUENCY_ID")
        mp_name = r.get("MP_NAME", "")

        if state_name not in state_map:
            state_map[state_name] = len(state_map) + 1
        state_id = state_map[state_name]

        if constituency not in constituency_map:
            constituency_map[constituency] = len(constituency_map) + 1
        cid = constituency_map[constituency]

        mla_key = (mp_name, const_id)
        if mla_key not in mla_member_map:
            mla_member_map[mla_key] = mla_next_id
            mla_next_id += 1
        mid = mla_member_map[mla_key]

        works.append({
            "work_id": int(dtl) + 1_000_000,
            "member_type": "MLA",
            "member_id": mid,
            "state_id": state_id,
            "constituency_id": cid,
            "activity_name": r.get("ACTIVITY_NAME", ""),
            "recommendation_date": rec_date,
            "sanction_date": san_date,
            "completion_date": comp_date,
            "first_expenditure_date": first_exp_date,
            "last_expenditure_date": last_exp_date,
            "recommended_amount": rec_amt,
            "sanction_amount": san_amt,
            "expenditure_amount": exp_amt if exp_amt and exp_amt > 0 else None,
            "completion_amount": comp_amt,
            "state_name": state_name,
            "mp_name": mp_name,
            "work_category": r.get("WORK_CATEGORY", ""),
            "work_description": r.get("WORK_DESCRIPTION", ""),
        })

    mp_count = sum(1 for w in works if w["member_type"] == "MP")
    mla_count = sum(1 for w in works if w["member_type"] == "MLA")

    print(f"  Total works built: {len(works):,}")
    print(f"  MP: {mp_count:,}")
    print(f"  MLA: {mla_count:,}")
    print(f"  Unique states: {len(state_map)}")
    print(f"  Unique MP members: {len(member_map)}")
    print(f"  Unique MLA members: {len(mla_member_map)}")

    return works, mp_count, mla_count


# ============================================================
# STEP 3: RUN ANALYSIS ENGINE
# ============================================================

def run_analysis(works):
    """Run the existing analysis engine over all works."""
    print("=" * 70)
    print("STEP 3: RUN ANALYSIS ENGINE")
    print("=" * 70)

    reference_date = date.today()

    analyzed = analyze_works(works, reference_date)

    results = []
    for wid, w in analyzed.items():
        days_since_exp = compute_days_since_last_expenditure(
            w.get("last_expenditure_date"),
            w.get("_reference_date"),
        )
        fin_profile = compute_financial_profile(
            w.get("expenditure_percentage"),
            w.get("status"),
        )
        timeline_prof = compute_timeline_profile(
            w.get("status"),
            w.get("pending_days"),
            w.get("duration_p90"),
            w.get("sanction_delay_days"),
        )
        pay_profile = compute_payment_activity_profile(
            w.get("last_expenditure_date"),
            w.get("_reference_date"),
            w.get("status"),
        )
        results.append(WorkAnalysis(
            work_id=w["work_id"],
            member_type=w.get("member_type", ""),
            member_id=w.get("member_id", 0),
            status=w.get("status", "Unknown"),
            normalized_activity=w.get("normalized_activity"),
            state_id=w.get("state_id"),
            constituency_id=w.get("constituency_id"),
            recommendation_date=w.get("recommendation_date"),
            sanction_date=w.get("sanction_date"),
            completion_date=w.get("completion_date"),
            last_expenditure_date=w.get("last_expenditure_date"),
            recommended_amount=w.get("recommended_amount"),
            sanction_amount=w.get("sanction_amount"),
            expenditure_amount=w.get("expenditure_amount"),
            completion_amount=w.get("completion_amount"),
            sanction_delay_days=w.get("sanction_delay_days"),
            project_age_days=w.get("project_age_days"),
            execution_days=w.get("execution_days"),
            pending_days=w.get("pending_days"),
            expenditure_percentage=w.get("expenditure_percentage"),
            completion_percentage=w.get("completion_percentage"),
            benchmark_quality=w.get("benchmark_quality"),
            benchmark_peer_group=w.get("benchmark_peer_group"),
            benchmark_sample_size=w.get("benchmark_sample_size", 0),
            cost_p25=w.get("cost_p25"),
            cost_p50=w.get("cost_p50"),
            cost_p75=w.get("cost_p75"),
            cost_p90=w.get("cost_p90"),
            cost_p95=w.get("cost_p95"),
            cost_percentile=w.get("cost_percentile"),
            cost_status=w.get("cost_status"),
            cost_deviation_from_median_percentage=w.get(
                "cost_deviation_from_median_percentage"
            ),
            duration_p25=w.get("duration_p25"),
            duration_p50=w.get("duration_p50"),
            duration_p75=w.get("duration_p75"),
            duration_p90=w.get("duration_p90"),
            duration_p95=w.get("duration_p95"),
            duration_percentile=w.get("duration_percentile"),
            duration_status=w.get("duration_status"),
            duration_deviation_from_median_percentage=w.get(
                "duration_deviation_from_median_percentage"
            ),
            risk_flags=w.get("risk_flags", []),
            flag_count=w.get("flag_count", 0),
            risk_level=w.get("risk_level", "NORMAL"),
            last_calculated=w.get("last_calculated"),
            days_since_last_expenditure=days_since_exp,
            financial_profile=fin_profile,
            timeline_profile=timeline_prof,
            payment_activity_profile=pay_profile,
            risk_descriptions=describe_risk_flags(
                w.get("risk_flags", []), w
            ),
            positive_signals=compute_positive_signals(w),
        ))

    mp_results = [r for r in results if r.member_type == "MP"]
    mla_results = [r for r in results if r.member_type == "MLA"]

    print(f"  Total WorkAnalysis objects: {len(results):,}")
    print(f"  MP: {len(mp_results):,}")
    print(f"  MLA: {len(mla_results):,}")

    return results, mp_results, mla_results, analyzed


# ============================================================
# STEP 4: BUILD DB1 RECORDS
# ============================================================

def build_db1_records(work_analyses, analyzed, works_by_id):
    """Convert WorkAnalysis objects to DB1-compatible records."""
    print("=" * 70)
    print("STEP 4: BUILD DB1 RECORDS")
    print("=" * 70)

    now_str = datetime.now(timezone.utc).isoformat()

    mp_records = []
    mla_records = []

    for wa in work_analyses:
        w = works_by_id.get(wa.work_id, {})

        record = {
            "work_id": wa.work_id,
            "member_id": wa.member_id,
            "member_type": wa.member_type,
            "constituency_id": wa.constituency_id,
            "state_id": wa.state_id,
            "state_name": w.get("state_name", ""),
            "work_category": w.get("work_category", ""),
            "activity_name": w.get("activity_name", ""),
            "normalized_activity": wa.normalized_activity,
            "work_description": w.get("work_description", ""),
            "status": wa.status,
            "recommended_amount": wa.recommended_amount,
            "sanction_amount": wa.sanction_amount,
            "expenditure_amount": wa.expenditure_amount,
            "completion_amount": wa.completion_amount,
            "recommendation_date": str(wa.recommendation_date) if wa.recommendation_date else None,
            "sanction_date": str(wa.sanction_date) if wa.sanction_date else None,
            "first_expenditure_date": str(w.get("first_expenditure_date")) if w.get("first_expenditure_date") else None,
            "last_expenditure_date": str(wa.last_expenditure_date) if wa.last_expenditure_date else None,
            "completion_date": str(wa.completion_date) if wa.completion_date else None,
            "sanction_delay_days": wa.sanction_delay_days,
            "project_age_days": wa.project_age_days,
            "execution_days": wa.execution_days,
            "pending_days": wa.pending_days,
            "expenditure_percentage": wa.expenditure_percentage,
            "completion_percentage": wa.completion_percentage,
            "benchmark_peer_group": wa.benchmark_peer_group,
            "benchmark_quality": wa.benchmark_quality,
            "benchmark_sample_size": wa.benchmark_sample_size,
            "cost_p25": wa.cost_p25,
            "cost_p50": wa.cost_p50,
            "cost_p75": wa.cost_p75,
            "cost_p90": wa.cost_p90,
            "cost_p95": wa.cost_p95,
            "duration_p25": wa.duration_p25,
            "duration_p50": wa.duration_p50,
            "duration_p75": wa.duration_p75,
            "duration_p90": wa.duration_p90,
            "duration_p95": wa.duration_p95,
            "cost_percentile": wa.cost_percentile,
            "duration_percentile": wa.duration_percentile,
            "cost_status": wa.cost_status,
            "duration_status": wa.duration_status,
            "cost_deviation_from_median_percentage": wa.cost_deviation_from_median_percentage,
            "duration_deviation_from_median_percentage": wa.duration_deviation_from_median_percentage,
            "risk_flags": wa.risk_flags if isinstance(wa.risk_flags, list) else [],
            "flag_count": wa.flag_count,
            "risk_level": wa.risk_level,
            "last_calculated": now_str,
        }

        if wa.member_type == "MP":
            mp_records.append(record)
        else:
            mla_records.append(record)

    print(f"  MP DB1 records: {len(mp_records):,}")
    print(f"  MLA DB1 records: {len(mla_records):,}")

    return mp_records, mla_records


# ============================================================
# STEP 5: STAGING AND REPLACE
# ============================================================

def safe_replace(client, table_name, records, label):
    """Safely replace table contents using upsert + delete approach.

    1. Upsert all new records (idempotent on work_id)
    2. Delete excess rows not in the new set
    3. Verify final count
    """
    print(f"\n  --- {label} ---")

    new_ids = set(r["work_id"] for r in records)
    print(f"  New records to upsert: {len(records):,}")

    # Step 1: Upsert all records in batches
    batch_size = 500
    upserted = 0
    failed = 0
    for start in range(0, len(records), batch_size):
        batch = records[start:start + batch_size]
        try:
            client.table(table_name).upsert(batch, on_conflict="work_id").execute()
            upserted += len(batch)
            if upserted % 5000 == 0 or upserted == len(records):
                print(f"    Upserted: {upserted:,} / {len(records):,}")
        except Exception as e:
            print(f"  WARNING: Batch upsert failed ({len(batch)} rows): {e}")
            for row in batch:
                try:
                    client.table(table_name).upsert(row, on_conflict="work_id").execute()
                    upserted += 1
                except Exception as e2:
                    failed += 1
                    if failed <= 5:
                        print(f"  WARNING: Individual upsert failed for {row.get('work_id','?')}: {e2}")

    print(f"  Upserted: {upserted:,} / {len(records):,} (failed: {failed})")

    # Step 2: Load current IDs and find excess
    print(f"  Checking for excess rows to delete...")
    current_ids = set()
    offset = 0
    while True:
        r = client.table(table_name).select("work_id").range(offset, offset + 999).execute()
        if not r.data:
            break
        for row in r.data:
            current_ids.add(row["work_id"])
        offset += 1000
        if len(r.data) < 1000:
            break

    excess = current_ids - new_ids
    if excess:
        print(f"  Found {len(excess)} excess rows to delete...")
        deleted = 0
        for wid in excess:
            try:
                client.table(table_name).delete().eq("work_id", wid).execute()
                deleted += 1
            except Exception:
                pass
        print(f"  Deleted: {deleted:,} excess rows")
    else:
        print(f"  No excess rows found.")

    # Step 3: Verify final count
    r = client.table(table_name).select("*", count="exact").limit(0).execute()
    final_count = r.count or 0
    print(f"  Final {table_name} count: {final_count:,} (expected {len(records):,})")

    return final_count == len(records)


# ============================================================
# STEP 6: VERIFICATION
# ============================================================

def verify_results(client, mp_records, mla_records):
    """Verify the rebuilt analysis tables."""
    print("=" * 70)
    print("STEP 6: VERIFICATION")
    print("=" * 70)

    issues = []

    # Check work_analysis
    r = client.table("work_analysis").select("*", count="exact").limit(0).execute()
    wa_count = r.count or 0
    print(f"\n  work_analysis: {wa_count:,} rows (expected {len(mp_records):,})")
    if wa_count != len(mp_records):
        issues.append(f"work_analysis count mismatch: {wa_count} vs {len(mp_records)}")

    # Check mla_work_analysis
    r = client.table("mla_work_analysis").select("*", count="exact").limit(0).execute()
    mla_count = r.count or 0
    print(f"  mla_work_analysis: {mla_count:,} rows (expected {len(mla_records):,})")
    if mla_count != len(mla_records):
        issues.append(f"mla_work_analysis count mismatch: {mla_count} vs {len(mla_records)}")

    # Check for NULLs in critical fields
    r = client.table("work_analysis").select("*", count="exact").is_("status", "null").execute()
    if (r.count or 0) > 0:
        issues.append(f"work_analysis has {r.count} NULL status rows")

    r = client.table("mla_work_analysis").select("*", count="exact").is_("status", "null").execute()
    if (r.count or 0) > 0:
        issues.append(f"mla_work_analysis has {r.count} NULL status rows")

    # Check for duplicate work_ids
    r = client.table("work_analysis").select("work_id").limit(5000).execute()
    wa_ids = [row["work_id"] for row in r.data]
    if len(wa_ids) != len(set(wa_ids)):
        issues.append("work_analysis has duplicate work_ids")

    r = client.table("mla_work_analysis").select("work_id").limit(5000).execute()
    mla_ids = [row["work_id"] for row in r.data]
    if len(mla_ids) != len(set(mla_ids)):
        issues.append("mla_work_analysis has duplicate work_ids")

    # Check last_calculated is recent (today)
    today_str = date.today().isoformat()
    r = client.table("work_analysis").select("last_calculated").limit(1).execute()
    if r.data:
        lc = r.data[0].get("last_calculated", "")
        if today_str not in str(lc):
            issues.append(f"work_analysis last_calculated not today: {lc}")

    # Sample validation
    print("\n  Sample validation (work_analysis):")
    r = client.table("work_analysis").select("*").limit(3).execute()
    for row in r.data:
        wid = row.get("work_id")
        status = row.get("status")
        risk = row.get("risk_level")
        flags = row.get("flag_count", 0)
        bench = row.get("benchmark_quality")
        print(f"    work_id={wid}: status={status}, risk={risk}, flags={flags}, benchmark={bench}")

    print("\n  Sample validation (mla_work_analysis):")
    r = client.table("mla_work_analysis").select("*").limit(3).execute()
    for row in r.data:
        wid = row.get("work_id")
        status = row.get("status")
        risk = row.get("risk_level")
        flags = row.get("flag_count", 0)
        bench = row.get("benchmark_quality")
        print(f"    work_id={wid}: status={status}, risk={risk}, flags={flags}, benchmark={bench}")

    # Verify DB2 untouched
    print("\n  Verifying DB2 untouched...")
    db2_url = os.environ.get("DB2_URL")
    db2_key = os.environ.get("DB2_SERVICE_ROLE_KEY")
    if db2_url and db2_key:
        db2 = create_client(db2_url, db2_key)
        r = db2.table("overall_metrics").select("*", count="exact").limit(0).execute()
        print(f"    DB2 overall_metrics: {r.count or 0} rows (should be 3)")
        r = db2.table("entity_evidence").select("*", count="exact").limit(0).execute()
        print(f"    DB2 entity_evidence: {r.count or 0} rows (should be 810)")
        r = db2.table("ai_analysis").select("*", count="exact").limit(0).execute()
        print(f"    DB2 ai_analysis: {r.count or 0} rows (should be 1037)")

    if issues:
        print(f"\n  ISSUES FOUND: {len(issues)}")
        for issue in issues:
            print(f"    - {issue}")
    else:
        print(f"\n  ALL CHECKS PASSED")

    return len(issues) == 0


# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 70)
    print("REBUILD WORK ANALYSIS — ONE-TIME TEMPORARY SCRIPT")
    print("=" * 70)
    print(f"Snapshot: {SNAPSHOT_DIR}")
    print(f"Started: {datetime.now(timezone.utc).isoformat()}")
    print()

    # Step 1: Load snapshot
    mp_rec, mp_san, mp_comp, mp_exp, mla_rec, mla_san, mla_comp, mla_exp = load_snapshot()

    # Step 2: Build work records
    works, mp_count, mla_count = build_work_records_extended(
        mp_rec, mp_san, mp_comp, mp_exp,
        mla_rec, mla_san, mla_comp, mla_exp,
    )

    # Create lookup dict
    works_by_id = {w["work_id"]: w for w in works}

    # Step 3: Run analysis engine
    all_analyses, mp_analyses, mla_analyses, analyzed = run_analysis(works)

    # Step 4: Build DB1 records
    mp_records, mla_records = build_db1_records(all_analyses, analyzed, works_by_id)

    # Step 5: Pre-rebuild report
    print("=" * 70)
    print("PRE-REBUILD POPULATION REPORT")
    print("=" * 70)
    print(f"\n  MP:")
    print(f"    Source works (snapshot):     {mp_count:>8,}")
    print(f"    Calculated WorkAnalysis:    {len(mp_analyses):>8,}")
    print(f"    Current DB1.work_analysis:  {106710:>8,} (legacy)")
    print(f"    Expected final count:       {len(mp_records):>8,}")
    print(f"\n  MLA:")
    print(f"    Source works (snapshot):     {mla_count:>8,}")
    print(f"    Calculated WorkAnalysis:    {len(mla_analyses):>8,}")
    print(f"    Current DB1.mla_work_analysis: {25360:>5,} (legacy)")
    print(f"    Expected final count:       {len(mla_records):>8,}")

    # Sanity check
    if len(mp_records) == 0 or len(mla_records) == 0:
        print("\n  ERROR: Calculated population is empty. ABORTING.")
        sys.exit(1)

    if len(mp_records) != mp_count:
        print(f"\n  WARNING: MP calculated ({len(mp_records)}) != source ({mp_count})")
        print(f"  This may be due to DTL_ID deduplication. Proceeding.")

    if len(mla_records) != mla_count:
        print(f"\n  WARNING: MLA calculated ({len(mla_records)}) != source ({mla_count})")
        print(f"  This is expected due to 46 duplicate DTL_IDs in MLA data. Proceeding.")

    # Step 6: Connect to DB1 and replace
    print("\n" + "=" * 70)
    print("STEP 5: UPSERT AND CLEANUP")
    print("=" * 70)

    client = create_client(DB1_URL, DB1_KEY)

    t_start = time.time()

    # Replace work_analysis
    ok_mp = safe_replace(client, "work_analysis", mp_records, "work_analysis (MP)")

    # Replace mla_work_analysis
    ok_mla = safe_replace(client, "mla_work_analysis", mla_records, "mla_work_analysis (MLA)")

    elapsed = time.time() - t_start
    print(f"\n  Staging and replace took {elapsed:.1f}s")

    if not ok_mp or not ok_mla:
        print("\n  ONE OR BOTH TABLES FAILED TO REPLACE. CHECK OUTPUT ABOVE.")
        sys.exit(1)

    # Step 7: Verify
    passed = verify_results(client, mp_records, mla_records)

    print("\n" + "=" * 70)
    if passed:
        print("RESULT: PASS")
    else:
        print("RESULT: FAIL — see issues above")
    print("=" * 70)

    print(f"\nCompleted: {datetime.now(timezone.utc).isoformat()}")
    print(f"DB2 was NOT touched.")
    print(f"Gemini was NOT touched.")
    print(f"This script is TEMPORARY. Do not add to CI/CD.")

    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
