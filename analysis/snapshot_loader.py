"""Load NDJSON snapshot files into work records for analysis.

Replaces the obsolete audit_runner.load_all_data() and audit_runner.build_work_records()
with a direct loader that works with the current snapshot directory format.
"""

import json
from datetime import datetime
from collections import defaultdict
from pathlib import Path


def load_ndjson(snapshot_dir, subdir_name):
    """Load all part_*.ndjson files from a snapshot subdirectory."""
    subdir = Path(snapshot_dir) / subdir_name
    if not subdir.exists():
        return []
    files = sorted(subdir.glob("part_*.ndjson"))
    records = []
    for f in files:
        with open(f, "r") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
    return records


def parse_date(s):
    if not s:
        return None
    for fmt in ["%d-%b-%Y", "%Y-%m-%d", "%d-%m-%Y"]:
        try:
            return datetime.strptime(s, fmt).date()
        except (ValueError, TypeError):
            continue
    return None


def parse_amount(v):
    if v is None:
        return None
    try:
        val = float(v)
        return val if val > 0 else None
    except (ValueError, TypeError):
        return None


def load_all_data(snapshot_dir):
    """Load all raw NDJSON datasets from a snapshot directory.

    Returns:
        tuple of (mp_rec, mp_san, mp_comp, mp_exp, mla_rec, mla_san, mla_comp, mla_exp)
    """
    mp_rec = load_ndjson(snapshot_dir, "works_recommended")
    mp_san = load_ndjson(snapshot_dir, "works_sanctioned")
    mp_comp = load_ndjson(snapshot_dir, "works_completed")
    mp_exp = load_ndjson(snapshot_dir, "expenditure")
    mla_rec = load_ndjson(snapshot_dir, "mla_works_recommended")
    mla_san = load_ndjson(snapshot_dir, "mla_works_sanctioned")
    mla_comp = load_ndjson(snapshot_dir, "mla_works_completed")
    mla_exp = load_ndjson(snapshot_dir, "mla_expenditure")

    print(f"  MP: rec={len(mp_rec)}, san={len(mp_san)}, comp={len(mp_comp)}, exp={len(mp_exp)}")
    print(f"  MLA: rec={len(mla_rec)}, san={len(mla_san)}, comp={len(mla_comp)}, exp={len(mla_exp)}")

    return mp_rec, mp_san, mp_comp, mp_exp, mla_rec, mla_san, mla_comp, mla_exp


def build_work_records(mp_rec, mp_san, mp_comp, mp_exp,
                       mla_rec, mla_san, mla_comp, mla_exp):
    """Build unified work records from raw datasets.

    Returns:
        tuple of (works, state_map, constituency_map, member_map)
    """
    san_by_dtl = {}
    for r in mp_san:
        dtl = r.get("WORK_RECOMMENDATION_DTL_ID")
        if dtl:
            san_by_dtl[dtl] = r

    mla_san_by_dtl = {}
    for r in mla_san:
        dtl = r.get("WORK_RECOMMENDATION_DTL_ID")
        if dtl:
            mla_san_by_dtl[dtl] = r

    comp_by_dtl = {}
    for r in mp_comp:
        dtl = r.get("WORK_RECOMMENDATION_DTL_ID")
        if dtl:
            comp_by_dtl[dtl] = r

    mla_comp_by_dtl = {}
    for r in mla_comp:
        dtl = r.get("WORK_RECOMMENDATION_DTL_ID")
        if dtl:
            mla_comp_by_dtl[dtl] = r

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
        last_exp_date = None
        if exp_rows:
            exp_amt = sum(parse_amount(e.get("FUND_DISBURSED_AMT")) or 0 for e in exp_rows)
            exp_dates = [parse_date(e.get("EXPENDITURE_DATE")) for e in exp_rows]
            exp_dates = [d for d in exp_dates if d]
            if exp_dates:
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
            "last_expenditure_date": last_exp_date,
            "recommended_amount": rec_amt,
            "sanction_amount": san_amt,
            "expenditure_amount": exp_amt if exp_amt and exp_amt > 0 else None,
            "completion_amount": comp_amt,
            "state_name": state_name,
            "mp_name": mp_name,
            "work_category": r.get("WORK_CATEGORY", ""),
            "work_stage": r.get("WORK_STAGE", ""),
        })

    for r in mla_rec:
        dtl = r.get("WORK_RECOMMENDATION_DTL_ID")
        if not dtl:
            continue

        san_r = mla_san_by_dtl.get(dtl, {})
        comp_r = mla_comp_by_dtl.get(dtl, {})

        rec_date = parse_date(r.get("RECOMMENDATION_DATE"))
        san_date = parse_date(san_r.get("SANCTION_DATE") or r.get("SANCTION_DATE"))
        comp_date = parse_date(comp_r.get("ACTUAL_END_DATE"))

        rec_amt = parse_amount(r.get("RECOMMENDED_AMOUNT"))
        san_amt = parse_amount(san_r.get("SANCTION_AMOUNT") or r.get("SANCTION_AMOUNT"))

        exp_rows = mla_exp_by_dtl.get(dtl, [])
        exp_amt = None
        last_exp_date = None
        if exp_rows:
            exp_amt = sum(parse_amount(e.get("FUND_DISBURSED_AMT")) or 0 for e in exp_rows)
            exp_dates = [parse_date(e.get("EXPENDITURE_DATE")) for e in exp_rows]
            exp_dates = [d for d in exp_dates if d]
            if exp_dates:
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
            "work_id": int(dtl) + 1000000,
            "member_type": "MLA",
            "member_id": mid,
            "state_id": state_id,
            "constituency_id": cid,
            "activity_name": r.get("ACTIVITY_NAME", ""),
            "recommendation_date": rec_date,
            "sanction_date": san_date,
            "completion_date": comp_date,
            "last_expenditure_date": last_exp_date,
            "recommended_amount": rec_amt,
            "sanction_amount": san_amt,
            "expenditure_amount": exp_amt if exp_amt and exp_amt > 0 else None,
            "completion_amount": comp_amt,
            "state_name": state_name,
            "mp_name": mp_name,
            "work_category": r.get("WORK_CATEGORY", ""),
            "work_stage": r.get("WORK_STAGE", ""),
        })

    print(f"  Total works built: {len(works):,}")
    print(f"  MP: {sum(1 for w in works if w['member_type'] == 'MP'):,}")
    print(f"  MLA: {sum(1 for w in works if w['member_type'] == 'MLA'):,}")
    print(f"  Unique states: {len(state_map)}")
    print(f"  Unique MP members: {len(member_map)}")
    print(f"  Unique MLA members: {len(mla_member_map)}")

    return works, state_map, constituency_map, member_map
