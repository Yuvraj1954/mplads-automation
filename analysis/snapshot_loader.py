"""Load NDJSON snapshot files into work records for analysis.

All member IDs are resolved against DB1 authoritative master tables
(mps.mp_id, mlas.mla_id). Synthetic/encounter-order IDs are never used.
"""

import json
import os
import re
from datetime import datetime
from collections import defaultdict
from pathlib import Path


def normalize_member_name(name):
    """Normalize a member name for identity matching.

    Handles: title prefixes (Shri, Dr., Smt), parenthetical suffixes,
    case differences, extra whitespace, and common transliteration variants.
    """
    if not name:
        return ""
    n = name.strip()
    # Remove parenthetical suffixes like (2024-30)
    n = re.sub(r'\s*\(.*?\)\s*$', '', n)
    # Remove common title prefixes
    n = re.sub(r'^(Shri|Smt\.?|Dr\.?|Mrs\.?|Ms\.?|Late)\s+', '', n, flags=re.IGNORECASE)
    # Uppercase for comparison
    n = n.upper()
    # Normalize whitespace
    n = " ".join(n.split())
    return n


def build_db1_member_map(db1_url, db1_key):
    """Query DB1 mps and mlas to build authoritative member ID mapping.

    Returns:
        dict: {("MP", normalized_name): {"id": db1_mp_id, "constituency_id": cid},
               ("MLA", normalized_name): {"id": db1_mla_id, "constituency_id": cid}}

    Raises:
        RuntimeError: If DB1 cannot be reached or returns unexpected data.
    """
    from supabase import create_client

    client = create_client(db1_url, db1_key)

    # Query all MPs with constituency_id
    mp_rows = client.table("mps").select("mp_id, mp_name, constituency_id").execute().data
    if not mp_rows:
        raise RuntimeError("DB1 mps table returned zero rows")

    # Query all MLAs with constituency_id
    mla_rows = client.table("mlas").select("mla_id, mla_name, constituency_id").execute().data
    if not mla_rows:
        raise RuntimeError("DB1 mlas table returned zero rows")

    member_map = {}

    for row in mp_rows:
        norm = normalize_member_name(row["mp_name"])
        if norm:
            key = ("MP", norm)
            if key in member_map:
                raise RuntimeError(
                    f"DB1 duplicate normalized MP name: '{row['mp_name']}' "
                    f"(normalized: '{norm}') maps to both "
                    f"mp_id={member_map[key]['id']} and mp_id={row['mp_id']}"
                )
            member_map[key] = {
                "id": row["mp_id"],
                "constituency_id": row.get("constituency_id"),
            }

    for row in mla_rows:
        norm = normalize_member_name(row["mla_name"])
        if norm:
            key = ("MLA", norm)
            if key in member_map:
                raise RuntimeError(
                    f"DB1 duplicate normalized MLA name: '{row['mla_name']}' "
                    f"(normalized: '{norm}') maps to both "
                    f"mla_id={member_map[key]['id']} and mla_id={row['mla_id']}"
                )
            member_map[key] = {
                "id": row["mla_id"],
                "constituency_id": row.get("constituency_id"),
            }

    print(f"  DB1 member map: {len(member_map)} entries "
          f"({len(mp_rows)} MPs + {len(mla_rows)} MLAs)")

    return member_map


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
    """Parse a date string into a date object.

    Supports:
        %d-%b-%Y           (e.g., 04-Jun-2024)
        %Y-%m-%d           (e.g., 2024-06-04)
        %d-%m-%Y           (e.g., 04-06-2024)
        %b %d, %Y %I:%M:%S %p  (e.g., Jun 4, 2024 12:00:00 AM)
        %b %d, %Y          (e.g., Jun 4, 2024)
    """
    if not s:
        return None
    for fmt in [
        "%Y-%m-%d",
        "%d-%b-%Y",
        "%d-%m-%Y",
        "%b %d, %Y %I:%M:%S %p",
        "%b %d, %Y",
    ]:
        try:
            return datetime.strptime(s.strip(), fmt).date()
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
                       mla_rec, mla_san, mla_comp, mla_exp,
                       db1_member_map=None):
    """Build unified work records from raw datasets.

    All member IDs are resolved against DB1 authoritative master tables.
    If db1_member_map is provided, every NDJSON member name must resolve
    to a DB1 mp_id/mla_id. Unresolved names raise ValueError.

    Args:
        db1_member_map: dict mapping ("MP"/"MLA", normalized_name) -> db1_id.
                        If None, raises RuntimeError (synthetic IDs are never used).

    Returns:
        tuple of (works, state_map, constituency_map, member_map)
        where member_map is {normalized_name: db1_id} for all resolved members.
    """
    if db1_member_map is None:
        raise RuntimeError(
            "build_work_records() requires db1_member_map. "
            "Synthetic member IDs are not permitted. "
            "Call build_db1_member_map() first."
        )

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
    member_map = {}  # normalized_name -> db1_id (authoritative)

    unresolved_mp = []
    unresolved_mla = []

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

        state_key = state_name.strip().upper()
        if state_key not in state_map:
            state_map[state_key] = len(state_map) + 1
        state_id = state_map[state_key]

        const_key = constituency.strip().upper()
        if const_key not in constituency_map:
            constituency_map[const_key] = len(constituency_map) + 1
        cid = constituency_map[const_key]

        norm_name = normalize_member_name(mp_name) or mp_name
        _entry = db1_member_map.get(("MP", norm_name))
        mid = _entry["id"] if _entry else None
        if mid is None:
            unresolved_mp.append(mp_name)
            continue

        if norm_name not in member_map:
            member_map[norm_name] = mid

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
        mp_name = r.get("MP_NAME", "")

        state_key = state_name.strip().upper()
        if state_key not in state_map:
            state_map[state_key] = len(state_map) + 1
        state_id = state_map[state_key]

        const_key = constituency.strip().upper()
        if const_key not in constituency_map:
            constituency_map[const_key] = len(constituency_map) + 1
        cid = constituency_map[const_key]

        norm_name = normalize_member_name(mp_name) or mp_name
        _entry = db1_member_map.get(("MLA", norm_name))
        mid = _entry["id"] if _entry else None
        if mid is None:
            unresolved_mla.append(mp_name)
            continue

        if norm_name not in member_map:
            member_map[norm_name] = mid

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

    if unresolved_mp:
        raise ValueError(
            f"UNRESOLVED MP NAMES ({len(unresolved_mp)}): "
            f"{unresolved_mp[:20]}"
        )
    if unresolved_mla:
        raise ValueError(
            f"UNRESOLVED MLA NAMES ({len(unresolved_mla)}): "
            f"{unresolved_mla[:20]}"
        )

    print(f"  Total works built: {len(works):,}")
    print(f"  MP: {sum(1 for w in works if w['member_type'] == 'MP'):,}")
    print(f"  MLA: {sum(1 for w in works if w['member_type'] == 'MLA'):,}")
    print(f"  Unique states: {len(state_map)}")
    print(f"  Resolved members: {len(member_map)}")

    return works, state_map, constituency_map, member_map
