"""Allocation matching (Phase: Data Quality / Allocation Foundation).

Strategy:
  - MP:  mp_allocations.mp_id is 1:1 with mps.mp_id (verified). Direct join.
  - MLA: master mla_allocations.mla_id (1..233) does NOT match member_metrics
    member_id (which uses loader-assigned ids >= 100000). The master mla
    identity (mlas) is joined by (normalized name, tenure, house) to
    member_metrics. Normalisation strips common honorifics ("Shri", "Smt.",
    "Dr.", "Ms.", etc.) and collapses whitespace so the two name spellings
    actually match. When two member_metrics rows share a normalized key the
    one with the largest allocated_amount is chosen; ties go to the first.

This module is pure (no DB I/O); persist in a separate orchestrator.
"""

from collections import defaultdict
import re


SOURCE_ALLOCATION_TABLE = "allocation_table"
SOURCE_UNMATCHED = "unmatched"

_HONORIFICS = re.compile(
    r"^(shri|smt\.?|dr\.?|ms\.?|prof\.?|mr\.?|mrs\.?)\s+",
    re.IGNORECASE,
)


def _norm(s):
    s = (s or "").strip()
    # Strip a leading honorific for cross-source name matching.
    s = _HONORIFICS.sub("", s)
    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s.lower()


def build_canonical_allocation(db1_rows_alloc, db1_rows_master, db2_member_rows):
    """Return list of dicts for upsert into member_metrics.allocated_amount."""
    masters_mla = [m for m in db1_rows_master if m.get("member_type") == "MLA"]
    mp_member_by_id = {r["member_id"]: r for r in db2_member_rows
                       if r.get("member_type") == "MP"}

    # MLA allocations all have source_house_of_parliament = 1 (Rajya Sabha).
    # Normalise that to the canonical "Rajya Sabha" string so it matches
    # member_metrics.house_name.
    def _mla_house(r):
        h = r.get("source_house_of_parliament")
        if h in (1, "1", "1.0"):
            return "Rajya Sabha"
        return _norm(h)

    alloc_mla_idx = defaultdict(list)
    for r in db1_rows_alloc:
        if r.get("member_type") != "MLA":
            continue
        m = next((x for x in masters_mla
                  if x.get("master_id") == r["master_id"]), None)
        if not m:
            continue
        key = (_norm(m.get("name")), _norm(r.get("source_tenure")), _mla_house(r))
        alloc_mla_idx[key].append(r)

    out = []
    matched_mp = 0
    for mid in mp_member_by_id:
        a = next((x for x in db1_rows_alloc
                   if x.get("member_type") == "MP" and x.get("master_id") == mid), None)
        if a:
            out.append({
                "member_id": mid, "member_type": "MP",
                "allocated_amount": float(a["allocated_amount"]),
                "allocated_source": SOURCE_ALLOCATION_TABLE,
                "allocated_confidence": "HIGH",
            })
            matched_mp += 1
        else:
            out.append({
                "member_id": mid, "member_type": "MP",
                "allocated_amount": 0.0,
                "allocated_source": SOURCE_UNMATCHED,
                "allocated_confidence": "LOW",
            })

    matched_mla = 0
    for r in db2_member_rows:
        if r.get("member_type") != "MLA":
            continue
        key = (_norm(r.get("member_name")), _norm(r.get("tenure")), _norm(r.get("house_name")))
        candidates = alloc_mla_idx.get(key, [])
        if len(candidates) == 1:
            a = candidates[0]
            out.append({
                "member_id": r["member_id"], "member_type": "MLA",
                "allocated_amount": float(a["allocated_amount"]),
                "allocated_source": SOURCE_ALLOCATION_TABLE,
                "allocated_confidence": "HIGH",
            })
            matched_mla += 1
        elif len(candidates) > 1:
            a = max(candidates, key=lambda x: float(x["allocated_amount"]))
            out.append({
                "member_id": r["member_id"], "member_type": "MLA",
                "allocated_amount": float(a["allocated_amount"]),
                "allocated_source": SOURCE_ALLOCATION_TABLE,
                "allocated_confidence": "MEDIUM",
            })
            matched_mla += 1
        else:
            out.append({
                "member_id": r["member_id"], "member_type": "MLA",
                "allocated_amount": 0.0,
                "allocated_source": SOURCE_UNMATCHED,
                "allocated_confidence": "LOW",
            })

    return out, {"mp": matched_mp, "mla": matched_mla}
