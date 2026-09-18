"""Zero-work member injection from allocated_limit snapshots.

Discovers the authoritative MP/MLA master population from:
- allocated_limit (Lok Sabha MPs)
- mla_allocated_limit (Rajya Sabha MPs)

Injects zero-work members into MemberMetrics after normal work-derived
analysis, ensuring they reach Evidence and Gemini.

Design:
    SNAPSHOT DATA → MASTER POPULATION → ZERO-WORK INJECTION → EVIDENCE → GEMINI

This module does NOT:
- Classify members as "lazy" or "inactive"
- Fabricate financial/execution/risk metrics
- Create anomaly scores for zero-work members
- Change existing work-bearing member analysis
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from dataclasses import dataclass

from analysis.models import MemberMetrics


def parse_date(s):
    """Parse date from various formats found in snapshot data."""
    if not s:
        return None
    for fmt in ["%d-%b-%Y", "%Y-%m-%d", "%d-%m-%Y",
                "%b %d, %Y %I:%M:%S %p", "%b %d, %Y"]:
        try:
            return datetime.strptime(s, fmt).date()
        except (ValueError, TypeError):
            continue
    return None


def load_allocated_limit(snapshot_dir):
    """Load MP master population from allocated_limit snapshot.

    Args:
        snapshot_dir: path to snapshot directory containing allocated_limit/

    Returns:
        list of dicts with member info from allocated_limit
    """
    subdir = Path(snapshot_dir) / "allocated_limit"
    if not subdir.exists():
        return []

    records = []
    for f in sorted(subdir.glob("part_*.ndjson")):
        with open(f, "r") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue

                mp_name = record.get("MP_NAME")
                if not mp_name:
                    continue

                records.append({
                    "member_name": mp_name,
                    "member_type": "MP",
                    "state_name": record.get("STATE_NAME", ""),
                    "constituency": record.get("CONSTITUENCY", ""),
                    "house_name": record.get("HOUSE_NAME", ""),
                    "tenure": record.get("TENURE", ""),
                    "tenure_start_date": record.get("TENURE_START_DATE"),
                    "tenure_end_date": record.get("TENURE_END_DATE"),
                    "allocated_amount": record.get("ALLOCATED_AMT"),
                })

    return records


def load_mla_allocated_limit(snapshot_dir):
    """Load MLA master population from mla_allocated_limit snapshot.

    Args:
        snapshot_dir: path to snapshot directory containing mla_allocated_limit/

    Returns:
        list of dicts with member info from mla_allocated_limit
    """
    subdir = Path(snapshot_dir) / "mla_allocated_limit"
    if not subdir.exists():
        return []

    records = []
    for f in sorted(subdir.glob("part_*.ndjson")):
        with open(f, "r") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue

                mp_name = record.get("MP_NAME")
                if not mp_name:
                    continue

                records.append({
                    "member_name": mp_name,
                    "member_type": "MLA",
                    "state_name": record.get("STATE_NAME", ""),
                    "constituency": record.get("CONSTITUENCY", ""),
                    "house_name": record.get("HOUSE_NAME", ""),
                    "tenure": record.get("TENURE", ""),
                    "tenure_start_date": record.get("TENURE_START_DATE"),
                    "tenure_end_date": record.get("TENURE_END_DATE"),
                    "allocated_amount": record.get("ALLOCATED_AMT"),
                })

    return records


def discover_master_population(snapshot_dir):
    """Discover the authoritative MP/MLA master population from snapshots.

    Combines allocated_limit (Lok Sabha MPs) and mla_allocated_limit
    (Rajya Sabha MPs / MLAs) into a unified master population.

    Args:
        snapshot_dir: path to snapshot directory

    Returns:
        dict with:
            - mp_records: list of MP master population records
            - mla_records: list of MLA master population records
            - all_records: combined list
            - mp_count: total MP count
            - mla_count: total MLA count
            - total_count: total master population
    """
    mp_records = load_allocated_limit(snapshot_dir)
    mla_records = load_mla_allocated_limit(snapshot_dir)

    return {
        "mp_records": mp_records,
        "mla_records": mla_records,
        "all_records": mp_records + mla_records,
        "mp_count": len(mp_records),
        "mla_count": len(mla_records),
        "total_count": len(mp_records) + len(mla_records),
    }


def _build_state_map(master_records):
    """Build state_name → state_id mapping from master records."""
    state_map = {}
    for r in master_records:
        state_name = r.get("state_name", "")
        if state_name and state_name not in state_map:
            state_map[state_name] = len(state_map) + 1
    return state_map


def _build_member_name_to_id(master_records, existing_member_map):
    """Build member_name → member_id mapping, using existing IDs where possible.

    For members already in the works pipeline, reuse their existing member_id.
    For new zero-work members, assign new sequential IDs.
    """
    name_to_id = {}
    next_id = max(existing_member_map.values()) + 1 if existing_member_map else 1

    # First, map existing members from works pipeline
    for name, mid in existing_member_map.items():
        name_to_id[name] = mid

    # Then, assign IDs to new zero-work members
    for r in master_records:
        name = r["member_name"]
        if name not in name_to_id:
            name_to_id[name] = next_id
            next_id += 1

    return name_to_id


def inject_zero_work_members(member_metrics, snapshot_dir,
                             db1_member_map,
                             state_metrics_by_id=None,
                             works=None):
    """Inject zero-work members using DB1 as canonical population source.

    Iterates over ALL canonical DB1 members (from db1_member_map) and creates
    zero-work MemberMetrics for any member not already present from work analysis.

    Args:
        member_metrics: list of existing MemberMetrics from work analysis
        snapshot_dir: path to snapshot directory (for allocation context)
        db1_member_map: dict from build_db1_member_map() — the canonical
                        {("MP"/"MLA", norm_name): {"id": db1_id, "constituency_id": cid}}
        state_metrics_by_id: optional dict of state_id → StateMetrics (unused, kept for compat)
        works: optional list of raw work records (unused, kept for compat)

    Returns:
        list of all MemberMetrics (existing + injected zero-work members)
    """
    from analysis.snapshot_loader import normalize_member_name

    if not db1_member_map:
        return member_metrics

    # Build lookup of existing work-derived members by normalized name
    work_derived_lookup = {}  # (member_type, norm_name) -> member_id
    for m in member_metrics:
        mname = getattr(m, "member_name", None)
        if mname:
            norm = normalize_member_name(mname) or mname
            work_derived_lookup[(m.member_type, norm)] = m.member_id

    # Build set of existing (member_type, member_id) for fast membership check
    existing_ids = {(m.member_type, m.member_id) for m in member_metrics}

    # Load NDJSON allocation context for enrichment (state_name, allocated_amount)
    master = discover_master_population(snapshot_dir)
    context_by_name = {}
    if master["all_records"]:
        for record in master["all_records"]:
            name = record.get("member_name")
            if name:
                norm = normalize_member_name(name) or name
                if norm not in context_by_name:
                    context_by_name[norm] = record

    # Attach master context to WORK-BEARING members (enrichment)
    for m in member_metrics:
        mname = getattr(m, "member_name", None)
        if mname:
            norm = normalize_member_name(mname) or mname
        else:
            norm = None
        rec = context_by_name.get(norm) if norm else None
        if rec:
            if getattr(m, "_master_record", None) is None:
                m._master_record = rec
            if not getattr(m, "state_name", None):
                m.state_name = rec.get("state_name")

    # Iterate over ALL canonical DB1 members — DB1 is the population source
    zero_work_members = []
    for (member_type, norm_name), db1_info in db1_member_map.items():
        db1_id = db1_info["id"]
        constituency_id = db1_info.get("constituency_id")

        # Check if member already exists in work-derived metrics
        if (member_type, db1_id) in existing_ids:
            continue

        # Also check by normalized name (work-derived may have used a slightly
        # different name but resolved to the same DB1 ID)
        if (member_type, norm_name) in work_derived_lookup:
            continue

        # Get context from NDJSON allocation files if available
        ctx = context_by_name.get(norm_name)
        state_name = ctx.get("state_name", "") if ctx else ""
        allocated_amount = ctx.get("allocated_amount") if ctx else None
        member_name = ctx.get("member_name", norm_name) if ctx else norm_name
        house_name = ctx.get("house_name", "") if ctx else ""
        tenure_label = ctx.get("tenure", "") if ctx else ""
        tenure_start = parse_date(ctx.get("tenure_start_date")) if ctx else None
        tenure_end = parse_date(ctx.get("tenure_end_date")) if ctx else None

        # state_id: look up from state_metrics_by_id, or build from context
        state_id = None
        if state_name:
            if state_metrics_by_id:
                for sid, sm in state_metrics_by_id.items():
                    if getattr(sm, "state_name", "") == state_name:
                        state_id = sid
                        break

        # Create zero-work MemberMetrics with canonical DB1 ID
        m = MemberMetrics(
            member_id=db1_id,
            member_type=member_type,
            member_name=member_name,
            state_id=state_id,
            constituency_id=constituency_id,
            total_works=0,
            recommended_works=0,
            sanctioned_works=0,
            completed_works=0,
            ongoing_works=0,
            pending_works=0,
            recommended_amount=0,
            sanctioned_amount=0,
            expenditure_amount=0,
            completion_amount=0,
            unspent_amount=0,
            avg_sanction_delay_days=None,
            median_sanction_delay_days=None,
            avg_execution_days=None,
            median_execution_days=None,
            avg_project_age_days=None,
            max_project_age_days=None,
            avg_pending_days=None,
            max_pending_days=None,
            flagged_works=0,
            medium_risk_works=0,
            high_risk_works=0,
            flagged_rate_pct=0,
            high_risk_rate_pct=0,
            cost_anomaly_works=0,
            duration_anomaly_works=0,
            expenditure_over_sanction_works=0,
            expenditure_over_recommendation_works=0,
            negative_sanction_delay_works=0,
            negative_execution_works=0,
            completion_before_sanction_works=0,
            expenditure_before_sanction_works=0,
            overdue_over_1_year=0,
            overdue_over_2_years=0,
            avg_cost_percentile=None,
            avg_duration_percentile=None,
            avg_cost_deviation_pct=None,
            avg_duration_deviation_pct=None,
            completion_rate_pct=0,
            sanction_rate_pct=0,
            sanction_conversion_pct=0,
            expenditure_sanction_utilization_pct=0,
            expenditure_recommendation_pct=0,
            zero_work_member=True,
            low_sample_member=True,
            ranking_qualified=False,
        )

        # Attach context for evidence building
        m._master_record = ctx
        m._tenure_start = tenure_start
        m._tenure_end = tenure_end
        m._allocated_amount = allocated_amount
        m._house_name = house_name
        m._tenure_label = tenure_label

        zero_work_members.append(m)

    all_members = list(member_metrics) + zero_work_members

    # Count by type for reporting
    db1_mp_count = sum(1 for k in db1_member_map if k[0] == "MP")
    db1_mla_count = sum(1 for k in db1_member_map if k[0] == "MLA")

    print(f"  DB1 canonical population: {len(db1_member_map)} "
          f"({db1_mp_count} MPs + {db1_mla_count} MLAs)")
    print(f"  Zero-work members injected: {len(zero_work_members)}")
    print(f"  Total members after injection: {len(all_members)}")

    return all_members


def attach_master_records(member_metrics, snapshot_dir, verbose=True):
    """Attach the authoritative master-population record to members matched by
    name — provides allocated_amount + identity context (state_name, house,
    tenure) for WORK-BEARING members. Does NOT inject new members.

    This is the additive companion to inject_zero_work_members for the
    affected-only pipeline path, where zero-work injection is skipped but
    allocation/identity context is still required for accurate metrics.
    """
    master = discover_master_population(snapshot_dir)
    if not master["all_records"]:
        return member_metrics

    by_name = {}
    for r in master["all_records"]:
        name = r.get("member_name")
        if name:
            norm = normalize_member_name(name) or name
            if norm not in by_name:
                by_name[norm] = r

    attached = 0
    for m in member_metrics:
        mname = getattr(m, "member_name", None)
        if mname:
            norm = normalize_member_name(mname) or mname
        else:
            norm = None
        rec = by_name.get(norm) if norm else None
        if rec:
            if getattr(m, "_master_record", None) is None:
                m._master_record = rec
                attached += 1
            if not getattr(m, "state_name", None):
                m.state_name = rec.get("state_name")

    if verbose:
        print(f"  Master records attached to {attached} work-bearing members")
    return member_metrics


def get_master_population_context(member_metrics):
    """Extract master population context for evidence building.

    For zero-work members, returns the attached master population context.
    For work-bearing members, returns None (they get context from works).

    Args:
        member_metrics: list of MemberMetrics

    Returns:
        dict mapping (member_type, member_id) → master context dict or None
    """
    context_map = {}

    for m in member_metrics:
        master_record = getattr(m, '_master_record', None)
        if master_record and getattr(m, 'zero_work_member', False):
            context_map[(m.member_type, m.member_id)] = {
                "member_name": m.member_name,
                "member_type": m.member_type,
                "state_name": master_record.get("state_name", ""),
                "constituency": master_record.get("constituency", ""),
                "house_name": master_record.get("house_name", ""),
                "tenure": master_record.get("tenure", ""),
                "tenure_start_date": master_record.get("tenure_start_date"),
                "tenure_end_date": master_record.get("tenure_end_date"),
                "allocated_amount": master_record.get("allocated_amount"),
                "tenure_start": getattr(m, '_tenure_start', None),
                "tenure_end": getattr(m, '_tenure_end', None),
            }
        else:
            context_map[(m.member_type, m.member_id)] = None

    return context_map
