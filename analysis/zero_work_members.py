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
                             state_metrics_by_id=None,
                             works=None):
    """Inject zero-work members into MemberMetrics after normal analysis.

    Discovers the master population from allocated_limit snapshots and creates
    MemberMetrics for members not already present (zero-work members).

    Args:
        member_metrics: list of existing MemberMetrics from work analysis
        snapshot_dir: path to snapshot directory with allocated_limit data
        state_metrics_by_id: optional dict of state_id → StateMetrics
        works: optional list of raw work records (for member name mapping)

    Returns:
        list of all MemberMetrics (existing + injected zero-work members)
    """
    # Discover master population
    master = discover_master_population(snapshot_dir)
    if not master["all_records"]:
        return member_metrics

    # Build state map from master records
    state_name_to_id = _build_state_map(master["all_records"])

    # Build member name → (member_type, member_id) mapping from works
    # This allows matching master population members to work-derived metrics
    # Keyed by (member_type, name) so an MP and a Rajya Sabha member with the
    # same name can never collide (Phase 3 identity correctness).
    work_member_names = {}  # (member_type, name) -> (member_type, member_id)
    if works:
        for w in works:
            mp_name = w.get("mp_name", "")
            if mp_name:
                member_type = w.get("member_type", "MP")
                member_id = w.get("member_id")
                key = (member_type, mp_name)
                if key not in work_member_names:
                    work_member_names[key] = (member_type, member_id)

    # Build existing member IDs set
    existing_ids = {(m.member_type, m.member_id) for m in member_metrics}

    # ------------------------------------------------------------------
    # APPENDED: Attach the authoritative master-population record to
    # WORK-BEARING members too (matched by name). Previously only zero-work
    # members carried `_master_record`, so `allocated_amount`, state_name and
    # house/tenure context were 0/NULL for every member that actually had
    # works. This makes allocation + identity available for all members.
    # ------------------------------------------------------------------
    master_by_name = {}
    for record in master["all_records"]:
        name = record.get("member_name")
        if name and name not in master_by_name:
            master_by_name[name] = record
    for m in member_metrics:
        rec = master_by_name.get(getattr(m, "member_name", None))
        if rec:
            if getattr(m, "_master_record", None) is None:
                m._master_record = rec
            if not getattr(m, "state_name", None):
                m.state_name = rec.get("state_name")
    # ------------------------------------------------------------------

    # Build member name → ID mapping for zero-work members
    # Use existing IDs where members match by name
    # Separate id spaces per member_type so new zero-work MP ids never spill
    # into the Rajya Sabha id space (>=100000) and vice versa.
    def _max_id_for(mt):
        return max((m.member_id for m in member_metrics
                    if m.member_type == mt), default=0)

    next_id = {
        "MP": _max_id_for("MP") + 1,
        "MLA": max(_max_id_for("MLA") + 1, 100000),
    }

    name_to_id = {}
    # First, map work-derived member names to their IDs
    for (mt, name), (_mt, mid) in work_member_names.items():
        name_to_id[(mt, name)] = mid

    # Then, assign new IDs to zero-work members not in works
    for record in master["all_records"]:
        mt = record["member_type"]
        key = (mt, record["member_name"])
        if key not in name_to_id:
            name_to_id[key] = next_id[mt]
            next_id[mt] += 1

    # Create zero-work MemberMetrics
    zero_work_members = []

    for record in master["all_records"]:
        member_name = record["member_name"]
        member_type = record["member_type"]

        # Check if member already exists in work-derived metrics
        # Match by name -> work_member_names mapping
        wk_key = (member_type, member_name)
        if wk_key in work_member_names:
            mapped_type, mapped_id = work_member_names[wk_key]
            if (mapped_type, mapped_id) in existing_ids:
                continue

        # Assign member_id (typed name map)
        member_id = name_to_id[wk_key]

        # Get state_id
        state_name = record.get("state_name", "")
        state_id = state_name_to_id.get(state_name)

        # Parse tenure dates for context
        tenure_start = parse_date(record.get("tenure_start_date"))
        tenure_end = parse_date(record.get("tenure_end_date"))

        # Create MemberMetrics with zero works
        m = MemberMetrics(
            member_id=member_id,
            member_type=member_type,
            member_name=member_name,
            state_id=state_id,
            constituency_id=None,  # Not available in snapshot
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

        # Attach master population context as extra attributes
        # These preserve reliable identity/context for evidence building
        m._master_record = record
        m._tenure_start = tenure_start
        m._tenure_end = tenure_end
        m._allocated_amount = record.get("allocated_amount")
        m._house_name = record.get("house_name", "")
        m._tenure_label = record.get("tenure", "")

        zero_work_members.append(m)

    all_members = list(member_metrics) + zero_work_members

    print(f"  Master population discovered: {master['total_count']}")
    print(f"  MP: {master['mp_count']}, MLA: {master['mla_count']}")
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
        if name and name not in by_name:
            by_name[name] = r

    attached = 0
    for m in member_metrics:
        rec = by_name.get(getattr(m, "member_name", None))
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
