"""AffectedScope — Single authoritative object for pipeline change tracking.

Every downstream stage consumes this scope instead of independently
rediscovering affected entities.

Design:
    AffectedScope is built ONCE from the delta files and DB1 lookups.
    It resolves child-table changes → parent work → member → state.
    All downstream stages read from this object.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple


@dataclass
class AffectedScope:
    """Authoritative affected-scope for a pipeline run.

    Attributes:
        changed_tables: Set of source table names that had changes.
        change_types: Per-table change type (append, update, mixed).
        work_ids: MP work IDs affected (snapshot work_id format).
        mla_work_ids: MLA work IDs affected.
        member_keys: (member_type, member_id) pairs affected.
        state_ids: State IDs affected.
        full_rebuild_required: If True, skip incremental optimization.
        has_changes: If False, pipeline should fast-exit.
        delta_record_count: Total records in the delta.
        affected_work_count: Resolved affected work count.
        affected_member_count: Resolved affected member count.
        affected_state_count: Resolved affected state count.
    """
    changed_tables: Set[str] = field(default_factory=set)
    change_types: Dict[str, str] = field(default_factory=dict)
    work_ids: Set[int] = field(default_factory=set)
    mla_work_ids: Set[int] = field(default_factory=set)
    member_keys: Set[Tuple[str, int]] = field(default_factory=set)
    state_ids: Set[int] = field(default_factory=set)
    full_rebuild_required: bool = False
    has_changes: bool = False
    delta_record_count: int = 0
    affected_work_count: int = 0
    affected_member_count: int = 0
    affected_state_count: int = 0

    @property
    def mp_work_ids(self) -> Set[int]:
        return self.work_ids

    @property
    def all_work_ids(self) -> Set[int]:
        return self.work_ids | self.mla_work_ids

    @property
    def mp_member_keys(self) -> Set[Tuple[str, int]]:
        return {k for k in self.member_keys if k[0] == "MP"}

    @property
    def mla_member_keys(self) -> Set[Tuple[str, int]]:
        return {k for k in self.member_keys if k[0] == "MLA"}

    def is_empty(self) -> bool:
        return not self.has_changes or self.affected_work_count == 0

    def summary(self) -> Dict:
        return {
            "has_changes": self.has_changes,
            "changed_tables": sorted(self.changed_tables),
            "delta_records": self.delta_record_count,
            "affected_works": self.affected_work_count,
            "affected_members": self.affected_member_count,
            "affected_states": self.affected_state_count,
            "full_rebuild_required": self.full_rebuild_required,
        }


# ---------------------------------------------------------------------------
# Child-table → Parent work resolution
# ---------------------------------------------------------------------------

# Maps child table names to the column that holds the parent work DTL_ID.
_CHILD_TO_WORK_COLUMN = {
    "works_recommended": "WORK_RECOMMENDATION_DTL_ID",
    "works_sanctioned": "WORK_RECOMMENDATION_DTL_ID",
    "works_completed": "WORK_RECOMMENDATION_DTL_ID",
    "expenditure": "WORK_RECOMMENDATION_DTL_ID",
    "mla_works_recommended": "WORK_RECOMMENDATION_DTL_ID",
    "mla_works_sanctioned": "WORK_RECOMMENDATION_DTL_ID",
    "mla_works_completed": "WORK_RECOMMENDATION_DTL_ID",
    "mla_expenditure": "WORK_RECOMMENDATION_DTL_ID",
}

# Tables that ARE the parent work record.
_PARENT_WORK_TABLES = {"works_recommended", "mla_works_recommended"}

# Tables that don't map to a specific work (skip propagation).
_SKIP_PROPAGATION = {"allocated_limit", "calamity", "mla_allocated_limit", "mla_calamity"}


def resolve_delta_to_work_ids(
    delta_records: List[Dict],
    changed_tables: Set[str],
) -> Tuple[Set[int], Set[int]]:
    """Resolve delta records to parent work IDs.

    For parent work tables (works_recommended, mla_works_recommended):
        The DTL_ID in the record IS the work ID.

    For child tables (expenditure, works_sanctioned, works_completed):
        The WORK_RECOMMENDATION_DTL_ID maps to the parent work.

    Returns:
        (mp_work_ids, mla_work_ids) — snapshot-format work IDs
        (MP: raw DTL_ID, MLA: DTL_ID + 1_000_000)
    """
    mp_work_ids: Set[int] = set()
    mla_work_ids: Set[int] = set()

    for table in changed_tables:
        if table in _SKIP_PROPAGATION:
            continue

        is_mla = table.startswith("mla_")
        dtl_col = _CHILD_TO_WORK_COLUMN.get(table, "WORK_RECOMMENDATION_DTL_ID")

        for rec in delta_records:
            if rec.get("_table") != table:
                continue

            dtl_id = rec.get(dtl_col) or rec.get("WORK_RECOMMENDATION_DTL_ID")
            if dtl_id is None:
                continue

            dtl_id = int(dtl_id)
            if is_mla:
                mla_work_ids.add(dtl_id + 1_000_000)
            else:
                mp_work_ids.add(dtl_id)

    return mp_work_ids, mla_work_ids


def build_affected_scope_from_delta(
    delta_records: List[Dict],
    changed_tables: Set[str],
) -> AffectedScope:
    """Build an AffectedScope from raw delta records.

    This is the entry point for constructing the scope.
    It resolves child-table changes to parent work IDs.
    """
    scope = AffectedScope()
    scope.changed_tables = changed_tables
    scope.delta_record_count = len(delta_records)
    scope.has_changes = len(delta_records) > 0

    if not scope.has_changes:
        return scope

    # Classify change types per table
    for table in changed_tables:
        table_recs = [r for r in delta_records if r.get("_table") == table]
        has_append = any(r.get("_op") == "append" for r in table_recs)
        has_update = any(r.get("_op") == "update" for r in table_recs)
        if has_append and has_update:
            scope.change_types[table] = "mixed"
        elif has_append:
            scope.change_types[table] = "append"
        else:
            scope.change_types[table] = "update"

    # Resolve to work IDs
    mp_ids, mla_ids = resolve_delta_to_work_ids(delta_records, changed_tables)
    scope.work_ids = mp_ids
    scope.mla_work_ids = mla_ids
    scope.affected_work_count = len(mp_ids) + len(mla_ids)

    return scope


def resolve_members_and_states(
    scope: AffectedScope,
    works_by_id: Dict[int, Dict],
) -> None:
    """Resolve affected work IDs → member keys → state IDs.

    Mutates `scope` in-place to populate member_keys and state_ids.

    Args:
        scope: The affected scope (work_ids must already be populated).
        works_by_id: Dict mapping work_id → work record dict.
            Must contain 'member_type', 'member_id', 'state_id'.
    """
    for wid in scope.all_work_ids:
        w = works_by_id.get(wid)
        if w is None:
            continue
        mt = w.get("member_type")
        mid = w.get("member_id")
        sid = w.get("state_id")
        if mt and mid is not None:
            scope.member_keys.add((mt, int(mid)))
        if sid is not None:
            scope.state_ids.add(int(sid))

    scope.affected_member_count = len(scope.member_keys)
    scope.affected_state_count = len(scope.state_ids)


def classify_calculation_scope() -> Dict[str, str]:
    """Classify every pipeline calculation as LOCAL/ENTITY/GLOBAL.

    Returns dict mapping calculation name to scope type.
    """
    return {
        # LOCAL — recomputed from affected records only
        "work_analysis": "LOCAL",
        "work_risk_flags": "LOCAL",
        "work_benchmarks": "LOCAL",  # depends on global percentiles though

        # ENTITY — member/state-specific
        "member_metrics": "ENTITY",
        "state_metrics": "ENTITY",
        "member_performance": "ENTITY",
        "state_performance": "ENTITY",
        "member_risk": "ENTITY",
        "state_risk": "ENTITY",
        "anomaly_score": "ENTITY",

        # GLOBAL — depends on entire population
        "national_totals": "GLOBAL",
        "national_ranking": "GLOBAL",
        "peer_ranking": "GLOBAL",
        "percentile": "GLOBAL",
        "national_statistics": "GLOBAL",
        "overall_metrics": "GLOBAL",
        "trends": "GLOBAL",  # yearly aggregation
        "category_metrics": "GLOBAL",  # depends on all works
        "fy_metrics": "GLOBAL",
        "clustering": "GLOBAL",  # K-Means needs full population
        "isolation_forest": "GLOBAL",  # needs full population for contamination
        "intelligence_assembly": "GLOBAL",  # needs ranking/percentile
        "evidence": "ENTITY",
        "ai_analysis": "ENTITY",
    }


# Which GLOBAL calculations MUST run when ANY work changes?
# These cannot be skipped even for small deltas.
_ALWAYS_GLOBAL = {
    "national_totals",
    "national_ranking",
    "peer_ranking",
    "percentile",
    "national_statistics",
    "overall_metrics",
    "intelligence_assembly",
}

# Which GLOBAL calculations can be SKIPPED if only specific tables changed?
# e.g., if only expenditure changed, trends might not need full rebuild.
_CONDITIONAL_GLOBAL = {
    "trends",  # needs rebuild if works changed
    "category_metrics",  # needs rebuild if works changed
    "fy_metrics",  # needs rebuild if works changed
    "clustering",  # needs rebuild if member_metrics changed significantly
    "isolation_forest",  # needs rebuild if work features changed
}


def should_run_global_calc(
    calc_name: str,
    scope: AffectedScope,
) -> bool:
    """Determine if a global calculation needs to run.

    Returns True if the calculation should execute.
    """
    if calc_name in _ALWAYS_GLOBAL:
        return True

    if calc_name in _CONDITIONAL_GLOBAL:
        # If any work changed, these need rebuild
        if scope.affected_work_count > 0:
            return True
        return False

    return False
