from datetime import date, datetime, timezone
from dataclasses import dataclass, field
from typing import Optional
import numpy as np


SYSTEM_VERSION = "entity_anomaly_v1"

FEATURE_DEFINITIONS = [
    {
        "name": "flagged_rate_pct",
        "source_field": "flagged_rate_pct",
        "direction": "HIGH",
        "description": "Flagged-work rate is substantially above the comparable entity median.",
        "entity_types": ["member"],
    },
    {
        "name": "high_risk_rate_pct",
        "source_field": "high_risk_rate_pct",
        "direction": "HIGH",
        "description": "High-risk work rate is substantially above the comparable entity median.",
        "entity_types": ["member"],
    },
    {
        "name": "completion_rate_pct",
        "source_field": "completion_rate_pct",
        "direction": "LOW",
        "description": "Completion rate is substantially below the comparable entity median.",
        "entity_types": ["member"],
    },
    {
        "name": "expenditure_utilization_pct",
        "source_field": "expenditure_sanction_utilization_pct",
        "direction": "LOW",
        "description": "Expenditure utilization is substantially below the comparable entity median.",
        "entity_types": ["member"],
    },
    {
        "name": "avg_sanction_delay_days",
        "source_field": "avg_sanction_delay_days",
        "direction": "HIGH",
        "description": "Average sanction delay is substantially above the comparable entity median.",
        "entity_types": ["member"],
    },
    {
        "name": "overdue_rate",
        "source_field": "overdue_over_1_year",
        "rate_base": "total_works",
        "direction": "HIGH",
        "description": "Rate of works overdue over 1 year is substantially above the comparable entity median.",
        "entity_types": ["member"],
    },
    {
        "name": "cost_anomaly_rate",
        "source_field": "cost_anomaly_works",
        "rate_base": "total_works",
        "direction": "HIGH",
        "description": "Rate of cost-anomalous works is substantially above the comparable entity median.",
        "entity_types": ["member"],
    },
    {
        "name": "duration_anomaly_rate",
        "source_field": "duration_anomaly_works",
        "rate_base": "total_works",
        "direction": "HIGH",
        "description": "Rate of duration-anomalous works is substantially above the comparable entity median.",
        "entity_types": ["member"],
    },
    {
        "name": "avg_cost_percentile",
        "source_field": "avg_cost_percentile",
        "direction": "HIGH",
        "description": "Average cost percentile is substantially above the comparable entity median.",
        "entity_types": ["member"],
    },
    {
        "name": "expenditure_over_sanction_rate",
        "source_field": "expenditure_over_sanction_works",
        "rate_base": "total_works",
        "direction": "HIGH",
        "description": "Rate of works exceeding sanctioned amount is substantially above the comparable entity median.",
        "entity_types": ["member"],
    },
    {
        "name": "negative_delay_rate",
        "source_field": "negative_sanction_delay_works",
        "rate_base": "total_works",
        "direction": "HIGH",
        "description": "Rate of works with negative sanction delay is substantially above the comparable entity median.",
        "entity_types": ["member"],
    },
    {
        "name": "avg_project_age_days",
        "source_field": "avg_project_age_days",
        "direction": "BOTH",
        "description": "Average project age deviates substantially from the comparable entity median.",
        "entity_types": ["member"],
    },
]

CONSISTENCY_CONSTANT = 0.6745
CLIP_MIN = -5.0
CLIP_MAX = 5.0
Z_ANOMALY_THRESHOLD = 2.0
COMPOSITE_THRESHOLD_HIGH_PERCENTILE = 95
COMPOSITE_THRESHOLD_MEDIUM_PERCENTILE = 80

CONF_THRESHOLD_HIGH = 50
CONF_THRESHOLD_MEDIUM = 20


@dataclass
class EntityAnomalyResult:
    entity_id: int
    entity_type: str
    member_type: Optional[str] = None
    state_id: Optional[int] = None
    anomaly_score: float = 0.0
    anomaly_level: str = "NORMAL"
    confidence_level: str = "LOW"
    contributing_features: list = field(default_factory=list)
    supporting_metrics: dict = field(default_factory=dict)
    system_version: str = SYSTEM_VERSION
    calculated_at: Optional[datetime] = None
    reference_date: Optional[date] = None


def robust_z_score(values):
    arr = np.array(values, dtype=float)
    arr = arr[~np.isnan(arr)]
    if len(arr) == 0:
        return np.array([])
    median = np.median(arr)
    mad = np.median(np.abs(arr - median))
    if mad == 0:
        return np.zeros(len(values))
    z = CONSISTENCY_CONSTANT * (np.array(values) - median) / mad
    return np.clip(z, CLIP_MIN, CLIP_MAX)


def _extract_feature_value(entity, feature_def):
    source = feature_def.get("source_field")
    rate_base = feature_def.get("rate_base")
    raw = getattr(entity, source, None)
    if raw is None:
        return None
    if rate_base is not None:
        base_val = getattr(entity, rate_base, None)
        if base_val is None or base_val == 0:
            return None
        return (raw / base_val) * 100
    return float(raw)


def _compute_population_zscores(entities, feature_def, entity_type_filter):
    values = []
    valid_entities = []
    for e in entities:
        if entity_type_filter and getattr(e, "member_type", None) != entity_type_filter:
            continue
        val = _extract_feature_value(e, feature_def)
        if val is not None:
            values.append(val)
            valid_entities.append(e)

    if len(values) < 5:
        return {}

    z_array = robust_z_score(values)
    result = {}
    for e, z in zip(valid_entities, z_array):
        key = (e.member_type, e.member_id)
        result[key] = float(z)
    return result


def _compute_composite(feature_zscores, feature_defs):
    contributions = []
    for fdef in feature_defs:
        fname = fdef["name"]
        direction = fdef["direction"]
        z = feature_zscores.get(fname)
        if z is None:
            continue
        if direction == "HIGH":
            contributions.append(max(0.0, z))
        elif direction == "LOW":
            contributions.append(max(0.0, -z))
        elif direction == "BOTH":
            contributions.append(abs(z))
    if not contributions:
        return 0.0
    return float(np.mean(contributions))


def _classify_level(composite_score, high_threshold, medium_threshold):
    if composite_score > high_threshold:
        return "HIGH"
    if composite_score > medium_threshold:
        return "MEDIUM"
    return "NORMAL"


def _classify_confidence(total_works):
    if total_works >= CONF_THRESHOLD_HIGH:
        return "HIGH"
    if total_works >= CONF_THRESHOLD_MEDIUM:
        return "MEDIUM"
    return "LOW"


def _build_explanations(feature_zscores, feature_defs, feature_values):
    explanations = []
    for fdef in feature_defs:
        fname = fdef["name"]
        z = feature_zscores.get(fname)
        if z is None:
            continue
        if abs(z) < Z_ANOMALY_THRESHOLD:
            continue
        direction_label = "HIGH" if z > 0 else "LOW"
        val = feature_values.get(fname)
        explanations.append({
            "feature": fname,
            "feature_value": round(val, 2) if val is not None else None,
            "z_score": round(z, 2),
            "direction": direction_label,
            "description": fdef["description"],
        })
    explanations.sort(key=lambda x: -abs(x["z_score"]))
    return explanations[:5]


def _build_supporting_metrics(entity):
    return {
        "total_works": getattr(entity, "total_works", 0),
        "flagged_rate_pct": getattr(entity, "flagged_rate_pct", None),
        "high_risk_rate_pct": getattr(entity, "high_risk_rate_pct", None),
        "completion_rate_pct": getattr(entity, "completion_rate_pct", None),
        "expenditure_utilization_pct": getattr(
            entity, "expenditure_sanction_utilization_pct", None
        ),
        "avg_sanction_delay_days": getattr(entity, "avg_sanction_delay_days", None),
        "overdue_rate": (
            round(getattr(entity, "overdue_over_1_year", 0) / max(getattr(entity, "total_works", 1), 1) * 100, 2)
            if getattr(entity, "total_works", 0) > 0 else None
        ),
        "cost_anomaly_rate": (
            round(getattr(entity, "cost_anomaly_works", 0) / max(getattr(entity, "total_works", 1), 1) * 100, 2)
            if getattr(entity, "total_works", 0) > 0 else None
        ),
        "duration_anomaly_rate": (
            round(getattr(entity, "duration_anomaly_works", 0) / max(getattr(entity, "total_works", 1), 1) * 100, 2)
            if getattr(entity, "total_works", 0) > 0 else None
        ),
        "avg_cost_percentile": getattr(entity, "avg_cost_percentile", None),
    }


def compute_entity_anomalies(member_metrics, reference_date=None):
    if reference_date is None:
        reference_date = date.today()

    qualified = [m for m in member_metrics if m.ranking_qualified]
    mp_qualified = [m for m in qualified if m.member_type == "MP"]
    mla_qualified = [m for m in qualified if m.member_type == "MLA"]

    mp_features = [
        f for f in FEATURE_DEFINITIONS if "member" in f["entity_types"]
    ]

    population_zscores = {}

    for fdef in mp_features:
        mp_z = _compute_population_zscores(mp_qualified, fdef, "MP")
        for key, z in mp_z.items():
            if key not in population_zscores:
                population_zscores[key] = {}
            population_zscores[key][fdef["name"]] = z

        mla_z = _compute_population_zscores(mla_qualified, fdef, "MLA")
        for key, z in mla_z.items():
            if key not in population_zscores:
                population_zscores[key] = {}
            population_zscores[key][fdef["name"]] = z

    all_composites = []
    entity_results = {}

    for entity in qualified:
        key = (entity.member_type, entity.member_id)
        feature_zs = population_zscores.get(key, {})

        feature_vals = {}
        for fdef in mp_features:
            val = _extract_feature_value(entity, fdef)
            if val is not None:
                feature_vals[fdef["name"]] = val

        composite = _compute_composite(feature_zs, mp_features)
        all_composites.append(composite)
        entity_results[key] = {
            "entity": entity,
            "feature_zscores": feature_zs,
            "feature_values": feature_vals,
            "composite": composite,
        }

    if not all_composites:
        return []

    composite_arr = np.array(all_composites)
    high_threshold = float(np.percentile(
        composite_arr, COMPOSITE_THRESHOLD_HIGH_PERCENTILE
    ))
    medium_threshold = float(np.percentile(
        composite_arr, COMPOSITE_THRESHOLD_MEDIUM_PERCENTILE
    ))

    results = []
    for key, data in entity_results.items():
        entity = data["entity"]
        composite = data["composite"]
        feature_zs = data["feature_zscores"]
        feature_vals = data["feature_values"]

        level = _classify_level(composite, high_threshold, medium_threshold)
        confidence = _classify_confidence(entity.total_works)
        explanations = _build_explanations(feature_zs, mp_features, feature_vals)
        supporting = _build_supporting_metrics(entity)

        results.append(EntityAnomalyResult(
            entity_id=entity.member_id,
            entity_type="member",
            member_type=entity.member_type,
            state_id=entity.state_id,
            anomaly_score=round(composite, 4),
            anomaly_level=level,
            confidence_level=confidence,
            contributing_features=explanations,
            supporting_metrics=supporting,
            system_version=SYSTEM_VERSION,
            calculated_at=datetime.now(timezone.utc),
            reference_date=reference_date,
        ))

    return results


def compute_state_anomalies(state_metrics, reference_date=None):
    if reference_date is None:
        reference_date = date.today()

    qualified = [s for s in state_metrics if s.ranking_qualified]

    state_features = [
        {
            "name": "risk_rate",
            "source_field": "risk_rate_pct",
            "direction": "HIGH",
            "description": "Risk rate is substantially above the state median.",
        },
        {
            "name": "completion_rate",
            "source_field": "completion_rate_pct",
            "direction": "LOW",
            "description": "Completion rate is substantially below the state median.",
        },
        {
            "name": "expenditure_utilization",
            "source_field": "expenditure_utilization_pct",
            "direction": "LOW",
            "description": "Expenditure utilization is substantially below the state median.",
        },
        {
            "name": "avg_sanction_delay",
            "source_field": "avg_sanction_delay_days",
            "direction": "HIGH",
            "description": "Average sanction delay is substantially above the state median.",
        },
        {
            "name": "cost_anomaly_rate",
            "source_field": "cost_anomaly_works",
            "rate_base": "total_works",
            "direction": "HIGH",
            "description": "Rate of cost-anomalous works is substantially above the state median.",
        },
        {
            "name": "duration_anomaly_rate",
            "source_field": "duration_anomaly_works",
            "rate_base": "total_works",
            "direction": "HIGH",
            "description": "Rate of duration-anomalous works is substantially above the state median.",
        },
        {
            "name": "overdue_rate",
            "source_field": "overdue_over_1_year",
            "rate_base": "total_works",
            "direction": "HIGH",
            "description": "Rate of works overdue over 1 year is substantially above the state median.",
        },
    ]

    population_zscores = {}
    for fdef in state_features:
        z_map = _compute_population_zscores_state(qualified, fdef)
        for state_id, z in z_map.items():
            if state_id not in population_zscores:
                population_zscores[state_id] = {}
            population_zscores[state_id][fdef["name"]] = z

    all_composites = []
    entity_results = {}

    for state in qualified:
        feature_zs = population_zscores.get(state.state_id, {})

        feature_vals = {}
        for fdef in state_features:
            val = _extract_feature_value(state, fdef)
            if val is not None:
                feature_vals[fdef["name"]] = val

        composite = _compute_composite(feature_zs, state_features)
        all_composites.append(composite)
        entity_results[state.state_id] = {
            "state": state,
            "feature_zscores": feature_zs,
            "feature_values": feature_vals,
            "composite": composite,
        }

    if not all_composites:
        return []

    composite_arr = np.array(all_composites)
    high_threshold = float(np.percentile(
        composite_arr, COMPOSITE_THRESHOLD_HIGH_PERCENTILE
    ))
    medium_threshold = float(np.percentile(
        composite_arr, COMPOSITE_THRESHOLD_MEDIUM_PERCENTILE
    ))

    results = []
    for state_id, data in entity_results.items():
        state = data["state"]
        composite = data["composite"]
        feature_zs = data["feature_zscores"]
        feature_vals = data["feature_values"]

        total = state.total_works
        if total >= 100:
            confidence = "HIGH"
        elif total >= 50:
            confidence = "MEDIUM"
        else:
            confidence = "LOW"

        level = _classify_level(composite, high_threshold, medium_threshold)
        explanations = _build_explanations(feature_zs, state_features, feature_vals)
        supporting = {
            "total_works": state.total_works,
            "active_members": state.active_members,
            "risk_rate_pct": state.risk_rate_pct,
            "completion_rate_pct": state.completion_rate_pct,
            "expenditure_utilization_pct": state.expenditure_utilization_pct,
            "avg_sanction_delay_days": state.avg_sanction_delay_days,
        }

        results.append(EntityAnomalyResult(
            entity_id=state_id,
            entity_type="state",
            state_id=state_id,
            anomaly_score=round(composite, 4),
            anomaly_level=level,
            confidence_level=confidence,
            contributing_features=explanations,
            supporting_metrics=supporting,
            system_version=SYSTEM_VERSION,
            calculated_at=datetime.now(timezone.utc),
            reference_date=reference_date,
        ))

    return results


def _compute_population_zscores_state(states, feature_def):
    values = []
    valid_states = []
    for s in states:
        val = _extract_feature_value(s, feature_def)
        if val is not None:
            values.append(val)
            valid_states.append(s)

    if len(values) < 5:
        return {}

    z_array = robust_z_score(values)
    result = {}
    for s, z in zip(valid_states, z_array):
        result[s.state_id] = float(z)
    return result
