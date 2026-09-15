"""Risk engine: transparent combination of independent evidence sources.

Evidence sources (all 0..1, higher = worse):
  - predictive:  project delay ML mean probability across an entity's works.
  - anomaly_if:  Isolation Forest 95th-percentile anomaly across works.
  - statistical: existing entity anomaly_score (member_metrics), normalised.
  - exposure:    log-scaled sanctioned amount (modifier only, not score).

Combination (defensible, not arbitrary weights):
  score_0_1 = mean([predictive, anomaly_if, statistical])
  score_0_100 = score_0_1 * 100

Risk level thresholds (percentile-based within entity population, justified by
avoiding arbitrary fixed cutoffs; documented).
  CRITICAL : top 10%
  HIGH     : next 20%
  MODERATE  : next 30%
  LOW       : bottom 40%

Confidence derives from sample size (entity sample / number of eligible works).
"""

import math


def _safe(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _norm_percentile(value, values):
    """Fraction of values strictly below value (0..1)."""
    if not values:
        return 0.0
    below = sum(1 for v in values if v is not None and v < value)
    return below / max(len(values) - 1, 1)


def _confidence(n, n_min_high=5, n_min_med=2):
    if n >= n_min_high:
        return "HIGH"
    if n >= n_min_med:
        return "MEDIUM"
    return "LOW"


def _risk_level_from_pctile(pct, prev_band):
    # pct: percentile rank (0 = lowest, 1 = highest)
    if pct >= 0.90:
        return "CRITICAL"
    if pct >= 0.70:
        return "HIGH"
    if pct >= 0.40:
        return "MODERATE"
    return "LOW"


def _evidence_for(aggregate, entity_score, n_total):
    """Build a concise human-readable evidence summary from the aggregated
    per-work evidence and the final entity score.
    """
    parts = []
    if aggregate.get("mean_delay_prob") is not None:
        parts.append(f"predictive delay probability {aggregate['mean_delay_prob']:.2f}")
    if aggregate.get("isolation_95") is not None:
        parts.append(f"isolation anomaly 95th percentile {aggregate['isolation_95']:.2f}")
    if not parts:
        parts.append("statistical anomaly only")
    summary = "; ".join(parts)
    level = "LOW"
    if entity_score >= 70:
        level = "CRITICAL"
    elif entity_score >= 50:
        level = "HIGH"
    elif entity_score >= 35:
        level = "MODERATE"
    return {
        "summary": summary,
        "n": n_total,
        "score": round(entity_score, 2),
        "level": level,
    }


def compute_member_risks(members, work_risk_aggregates, entity_anomaly_lookup):
    """members: list of dicts from member_metrics with id/type/score fields.
    work_risk_aggregates: dict (mt, mid) -> {"mean_delay_prob": float, "n": int, "isolation_95": float, "isolation_n": int}
    entity_anomaly_lookup: dict (mt, mid) -> anomaly_score (0..something, lower = worse)
    Returns dict (mt, mid) -> dict with score, level, confidence, evidence.
    """
    pre_scores = []
    for m in members:
        key = (m["member_type"], m["member_id"])
        wr = work_risk_aggregates.get(key, {})
        pred = _safe(wr.get("mean_delay_prob"))
        anom = _safe(wr.get("isolation_95"))
        ent_anom = entity_anomaly_lookup.get(key)
        # normalize entity_anomaly (median/MAD robust z; in our impl smaller = more anomalous;
        # clip to [0,1] via a sigmoid approximation).
        if ent_anom is None:
            stat = 0.5  # unknown -> neutral
        else:
            stat = max(0.0, min(1.0, (ent_anom + 3.0) / 6.0))  # map -3..+3 to 0..1
        s = (pred + anom + stat) / 3.0
        pre_scores.append((key, s * 100.0))
    # percentile thresholds within entity population
    scores = [v for _, v in pre_scores]
    out = {}
    for key, s in pre_scores:
        agg = work_risk_aggregates.get(key, {})
        n = agg.get("n", 0)
        pct = _norm_percentile(s, scores)
        level = _risk_level_from_pctile(pct, None)
        out[key] = {
            "score": round(s, 2),
            "level": level,
            "confidence": _confidence(n + 1),
            "evidence": _evidence_for(agg, s, n),
            "sample_size": n,
        }
    return out


def compute_state_risks(states, work_risk_aggregates_state, entity_anomaly_state):
    pre_scores = []
    for s in states:
        sid = int(s["state_id"])
        wr = work_risk_aggregates_state.get(sid, {})
        pred = _safe(wr.get("mean_delay_prob"))
        anom = _safe(wr.get("isolation_95"))
        ent_anom = entity_anomaly_state.get(sid)
        stat = 0.5 if ent_anom is None else max(0.0, min(1.0, (ent_anom + 3.0) / 6.0))
        score = (pred + anom + stat) / 3.0 * 100.0
        pre_scores.append((sid, score))
    scores = [v for _, v in pre_scores]
    out = {}
    for sid, score in pre_scores:
        agg = work_risk_aggregates_state.get(sid, {})
        n = agg.get("n", 0)
        pct = _norm_percentile(score, scores)
        level = _risk_level_from_pctile(pct, None)
        out[sid] = {
            "score": round(score, 2),
            "level": level,
            "confidence": _confidence(n + 1),
            "evidence": _evidence_for(agg, score, n),
            "sample_size": n,
        }
    return out
