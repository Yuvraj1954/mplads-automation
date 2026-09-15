"""Isolation Forest anomaly detection per work.

Purpose: detect unusual observations (not accusations of fraud). The score
is normalised within the dataset; labels are derived from quantiles so the
thresholds are stable across populations.
"""

import os
import pickle

import numpy as np
from sklearn.ensemble import IsolationForest


ARTIFACT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "artifacts")


def _safe(v, default=np.nan):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


FEATURE_NAMES = ["sanction_amount", "recommended_amount", "expenditure_amount",
                 "sanction_delay_days", "execution_days", "project_age_days",
                 "cost_percentile", "duration_percentile"]


def _build_matrix(works):
    keys, rows = [], []
    for w in works:
        keys.append(w)
        rows.append([_safe(w.get(f)) for f in FEATURE_NAMES])
    return keys, np.array(rows, dtype=float) if rows else np.zeros((0, len(FEATURE_NAMES)))


def fit_and_score(works, random_state=42, contamination=0.05):
    """Fit IsolationForest on current works; return per-work scores [0..1]
    where higher = more anomalous, plus labels NORMAL/UNUSUAL/HIGHLY_UNUSUAL."""
    keys, X = _build_matrix(works)
    if len(keys) < 30:
        return []
    imputer = np.nanmedian(X, axis=0)
    X_filled = np.where(np.isnan(X), imputer, X)
    # Standardize (rough)
    sd = X_filled.std(axis=0)
    sd = np.where(sd == 0, 1.0, sd)
    X_std = (X_filled - X_filled.mean(axis=0)) / sd
    iso = IsolationForest(n_estimators=200, contamination=contamination,
                          random_state=random_state)
    iso.fit(X_std)
    raw = -iso.score_samples(X_std)  # higher = more anomalous
    lo, hi = float(np.percentile(raw, 50)), float(np.percentile(raw, 95))
    span = max(hi - lo, 1e-9)
    norm = np.clip((raw - lo) / span, 0.0, 1.0)
    out = []
    for k, s in zip(keys, norm):
        if s >= 0.66:   lvl = "HIGHLY_UNUSUAL"
        elif s >= 0.33: lvl = "UNUSUAL"
        else:           lvl = "NORMAL"
        out.append({"work_id": k.get("work_id"),
                    "member_type": k.get("member_type"),
                    "isolation_score": float(s),
                    "isolation_level": lvl})
    os.makedirs(ARTIFACT_DIR, exist_ok=True)
    art = os.path.join(ARTIFACT_DIR, "isolation_forest.pkl")
    with open(art, "wb") as f:
        pickle.dump({"model": iso, "feature_names": FEATURE_NAMES,
                     "imputer": imputer.tolist(),
                     "norm_lo": lo, "norm_hi": hi}, f)
    return out
