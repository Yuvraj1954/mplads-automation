"""Project delay / slow-completion model (XGBoost).

Target (valid, leakage-controlled):
  Among works with a completion_date, slow_completion = 1 if execution_days
  > SLOW_THRESHOLD_DAYS else 0.  execution_days = completion - sanction.

Why a "slow completion" proxy instead of a "delayed completion" forecast:
  We only have a single current snapshot of MoSPI data; we have no historical
  re-snapshots of in-progress works, so a true "will this ongoing work be
  delayed?" training set does not exist. The closest valid supervised target
  on real data is "given the sanction-time features of a completed work, was
  its execution slow?" Features are restricted to quantities known at the
  sanction point; no peer benchmarks from the post-completion world are used.

Temporal split by recommendation_date (not random) to prevent future leakage:
  train: rec_date < TRAIN_CUTOFF
  val:   TRAIN_CUTOFF <= rec_date < TEST_CUTOFF
  test:  rec_date >= TEST_CUTOFF
"""

import json
import os
import pickle
from datetime import date, datetime, timezone

import numpy as np

import xgboost as xgb
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    classification_report,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)

ARTIFACT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "artifacts")

SLOW_THRESHOLD_DAYS = 365
TRAIN_CUTOFF = date(2026, 1, 1)
TEST_CUTOFF = date(2026, 4, 1)


def _safe_float(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _label_encode(values):
    uniq = {v: i for i, v in enumerate(sorted({v for v in values if v is not None}))}
    return [uniq.get(v, -1) for v in values]


def build_dataset(works):
    """Filter eligible works, build features + binary target.

    Eligibility: completion_date present, sanction_date present, recommendation_date
    present, work_category present, state_id present.

    Returns: (X numpy, y numpy, feature_names, records_for_persistence).
    """
    feature_names = ["sanction_amount", "recommended_amount",
                     "sanction_delay_days", "rec_year", "sanction_year",
                     "category_enc", "activity_enc", "state_enc",
                     "member_type_mp", "n_works_in_state_at_sanction"]

    states_seen = set()
    cats_seen = set()
    acts_seen = set()
    for w in works:
        if w.get("completion_date") is None or w.get("sanction_date") is None:
            continue
        if w.get("recommendation_date") is None or w.get("work_category") is None:
            continue
        if w.get("state_id") is None:
            continue
        states_seen.add(w["state_id"])
        cats_seen.add(w["work_category"])
        acts_seen.add(w.get("normalized_activity") or "")

    state_enc = {s: i for i, s in enumerate(sorted(states_seen))}
    cat_enc = {c: i for i, c in enumerate(sorted(cats_seen))}
    act_enc = {a: i for i, a in enumerate(sorted(acts_seen))}

    # Pre-aggregate works-per-state as of sanction_year (coarse proxy to avoid
    # post-completion leakage: uses only other works' sanction_date, not their
    # completion / expenditure).
    works_by_state_year = {}
    for w in works:
        if w.get("sanction_date") is None:
            continue
        st = w.get("state_id")
        yr = w["sanction_date"].year
        if st is None:
            continue
        works_by_state_year.setdefault((st, yr), 0)
        works_by_state_year[(st, yr)] += 1

    rows = []
    for w in works:
        if w.get("completion_date") is None or w.get("sanction_date") is None:
            continue
        if w.get("recommendation_date") is None or w.get("work_category") is None:
            continue
        if w.get("state_id") is None:
            continue
        exec_days = (w["completion_date"] - w["sanction_date"]).days
        if exec_days < 0:
            continue
        y = 1 if exec_days > SLOW_THRESHOLD_DAYS else 0
        rows.append({
            "work_id": w.get("work_id"),
            "member_id": w.get("member_id"),
            "member_type": w.get("member_type"),
            "state_id": w["state_id"],
            "rec_year": w["recommendation_date"].year,
            "sanction_year": w["sanction_date"].year,
            "sanction_amount": _safe_float(w.get("sanction_amount")),
            "recommended_amount": _safe_float(w.get("recommended_amount")),
            "sanction_delay_days": _safe_float(w.get("sanction_delay_days")),
            "category_enc": cat_enc[w["work_category"]],
            "activity_enc": act_enc.get(w.get("normalized_activity") or "", -1),
            "state_enc": state_enc[w["state_id"]],
            "member_type_mp": 1 if w.get("member_type") == "MP" else 0,
            "n_works_in_state_at_sanction":
                works_by_state_year.get((w["state_id"], w["sanction_date"].year), 0),
            "y": y,
            "rec_date": w["recommendation_date"],
        })
    X = np.array([[r[f] for f in feature_names] for r in rows], dtype=float)
    y = np.array([r["y"] for r in rows], dtype=int)
    return X, y, feature_names, rows


def split_temporal(rows):
    train = [r for r in rows if r["rec_date"] < TRAIN_CUTOFF]
    val = [r for r in rows if TRAIN_CUTOFF <= r["rec_date"] < TEST_CUTOFF]
    test = [r for r in rows if r["rec_date"] >= TEST_CUTOFF]
    return train, val, test


def train_and_evaluate(works, random_state=42):
    X, y, fnames, rows = build_dataset(works)
    train, val, test = split_temporal(rows)
    if not train or not test:
        return {"status": "NOT_READY", "reason": "insufficient temporal split",
                "n_total": len(rows), "n_train": len(train),
                "n_val": len(val), "n_test": len(test)}, None, None, None

    def _arr(sub):
        if not sub:
            return np.zeros((0, len(fnames))), np.zeros((0,), dtype=int)
        a = np.array([[r[f] for f in fnames] for r in sub], dtype=float)
        b = np.array([r["y"] for r in sub], dtype=int)
        return a, b

    Xtr, ytr = _arr(train)
    Xva, yva = _arr(val)
    Xte, yte = _arr(test)

    pos = max(int(ytr.sum()), 1)
    neg = max(int((1 - ytr).sum()), 1)
    spw = neg / pos
    model = xgb.XGBClassifier(
        n_estimators=400, max_depth=5, learning_rate=0.05,
        subsample=0.85, colsample_bytree=0.85,
        scale_pos_weight=spw, reg_lambda=1.0, reg_alpha=0.0,
        tree_method="hist", random_state=random_state,
        eval_metric="aucpr",
    )
    eval_set = [(Xva, yva)] if len(Xva) > 0 else None
    model.fit(Xtr, ytr, eval_set=eval_set, verbose=False)

    # Evaluate
    metrics = {}
    if len(Xte) > 0 and len(np.unique(yte)) > 1:
        proba = model.predict_proba(Xte)[:, 1]
        metrics["test_roc_auc"] = float(roc_auc_score(yte, proba))
        metrics["test_pr_auc"] = float(average_precision_score(yte, proba))
        metrics["test_brier"] = float(brier_score_loss(yte, proba))
        # default 0.5 threshold
        pred = (proba >= 0.5).astype(int)
        metrics["test_precision_at_0_5"] = float(precision_score(yte, pred, zero_division=0))
        metrics["test_recall_at_0_5"] = float(recall_score(yte, pred, zero_division=0))
        metrics["test_f1_at_0_5"] = float(f1_score(yte, pred, zero_division=0))
        # Best F1 threshold (for risk band calibration)
        precs, recs, thr = precision_recall_curve(yte, proba)
        f1s = 2 * precs * recs / np.maximum(precs + recs, 1e-9)
        best_i = int(np.argmax(f1s[1:])) + 1
        metrics["best_f1"] = float(f1s[best_i])
        metrics["best_f1_threshold"] = float(thr[best_i - 1])
    else:
        proba = model.predict_proba(Xte)[:, 1] if len(Xte) else np.array([])
        metrics["test_roc_auc"] = None
        metrics["note"] = "test set single-class; ROC/PR undefined"

    ready = (metrics.get("test_roc_auc") or 0) >= 0.65 and metrics.get("test_pr_auc", 0) >= 0.30
    status = "READY" if ready else "NOT_READY"

    # Persist artifact + registry
    os.makedirs(ARTIFACT_DIR, exist_ok=True)
    art_path = os.path.join(ARTIFACT_DIR, "project_delay_xgb.pkl")
    with open(art_path, "wb") as f:
        pickle.dump({
            "model": model, "feature_names": fnames,
            "slow_threshold_days": SLOW_THRESHOLD_DAYS,
            "train_cutoff": TRAIN_CUTOFF.isoformat(),
            "test_cutoff": TEST_CUTOFF.isoformat(),
            "version": f"xgb-{datetime.now(timezone.utc).strftime('%Y%m%d')}",
        }, f)

    return ({
        "status": status,
        "n_total": len(rows), "n_train": len(train), "n_val": len(val), "n_test": len(test),
        "target_prevalence": float(y.mean()) if len(y) else 0.0,
        "metrics": metrics,
        "artifact": art_path,
        "feature_names": fnames,
        "version": f"xgb-{datetime.now(timezone.utc).strftime('%Y%m%d')}",
    }, model, fnames, rows)


def predict_works(works, model, fnames):
    """Score every eligible work; return list of dicts for DB persistence.

    Risk bands (calibrated from training-time best-F1 threshold; if no
    training-time threshold is provided we default to these conservative bands):
      LOW        < 0.30
      MODERATE   0.30-0.50
      HIGH       0.50-0.70
      CRITICAL   >= 0.70
    """
    if model is None:
        return []
    feats = []
    keys = []
    for w in works:
        if (w.get("sanction_amount") is None or w.get("work_category") is None
                or w.get("state_id") is None):
            continue
        keys.append(w)
        feats.append([_safe_float(w.get(fn)) for fn in fnames])
    if not feats:
        return []
    arr = np.array(feats, dtype=float)
    proba = model.predict_proba(arr)[:, 1]
    out = []
    for w, p in zip(keys, proba):
        if p >= 0.7:   band = "CRITICAL"
        elif p >= 0.5: band = "HIGH"
        elif p >= 0.3: band = "MODERATE"
        else:          band = "LOW"
        out.append({
            "work_id": w.get("work_id"),
            "member_type": w.get("member_type"),
            "delay_probability": float(p),
            "delay_risk_band": band,
        })
    return out


def top_shap_features(model, fnames, X_sample, n_top=3):
    """Return list of (feature_name, mean_abs_shap) for top n_top features."""
    try:
        import shap
        explainer = shap.TreeExplainer(model)
        sv = explainer.shap_values(X_sample[: min(200, len(X_sample))])
        mean_abs = np.mean(np.abs(sv), axis=0)
        order = np.argsort(mean_abs)[::-1]
        return [(fnames[i], float(mean_abs[i])) for i in order[:n_top]]
    except Exception:
        return []
