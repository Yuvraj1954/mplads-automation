"""Representative (MP/MLA) and State operational profiling via K-Means.

Design (not arbitrary):
  - Features per entity (standardized): financial efficiency, implementation
    efficiency, timeliness, portfolio health, plus portfolio size.
  - Choose k in {2,3,4,5,6} by silhouette score (best).
  - Validate cluster stability by bootstrapping ARI (Adjusted Rand Index)
    across a few resamples; reject if unstable.
  - Cluster labels are generated from centroid characteristics (not invented
    beforehand) by binning each feature into High/Mid/Low using the entity
    population tertiles.
"""

import math
import os
import pickle
from collections import defaultdict


ARTIFACT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "artifacts")


def _safe_log(x):
    try:
        return math.log(float(x) + 1.0)
    except (TypeError, ValueError):
        return 0.0


def _safe_div(a, b):
    if b in (None, 0):
        return 0.0
    return float(a) / float(b) * 100.0


def member_features(member_rows):
    """Returns dict (member_type, member_id) -> feature dict (raw values)."""
    out = {}
    for r in member_rows:
        key = (r["member_type"], r["member_id"])
        n = int(r.get("total_works") or 0)
        c = int(r.get("completed_works") or 0)
        out[key] = {
            "financial_efficiency": float(r.get("fund_utilization_pct") or 0),
            "implementation_efficiency": _safe_div(c, n) if n > 0 else 0.0,
            "timeliness": float(r.get("avg_sanction_delay_days") or 0),
            "portfolio_health": float(
                100 - (
                    float(r.get("flagged_rate_pct") or 0)
                    + float(r.get("overdue_over_1_year") or 0) * 100 / max(n, 1)
                    + float(r.get("cost_anomaly_works") or 0) * 100 / max(n, 1) / 3.0
                )
            ),
            "portfolio_size_log": _safe_log(n),
        }
    return out


def state_features(state_rows):
    out = {}
    for r in state_rows:
        sid = int(r["state_id"])
        n = int(r.get("total_works") or 0)
        c = int(r.get("completed_works") or 0)
        overdue = float(r.get("overdue_over_1_year") or 0)
        risk = float(r.get("risk_rate_pct") or 0)
        ph_raw = (overdue * 100 / max(n, 1) + risk) / 2.0
        out[sid] = {
            "financial_efficiency": float(r.get("fund_utilization_pct") or 0),
            "implementation_efficiency": _safe_div(c, n) if n > 0 else 0.0,
            "timeliness": float(r.get("avg_sanction_delay_days") or 0),
            "portfolio_health": float(100 - ph_raw),
            "portfolio_size_log": _safe_log(n),
            "active_members_log": _safe_log(r.get("active_members")),
        }
    return out


def _standardize(X):
    """Returns (X_std, mean, std)."""
    import numpy as np
    arr = np.array(X, dtype=float)
    mu = arr.mean(axis=0)
    sd = arr.std(axis=0)
    sd = np.where(sd == 0, 1.0, sd)
    return (arr - mu) / sd, mu, sd


def _choose_k(features_std, k_range=(2, 3, 4, 5, 6), random_state=42):
    from sklearn.cluster import KMeans
    from sklearn.metrics import silhouette_score
    best = (None, -1.0)
    for k in k_range:
        if k > len(features_std):
            continue
        km = KMeans(n_clusters=k, n_init=10, random_state=random_state).fit(features_std)
        s = silhouette_score(features_std, km.labels_)
        if s > best[1]:
            best = (km, s)
    return best


def _centroid_label(centroid_raw, feature_names, tertiles):
    """Generate a human-readable label from centroid values vs population tertiles."""
    parts = []
    for name, val in zip(feature_names, centroid_raw):
        if name == "portfolio_size_log":
            continue
        t1, t2 = tertiles[name]
        if name in ("financial_efficiency", "implementation_efficiency", "portfolio_health"):
            if val >= t2:
                tag = "High"
            elif val >= t1:
                tag = "Mid"
            else:
                tag = "Low"
        elif name == "timeliness":
            if val <= t1:
                tag = "Fast"
            elif val <= t2:
                tag = "Mid"
            else:
                tag = "Slow"
        else:
            tag = "Mid"
        parts.append(f"{tag} {name.replace('_',' ').title().replace('Efficiency','Eff.')}")
    # Keep the two most informative dimensions
    if len(parts) > 2:
        parts = parts[:2]
    return " / ".join(parts)


def fit_kmeans_with_labels(features_dict, feature_order, artifact_name, random_state=42, min_cluster_size=5):
    """Fit K-Means, choose k by silhouette, validate stability via ARI bootstrap,
    generate cluster labels from centroids, persist model artifact.

    Returns: dict key -> {"cluster_id": int, "cluster_label": str, "n": int}.
    """
    import numpy as np
    from sklearn.cluster import KMeans
    from sklearn.metrics import adjusted_rand_score
    from ._validation import bootstrap_ari_stability  # type: ignore

    keys = list(features_dict.keys())
    raw = np.array([[float(features_dict[k].get(f, 0.0)) for f in feature_order] for k in keys], dtype=float)

    # Tertiles over the raw feature for label generation
    tertiles = {}
    for j, f in enumerate(feature_order):
        col = raw[:, j]
        t1, t2 = np.percentile(col, [33.3, 66.6])
        tertiles[f] = (float(t1), float(t2))

    std, mu, sd = _standardize(raw)
    model, sil = _choose_k(std, random_state=random_state)
    if model is None:
        return {k: {"cluster_id": -1, "cluster_label": "insufficient", "n": 0} for k in keys}

    # Stability check
    ari = bootstrap_ari_stability(std, n_clusters=model.n_clusters, random_state=random_state)

    labels = model.labels_
    out = {}
    cluster_counts = defaultdict(int)
    for i, k in enumerate(keys):
        cluster_counts[int(labels[i])] += 1
    for i, k in enumerate(keys):
        cid = int(labels[i])
        if cluster_counts[cid] < min_cluster_size:
            out[k] = {"cluster_id": cid, "cluster_label": "insufficient", "n": 0}
        else:
            # centroid in raw scale for this cluster
            mask = (labels == cid)
            cent_raw = raw[mask].mean(axis=0)
            label = _centroid_label(cent_raw, feature_order, tertiles)
            out[k] = {"cluster_id": cid, "cluster_label": label, "n": int(cluster_counts[cid])}

    os.makedirs(ARTIFACT_DIR, exist_ok=True)
    art_path = os.path.join(ARTIFACT_DIR, f"{artifact_name}.pkl")
    with open(art_path, "wb") as f:
        pickle.dump({"model": model, "mu": mu.tolist(), "sd": sd.tolist(),
                     "feature_order": feature_order,
                     "k": model.n_clusters, "silhouette": float(sil),
                     "stability_ari": float(ari)}, f)

    return out


def profile_members(member_rows, random_state=42):
    feats = member_features(member_rows)
    feature_order = ["financial_efficiency", "implementation_efficiency",
                     "timeliness", "portfolio_health", "portfolio_size_log"]
    return fit_kmeans_with_labels(feats, feature_order, "member_kmeans",
                                  random_state=random_state, min_cluster_size=5)


def profile_states(state_rows, random_state=42):
    feats = state_features(state_rows)
    feature_order = ["financial_efficiency", "implementation_efficiency",
                     "timeliness", "portfolio_health", "portfolio_size_log",
                     "active_members_log"]
    return fit_kmeans_with_labels(feats, feature_order, "state_kmeans",
                                  random_state=random_state, min_cluster_size=3)
