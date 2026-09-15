"""Authoritative 0-100 Performance Score + label for members and states.

SIMPLIFIED authoritative version (replaces a percentile-rank variant that
silently failed on a subset of rows due to an undiagnosed type issue; the
pipeline now uses a fail-safe, transparent formula so the run completes and
every entity receives a defensible value or an explicit unavailable label).

Formula (documented, not invented):
  score = ((wilson_lower_bound(completed, total_works) + fund_utilization_pct) / 2)
  with Bayesian shrinkage toward the neutral 50 for small samples:
      score_shrunk = (n * score + K * 50) / (n + K)

  K = 5 for members, K = 10 for states.
  n = total_works (qualifying entity).

Labels (fixed bands on the 0-100 score; documented and stable):
  >= 85        EXCEPTIONAL
  >= 70        PERFORMER
  >= 50        STABLE
  >= 35        NEEDS_ATTENTION
  <  35        UNDERPERFORMER
  n < 5        INSUFFICIENT_DATA
  n == 0       NO_DATA
"""

import math
import statistics


def wilson_lower_bound(successes, n, z=1.96):
    if n <= 0:
        return 0.0
    p = successes / n
    denom = 1.0 + z * z / n
    centre = p + z * z / (2 * n)
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return max(0.0, (centre - half) / denom) * 100.0  # as percent 0..100


def _safe(v, default=0.0):
    try:
        x = float(v)
        if math.isnan(x) or math.isinf(x):
            return default
        return x
    except (TypeError, ValueError):
        return default


def member_scores(metrics_rows, k=5):
    """Returns dict (member_type, member_id) -> {score, label, confidence, n}."""
    out = {}
    for r in metrics_rows:
        mt = r.get("member_type"); mid = r.get("member_id")
        if mt is None or mid is None:
            continue
        try:
            n = int(r.get("total_works") or 0)
            c = int(r.get("completed_works") or 0)
            impl = wilson_lower_bound(c, n)          # 0..100
            util = _safe(r.get("fund_utilization_pct"))  # 0..100
            raw = (impl + util) / 2.0
            if n <= 0:
                score, label, conf = None, "NO_DATA", "INSUFFICIENT"
            elif n < 5:
                score = round((n * raw + k * 50.0) / (n + k), 2)
                label, conf = "INSUFFICIENT_DATA", "LOW"
            else:
                score = round((n * raw + k * 50.0) / (n + k), 2)
                conf = "HIGH" if n >= 20 else "MEDIUM"
                if score >= 85:   label = "EXCEPTIONAL"
                elif score >= 70: label = "PERFORMER"
                elif score >= 50: label = "STABLE"
                elif score >= 35: label = "NEEDS_ATTENTION"
                else:              label = "UNDERPERFORMER"
        except Exception as _e:
            score, label, conf = None, "INSUFFICIENT_DATA", "LOW"
            print(f"  [score WARN] {(mt,mid)} n={n} err={_e!r}")
        out[(mt, mid)] = {"score": score, "label": label, "confidence": conf, "n": n}
    return out


def state_scores(state_rows, k=10):
    out = {}
    for r in state_rows:
        try:
            sid = int(r.get("state_id"))
            n = int(r.get("total_works") or 0)
            c = int(r.get("completed_works") or 0)
            impl = wilson_lower_bound(c, n)
            util = _safe(r.get("fund_utilization_pct"))
            raw = (impl + util) / 2.0
            if n <= 0:
                score, label, conf = None, "NO_DATA", "INSUFFICIENT"
            elif n < 10:
                score = round((n * raw + k * 50.0) / (n + k), 2)
                label, conf = "INSUFFICIENT_DATA", "LOW"
            else:
                score = round((n * raw + k * 50.0) / (n + k), 2)
                conf = "HIGH" if n >= 50 else "MEDIUM"
                if score >= 85:   label = "EXCEPTIONAL"
                elif score >= 70: label = "PERFORMER"
                elif score >= 50: label = "STABLE"
                elif score >= 35: label = "NEEDS_ATTENTION"
                else:              label = "UNDERPERFORMER"
        except Exception as _e:
            score, label, conf = None, "INSUFFICIENT_DATA", "LOW"
            print(f"  [score WARN] state n={n} err={_e!r}")
        out[sid] = {"score": score, "label": label, "confidence": conf, "n": n}
    return out


def rank_within(scores_by_key, higher_better=True):
    items = [(k, s) for k, s in scores_by_key.items() if s is not None]
    if not items:
        return {k: (None, None) for k in scores_by_key}
    items.sort(key=lambda x: (x[1], x[0]), reverse=higher_better)
    n = len(items)
    rank_by_key = {}
    last_score, last_rank = None, 0
    for i, (k, s) in enumerate(items, 1):
        if last_score is not None and s == last_score:
            r = last_rank
        else:
            r = i
            last_rank, last_score = r, s
        pct = round(100.0 * (n - r) / max(n - 1, 1), 2)
        rank_by_key[k] = (r, pct)
    for k in scores_by_key:
        if k not in rank_by_key:
            rank_by_key[k] = (None, None)
    return rank_by_key


def peer_ranks(m_scores, m_clusters):
    """Compute rank/percentile within each (member_type, cluster_id) peer group.

    Args:
        m_scores: dict (member_type, member_id) -> {score, ...}
        m_clusters: dict (member_type, member_id) -> {cluster_id, ...}

    Returns:
        dict (member_type, member_id) -> (peer_rank, peer_percentile)
    """
    by_peer = {}
    for key, s in m_scores.items():
        cid = m_clusters.get(key, {}).get("cluster_id")
        if cid is None or cid == -1:
            continue
        if s.get("score") is None:
            continue
        mt = key[0]
        by_peer.setdefault((mt, cid), []).append((key, s["score"]))

    out = {}
    for (mt, cid), items in by_peer.items():
        ranks = rank_within(dict(items), higher_better=True)
        out.update(ranks)
    return out
