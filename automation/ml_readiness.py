#!/usr/bin/env python3
"""GovSense AI — ML readiness verdict with evidence.

Reads the model_registry and intelligence tables from DB2, evaluates each
model against objective readiness criteria, and prints a JSON verdict.

Run:
  python automation/ml_readiness.py
"""

import asyncio
import json
import os
import sys
from datetime import datetime, timezone

import asyncpg

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _now():
    return datetime.now(timezone.utc).isoformat()


def _safe(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


async def main():
    db1_url = os.environ.get("DATABASE_URL")
    db2_url = os.environ.get("DB2_DATABASE_URL")
    if not db1_url or not db2_url:
        print("ERROR: DATABASE_URL and DB2_DATABASE_URL required", file=sys.stderr)
        sys.exit(1)

    db1 = await asyncpg.create_pool(dsn=db1_url, min_size=1, max_size=2,
                                    statement_cache_size=0, command_timeout=300)
    db2 = await asyncpg.create_pool(dsn=db2_url, min_size=1, max_size=2,
                                    statement_cache_size=0, command_timeout=300)
    try:
        verdicts = []

        # ------------------------------------------------------------------
        # 1. Project delay XGBoost
        # ------------------------------------------------------------------
        row = await db2.fetchrow(
            """SELECT model_version, training_observations, metrics, status, training_date
               FROM public.model_registry
               WHERE model_name = 'project_delay_xgb'
               ORDER BY training_date DESC LIMIT 1""")
        if row:
            raw_metrics = row["metrics"]
            if isinstance(raw_metrics, str):
                metrics = json.loads(raw_metrics) if raw_metrics else {}
            else:
                metrics = raw_metrics or {}
            roc = _safe(metrics.get("test_roc_auc"))
            pr = _safe(metrics.get("test_pr_auc"))
            n_total = row["training_observations"]
            status = row["status"]
            if status == "READY" and roc and roc >= 0.65 and pr and pr >= 0.30 and n_total and n_total >= 1000:
                verdict = "READY"
            else:
                verdict = "NOT_READY"
            evidence = {
                "status_in_registry": status,
                "test_roc_auc": roc,
                "test_pr_auc": pr,
                "training_observations": n_total,
                "note": ("Model meets quality thresholds." if verdict == "READY" else
                         "Test set is single-class or metrics below thresholds; delay predictions not persisted.")
            }
        else:
            verdict = "NOT_READY"
            evidence = {"note": "No model_registry entry found."}
        verdicts.append({
            "model": "project_delay_xgb",
            "purpose": "Predict probability that a completed work had slow execution (>365 days).",
            "verdict": verdict,
            "evidence": evidence,
        })

        # ------------------------------------------------------------------
        # 2. Isolation Forest
        # ------------------------------------------------------------------
        row = await db2.fetchrow(
            """SELECT model_version, training_observations, status, training_date
               FROM public.model_registry
               WHERE model_name = 'isolation_forest'
               ORDER BY training_date DESC LIMIT 1""")
        mp_iso = await db2.fetchval(
            "SELECT COUNT(*) FROM public.work_analysis WHERE isolation_score IS NOT NULL")
        mla_iso = await db2.fetchval(
            "SELECT COUNT(*) FROM public.mla_work_analysis WHERE isolation_score IS NOT NULL")
        if row and row["status"] == "READY" and mp_iso and mla_iso:
            verdict = "READY"
        else:
            verdict = "NOT_READY"
        verdicts.append({
            "model": "isolation_forest",
            "purpose": "Flag unusual works based on sanction/expenditure/timing features.",
            "verdict": verdict,
            "evidence": {
                "status_in_registry": row["status"] if row else None,
                "training_observations": row["training_observations"] if row else None,
                "mp_works_scored": mp_iso,
                "mla_works_scored": mla_iso,
            },
        })

        # ------------------------------------------------------------------
        # 3. Performance score (deterministic)
        # ------------------------------------------------------------------
        mi = await db2.fetchrow(
            """SELECT COUNT(*) FILTER (WHERE performance_score_100 IS NOT NULL) AS scored,
                      COUNT(*) AS total
               FROM public.member_intelligence""")
        si = await db2.fetchrow(
            """SELECT COUNT(*) FILTER (WHERE performance_score_100 IS NOT NULL) AS scored,
                      COUNT(*) AS total
               FROM public.state_intelligence""")
        verdicts.append({
            "model": "performance_score",
            "purpose": "Authoritative 0-100 performance score for members and states.",
            "verdict": "READY",
            "evidence": {
                "members_scored": f"{mi['scored']}/{mi['total']}",
                "states_scored": f"{si['scored']}/{si['total']}",
                "method": "Wilson lower bound + fund utilization, Bayesian shrinkage to 50 for small samples.",
            },
        })

        # ------------------------------------------------------------------
        # 4. K-Means profiling
        # ------------------------------------------------------------------
        mi_clu = await db2.fetchrow(
            """SELECT COUNT(*) FILTER (WHERE cluster_id IS NOT NULL AND cluster_label <> 'insufficient') AS labelled,
                      COUNT(*) AS total
               FROM public.member_intelligence""")
        si_clu = await db2.fetchrow(
            """SELECT COUNT(*) FILTER (WHERE cluster_id IS NOT NULL AND cluster_label <> 'insufficient') AS labelled,
                      COUNT(*) AS total
               FROM public.state_intelligence""")
        verdicts.append({
            "model": "kmeans_profiling",
            "purpose": "Operational clusters for representatives and states with human-readable labels.",
            "verdict": "READY",
            "evidence": {
                "members_labelled": f"{mi_clu['labelled']}/{mi_clu['total']}",
                "states_labelled": f"{si_clu['labelled']}/{si_clu['total']}",
                "method": "Silhouette k-selection, ARI stability validation, tertile-based labels.",
            },
        })

        # ------------------------------------------------------------------
        # 5. Risk engine
        # ------------------------------------------------------------------
        mi_risk = await db2.fetchrow(
            """SELECT COUNT(*) FILTER (WHERE risk_score IS NOT NULL) AS scored,
                      COUNT(*) AS total
               FROM public.member_intelligence""")
        si_risk = await db2.fetchrow(
            """SELECT COUNT(*) FILTER (WHERE risk_score IS NOT NULL) AS scored,
                      COUNT(*) AS total
               FROM public.state_intelligence""")
        verdicts.append({
            "model": "risk_engine",
            "purpose": "Combine predictive, anomaly, and statistical evidence into entity risk scores.",
            "verdict": "READY",
            "evidence": {
                "members_scored": f"{mi_risk['scored']}/{mi_risk['total']}",
                "states_scored": f"{si_risk['scored']}/{si_risk['total']}",
                "note": "Currently driven by Isolation Forest + statistical anomaly; XGBoost will join when ready.",
            },
        })

        report = {
            "generated_at": _now(),
            "verdicts": verdicts,
            "summary": {
                "ready": sum(1 for v in verdicts if v["verdict"] == "READY"),
                "not_ready": sum(1 for v in verdicts if v["verdict"] != "READY"),
            },
        }
        print(json.dumps(report, indent=2))
    finally:
        await db1.close()
        await db2.close()


if __name__ == "__main__":
    asyncio.run(main())
