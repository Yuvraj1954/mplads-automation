#!/usr/bin/env python3
"""GovSense AI — Intelligence layer validation.

Connects to DB2 and checks mathematical, population, and rank-consistency
invariants for the intelligence tables. Prints a JSON report and exits with
non-zero status if any HARD check fails.

Run:
  python automation/validate_intelligence.py
"""

import asyncio
import json
import os
import sys
from collections import defaultdict
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


def label_for_score(score):
    if score is None:
        return None
    if score >= 85:
        return "EXCEPTIONAL"
    if score >= 70:
        return "PERFORMER"
    if score >= 50:
        return "STABLE"
    if score >= 35:
        return "NEEDS_ATTENTION"
    return "UNDERPERFORMER"


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
        checks = []

        # ------------------------------------------------------------------
        # 1. Population counts
        # ------------------------------------------------------------------
        mm_total = await db2.fetchval("SELECT COUNT(*) FROM public.member_metrics")
        mi_total = await db2.fetchval("SELECT COUNT(*) FROM public.member_intelligence")
        sm_total = await db2.fetchval("SELECT COUNT(*) FROM public.state_metrics")
        si_total = await db2.fetchval("SELECT COUNT(*) FROM public.state_intelligence")
        checks.append({
            "name": "member_intelligence population",
            "status": "PASS" if mi_total == mm_total else "FAIL",
            "detail": f"member_metrics={mm_total} member_intelligence={mi_total}",
            "severity": "HARD",
        })
        checks.append({
            "name": "state_intelligence population",
            "status": "PASS" if si_total == sm_total else "FAIL",
            "detail": f"state_metrics={sm_total} state_intelligence={si_total}",
            "severity": "HARD",
        })

        # ------------------------------------------------------------------
        # 2. Score bounds and label consistency
        # ------------------------------------------------------------------
        mi_rows = await db2.fetch(
            """SELECT member_id, member_type, performance_score_100, performance_label,
                      national_rank, national_percentile, sample_size
               FROM public.member_intelligence""")
        score_violations = 0
        label_violations = 0
        for r in mi_rows:
            s = _safe(r["performance_score_100"])
            if s is not None and (s < 0 or s > 100):
                score_violations += 1
            expected = label_for_score(s)
            if expected and r["performance_label"] not in (expected, "INSUFFICIENT_DATA", "NO_DATA"):
                label_violations += 1
        checks.append({
            "name": "member score bounds [0,100]",
            "status": "PASS" if score_violations == 0 else "FAIL",
            "detail": f"violations={score_violations}",
            "severity": "HARD",
        })
        checks.append({
            "name": "member label consistency",
            "status": "PASS" if label_violations == 0 else "FAIL",
            "detail": f"violations={label_violations}",
            "severity": "HARD",
        })

        si_rows = await db2.fetch(
            """SELECT state_id, performance_score_100, performance_label, sample_size
               FROM public.state_intelligence""")
        s_score_violations = 0
        s_label_violations = 0
        for r in si_rows:
            s = _safe(r["performance_score_100"])
            if s is not None and (s < 0 or s > 100):
                s_score_violations += 1
            expected = label_for_score(s)
            if expected and r["performance_label"] not in (expected, "INSUFFICIENT_DATA", "NO_DATA"):
                s_label_violations += 1
        checks.append({
            "name": "state score bounds [0,100]",
            "status": "PASS" if s_score_violations == 0 else "FAIL",
            "detail": f"violations={s_score_violations}",
            "severity": "HARD",
        })
        checks.append({
            "name": "state label consistency",
            "status": "PASS" if s_label_violations == 0 else "FAIL",
            "detail": f"violations={s_label_violations}",
            "severity": "HARD",
        })

        # ------------------------------------------------------------------
        # 3. National rank / percentile consistency within member_type
        # ------------------------------------------------------------------
        by_type = defaultdict(list)
        for r in mi_rows:
            s = _safe(r["performance_score_100"])
            if s is not None:
                by_type[r["member_type"]].append((r["member_id"], s, _safe(r["national_rank"]), _safe(r["national_percentile"])))

        rank_violations = 0
        pct_violations = 0
        monotonic_violations = 0
        for mt, items in by_type.items():
            items.sort(key=lambda x: (x[1], x[0]), reverse=True)
            n = len(items)
            last_score, last_rank = None, 0
            for i, (mid, s, rk, pct) in enumerate(items, 1):
                if last_score is not None and s == last_score:
                    expected_rank = last_rank
                else:
                    expected_rank = i
                    last_rank, last_score = i, s
                if rk is not None and rk != expected_rank:
                    rank_violations += 1
                expected_pct = round(100.0 * (n - expected_rank) / max(n - 1, 1), 2)
                if pct is not None and abs(pct - expected_pct) > 0.05:
                    pct_violations += 1
            # monotonicity: higher score => rank not worse
            for i in range(1, len(items)):
                prev_score, prev_rank = items[i - 1][1], items[i - 1][2]
                cur_score, cur_rank = items[i][1], items[i][2]
                if prev_rank and cur_rank and prev_score > cur_score and prev_rank > cur_rank:
                    monotonic_violations += 1
        checks.append({
            "name": "national rank consistency",
            "status": "PASS" if rank_violations == 0 else "FAIL",
            "detail": f"violations={rank_violations}",
            "severity": "HARD",
        })
        checks.append({
            "name": "national percentile consistency",
            "status": "PASS" if pct_violations == 0 else "FAIL",
            "detail": f"violations={pct_violations}",
            "severity": "HARD",
        })
        checks.append({
            "name": "rank-score monotonicity",
            "status": "PASS" if monotonic_violations == 0 else "FAIL",
            "detail": f"violations={monotonic_violations}",
            "severity": "HARD",
        })

        # ------------------------------------------------------------------
        # 4. Peer rank / percentile within (member_type, cluster_id)
        # ------------------------------------------------------------------
        peer_rows = await db2.fetch(
            """SELECT member_type, cluster_id, member_id, performance_score_100, peer_rank, peer_percentile
               FROM public.member_intelligence
               WHERE cluster_id IS NOT NULL AND peer_rank IS NOT NULL""")
        by_cluster = defaultdict(list)
        for r in peer_rows:
            s = _safe(r["performance_score_100"])
            if s is not None:
                by_cluster[(r["member_type"], r["cluster_id"])].append(
                    (r["member_id"], s, _safe(r["peer_rank"]), _safe(r["peer_percentile"])))

        peer_rank_violations = 0
        peer_pct_violations = 0
        for key, items in by_cluster.items():
            items.sort(key=lambda x: (x[1], x[0]), reverse=True)
            n = len(items)
            last_score, last_rank = None, 0
            for i, (mid, s, rk, pct) in enumerate(items, 1):
                if last_score is not None and s == last_score:
                    expected_rank = last_rank
                else:
                    expected_rank = i
                    last_rank, last_score = i, s
                if rk != expected_rank:
                    peer_rank_violations += 1
                expected_pct = round(100.0 * (n - expected_rank) / max(n - 1, 1), 2)
                if pct is not None and abs(pct - expected_pct) > 0.05:
                    peer_pct_violations += 1
        checks.append({
            "name": "peer rank consistency within cluster",
            "status": "PASS" if peer_rank_violations == 0 else "FAIL",
            "detail": f"violations={peer_rank_violations}",
            "severity": "HARD",
        })
        checks.append({
            "name": "peer percentile consistency within cluster",
            "status": "PASS" if peer_pct_violations == 0 else "FAIL",
            "detail": f"violations={peer_pct_violations}",
            "severity": "HARD",
        })

        # ------------------------------------------------------------------
        # 5. Risk score bounds and level consistency
        # ------------------------------------------------------------------
        risk_rows = await db2.fetch(
            """SELECT member_id, member_type, risk_score, risk_level
               FROM public.member_intelligence WHERE risk_score IS NOT NULL""")
        risk_bounds = 0
        risk_level_bad = 0
        for r in risk_rows:
            s = _safe(r["risk_score"])
            if s is not None and (s < 0 or s > 100):
                risk_bounds += 1
            lvl = r["risk_level"]
            if lvl and lvl not in ("LOW", "MODERATE", "HIGH", "CRITICAL"):
                risk_level_bad += 1
        checks.append({
            "name": "member risk score bounds [0,100]",
            "status": "PASS" if risk_bounds == 0 else "FAIL",
            "detail": f"violations={risk_bounds}",
            "severity": "HARD",
        })
        checks.append({
            "name": "member risk level enum",
            "status": "PASS" if risk_level_bad == 0 else "FAIL",
            "detail": f"bad_values={risk_level_bad}",
            "severity": "HARD",
        })

        # ------------------------------------------------------------------
        # 6. Model registry entries present
        # ------------------------------------------------------------------
        models = await db2.fetch(
            """SELECT DISTINCT model_name FROM public.model_registry""")
        model_names = {r["model_name"] for r in models}
        required = {"project_delay_xgb", "isolation_forest"}
        missing = required - model_names
        checks.append({
            "name": "model registry entries",
            "status": "PASS" if not missing else "FAIL",
            "detail": f"present={sorted(model_names)} missing={sorted(missing)}",
            "severity": "HARD",
        })

        # ------------------------------------------------------------------
        # 7. Per-work ML columns populated
        # ------------------------------------------------------------------
        mp_iso = await db1.fetchval(
            "SELECT COUNT(*) FROM public.work_analysis WHERE isolation_score IS NOT NULL")
        mla_iso = await db1.fetchval(
            "SELECT COUNT(*) FROM public.mla_work_analysis WHERE isolation_score IS NOT NULL")
        checks.append({
            "name": "per-work isolation scores populated",
            "status": "PASS" if mp_iso > 0 and mla_iso > 0 else "FAIL",
            "detail": f"MP={mp_iso} MLA={mla_iso}",
            "severity": "HARD",
        })

        # ------------------------------------------------------------------
        # Report
        # ------------------------------------------------------------------
        hard_failures = [c for c in checks if c["severity"] == "HARD" and c["status"] != "PASS"]
        report = {
            "validated_at": _now(),
            "checks": checks,
            "summary": {
                "total": len(checks),
                "passed": sum(1 for c in checks if c["status"] == "PASS"),
                "failed": sum(1 for c in checks if c["status"] != "PASS"),
                "hard_failures": len(hard_failures),
            },
        }
        print(json.dumps(report, indent=2))
        if hard_failures:
            sys.exit(1)
    finally:
        await db1.close()
        await db2.close()


if __name__ == "__main__":
    asyncio.run(main())
