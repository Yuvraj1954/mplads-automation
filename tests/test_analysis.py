from datetime import date
from analysis.normalize import normalize_activity
from analysis.status import classify_status
from analysis.lifecycle import (
    compute_sanction_delay_days,
    compute_project_age_days,
    compute_execution_days,
    compute_pending_days,
)
from analysis.financial import compute_financial
from analysis.benchmarks import (
    percentile_cont,
    compute_percentiles,
    classify_percentile,
    classify_cost_status,
    classify_duration_status,
    compute_median_deviation,
    build_benchmark_groups,
    select_benchmark,
    compute_benchmarks_for_group,
)
from analysis.risk import (
    compute_risk_flags,
    classify_risk_level,
)
from analysis.phase_a_member import compute_member_metrics
from analysis.phase_a_state import compute_state_metrics
from analysis.phase_a_statistics import compute_statistics
from analysis.phase_a_trends import compute_trends
from analysis.affected import (
    expand_affected_works,
    expand_time_sensitive,
    compute_sanction_delay_global_distribution,
)
from analysis.work_analysis import compute_work_analyses
from analysis.pipeline import AnalysisPipeline
from analysis.models import (
    WorkAnalysis, MemberMetrics, StateMetrics,
    NationalStatistics, TrendRecord,
)


def test_normalize_activity():
    assert normalize_activity(
        "WS/MP190/2023-2024/1302-Street lights"
    ) == "Street lights"

    assert normalize_activity(
        "WS/MP081/2023-2024/1775-Construction"
    ) == "Construction"

    assert normalize_activity(
        "NA-Roads"
    ) == "NA-Roads"

    assert normalize_activity(None) is None
    assert normalize_activity("") == ""


def test_classify_status():
    assert classify_status(date(2024, 1, 1), date(2024, 2, 1),
                           date(2024, 3, 1)) == "Completed"
    assert classify_status(date(2024, 1, 1), date(2024, 2, 1),
                           None) == "In Progress"
    assert classify_status(date(2024, 1, 1), None,
                           None) == "Recommended"
    assert classify_status(None, None, None) == "Unknown"


def test_sanction_delay_days():
    assert compute_sanction_delay_days(
        date(2024, 1, 1), date(2024, 1, 31)
    ) == 30
    assert compute_sanction_delay_days(None, date(2024, 1, 31)) is None
    assert compute_sanction_delay_days(date(2024, 1, 1), None) is None


def test_project_age_days():
    ref = date(2024, 12, 31)
    assert compute_project_age_days(date(2024, 1, 1), ref) == 365
    assert compute_project_age_days(None, ref) is None


def test_execution_days():
    assert compute_execution_days(
        date(2024, 1, 1), date(2024, 6, 1)
    ) == 152
    assert compute_execution_days(
        date(2024, 6, 1), date(2024, 1, 1)
    ) is None
    assert compute_execution_days(None, date(2024, 6, 1)) is None


def test_pending_days():
    assert compute_pending_days("In Progress", date(2024, 1, 1),
                                date(2024, 7, 1)) == 182
    assert compute_pending_days("Completed", date(2024, 1, 1),
                                date(2024, 7, 1)) is None
    assert compute_pending_days("In Progress", None,
                                date(2024, 7, 1)) is None


def test_financial():
    r = compute_financial(100000, 80000, 50000, 40000)
    assert r["expenditure_percentage"] == 62.5
    assert r["completion_percentage"] == 50.0

    r2 = compute_financial(100000, 0, 0, 0)
    assert r2["expenditure_percentage"] is None
    assert r2["completion_percentage"] is None


def test_percentile_cont():
    assert percentile_cont([1, 2, 3, 4, 5], 50) == 3.0
    assert percentile_cont([1, 2, 3, 4, 5], 25) == 2.0
    assert percentile_cont([1, 2, 3, 4, 5], 75) == 4.0
    assert percentile_cont([1, 2, 3, 4, 5], 90) == 4.6
    assert percentile_cont([1, 2, 3, 4, 5], 95) == 4.8
    assert percentile_cont([], 50) is None
    assert percentile_cont([5], 50) == 5.0


def test_compute_percentiles():
    p = compute_percentiles([10, 20, 30, 40, 50])
    assert "p25" in p
    assert "p50" in p
    assert "p75" in p
    assert "p90" in p
    assert "p95" in p
    assert p["p50"] == 30.0


def test_classify_percentile():
    pcts = {"p25": 100, "p50": 200, "p75": 300, "p90": 400, "p95": 500}
    assert classify_percentile(50, pcts) == 10
    assert classify_percentile(150, pcts) == 25
    assert classify_percentile(250, pcts) == 50
    assert classify_percentile(350, pcts) == 75
    assert classify_percentile(450, pcts) == 90
    assert classify_percentile(550, pcts) == 97.5
    assert classify_percentile(50, {}) is None


def test_cost_status():
    assert classify_cost_status(97.5, 10, "Completed") == "VERY_HIGH"
    assert classify_cost_status(92, 10, "Completed") == "HIGH"
    assert classify_cost_status(80, 10, "Completed") == "ABOVE_NORMAL"
    assert classify_cost_status(50, 10, "Completed") == "NORMAL"
    assert classify_cost_status(15, 10, "Completed") == "BELOW_NORMAL"
    assert classify_cost_status(5, 10, "Completed") == "VERY_LOW"
    assert classify_cost_status(50, 3, "Completed") == "NO_RELIABLE_BENCHMARK"
    assert classify_cost_status(50, 10, "In Progress") == "NOT_APPLICABLE"


def test_duration_status():
    assert classify_duration_status(97.5, 10, "Completed") == "VERY_LONG"
    assert classify_duration_status(92, 10, "Completed") == "LONG"
    assert classify_duration_status(50, 10, "Completed") == "NORMAL"
    assert classify_duration_status(50, 3, "Completed") == "NO_RELIABLE_BENCHMARK"


def test_median_deviation():
    assert compute_median_deviation(150, 100) == 50.0
    assert compute_median_deviation(50, 100) == -50.0
    assert compute_median_deviation(100, 0) is None
    assert compute_median_deviation(None, 100) is None


def test_risk_flags():
    work = {
        "sanction_delay_days": 500,
        "status": "In Progress",
        "pending_days": 400,
        "sanction_amount": 100000,
        "expenditure_amount": 0,
        "last_expenditure_date": None,
        "completion_date": None,
        "cost_status": "VERY_HIGH",
        "duration_status": None,
        "_reference_date": date(2024, 12, 31),
    }
    thresholds = {"delay_p75": 100, "delay_p90": 200, "delay_p95": 300}
    flags = compute_risk_flags(work, thresholds, 300)
    assert "SANCTION_DELAY_VERY_HIGH" in flags
    assert "LOW_EXPENDITURE" in flags
    assert "COST_ANOMALY" in flags


def test_risk_level():
    assert classify_risk_level(["OVER_EXPENDITURE"]) == "HIGH"
    assert classify_risk_level(["A", "B", "C"]) == "HIGH"
    assert classify_risk_level(["A", "B"]) == "MEDIUM"
    assert classify_risk_level(["A"]) == "LOW"
    assert classify_risk_level([]) == "NORMAL"


def test_full_work_analysis():
    works = [
        {
            "work_id": 1,
            "member_type": "MP",
            "member_id": 10,
            "recommendation_date": date(2023, 6, 1),
            "sanction_date": date(2023, 9, 1),
            "completion_date": date(2024, 3, 1),
            "last_expenditure_date": date(2024, 2, 15),
            "recommended_amount": 500000,
            "sanction_amount": 400000,
            "expenditure_amount": 350000,
            "completion_amount": 350000,
            "activity_name": "WS/MP001/2023-2024/123-Roads",
            "state_id": 1,
            "constituency_id": 101,
        },
        {
            "work_id": 2,
            "member_type": "MP",
            "member_id": 10,
            "recommendation_date": date(2024, 1, 1),
            "sanction_date": None,
            "completion_date": None,
            "last_expenditure_date": None,
            "recommended_amount": 200000,
            "sanction_amount": None,
            "expenditure_amount": None,
            "completion_amount": None,
            "activity_name": "NA-Lighting",
            "state_id": 1,
            "constituency_id": 101,
        },
    ]

    results = compute_work_analyses(works, date(2024, 12, 31))

    assert len(results) == 2

    w1 = next(r for r in results if r.work_id == 1)
    assert w1.status == "Completed"
    assert w1.normalized_activity == "Roads"
    assert w1.sanction_delay_days == 92
    assert w1.execution_days == 182
    assert w1.expenditure_percentage == 87.5
    assert w1.completion_percentage == 87.5

    w2 = next(r for r in results if r.work_id == 2)
    assert w2.status == "Recommended"
    assert w2.normalized_activity == "NA-Lighting"
    assert w2.expenditure_percentage is None


def test_member_metrics():
    works = [
        {
            "work_id": 1,
            "member_type": "MP",
            "member_id": 10,
            "recommendation_date": date(2023, 6, 1),
            "sanction_date": date(2023, 9, 1),
            "completion_date": date(2024, 3, 1),
            "last_expenditure_date": date(2024, 2, 15),
            "recommended_amount": 500000,
            "sanction_amount": 400000,
            "expenditure_amount": 350000,
            "completion_amount": 350000,
            "activity_name": "Roads",
            "state_id": 1,
            "constituency_id": 101,
        },
    ]

    analyses = compute_work_analyses(works, date(2024, 12, 31))
    metrics = compute_member_metrics(analyses)

    assert len(metrics) == 1
    m = metrics[0]
    assert m.member_id == 10
    assert m.total_works == 1
    assert m.completed_works == 1
    assert m.recommended_amount == 500000
    assert m.sanctioned_amount == 400000


def test_state_metrics():
    works = [
        {
            "work_id": 1,
            "member_type": "MP",
            "member_id": 10,
            "recommendation_date": date(2023, 6, 1),
            "sanction_date": date(2023, 9, 1),
            "completion_date": date(2024, 3, 1),
            "last_expenditure_date": date(2024, 2, 15),
            "recommended_amount": 500000,
            "sanction_amount": 400000,
            "expenditure_amount": 350000,
            "completion_amount": 350000,
            "activity_name": "Roads",
            "state_id": 1,
            "constituency_id": 101,
        },
    ]

    analyses = compute_work_analyses(works, date(2024, 12, 31))
    member_metrics = compute_member_metrics(analyses)
    member_map = {(m.member_type, m.member_id): m for m in member_metrics}
    state_metrics = compute_state_metrics(analyses, member_map)

    assert len(state_metrics) == 1
    sm = state_metrics[0]
    assert sm.state_id == 1
    assert sm.total_works == 1
    assert sm.completed_works == 1


def test_statistics():
    works = [
        {
            "work_id": i,
            "member_type": "MP",
            "member_id": 10 + (i % 3),
            "recommendation_date": date(2023, 6, 1),
            "sanction_date": date(2023, 9, 1),
            "completion_date": date(2024, 3, 1),
            "last_expenditure_date": date(2024, 2, 15),
            "recommended_amount": 500000 + i * 10000,
            "sanction_amount": 400000 + i * 10000,
            "expenditure_amount": 350000 + i * 10000,
            "completion_amount": 350000 + i * 10000,
            "activity_name": "Roads",
            "state_id": 1,
            "constituency_id": 101,
        }
        for i in range(10)
    ]

    analyses = compute_work_analyses(works, date(2024, 12, 31))
    member_metrics = compute_member_metrics(analyses)
    stats = compute_statistics(member_metrics)

    assert len(stats) > 0
    total_works_stat = next(
        s for s in stats if s.metric_name == "total_works"
    )
    assert total_works_stat.count == 3
    assert total_works_stat.mean > 0


def test_trends():
    works = [
        {
            "work_id": 1,
            "member_type": "MP",
            "member_id": 10,
            "recommendation_date": date(2023, 6, 1),
            "sanction_date": date(2023, 9, 1),
            "completion_date": date(2024, 3, 1),
            "last_expenditure_date": None,
            "recommended_amount": 500000,
            "sanction_amount": 400000,
            "expenditure_amount": 350000,
            "completion_amount": 350000,
            "activity_name": "Roads",
            "state_id": 1,
            "constituency_id": 101,
        },
    ]

    analyses = compute_work_analyses(works, date(2024, 12, 31))
    trends = compute_trends(analyses)

    assert len(trends) >= 1
    t = trends[0]
    assert t.year == 2023
    assert t.total_works == 1


def test_affected_expansion():
    works_by_id = {
        1: {"work_id": 1, "member_type": "MP", "member_id": 10,
            "normalized_activity": "Roads", "state_id": 1,
            "status": "Completed"},
        2: {"work_id": 2, "member_type": "MP", "member_id": 10,
            "normalized_activity": "Roads", "state_id": 1,
            "status": "Completed"},
        3: {"work_id": 3, "member_type": "MP", "member_id": 20,
            "normalized_activity": "Roads", "state_id": 2,
            "status": "Completed"},
        4: {"work_id": 4, "member_type": "MP", "member_id": 30,
            "normalized_activity": "Lighting", "state_id": 1,
            "status": "Completed"},
    }

    affected, members = expand_affected_works(
        {1}, works_by_id, {}, date(2024, 12, 31)
    )

    assert 1 in affected
    assert 2 in affected
    assert 3 in affected  # national benchmark expansion includes all "Roads"
    assert 4 not in affected


def test_time_sensitive():
    works_by_id = {
        1: {"work_id": 1, "status": "In Progress",
            "sanction_date": date(2024, 1, 1),
            "completion_date": None,
            "recommendation_date": date(2023, 6, 1),
            "last_expenditure_date": None},
        2: {"work_id": 2, "status": "Completed",
            "sanction_date": date(2024, 1, 1),
            "completion_date": date(2024, 6, 1),
            "recommendation_date": date(2023, 6, 1),
            "last_expenditure_date": None},
        3: {"work_id": 3, "status": "Recommended",
            "sanction_date": None,
            "completion_date": None,
            "recommendation_date": date(2024, 10, 1),
            "last_expenditure_date": None},
    }

    ts = expand_time_sensitive(works_by_id, {}, date(2024, 12, 31))
    assert 1 in ts
    assert 2 not in ts
    assert 3 in ts


def test_pipeline_full():
    works = [
        {
            "work_id": 1,
            "member_type": "MP",
            "member_id": 10,
            "recommendation_date": date(2023, 6, 1),
            "sanction_date": date(2023, 9, 1),
            "completion_date": date(2024, 3, 1),
            "last_expenditure_date": date(2024, 2, 15),
            "recommended_amount": 500000,
            "sanction_amount": 400000,
            "expenditure_amount": 350000,
            "completion_amount": 350000,
            "activity_name": "Roads",
            "state_id": 1,
            "constituency_id": 101,
        },
    ]

    pipeline = AnalysisPipeline(works)
    pipeline.run_full(date(2024, 12, 31))

    assert len(pipeline.work_analyses) == 1
    assert len(pipeline.member_metrics) == 1
    assert len(pipeline.state_metrics) == 1
    assert len(pipeline.statistics) > 0
    assert len(pipeline.trends) >= 1


def test_pipeline_delta():
    works = [
        {
            "work_id": 1,
            "member_type": "MP",
            "member_id": 10,
            "recommendation_date": date(2023, 6, 1),
            "sanction_date": date(2023, 9, 1),
            "completion_date": date(2024, 3, 1),
            "last_expenditure_date": date(2024, 2, 15),
            "recommended_amount": 500000,
            "sanction_amount": 400000,
            "expenditure_amount": 350000,
            "completion_amount": 350000,
            "activity_name": "Roads",
            "state_id": 1,
            "constituency_id": 101,
        },
        {
            "work_id": 2,
            "member_type": "MP",
            "member_id": 20,
            "recommendation_date": date(2024, 1, 1),
            "sanction_date": None,
            "completion_date": None,
            "last_expenditure_date": None,
            "recommended_amount": 200000,
            "sanction_amount": None,
            "expenditure_amount": None,
            "completion_amount": None,
            "activity_name": "Lighting",
            "state_id": 2,
            "constituency_id": 201,
        },
    ]

    pipeline = AnalysisPipeline(works)
    pipeline.run_delta({1}, date(2024, 12, 31))

    assert len(pipeline.work_analyses) >= 1


def test_benchmark_groups():
    from datetime import timedelta
    works = [
        {
            "work_id": i,
            "member_type": "MP",
            "normalized_activity": "Roads",
            "state_id": 1,
            "status": "Completed",
            "sanction_amount": 100000 + i * 10000,
            "sanction_date": date(2024, 1, 1),
            "completion_date": date(2024, 1, 1) + timedelta(days=30 * (i + 1)),
        }
        for i in range(10)
    ]

    state_g, national_g = build_benchmark_groups(works, "MP")
    assert "Roads|1" in state_g
    assert "Roads" in national_g

    bench = select_benchmark("Roads", 1, state_g, national_g)
    assert bench["benchmark_sample_size"] == 10
    assert bench["benchmark_quality"] == "ACTIVITY_STATE_GOOD"


def test_delay_distribution():
    works_by_id = {
        1: {"sanction_delay_days": 30},
        2: {"sanction_delay_days": 60},
        3: {"sanction_delay_days": 90},
        4: {"sanction_delay_days": 120},
        5: {"sanction_delay_days": 150},
    }

    dist = compute_sanction_delay_global_distribution(works_by_id)
    assert dist["delay_p75"] is not None
    assert dist["delay_p90"] is not None
    assert dist["delay_p95"] is not None
    assert dist["delay_p75"] <= dist["delay_p90"] <= dist["delay_p95"]
