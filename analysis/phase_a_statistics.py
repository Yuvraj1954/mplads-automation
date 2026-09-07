import numpy as np
from analysis.models import NationalStatistics
from analysis.benchmarks import percentile_cont


METRIC_FIELDS = [
    "total_works",
    "recommended_works",
    "sanctioned_works",
    "completed_works",
    "ongoing_works",
    "pending_works",
    "recommended_amount",
    "sanctioned_amount",
    "expenditure_amount",
    "completion_amount",
    "unspent_amount",
    "avg_sanction_delay_days",
    "median_sanction_delay_days",
    "avg_execution_days",
    "median_execution_days",
    "avg_project_age_days",
    "max_project_age_days",
    "avg_pending_days",
    "max_pending_days",
    "flagged_works",
    "medium_risk_works",
    "high_risk_works",
    "flagged_rate_pct",
    "high_risk_rate_pct",
    "cost_anomaly_works",
    "duration_anomaly_works",
    "expenditure_over_sanction_works",
    "expenditure_over_recommendation_works",
    "negative_sanction_delay_works",
    "negative_execution_works",
    "completion_before_sanction_works",
    "expenditure_before_sanction_works",
    "overdue_over_1_year",
    "overdue_over_2_years",
    "avg_cost_percentile",
    "avg_duration_percentile",
    "avg_cost_deviation_pct",
    "avg_duration_deviation_pct",
    "completion_rate_pct",
    "sanction_rate_pct",
    "sanction_conversion_pct",
    "expenditure_sanction_utilization_pct",
    "expenditure_recommendation_pct",
]


def compute_statistics(member_metrics_list, member_type_filter=None):
    active = [m for m in member_metrics_list if m.total_works > 0]
    if member_type_filter:
        active = [m for m in active if m.member_type == member_type_filter]

    results = []
    for field in METRIC_FIELDS:
        values = []
        for m in active:
            val = getattr(m, field, None)
            if val is not None:
                values.append(float(val))

        if not values:
            continue

        arr = np.array(values)
        stat = NationalStatistics(
            metric_name=field,
            member_type=member_type_filter,
            count=len(values),
            mean=round(float(np.mean(arr)), 4),
            std_dev=round(float(np.std(arr, ddof=0)), 4),
            minimum=round(float(np.min(arr)), 4),
            p25=round(float(percentile_cont(arr, 25)), 4),
            median=round(float(np.median(arr)), 4),
            p75=round(float(percentile_cont(arr, 75)), 4),
            p90=round(float(percentile_cont(arr, 90)), 4),
            p95=round(float(percentile_cont(arr, 95)), 4),
            maximum=round(float(np.max(arr)), 4),
            iqr=round(
                float(percentile_cont(arr, 75) - percentile_cont(arr, 25)), 4
            ),
        )
        results.append(stat)

    return results
