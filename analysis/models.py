from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional
from enum import Enum


class MemberType(Enum):
    MP = "MP"
    MLA = "MLA"


class WorkStatus(Enum):
    COMPLETED = "Completed"
    IN_PROGRESS = "In Progress"
    RECOMMENDED = "Recommended"
    UNKNOWN = "Unknown"


class BenchmarkQuality(Enum):
    ACTIVITY_STATE_STRONG = "ACTIVITY_STATE_STRONG"
    ACTIVITY_STATE_GOOD = "ACTIVITY_STATE_GOOD"
    ACTIVITY_STATE_LIMITED = "ACTIVITY_STATE_LIMITED"
    ACTIVITY_NATIONAL_STRONG = "ACTIVITY_NATIONAL_STRONG"
    ACTIVITY_NATIONAL_GOOD = "ACTIVITY_NATIONAL_GOOD"
    ACTIVITY_NATIONAL_LIMITED = "ACTIVITY_NATIONAL_LIMITED"
    INSUFFICIENT = "INSUFFICIENT"


class BenchmarkPeerGroup(Enum):
    ACTIVITY_STATE = "ACTIVITY + STATE"
    ACTIVITY_NATIONAL = "ACTIVITY NATIONAL"
    NONE = "NONE"


class CostStatus(Enum):
    VERY_HIGH = "VERY_HIGH"
    HIGH = "HIGH"
    ABOVE_NORMAL = "ABOVE_NORMAL"
    NORMAL = "NORMAL"
    BELOW_NORMAL = "BELOW_NORMAL"
    VERY_LOW = "VERY_LOW"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    NO_RELIABLE_BENCHMARK = "NO_RELIABLE_BENCHMARK"


class DurationStatus(Enum):
    VERY_LONG = "VERY_LONG"
    LONG = "LONG"
    ABOVE_NORMAL = "ABOVE_NORMAL"
    NORMAL = "NORMAL"
    BELOW_NORMAL = "BELOW_NORMAL"
    VERY_SHORT = "VERY_SHORT"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    NO_RELIABLE_BENCHMARK = "NO_RELIABLE_BENCHMARK"


class RiskLevel(Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    NORMAL = "NORMAL"


@dataclass
class WorkRecord:
    work_id: int
    member_type: str
    member_id: int
    recommendation_date: Optional[date] = None
    sanction_date: Optional[date] = None
    completion_date: Optional[date] = None
    last_expenditure_date: Optional[date] = None
    recommended_amount: Optional[float] = None
    sanction_amount: Optional[float] = None
    expenditure_amount: Optional[float] = None
    completion_amount: Optional[float] = None
    activity_name: Optional[str] = None
    state_id: Optional[int] = None
    state_name: Optional[str] = None
    constituency_id: Optional[int] = None


@dataclass
class WorkAnalysis:
    work_id: int
    member_type: str
    member_id: int
    status: str
    normalized_activity: Optional[str] = None
    state_id: Optional[int] = None
    constituency_id: Optional[int] = None
    recommendation_date: Optional[date] = None
    sanction_date: Optional[date] = None
    completion_date: Optional[date] = None
    last_expenditure_date: Optional[date] = None
    recommended_amount: Optional[float] = None
    sanction_amount: Optional[float] = None
    expenditure_amount: Optional[float] = None
    completion_amount: Optional[float] = None
    sanction_delay_days: Optional[int] = None
    project_age_days: Optional[int] = None
    execution_days: Optional[int] = None
    pending_days: Optional[int] = None
    expenditure_percentage: Optional[float] = None
    completion_percentage: Optional[float] = None
    benchmark_quality: Optional[str] = None
    benchmark_peer_group: Optional[str] = None
    benchmark_sample_size: int = 0
    cost_p25: Optional[float] = None
    cost_p50: Optional[float] = None
    cost_p75: Optional[float] = None
    cost_p90: Optional[float] = None
    cost_p95: Optional[float] = None
    cost_percentile: Optional[float] = None
    cost_status: Optional[str] = None
    cost_deviation_from_median_percentage: Optional[float] = None
    duration_p25: Optional[float] = None
    duration_p50: Optional[float] = None
    duration_p75: Optional[float] = None
    duration_p90: Optional[float] = None
    duration_p95: Optional[float] = None
    duration_percentile: Optional[float] = None
    duration_status: Optional[str] = None
    duration_deviation_from_median_percentage: Optional[float] = None
    risk_flags: list = field(default_factory=list)
    flag_count: int = 0
    risk_level: str = "NORMAL"
    last_calculated: Optional[datetime] = None
    days_since_last_expenditure: Optional[int] = None
    financial_profile: Optional[str] = None
    timeline_profile: Optional[str] = None
    payment_activity_profile: Optional[str] = None
    risk_descriptions: list = field(default_factory=list)
    positive_signals: list = field(default_factory=list)


@dataclass
class BenchmarkGroup:
    group_key: str
    member_type: str
    normalized_activity: str
    state_id: Optional[int] = None
    works: list = field(default_factory=list)
    sanction_amounts: list = field(default_factory=list)
    durations: list = field(default_factory=list)
    sample_size: int = 0


@dataclass
class MemberMetrics:
    member_id: int
    member_type: str
    member_name: Optional[str] = None
    state_id: Optional[int] = None
    state_name: Optional[str] = None
    constituency_id: Optional[int] = None
    total_works: int = 0
    recommended_works: int = 0
    sanctioned_works: int = 0
    completed_works: int = 0
    ongoing_works: int = 0
    pending_works: int = 0
    recommended_amount: float = 0
    sanctioned_amount: float = 0
    expenditure_amount: float = 0
    completion_amount: float = 0
    unspent_amount: float = 0
    avg_sanction_delay_days: Optional[float] = None
    median_sanction_delay_days: Optional[float] = None
    avg_execution_days: Optional[float] = None
    median_execution_days: Optional[float] = None
    avg_project_age_days: Optional[float] = None
    max_project_age_days: Optional[int] = None
    avg_pending_days: Optional[float] = None
    max_pending_days: Optional[int] = None
    flagged_works: int = 0
    medium_risk_works: int = 0
    high_risk_works: int = 0
    flagged_rate_pct: float = 0
    high_risk_rate_pct: float = 0
    cost_anomaly_works: int = 0
    duration_anomaly_works: int = 0
    expenditure_over_sanction_works: int = 0
    expenditure_over_recommendation_works: int = 0
    negative_sanction_delay_works: int = 0
    negative_execution_works: int = 0
    completion_before_sanction_works: int = 0
    expenditure_before_sanction_works: int = 0
    overdue_over_1_year: int = 0
    overdue_over_2_years: int = 0
    avg_cost_percentile: Optional[float] = None
    avg_duration_percentile: Optional[float] = None
    avg_cost_deviation_pct: Optional[float] = None
    avg_duration_deviation_pct: Optional[float] = None
    completion_rate_pct: float = 0
    sanction_rate_pct: float = 0
    sanction_conversion_pct: float = 0
    expenditure_sanction_utilization_pct: float = 0
    expenditure_recommendation_pct: float = 0
    # Per-work sanction cost statistics (populated from work_analysis,
    # not from a member-level ratio). None when no work has a sanction amount.
    avg_work_cost: Optional[float] = None
    median_work_cost: Optional[float] = None
    zero_work_member: bool = False
    low_sample_member: bool = False
    ranking_qualified: bool = False


@dataclass
class StateMetrics:
    state_id: int
    state_name: Optional[str] = None
    member_type: Optional[str] = None
    total_works: int = 0
    active_members: int = 0
    mp_active_members: int = 0
    mla_active_members: int = 0
    recommended_works: int = 0
    sanctioned_works: int = 0
    completed_works: int = 0
    ongoing_works: int = 0
    recommended_amount: float = 0
    sanctioned_amount: float = 0
    expenditure_amount: float = 0
    completion_amount: float = 0
    avg_sanction_delay_days: Optional[float] = None
    median_sanction_delay_days: Optional[float] = None
    avg_execution_days: Optional[float] = None
    median_execution_days: Optional[float] = None
    flagged_works: int = 0
    high_risk_works: int = 0
    overdue_over_1_year: int = 0
    overdue_over_2_years: int = 0
    completion_rate_pct: float = 0
    sanction_rate_pct: float = 0
    expenditure_utilization_pct: float = 0
    sanction_conversion_pct: float = 0
    risk_rate_pct: float = 0
    cost_anomaly_works: int = 0
    duration_anomaly_works: int = 0
    expenditure_over_sanction_works: int = 0
    expenditure_over_recommendation_works: int = 0
    negative_execution_works: int = 0
    negative_sanction_delay_works: int = 0
    # Per-work sanction cost statistics (see MemberMetrics).
    avg_work_cost: Optional[float] = None
    median_work_cost: Optional[float] = None
    ranking_qualified: bool = False


@dataclass
class NationalStatistics:
    metric_name: str
    member_type: Optional[str] = None
    count: int = 0
    mean: Optional[float] = None
    std_dev: Optional[float] = None
    minimum: Optional[float] = None
    p25: Optional[float] = None
    median: Optional[float] = None
    p75: Optional[float] = None
    p90: Optional[float] = None
    p95: Optional[float] = None
    maximum: Optional[float] = None
    iqr: Optional[float] = None
    calculated_at: Optional[datetime] = None


@dataclass
class TrendRecord:
    year: int
    member_type: str
    is_partial_year: bool = False
    total_works: int = 0
    recommended_works: int = 0
    sanctioned_works: int = 0
    completed_works: int = 0
    ongoing_works: int = 0
    recommended_amount: float = 0
    sanctioned_amount: float = 0
    expenditure_amount: float = 0
    completion_amount: float = 0
    avg_sanction_delay_days: Optional[float] = None
    avg_execution_days: Optional[float] = None
    completion_rate_pct: float = 0
