from datetime import date, datetime, timezone

from analysis.work_analysis import compute_work_analyses
from analysis.phase_a_member import compute_member_metrics
from analysis.phase_a_state import compute_state_metrics
from analysis.phase_a_statistics import compute_statistics
from analysis.phase_a_trends import compute_trends
from analysis.affected import (
    expand_affected_works,
    expand_time_sensitive,
)


class AnalysisPipeline:
    def __init__(self, works, member_metrics_by_id=None,
                 state_metrics_by_id=None):
        self.works = works
        self.works_by_id = {w["work_id"]: w for w in works}
        self.member_metrics_by_id = member_metrics_by_id or {}
        self.state_metrics_by_id = state_metrics_by_id or {}
        self.work_analyses = []
        self.member_metrics = []
        self.state_metrics = []
        self.statistics = []
        self.trends = []

        self.mp_member_metrics = []
        self.mla_member_metrics = []
        self.mp_state_metrics = []
        self.mla_state_metrics = []
        self.mp_statistics = []
        self.mla_statistics = []
        self.mp_trends = []
        self.mla_trends = []

    def run_full(self, reference_date=None):
        if reference_date is None:
            reference_date = date.today()

        self.work_analyses = compute_work_analyses(
            self.works, reference_date
        )

        self.member_metrics = compute_member_metrics(
            self.work_analyses
        )
        self.member_metrics_by_id = {
            (m.member_type, m.member_id): m
            for m in self.member_metrics
        }

        self.state_metrics = compute_state_metrics(
            self.work_analyses, self.member_metrics_by_id
        )
        self.state_metrics_by_id = {
            m.state_id: m for m in self.state_metrics
        }

        self.statistics = compute_statistics(self.member_metrics)
        self.trends = compute_trends(self.work_analyses,
                                      reference_date=reference_date)

        self.mp_member_metrics = compute_member_metrics(
            self.work_analyses, member_type_filter="MP"
        )
        self.mla_member_metrics = compute_member_metrics(
            self.work_analyses, member_type_filter="MLA"
        )

        self.mp_state_metrics = compute_state_metrics(
            self.work_analyses, self.member_metrics_by_id,
            member_type_filter="MP"
        )
        self.mla_state_metrics = compute_state_metrics(
            self.work_analyses, self.member_metrics_by_id,
            member_type_filter="MLA"
        )

        self.mp_statistics = compute_statistics(
            self.member_metrics, member_type_filter="MP"
        )
        self.mla_statistics = compute_statistics(
            self.member_metrics, member_type_filter="MLA"
        )

        self.mp_trends = compute_trends(
            self.work_analyses, member_type_filter="MP",
            reference_date=reference_date
        )
        self.mla_trends = compute_trends(
            self.work_analyses, member_type_filter="MLA",
            reference_date=reference_date
        )

        return self

    def run_delta(self, delta_work_ids, reference_date=None):
        if reference_date is None:
            reference_date = date.today()

        affected_works, affected_members = expand_affected_works(
            delta_work_ids, self.works_by_id,
            {}, reference_date
        )

        time_sensitive = expand_time_sensitive(
            self.works_by_id, {}, reference_date
        )
        affected_works.update(time_sensitive)

        affected_work_list = [
            self.works_by_id[wid]
            for wid in affected_works
            if wid in self.works_by_id
        ]

        if not affected_work_list:
            return self

        self.work_analyses = compute_work_analyses(
            affected_work_list, reference_date
        )

        self.member_metrics = compute_member_metrics(
            self.work_analyses
        )

        self.state_metrics = compute_state_metrics(
            self.work_analyses, self.member_metrics_by_id
        )

        self.statistics = compute_statistics(self.member_metrics)
        self.trends = compute_trends(self.work_analyses,
                                      reference_date=reference_date)

        return self
