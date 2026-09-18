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
        import time as _t
        if reference_date is None:
            reference_date = date.today()

        t0 = _t.time()
        print("    [pipeline] compute_work_analyses...", end=" ", flush=True)
        self.work_analyses = compute_work_analyses(
            self.works, reference_date
        )
        print(f"OK ({len(self.work_analyses)} analyses, {_t.time()-t0:.1f}s)", flush=True)

        t0 = _t.time()
        print("    [pipeline] compute_member_metrics...", end=" ", flush=True)
        self.member_metrics = compute_member_metrics(
            self.work_analyses
        )
        self.member_metrics_by_id = {
            (m.member_type, m.member_id): m
            for m in self.member_metrics
        }
        print(f"OK ({len(self.member_metrics)} members, {_t.time()-t0:.1f}s)", flush=True)

        t0 = _t.time()
        print("    [pipeline] compute_state_metrics...", end=" ", flush=True)
        self.state_metrics = compute_state_metrics(
            self.work_analyses, self.member_metrics_by_id
        )
        self.state_metrics_by_id = {
            m.state_id: m for m in self.state_metrics
        }
        print(f"OK ({len(self.state_metrics)} states, {_t.time()-t0:.1f}s)", flush=True)

        t0 = _t.time()
        print("    [pipeline] compute_statistics...", end=" ", flush=True)
        self.statistics = compute_statistics(self.member_metrics)
        print(f"OK ({_t.time()-t0:.1f}s)", flush=True)

        t0 = _t.time()
        print("    [pipeline] compute_trends (ALL)...", end=" ", flush=True)
        self.trends = compute_trends(self.work_analyses,
                                      reference_date=reference_date)
        print(f"OK ({_t.time()-t0:.1f}s)", flush=True)

        t0 = _t.time()
        print("    [pipeline] compute MP/MLA splits (parallel)...", end=" ", flush=True)
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def _compute_mp():
            mp_mm = compute_member_metrics(self.work_analyses, member_type_filter="MP")
            mp_sm = compute_state_metrics(self.work_analyses, self.member_metrics_by_id, member_type_filter="MP")
            mp_st = compute_statistics(self.member_metrics, member_type_filter="MP")
            mp_tr = compute_trends(self.work_analyses, member_type_filter="MP", reference_date=reference_date)
            return mp_mm, mp_sm, mp_st, mp_tr

        def _compute_mla():
            mla_mm = compute_member_metrics(self.work_analyses, member_type_filter="MLA")
            mla_sm = compute_state_metrics(self.work_analyses, self.member_metrics_by_id, member_type_filter="MLA")
            mla_st = compute_statistics(self.member_metrics, member_type_filter="MLA")
            mla_tr = compute_trends(self.work_analyses, member_type_filter="MLA", reference_date=reference_date)
            return mla_mm, mla_sm, mla_st, mla_tr

        with ThreadPoolExecutor(max_workers=2) as executor:
            mp_future = executor.submit(_compute_mp)
            mla_future = executor.submit(_compute_mla)
            self.mp_member_metrics, self.mp_state_metrics, self.mp_statistics, self.mp_trends = mp_future.result()
            self.mla_member_metrics, self.mla_state_metrics, self.mla_statistics, self.mla_trends = mla_future.result()
        print(f"OK ({_t.time()-t0:.1f}s)", flush=True)

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

        self.member_metrics_by_id = {
            (m.member_type, m.member_id): m
            for m in self.member_metrics
        }

        self.state_metrics = compute_state_metrics(
            self.work_analyses, self.member_metrics_by_id
        )

        self.statistics = compute_statistics(self.member_metrics)
        self.trends = compute_trends(self.work_analyses,
                                      reference_date=reference_date)

        return self
