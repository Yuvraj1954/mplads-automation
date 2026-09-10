from datetime import date, timedelta

from analysis.work_profile import (
    compute_days_since_last_expenditure,
    compute_financial_profile,
    compute_timeline_profile,
    compute_payment_activity_profile,
)
from analysis.risk import (
    describe_risk_flags,
    compute_positive_signals,
    RISK_FLAG_DESCRIPTIONS,
    compute_risk_flags,
    classify_risk_level,
)
from analysis.work_analysis import compute_work_analyses


REF = date(2026, 9, 4)


class TestDaysSinceLastExpenditure:

    def test_valid_date(self):
        d = date(2026, 6, 1)
        assert compute_days_since_last_expenditure(d, REF) == 95

    def test_missing_date(self):
        assert compute_days_since_last_expenditure(None, REF) is None

    def test_missing_reference(self):
        assert compute_days_since_last_expenditure(date(2026, 6, 1),
                                                    None) is None

    def test_both_missing(self):
        assert compute_days_since_last_expenditure(None, None) is None

    def test_today(self):
        assert compute_days_since_last_expenditure(REF, REF) == 0

    def test_deterministic(self):
        d = date(2025, 1, 1)
        result = compute_days_since_last_expenditure(d, REF)
        assert result == 611
        assert compute_days_since_last_expenditure(d, REF) == result


class TestFinancialProfile:

    def test_strong_completed(self):
        assert compute_financial_profile(95.0, "Completed") == "STRONG"

    def test_strong_in_progress(self):
        assert compute_financial_profile(92.0, "In Progress") == "STRONG"

    def test_adequate_completed(self):
        assert compute_financial_profile(70.0, "Completed") == "ADEQUATE"

    def test_adequate_in_progress(self):
        assert compute_financial_profile(60.0, "In Progress") == "ADEQUATE"

    def test_low_completed(self):
        assert compute_financial_profile(30.0, "Completed") == "LOW"

    def test_low_in_progress(self):
        assert compute_financial_profile(10.0, "In Progress") == "LOW"

    def test_unknown_completed_no_data(self):
        assert compute_financial_profile(None, "Completed") == "UNKNOWN"

    def test_unknown_in_progress_no_data(self):
        assert compute_financial_profile(None, "In Progress") == "UNKNOWN"

    def test_unknown_recommended(self):
        assert compute_financial_profile(None, "Recommended") == "UNKNOWN"

    def test_unknown_recommended_with_data(self):
        assert compute_financial_profile(80.0, "Recommended") == "UNKNOWN"

    def test_boundary_90(self):
        assert compute_financial_profile(90.0, "Completed") == "STRONG"

    def test_boundary_89(self):
        assert compute_financial_profile(89.9, "Completed") == "ADEQUATE"

    def test_boundary_50(self):
        assert compute_financial_profile(50.0, "Completed") == "ADEQUATE"

    def test_boundary_49(self):
        assert compute_financial_profile(49.9, "Completed") == "LOW"

    def test_boundary_100(self):
        assert compute_financial_profile(100.0, "Completed") == "STRONG"


class TestTimelineProfile:

    def test_completed(self):
        assert compute_timeline_profile("Completed", None, None,
                                        100) == "ON_TRACK"

    def test_completed_negative_delay(self):
        assert compute_timeline_profile("Completed", None, None,
                                        -5) == "UNKNOWN"

    def test_recommended(self):
        assert compute_timeline_profile("Recommended", 100, 250,
                                        None) == "UNKNOWN"

    def test_no_pending(self):
        assert compute_timeline_profile("In Progress", None, 250,
                                        None) == "UNKNOWN"

    def test_negative_pending(self):
        assert compute_timeline_profile("In Progress", -5, 250,
                                        None) == "UNKNOWN"

    def test_on_track(self):
        assert compute_timeline_profile("In Progress", 100, 250,
                                        None) == "ON_TRACK"

    def test_on_track_exactly_threshold(self):
        assert compute_timeline_profile("In Progress", 250, 250,
                                        None) == "ON_TRACK"

    def test_delayed(self):
        assert compute_timeline_profile("In Progress", 300, 250,
                                        None) == "DELAYED"

    def test_severely_delayed(self):
        assert compute_timeline_profile("In Progress", 400, 250,
                                        None) == "SEVERELY_DELAYED"

    def test_no_duration_p90_uses_365(self):
        assert compute_timeline_profile("In Progress", 100, None,
                                        None) == "ON_TRACK"

    def test_no_duration_p90_delayed(self):
        assert compute_timeline_profile("In Progress", 400, None,
                                        None) == "DELAYED"

    def test_no_duration_p90_severe(self):
        assert compute_timeline_profile("In Progress", 550, None,
                                        None) == "SEVERELY_DELAYED"

    def test_negative_delay_does_not_contaminate(self):
        r = compute_timeline_profile("Completed", None, None, -10)
        assert r == "UNKNOWN"


class TestPaymentActivityProfile:

    def test_completed(self):
        assert compute_payment_activity_profile(
            date(2026, 6, 1), REF, "Completed") == "NO_DATA"

    def test_active(self):
        assert compute_payment_activity_profile(
            date(2026, 8, 1), REF, "In Progress") == "ACTIVE"

    def test_active_boundary(self):
        d = REF - timedelta(days=180)
        assert compute_payment_activity_profile(
            d, REF, "In Progress") == "ACTIVE"

    def test_slowing(self):
        d = REF - timedelta(days=181)
        assert compute_payment_activity_profile(
            d, REF, "In Progress") == "SLOWING"

    def test_slowing_boundary(self):
        d = REF - timedelta(days=365)
        assert compute_payment_activity_profile(
            d, REF, "In Progress") == "SLOWING"

    def test_stalled(self):
        d = REF - timedelta(days=366)
        assert compute_payment_activity_profile(
            d, REF, "In Progress") == "STALLED"

    def test_no_data(self):
        assert compute_payment_activity_profile(
            None, REF, "In Progress") == "NO_DATA"

    def test_no_reference(self):
        assert compute_payment_activity_profile(
            date(2026, 6, 1), None, "In Progress") == "NO_DATA"

    def test_negative_days(self):
        assert compute_payment_activity_profile(
            REF + timedelta(days=5), REF, "In Progress") == "NO_DATA"


class TestRiskDescriptions:

    def test_all_flags_have_descriptions(self):
        all_flags = [
            "SANCTION_DELAY_VERY_HIGH", "SANCTION_DELAY_HIGH",
            "SANCTION_DELAY_ELEVATED", "LONG_PENDING",
            "LOW_EXPENDITURE", "PAYMENT_STAGNATION",
            "COST_ANOMALY", "DURATION_ANOMALY",
            "OVER_EXPENDITURE", "POST_COMPLETION_PAYMENT",
            "SOURCE_DATA_DEFECT_NEGATIVE_DELAY",
        ]
        for flag in all_flags:
            assert flag in RISK_FLAG_DESCRIPTIONS, (
                f"{flag} missing from RISK_FLAG_DESCRIPTIONS"
            )

    def test_descriptions_have_required_fields(self):
        for flag, desc in RISK_FLAG_DESCRIPTIONS.items():
            assert "flag" in desc, f"{flag} missing 'flag'"
            assert "severity" in desc, f"{flag} missing 'severity'"
            assert "description" in desc, f"{flag} missing 'description'"
            assert desc["flag"] == flag
            assert desc["severity"] in ("FLAGGED", "WARNING", "INFO")
            assert len(desc["description"]) > 10

    def test_no_accusatory_language(self):
        banned = ["fraud", "corruption", "misuse", "embezzlement",
                  "bribery", "theft"]
        for flag, desc in RISK_FLAG_DESCRIPTIONS.items():
            lower = desc["description"].lower()
            for word in banned:
                assert word not in lower, (
                    f"{flag} description contains '{word}'"
                )

    def test_describe_risk_flags_returns_correct_count(self):
        work = {"sanction_delay_days": 200}
        result = describe_risk_flags(
            ["SANCTION_DELAY_HIGH", "LONG_PENDING"], work
        )
        assert len(result) == 2

    def test_describe_risk_flags_enriches_with_values(self):
        work = {"sanction_delay_days": 350}
        result = describe_risk_flags(["SANCTION_DELAY_VERY_HIGH"], work)
        assert "350 days" in result[0]["description"]

    def test_describe_risk_flags_long_pending_enriched(self):
        work = {"pending_days": 400}
        result = describe_risk_flags(["LONG_PENDING"], work)
        assert "400 days" in result[0]["description"]

    def test_describe_risk_flags_empty(self):
        result = describe_risk_flags([], {})
        assert result == []

    def test_describe_risk_flags_unknown_flag(self):
        result = describe_risk_flags(["UNKNOWN_FLAG"], {})
        assert len(result) == 1
        assert result[0]["flag"] == "UNKNOWN_FLAG"


class TestPositiveSignals:

    def test_sanction_matches_recommendation(self):
        work = {"recommended_amount": 500000, "sanction_amount": 500000}
        signals = compute_positive_signals(work)
        sigs = [s["signal"] for s in signals]
        assert "SANCTION_MATCHES_RECOMMENDATION" in sigs

    def test_sanction_mismatch(self):
        work = {"recommended_amount": 500000, "sanction_amount": 400000}
        signals = compute_positive_signals(work)
        sigs = [s["signal"] for s in signals]
        assert "SANCTION_MATCHES_RECOMMENDATION" not in sigs

    def test_recent_payment(self):
        work = {
            "last_expenditure_date": REF - timedelta(days=30),
            "_reference_date": REF,
            "status": "In Progress",
        }
        signals = compute_positive_signals(work)
        sigs = [s["signal"] for s in signals]
        assert "RECENT_PAYMENT_ACTIVITY" in sigs

    def test_old_payment_not_recent(self):
        work = {
            "last_expenditure_date": REF - timedelta(days=200),
            "_reference_date": REF,
            "status": "In Progress",
        }
        signals = compute_positive_signals(work)
        sigs = [s["signal"] for s in signals]
        assert "RECENT_PAYMENT_ACTIVITY" not in sigs

    def test_expenditure_on_track(self):
        work = {"expenditure_percentage": 85.0}
        signals = compute_positive_signals(work)
        sigs = [s["signal"] for s in signals]
        assert "EXPENDITURE_ON_TRACK" in sigs

    def test_expenditure_low_not_on_track(self):
        work = {"expenditure_percentage": 30.0}
        signals = compute_positive_signals(work)
        sigs = [s["signal"] for s in signals]
        assert "EXPENDITURE_ON_TRACK" not in sigs

    def test_completion_recorded(self):
        work = {"status": "Completed"}
        signals = compute_positive_signals(work)
        sigs = [s["signal"] for s in signals]
        assert "COMPLETION_RECORDED" in sigs

    def test_cost_within_peer_range(self):
        work = {"cost_status": "NORMAL"}
        signals = compute_positive_signals(work)
        sigs = [s["signal"] for s in signals]
        assert "COST_WITHIN_PEER_RANGE" in sigs

    def test_duration_within_peer_range(self):
        work = {"duration_status": "NORMAL"}
        signals = compute_positive_signals(work)
        sigs = [s["signal"] for s in signals]
        assert "DURATION_WITHIN_PEER_RANGE" in sigs

    def test_no_false_positive_empty_work(self):
        signals = compute_positive_signals({})
        assert isinstance(signals, list)

    def test_signals_have_required_fields(self):
        work = {
            "recommended_amount": 100,
            "sanction_amount": 100,
            "last_expenditure_date": REF - timedelta(days=10),
            "_reference_date": REF,
            "status": "In Progress",
            "expenditure_percentage": 95.0,
            "cost_status": "NORMAL",
            "duration_status": "NORMAL",
        }
        signals = compute_positive_signals(work)
        for sig in signals:
            assert "signal" in sig
            assert "description" in sig
            assert len(sig["description"]) > 5


class TestEndToEndWorkProfile:

    def _make_work(self, **overrides):
        base = {
            "work_id": 1,
            "member_type": "MP",
            "member_id": 100,
            "state_id": 1,
            "constituency_id": 10,
            "activity_name": "Construction of Road",
            "recommendation_date": date(2025, 1, 1),
            "sanction_date": date(2025, 3, 1),
            "completion_date": None,
            "last_expenditure_date": None,
            "recommended_amount": 500000,
            "sanction_amount": 500000,
            "expenditure_amount": None,
            "completion_amount": None,
        }
        base.update(overrides)
        return [base]

    def test_work_has_all_new_fields(self):
        works = self._make_work()
        result = compute_work_analyses(works, REF)
        w = result[0]
        assert hasattr(w, "days_since_last_expenditure")
        assert hasattr(w, "financial_profile")
        assert hasattr(w, "timeline_profile")
        assert hasattr(w, "payment_activity_profile")
        assert hasattr(w, "risk_descriptions")
        assert hasattr(w, "positive_signals")

    def test_risk_level_unchanged(self):
        works = self._make_work(
            sanction_date=date(2025, 1, 15),
        )
        result = compute_work_analyses(works, REF)
        w = result[0]
        assert w.risk_level == "NORMAL"
        assert w.flag_count == 0

    def test_completed_work_profile(self):
        works = self._make_work(
            completion_date=date(2025, 12, 1),
            last_expenditure_date=date(2025, 11, 1),
            expenditure_amount=500000,
        )
        result = compute_work_analyses(works, REF)
        w = result[0]
        assert w.status == "Completed"
        assert w.financial_profile in ("STRONG", "ADEQUATE", "LOW")
        assert w.timeline_profile == "ON_TRACK"
        assert w.payment_activity_profile == "NO_DATA"

    def test_in_progress_with_expenditure(self):
        works = self._make_work(
            last_expenditure_date=REF - timedelta(days=30),
            expenditure_amount=200000,
        )
        result = compute_work_analyses(works, REF)
        w = result[0]
        assert w.days_since_last_expenditure == 30
        assert w.financial_profile in ("STRONG", "ADEQUATE", "LOW", "UNKNOWN")
        assert w.payment_activity_profile == "ACTIVE"

    def test_recommended_work_profile(self):
        works = self._make_work(
            sanction_date=None,
            recommendation_date=date(2026, 8, 1),
        )
        result = compute_work_analyses(works, REF)
        w = result[0]
        assert w.status == "Recommended"
        assert w.financial_profile == "UNKNOWN"
        assert w.timeline_profile == "UNKNOWN"
        assert w.risk_flags == []
        assert w.risk_level == "NORMAL"

    def test_old_work_high_risk(self):
        works = self._make_work(
            sanction_date=date(2025, 1, 15),
            last_expenditure_date=REF - timedelta(days=500),
        )
        result = compute_work_analyses(works, REF)
        w = result[0]
        assert w.days_since_last_expenditure == 500
        assert w.payment_activity_profile == "STALLED"

    def test_positive_signal_present(self):
        works = self._make_work(
            recommended_amount=500000,
            sanction_amount=500000,
        )
        result = compute_work_analyses(works, REF)
        w = result[0]
        sigs = [s["signal"] for s in w.positive_signals]
        assert "SANCTION_MATCHES_RECOMMENDATION" in sigs
