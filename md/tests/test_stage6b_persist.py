"""Tests for Stage 6B work analysis persistence — delete-then-insert ordering
and ML fingerprint preservation lifecycle.

Tests cover:
- Rerunning an existing work_id does not produce duplicate-key error
- Exactly one fresh record remains after rerun
- Delete phase runs before insert phase
- Both MP and MLA tables are handled correctly
- ML fingerprint preservation when features unchanged
- ML staleness detection when features change
- ML values left NULL for new works
- category_metrics / fy_views SQL correctness
"""
import hashlib
import unittest
from unittest.mock import patch, MagicMock, call
from dataclasses import dataclass, field
from datetime import date


@dataclass
class MockWorkAnalysis:
    work_id: int
    member_id: int
    member_type: str = "MP"
    constituency_id: str = ""
    state_id: str = ""
    normalized_activity: str = ""
    status: str = "IN_PROGRESS"
    recommended_amount: float = 0
    sanction_amount: float = 0
    expenditure_amount: float = 0
    completion_amount: float = 0
    recommendation_date: date = None
    sanction_date: date = None
    completion_date: date = None
    last_expenditure_date: date = None
    first_expenditure_date: date = None
    sanction_delay_days: int = 0
    project_age_days: int = 0
    execution_days: int = 0
    pending_days: int = 0
    expenditure_percentage: float = 0
    completion_percentage: float = 0
    benchmark_peer_group: str = ""
    benchmark_quality: str = ""
    benchmark_sample_size: int = 0
    cost_p25: float = 0
    cost_p50: float = 0
    cost_p75: float = 0
    cost_p90: float = 0
    cost_p95: float = 0
    duration_p25: float = 0
    duration_p50: float = 0
    duration_p75: float = 0
    duration_p90: float = 0
    duration_p95: float = 0
    cost_percentile: float = 0
    duration_percentile: float = 0
    cost_status: str = ""
    duration_status: str = ""
    cost_deviation_from_median_percentage: float = 0
    duration_deviation_from_median_percentage: float = 0
    risk_flags: list = field(default_factory=list)
    flag_count: int = 0
    risk_level: str = "LOW"
    risk_score: float = 0
    activity_name: str = ""
    work_description: str = ""
    work_category: str = ""
    state_name: str = ""


def _compute_fp(sanction_amount=0, recommended_amount=0, expenditure_amount=0,
                sanction_delay_days=0, execution_days=0, project_age_days=0,
                cost_percentile=0, duration_percentile=0):
    """Helper: compute feature fingerprint matching pipeline logic."""
    cols = ("sanction_amount", "recommended_amount", "expenditure_amount",
            "sanction_delay_days", "execution_days", "project_age_days",
            "cost_percentile", "duration_percentile")
    vals = [sanction_amount, recommended_amount, expenditure_amount,
            sanction_delay_days, execution_days, project_age_days,
            cost_percentile, duration_percentile]
    parts = [f"{c}={v}" for c, v in zip(cols, vals)]
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


class TestStage6bDeleteInsertOrder:
    """Tests for delete-before-insert ordering in stage_work_analysis_persist."""

    @patch("automation.pipeline_controller.get_db2")
    @patch("automation.pipeline_controller.sb_delete")
    @patch("supabase.create_client")
    def test_rerun_existing_work_id_succeeds(self, mock_create_client, mock_sb_delete, mock_get_db2):
        """Rerunning a work_id that already exists should not error."""
        from automation.pipeline_controller import _sb_clients
        _sb_clients.clear()
        mock_get_db2.return_value = ("http://test", "test-key")
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_sb_delete.return_value = 200

        from automation.pipeline_controller import stage_work_analysis_persist

        wa = MockWorkAnalysis(work_id=154145, member_id=1, member_type="MP")
        result = stage_work_analysis_persist([wa], affected_work_ids={154145})

        assert result["total"] == 1
        assert result["mp_written"] == 1

    @patch("automation.pipeline_controller.get_db2")
    @patch("automation.pipeline_controller.sb_delete")
    @patch("supabase.create_client")
    def test_delete_runs_before_insert(self, mock_create_client, mock_sb_delete, mock_get_db2):
        """Verify delete phase executes before insert phase."""
        from automation.pipeline_controller import _sb_clients
        _sb_clients.clear()
        mock_get_db2.return_value = ("http://test", "test-key")
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client

        call_order = []

        def batch_delete_side_effect(*args, **kwargs):
            call_order.append("delete")
            return MagicMock()

        def sb_delete_side_effect(*args, **kwargs):
            call_order.append("delete")
            return 200

        def insert_side_effect(*args, **kwargs):
            call_order.append("insert")
            return MagicMock()

        mock_client.table.return_value.delete.return_value.in_.return_value.execute.side_effect = batch_delete_side_effect
        mock_sb_delete.side_effect = sb_delete_side_effect
        mock_client.table.return_value.insert.return_value.execute.side_effect = insert_side_effect

        from automation.pipeline_controller import stage_work_analysis_persist

        wa = MockWorkAnalysis(work_id=999, member_id=1, member_type="MP")
        stage_work_analysis_persist([wa], affected_work_ids={999})

        assert "delete" in call_order, f"No delete call. Order: {call_order}"
        assert "insert" in call_order, f"No insert call. Order: {call_order}"
        assert call_order.index("delete") < call_order.index("insert"), \
            f"Expected delete before insert, got: {call_order}"

    @patch("automation.pipeline_controller.get_db2")
    @patch("automation.pipeline_controller.sb_delete")
    @patch("supabase.create_client")
    def test_mla_work_id_offset_handled(self, mock_create_client, mock_sb_delete, mock_get_db2):
        """MLA work_ids with +1000000 offset are deleted correctly."""
        from automation.pipeline_controller import _sb_clients
        _sb_clients.clear()
        mock_get_db2.return_value = ("http://test", "test-key")
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_sb_delete.return_value = 200

        from automation.pipeline_controller import stage_work_analysis_persist

        wa = MockWorkAnalysis(work_id=1159378, member_id=2, member_type="MLA")
        result = stage_work_analysis_persist([wa], affected_work_ids={1159378})

        assert result["total"] == 1
        assert result["mla_written"] == 1

        delete_calls_in = [str(c) for c in mock_client.table.return_value.delete.return_value.in_.call_args_list]
        delete_calls_sb = [str(c) for c in mock_sb_delete.call_args_list]
        all_delete_calls = delete_calls_in + delete_calls_sb
        assert any("1159378" in c for c in all_delete_calls), \
            f"MLA work_id not deleted: in_={delete_calls_in}, sb_delete={delete_calls_sb}"

    @patch("automation.pipeline_controller.get_db2")
    @patch("automation.pipeline_controller.sb_delete")
    @patch("supabase.create_client")
    def test_full_mode_no_delete(self, mock_create_client, mock_sb_delete, mock_get_db2):
        """Full mode (affected_work_ids=None) should only insert, no delete."""
        from automation.pipeline_controller import _sb_clients
        _sb_clients.clear()
        mock_get_db2.return_value = ("http://test", "test-key")
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client

        from automation.pipeline_controller import stage_work_analysis_persist

        wa = MockWorkAnalysis(work_id=100, member_id=1, member_type="MP")
        result = stage_work_analysis_persist([wa], affected_work_ids=None)

        assert result["total"] == 1
        mock_sb_delete.assert_not_called()


class TestMLFingerprintPreservation:
    """TEST A/B/C: ML fingerprint preservation lifecycle."""

    @patch("automation.pipeline_controller.get_db2")
    @patch("automation.pipeline_controller.sb_delete")
    @patch("supabase.create_client")
    def test_ml_preserved_when_features_unchanged(self, mock_create_client, mock_sb_delete, mock_get_db2):
        """TEST A (inverse): Features unchanged → fingerprint match → ML preserved."""
        from automation.pipeline_controller import _sb_clients
        _sb_clients.clear()
        mock_get_db2.return_value = ("http://test", "test-key")
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_sb_delete.return_value = 200

        # Simulate existing row with ML values and matching fingerprint
        existing_fp = _compute_fp(sanction_amount=50000, recommended_amount=60000,
                                  expenditure_amount=40000, sanction_delay_days=30,
                                  execution_days=200, project_age_days=300,
                                  cost_percentile=0.5, duration_percentile=0.4)
        existing_row = {
            "work_id": 42,
            "delay_probability": 0.35,
            "delay_risk_band": "MODERATE",
            "isolation_score": 0.25,
            "isolation_level": "NORMAL",
            "feature_fingerprint": existing_fp,
        }

        # Mock the ML fetch query (for affected_work_ids)
        mock_resp = MagicMock()
        mock_resp.data = [existing_row]
        mock_select_obj = MagicMock()
        mock_select_obj.execute.return_value = mock_resp
        mock_select_obj.in_.return_value = mock_select_obj
        mock_client.table.return_value.select.return_value = mock_select_obj

        from automation.pipeline_controller import stage_work_analysis_persist

        # Work analysis with SAME ML-relevant features (fingerprint should match)
        wa = MockWorkAnalysis(
            work_id=42, member_id=1, member_type="MP",
            sanction_amount=50000, recommended_amount=60000,
            expenditure_amount=40000, sanction_delay_days=30,
            execution_days=200, project_age_days=300,
            cost_percentile=0.5, duration_percentile=0.4,
        )
        result = stage_work_analysis_persist([wa], affected_work_ids={42})

        assert result["total"] == 1
        assert result["ml_preserved"] == 1
        assert result["ml_stale"] == 0
        assert result["ml_new"] == 0

    @patch("automation.pipeline_controller.get_db2")
    @patch("automation.pipeline_controller.sb_delete")
    @patch("supabase.create_client")
    def test_ml_stale_when_features_change(self, mock_create_client, mock_sb_delete, mock_get_db2):
        """TEST B: Features changed → fingerprint mismatch → ML stale (not preserved)."""
        from automation.pipeline_controller import _sb_clients
        _sb_clients.clear()
        mock_get_db2.return_value = ("http://test", "test-key")
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_sb_delete.return_value = 200

        # Existing row has OLD fingerprint (different from what new analysis will produce)
        existing_fp = _compute_fp(sanction_amount=50000, recommended_amount=60000,
                                  expenditure_amount=40000, sanction_delay_days=30,
                                  execution_days=200, project_age_days=300,
                                  cost_percentile=0.5, duration_percentile=0.4)
        existing_row = {
            "work_id": 42,
            "delay_probability": 0.35,
            "delay_risk_band": "MODERATE",
            "isolation_score": 0.25,
            "isolation_level": "NORMAL",
            "feature_fingerprint": existing_fp,
        }

        mock_resp = MagicMock()
        mock_resp.data = [existing_row]
        mock_select_obj = MagicMock()
        mock_select_obj.execute.return_value = mock_resp
        mock_select_obj.in_.return_value = mock_select_obj
        mock_client.table.return_value.select.return_value = mock_select_obj

        from automation.pipeline_controller import stage_work_analysis_persist

        # Work analysis with DIFFERENT ML-relevant features (sanction_amount changed)
        wa = MockWorkAnalysis(
            work_id=42, member_id=1, member_type="MP",
            sanction_amount=99999, recommended_amount=60000,  # changed!
            expenditure_amount=40000, sanction_delay_days=30,
            execution_days=200, project_age_days=300,
            cost_percentile=0.5, duration_percentile=0.4,
        )
        result = stage_work_analysis_persist([wa], affected_work_ids={42})

        assert result["total"] == 1
        assert result["ml_preserved"] == 0
        assert result["ml_stale"] == 1
        assert result["ml_new"] == 0

    @patch("automation.pipeline_controller.get_db2")
    @patch("automation.pipeline_controller.sb_delete")
    @patch("supabase.create_client")
    def test_new_work_gets_null_ml(self, mock_create_client, mock_sb_delete, mock_get_db2):
        """TEST C (part 1): New work (no existing record) → ML left NULL → new count."""
        from automation.pipeline_controller import _sb_clients
        _sb_clients.clear()
        mock_get_db2.return_value = ("http://test", "test-key")
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_sb_delete.return_value = 200

        # No existing row for this work_id
        mock_resp = MagicMock()
        mock_resp.data = []
        mock_select_obj = MagicMock()
        mock_select_obj.execute.return_value = mock_resp
        mock_select_obj.in_.return_value = mock_select_obj
        mock_client.table.return_value.select.return_value = mock_select_obj

        from automation.pipeline_controller import stage_work_analysis_persist

        wa = MockWorkAnalysis(work_id=99999, member_id=1, member_type="MP")
        result = stage_work_analysis_persist([wa], affected_work_ids={99999})

        assert result["total"] == 1
        assert result["ml_preserved"] == 0
        assert result["ml_stale"] == 0
        assert result["ml_new"] == 1

    @patch("automation.pipeline_controller.get_db2")
    @patch("automation.pipeline_controller.sb_delete")
    @patch("supabase.create_client")
    def test_feature_fingerprint_computed_in_record(self, mock_create_client, mock_sb_delete, mock_get_db2):
        """Verify feature_fingerprint is included in inserted records."""
        from automation.pipeline_controller import _sb_clients
        _sb_clients.clear()
        mock_get_db2.return_value = ("http://test", "test-key")
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_sb_delete.return_value = 200

        # Capture the insert payload via the .insert() call
        insert_payloads = []
        original_insert = mock_client.table.return_value.insert
        def capture_insert(payload):
            insert_payloads.append(payload)
            mock_result = MagicMock()
            mock_result.execute.return_value = MagicMock()
            return mock_result
        mock_client.table.return_value.insert.side_effect = capture_insert

        from automation.pipeline_controller import stage_work_analysis_persist

        wa = MockWorkAnalysis(
            work_id=42, member_id=1, member_type="MP",
            sanction_amount=50000, recommended_amount=60000,
            expenditure_amount=40000, sanction_delay_days=30,
            execution_days=200, project_age_days=300,
            cost_percentile=0.5, duration_percentile=0.4,
        )
        stage_work_analysis_persist([wa], affected_work_ids=None)

        assert len(insert_payloads) >= 1
        # insert() is called with a list of records (batch)
        batch = insert_payloads[0]
        if isinstance(batch, list):
            record = batch[0]
        else:
            record = batch
        assert "feature_fingerprint" in record
        expected_fp = _compute_fp(
            sanction_amount=50000, recommended_amount=60000,
            expenditure_amount=40000, sanction_delay_days=30,
            execution_days=200, project_age_days=300,
            cost_percentile=0.5, duration_percentile=0.4,
        )
        assert record["feature_fingerprint"] == expected_fp


class TestCategoryFYViews:
    """TEST D/E/F: category_metrics and fy_metrics SQL VIEW correctness."""

    def test_category_metrics_sql_is_valid(self):
        """TEST D: Verify the category_metrics VIEW SQL parses correctly."""
        from automation.daily_pipeline import _CREATE_CATEGORY_FY_VIEWS_SQL
        # Basic structural check: the SQL must contain CREATE OR REPLACE VIEW
        assert "CREATE OR REPLACE VIEW public.category_metrics AS" in _CREATE_CATEGORY_FY_VIEWS_SQL
        assert "UNCLASSIFIED" in _CREATE_CATEGORY_FY_VIEWS_SQL
        assert "work_analysis" in _CREATE_CATEGORY_FY_VIEWS_SQL
        assert "mla_work_analysis" in _CREATE_CATEGORY_FY_VIEWS_SQL

    def test_fy_metrics_sql_is_valid(self):
        """TEST E: Verify the fy_metrics VIEW SQL parses correctly."""
        from automation.daily_pipeline import _CREATE_CATEGORY_FY_VIEWS_SQL
        assert "CREATE OR REPLACE VIEW public.fy_metrics AS" in _CREATE_CATEGORY_FY_VIEWS_SQL
        assert "fy_start" in _CREATE_CATEGORY_FY_VIEWS_SQL
        assert "recommendation_date" in _CREATE_CATEGORY_FY_VIEWS_SQL
        assert "sanction_date" in _CREATE_CATEGORY_FY_VIEWS_SQL
        assert "completion_date" in _CREATE_CATEGORY_FY_VIEWS_SQL
        assert "work_expenditures" in _CREATE_CATEGORY_FY_VIEWS_SQL

    def test_ensure_category_fy_views_exists(self):
        """TEST F: _ensure_category_fy_views function exists and is callable."""
        from automation.daily_pipeline import _ensure_category_fy_views
        assert callable(_ensure_category_fy_views)

    def test_ensure_feature_fingerprint_column_exists(self):
        """Verify _ensure_feature_fingerprint_column function exists."""
        from automation.daily_pipeline import _ensure_feature_fingerprint_column
        assert callable(_ensure_feature_fingerprint_column)


class TestIntelligenceBackfillMLUpdate:
    """Verify intelligence_backfill uses NULL-guarded UPDATE SQL."""

    def test_null_guarded_update_sql(self):
        """ML UPDATEs must include IS NULL guard to preserve existing values."""
        import ast
        import inspect
        source_file = open("/Users/yuvrajkumar/Desktop/Arrow_Escape/mplads-automation/automation/intelligence_backfill.py").read()
        # Check that the SQL templates include IS NULL conditions
        assert "isolation_score IS NULL OR isolation_level IS NULL" in source_file
        assert "delay_probability IS NULL OR delay_risk_band IS NULL" in source_file


if __name__ == "__main__":
    unittest.main()
