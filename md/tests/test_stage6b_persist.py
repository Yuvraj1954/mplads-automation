"""Tests for Stage 6B work analysis persistence — delete-then-insert ordering.

Tests cover:
- Rerunning an existing work_id does not produce duplicate-key error
- Exactly one fresh record remains after rerun
- Delete phase runs before insert phase
- Both MP and MLA tables are handled correctly
"""
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


class TestStage6bDeleteInsertOrder:
    """Tests for delete-before-insert ordering in stage_work_analysis_persist."""

    @patch("automation.pipeline_controller.get_db1")
    @patch("automation.pipeline_controller.sb_delete")
    @patch("supabase.create_client")
    def test_rerun_existing_work_id_succeeds(self, mock_create_client, mock_sb_delete, mock_get_db1):
        """Rerunning a work_id that already exists should not error."""
        mock_get_db1.return_value = ("http://test", "test-key")
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_sb_delete.return_value = 200

        from automation.pipeline_controller import stage_work_analysis_persist

        wa = MockWorkAnalysis(work_id=154145, member_id=1, member_type="MP")
        result = stage_work_analysis_persist([wa], affected_work_ids={154145})

        assert result["total"] == 1
        assert result["mp_written"] == 1

    @patch("automation.pipeline_controller.get_db1")
    @patch("automation.pipeline_controller.sb_delete")
    @patch("supabase.create_client")
    def test_delete_runs_before_insert(self, mock_create_client, mock_sb_delete, mock_get_db1):
        """Verify delete phase executes before insert phase."""
        mock_get_db1.return_value = ("http://test", "test-key")
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client

        call_order = []

        def delete_side_effect(*args, **kwargs):
            call_order.append("delete")
            return 200

        def insert_side_effect(*args, **kwargs):
            call_order.append("insert")
            return MagicMock()

        mock_sb_delete.side_effect = delete_side_effect
        mock_client.table.return_value.insert.return_value.execute.side_effect = insert_side_effect

        from automation.pipeline_controller import stage_work_analysis_persist

        wa = MockWorkAnalysis(work_id=999, member_id=1, member_type="MP")
        stage_work_analysis_persist([wa], affected_work_ids={999})

        assert "delete" in call_order, f"No delete call. Order: {call_order}"
        assert "insert" in call_order, f"No insert call. Order: {call_order}"
        assert call_order.index("delete") < call_order.index("insert"), \
            f"Expected delete before insert, got: {call_order}"

    @patch("automation.pipeline_controller.get_db1")
    @patch("automation.pipeline_controller.sb_delete")
    @patch("supabase.create_client")
    def test_mla_work_id_offset_handled(self, mock_create_client, mock_sb_delete, mock_get_db1):
        """MLA work_ids with +1000000 offset are deleted correctly."""
        mock_get_db1.return_value = ("http://test", "test-key")
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_sb_delete.return_value = 200

        from automation.pipeline_controller import stage_work_analysis_persist

        wa = MockWorkAnalysis(work_id=1159378, member_id=2, member_type="MLA")
        result = stage_work_analysis_persist([wa], affected_work_ids={1159378})

        assert result["total"] == 1
        assert result["mla_written"] == 1

        # Verify delete was called with the MLA work_id
        delete_calls = [str(c) for c in mock_sb_delete.call_args_list]
        assert any("1159378" in c for c in delete_calls), \
            f"MLA work_id not deleted: {delete_calls}"

    @patch("automation.pipeline_controller.get_db1")
    @patch("automation.pipeline_controller.sb_delete")
    @patch("supabase.create_client")
    def test_full_mode_no_delete(self, mock_create_client, mock_sb_delete, mock_get_db1):
        """Full mode (affected_work_ids=None) should only insert, no delete."""
        mock_get_db1.return_value = ("http://test", "test-key")
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client

        from automation.pipeline_controller import stage_work_analysis_persist

        wa = MockWorkAnalysis(work_id=100, member_id=1, member_type="MP")
        result = stage_work_analysis_persist([wa], affected_work_ids=None)

        assert result["total"] == 1
        # Delete should NOT be called in full mode
        mock_sb_delete.assert_not_called()


if __name__ == "__main__":
    unittest.main()
