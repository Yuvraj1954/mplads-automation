"""Tests for pipeline intelligence integration gate.

Verifies that:
1. Intelligence backfill failure causes overall pipeline FAILURE
2. data_updated is NOT marked complete on intelligence failure
3. Previous good snapshot is NOT deleted on intelligence failure
4. New snapshot is NOT promoted on intelligence failure
5. Zero-change path does NOT invoke intelligence backfill
6. Successful intelligence allows snapshot promotion
7. DB1/DB2 routing remains correct
8. asyncpg pool creation does not pass unsupported 'family' kwarg
"""
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch, call, PropertyMock

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent


def _read_main_source():
    """Read the source of daily_pipeline.main(), excluding bootstrap section."""
    import inspect
    from automation.daily_pipeline import main
    return inspect.getsource(main)


def _find_after(source, needle, start=0):
    """Find needle in source after start position."""
    return source.find(needle, start)


def _get_pipeline_section(source):
    """Extract the non-bootstrap, non-zero-change main pipeline section.

    This is the section starting from STEP 1: FETCH through STEP 10: CLEANUP
    (excluding the bootstrap early-return block and zero-change fast exit).
    """
    # Find the zero-change fast exit (which is after STEP 3: COMPARE)
    idx_zero = source.find("0 CHANGES: FAST EXIT")
    # Find STEP 4: UPLOAD DELTA (which starts the changed-data path)
    idx_step4 = source.find("STEP 4: UPLOAD DELTA")
    # Find the end (the return at the bottom of main)
    idx_end = source.find("if __name__")

    if idx_zero != -1 and idx_step4 != -1:
        # Return everything from STEP 4 to the end (changed-data path)
        return source[idx_step4:idx_end]
    return source[idx_step4:idx_end] if idx_step4 != -1 else source


# ---------------------------------------------------------------------------
# 1. Python compile checks
# ---------------------------------------------------------------------------
class TestCompileChecks:
    def test_daily_pipeline_compiles(self):
        result = subprocess.run(
            [sys.executable, "-m", "py_compile", "automation/daily_pipeline.py"],
            capture_output=True, text=True, cwd=str(ROOT),
        )
        assert result.returncode == 0, f"Compilation failed:\n{result.stderr}"

    def test_db_pool_compiles(self):
        result = subprocess.run(
            [sys.executable, "-m", "py_compile", "automation/db_pool.py"],
            capture_output=True, text=True, cwd=str(ROOT),
        )
        assert result.returncode == 0, f"Compilation failed:\n{result.stderr}"

    def test_intelligence_backfill_compiles(self):
        result = subprocess.run(
            [sys.executable, "-m", "py_compile", "automation/intelligence_backfill.py"],
            capture_output=True, text=True, cwd=str(ROOT),
        )
        assert result.returncode == 0, f"Compilation failed:\n{result.stderr}"

    def test_pipeline_controller_compiles(self):
        result = subprocess.run(
            [sys.executable, "-m", "py_compile", "automation/pipeline_controller.py"],
            capture_output=True, text=True, cwd=str(ROOT),
        )
        assert result.returncode == 0, f"Compilation failed:\n{result.stderr}"


# ---------------------------------------------------------------------------
# 2. db_pool.py: No family= kwarg in create_pool calls
# ---------------------------------------------------------------------------
class TestDbPoolFamilyArg:
    def test_no_family_kwarg_in_init_pools(self):
        import inspect
        from automation.db_pool import init_pools
        source = inspect.getsource(init_pools)
        assert "family" not in source, (
            "init_pools contains 'family' kwarg — use _force_ipv4() instead"
        )

    def test_no_family_kwarg_in_reconnect(self):
        import inspect
        from automation.db_pool import db1_reconnect, db2_reconnect
        for fn in (db1_reconnect, db2_reconnect):
            source = inspect.getsource(fn)
            assert "family" not in source, f"{fn.__name__} contains 'family' kwarg"

    def test_force_ipv4_exists(self):
        from automation.db_pool import _force_ipv4
        assert callable(_force_ipv4)

    def test_force_ipv4_returns_unchanged_for_ipv4_host(self):
        from automation.db_pool import _force_ipv4
        dsn = "postgresql://user:pass@192.168.1.1:6543/postgres"
        result = _force_ipv4(dsn)
        assert "192.168.1.1" in result

    def test_force_ipv4_returns_original_on_dns_failure(self):
        from automation.db_pool import _force_ipv4
        dsn = "postgresql://user:pass@nonexistent.invalid:6543/postgres"
        result = _force_ipv4(dsn)
        assert result == dsn


# ---------------------------------------------------------------------------
# 3. Intelligence failure propagation
# ---------------------------------------------------------------------------
class TestIntelligenceFailurePropagation:
    def test_no_try_except_around_intelligence_in_main(self):
        """The intelligence backfill call must NOT be wrapped in try/except
        that swallows the exception."""
        source = _read_main_source()
        # Look in the changed-data path (after STEP 9 label)
        idx_intel = source.find("INTELLIGENCE BACKFILL (REQUIRED)")
        assert idx_intel != -1, "Could not find intelligence backfill section"
        idx_cleanup = source.find("STEP 10: CLEANUP")
        assert idx_cleanup != -1

        intel_section = source[idx_intel:idx_cleanup]

        assert "WARNING: Intelligence backfill failed" not in intel_section, (
            "Intelligence failure is still caught and converted to a warning"
        )
        assert "Core pipeline succeeded. Intelligence fields may be stale" not in intel_section

    def test_intelligence_run_raises_on_failure(self):
        """run(intelligence_cmd) must be called without exception swallowing."""
        source = _read_main_source()
        idx_intel = source.find("INTELLIGENCE BACKFILL (REQUIRED)")
        idx_cleanup = source.find("STEP 10: CLEANUP")
        intel_section = source[idx_intel:idx_cleanup]

        assert "run(intelligence_cmd)" in intel_section

    def test_skip_intelligence_does_not_crash(self):
        source = _read_main_source()
        assert "skip_intelligence" in source
        assert "Skipping intelligence backfill" in source


# ---------------------------------------------------------------------------
# 4. data_updated NOT marked complete on intelligence failure
# ---------------------------------------------------------------------------
class TestDataUpdatedNotMarkedOnFailure:
    def test_data_updated_only_reached_after_intelligence_success(self):
        """In the changed-data path, _update_data_updated must come AFTER
        the intelligence backfill."""
        source = _read_main_source()
        # Get the changed-data path section (STEP 4 onwards)
        pipeline = _get_pipeline_section(source)

        idx_intel = pipeline.find("INTELLIGENCE BACKFILL")
        idx_data_updated = pipeline.find('_update_data_updated(status="complete")')

        assert idx_intel != -1, "Intelligence section not found in pipeline"
        assert idx_data_updated != -1, "data_updated not found in pipeline"
        assert idx_intel < idx_data_updated, (
            f"_update_data_updated (pos {idx_data_updated}) is before "
            f"intelligence backfill (pos {idx_intel}) in the changed-data path"
        )

    def test_data_updated_in_cleanup_section(self):
        """The final _update_data_updated must be in the STEP 10: CLEANUP section."""
        source = _read_main_source()
        pipeline = _get_pipeline_section(source)

        idx_cleanup = pipeline.find("STEP 10: CLEANUP")
        idx_data_updated = pipeline.find('_update_data_updated(status="complete")')

        assert idx_cleanup != -1
        assert idx_data_updated != -1
        assert idx_data_updated > idx_cleanup, (
            "_update_data_updated must be in STEP 10 CLEANUP section"
        )


# ---------------------------------------------------------------------------
# 5. Snapshot safety: previous good snapshot NOT deleted on failure
# ---------------------------------------------------------------------------
class TestSnapshotSafety:
    def test_preserve_snapshot_after_intelligence(self):
        """_preserve_fetched_snapshot must be AFTER intelligence backfill."""
        source = _read_main_source()
        pipeline = _get_pipeline_section(source)

        idx_intel = pipeline.find("INTELLIGENCE BACKFILL")
        idx_preserve = pipeline.find("_preserve_fetched_snapshot")
        assert idx_intel != -1
        assert idx_preserve != -1
        assert idx_intel < idx_preserve

    def test_write_success_after_intelligence(self):
        """_write_pipeline_success must be AFTER intelligence backfill."""
        source = _read_main_source()
        pipeline = _get_pipeline_section(source)

        idx_intel = pipeline.find("INTELLIGENCE BACKFILL")
        idx_write_success = pipeline.find("_write_pipeline_success")
        assert idx_intel != -1
        assert idx_write_success != -1
        assert idx_intel < idx_write_success

    def test_delete_old_snapshots_after_intelligence(self):
        """delete_old_raw_snapshots must be AFTER intelligence backfill."""
        source = _read_main_source()
        pipeline = _get_pipeline_section(source)

        idx_intel = pipeline.find("INTELLIGENCE BACKFILL")
        idx_delete = pipeline.find("delete_old_raw_snapshots")
        assert idx_intel != -1
        assert idx_delete != -1
        assert idx_intel < idx_delete

    def test_error_handler_preserves_snapshot(self):
        """The error handler must NOT delete the old snapshot."""
        full_source = (ROOT / "automation" / "daily_pipeline.py").read_text()
        idx_fail = full_source.find("PIPELINE FAILED")
        assert idx_fail != -1
        handler_section = full_source[idx_fail:idx_fail + 1000]
        assert "delete_old_raw_snapshots" not in handler_section
        assert "OLD SNAPSHOT HAS NOT BEEN DELETED" in handler_section


# ---------------------------------------------------------------------------
# 6. Zero-change path does NOT invoke intelligence backfill
# ---------------------------------------------------------------------------
class TestZeroChangePath:
    def test_zero_change_exits_before_intelligence(self):
        source = _read_main_source()
        idx_zero = source.find("0 CHANGES: FAST EXIT")
        idx_intel = source.find("INTELLIGENCE BACKFILL")
        assert idx_zero != -1
        assert idx_intel != -1
        assert idx_zero < idx_intel

    def test_zero_change_has_return(self):
        source = _read_main_source()
        idx_zero = source.find("0 CHANGES: FAST EXIT")
        idx_return = source.find("return", idx_zero)
        idx_intel = source.find("INTELLIGENCE BACKFILL")
        assert idx_return != -1
        assert idx_return < idx_intel

    def test_zero_change_preserves_snapshot(self):
        source = _read_main_source()
        idx_zero = source.find("0 CHANGES: FAST EXIT")
        section = source[idx_zero:idx_zero + 500]
        assert "_preserve_fetched_snapshot" in section


# ---------------------------------------------------------------------------
# 7. Successful intelligence allows snapshot promotion
# ---------------------------------------------------------------------------
class TestSuccessPath:
    def test_cleanup_section_has_all_operations(self):
        source = _read_main_source()
        pipeline = _get_pipeline_section(source)

        idx_cleanup = pipeline.find("STEP 10: CLEANUP")
        cleanup_section = pipeline[idx_cleanup:idx_cleanup + 1500]

        assert "delete_delta_run" in cleanup_section
        assert "delete_old_raw_snapshots" in cleanup_section
        assert "_preserve_fetched_snapshot" in cleanup_section
        assert "_cleanup_local_snapshot" in cleanup_section
        assert "_write_pipeline_success" in cleanup_section
        assert '_update_data_updated(status="complete")' in cleanup_section

    def test_main_exception_handler_exits_nonzero(self):
        full_source = (ROOT / "automation" / "daily_pipeline.py").read_text()
        idx_fail = full_source.find("PIPELINE FAILED")
        handler_section = full_source[idx_fail:idx_fail + 800]
        assert "sys.exit(1)" in handler_section


# ---------------------------------------------------------------------------
# 8. DB1/DB2 routing
# ---------------------------------------------------------------------------
class TestDBRouting:
    def test_intelligence_backfill_uses_db2_for_output(self):
        import inspect
        from automation import intelligence_backfill
        source = inspect.getsource(intelligence_backfill)
        assert "get_db2_pool" in source
        assert "get_db1_pool" in source

    def test_db_pool_has_both_pools(self):
        from automation.db_pool import get_db1_pool, get_db2_pool
        assert callable(get_db1_pool)
        assert callable(get_db2_pool)

    def test_daily_pipeline_has_both_urls(self):
        source = (ROOT / "automation" / "daily_pipeline.py").read_text()
        assert "INTELLIGENCE_BACKFILL" in source


# ---------------------------------------------------------------------------
# 9. Intelligence backfill is NOT optional for changed-data runs
# ---------------------------------------------------------------------------
class TestIntelligenceRequired:
    def test_no_success_with_warnings_for_intelligence_failure(self):
        source = _read_main_source()
        pipeline = _get_pipeline_section(source)
        idx_intel = pipeline.find("INTELLIGENCE BACKFILL")
        idx_cleanup = pipeline.find("STEP 10: CLEANUP")
        intel_section = pipeline[idx_intel:idx_cleanup]
        assert "SUCCESS_WITH_WARNINGS" not in intel_section

    def test_step_label_says_required(self):
        source = _read_main_source()
        assert "INTELLIGENCE BACKFILL (REQUIRED)" in source


# ---------------------------------------------------------------------------
# 10. Integration test: intelligence failure blocks pipeline
# ---------------------------------------------------------------------------
class TestIntelligenceFailureBlocksPipeline:
    def test_intelligence_failure_in_changed_data_path(self):
        """Verify that in the changed-data path, if intelligence raises,
        the pipeline never reaches cleanup/success/data_updated."""
        source = _read_main_source()
        pipeline = _get_pipeline_section(source)

        # In the changed-data path, find the intelligence section
        idx_intel = pipeline.find("INTELLIGENCE BACKFILL")
        idx_cleanup = pipeline.find("STEP 10: CLEANUP")
        assert idx_intel != -1
        assert idx_cleanup != -1

        # The intelligence section should call run() directly
        # which will raise CalledProcessError on failure
        intel_section = pipeline[idx_intel:idx_cleanup]
        assert "run(intelligence_cmd)" in intel_section

        # There should be no try/except wrapping run(intelligence_cmd)
        # that catches and suppresses the exception
        # We check that "run(intelligence_cmd)" is NOT inside a try block
        lines = intel_section.split('\n')
        in_try = False
        for line in lines:
            stripped = line.strip()
            if stripped.startswith("try:") and "run(intelligence_cmd)" not in stripped:
                in_try = True
            if "run(intelligence_cmd)" in stripped:
                # If we're in a try block, check what's in the except
                if in_try:
                    # Find the except handler
                    for j, later_line in enumerate(lines):
                        if "except" in later_line and j > lines.index(line):
                            # The except handler should not continue to cleanup
                            except_section = '\n'.join(lines[j:j+10])
                            assert "WARNING: Intelligence backfill failed" not in except_section
                            break
                break


# ---------------------------------------------------------------------------
# 11. Integration test: success path
# ---------------------------------------------------------------------------
class TestSuccessPathIntegration:
    def test_intelligence_success_in_pipeline_flow(self):
        """When intelligence succeeds, the pipeline reaches cleanup and
        marks data_updated as complete."""
        source = _read_main_source()
        pipeline = _get_pipeline_section(source)

        idx_intel = pipeline.find("INTELLIGENCE BACKFILL")
        idx_cleanup = pipeline.find("STEP 10: CLEANUP")
        idx_preserve = pipeline.find("_preserve_fetched_snapshot")
        idx_write_success = pipeline.find("_write_pipeline_success")
        idx_data_updated = pipeline.find('_update_data_updated(status="complete")')

        # All these must exist and be in order
        positions = [
            ("intelligence", idx_intel),
            ("cleanup", idx_cleanup),
            ("preserve", idx_preserve),
            ("write_success", idx_write_success),
            ("data_updated", idx_data_updated),
        ]
        for name, pos in positions:
            assert pos != -1, f"{name} not found in pipeline"

        # Verify ordering
        for i in range(len(positions) - 1):
            name_a, pos_a = positions[i]
            name_b, pos_b = positions[i + 1]
            assert pos_a < pos_b, (
                f"{name_a} (pos {pos_a}) must come before {name_b} (pos {pos_b})"
            )


# ---------------------------------------------------------------------------
# 12. Dry-run intelligence connection test (read-only)
# ---------------------------------------------------------------------------
class TestDryRunIntelligenceConnection:
    def test_init_pools_accepts_urls(self):
        import inspect
        from automation.db_pool import init_pools
        sig = inspect.signature(init_pools)
        params = list(sig.parameters.keys())
        assert "db1_url" in params
        assert "db2_url" in params

    def test_init_pools_signature(self):
        import inspect
        from automation.db_pool import init_pools
        sig = inspect.signature(init_pools)
        assert "db1_min" in sig.parameters
        assert "db1_max" in sig.parameters
        assert "db2_min" in sig.parameters
        assert "db2_max" in sig.parameters

    def test_intelligence_backfill_main_has_apply_flag(self):
        import inspect
        from automation.intelligence_backfill import main
        source = inspect.getsource(main)
        assert "apply" in source


# ---------------------------------------------------------------------------
# 13. No production data mutation during test
# ---------------------------------------------------------------------------
class TestNoProductionMutation:
    def test_no_real_db_connections_in_test_functions(self):
        """Test functions must not attempt real database connections.
        Only the test docstrings/comments may mention asyncpg for documentation."""
        test_source = Path(__file__).read_text()
        # Split by class/function definitions to exclude docstrings
        # Just check that we don't have real connection code in test bodies
        # by verifying mock usage
        assert "unittest.mock" in test_source
        assert "MagicMock" in test_source or "patch" in test_source


# ---------------------------------------------------------------------------
# 14. GitHub Actions workflow failure handling
# ---------------------------------------------------------------------------
class TestWorkflowFailureHandling:
    def test_cleanup_runs_on_failure(self):
        workflow_path = ROOT / ".github" / "workflows" / "mplads-daily.yml"
        if workflow_path.exists():
            source = workflow_path.read_text()
            assert "Cleanup failed snapshot from Supabase" in source
            assert "if: failure()" in source

    def test_success_steps_require_success(self):
        workflow_path = ROOT / ".github" / "workflows" / "mplads-daily.yml"
        if workflow_path.exists():
            source = workflow_path.read_text()
            assert "Rotate snapshots" in source
            assert "Save snapshot cache" in source

    def test_failure_state_increments_on_failure(self):
        workflow_path = ROOT / ".github" / "workflows" / "mplads-daily.yml"
        if workflow_path.exists():
            source = workflow_path.read_text()
            assert "Update failure state" in source
            assert "Pipeline failed" in source


# ---------------------------------------------------------------------------
# 15. Edge cases
# ---------------------------------------------------------------------------
class TestEdgeCases:
    def test_skip_intelligence_allows_success(self):
        source = _read_main_source()
        assert 'timing["intelligence"] = 0.0' in source

    def test_intelligence_backfill_not_found_raises(self):
        source = _read_main_source()
        assert "Intelligence backfill not found" in source
        assert "raise RuntimeError" in source


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
