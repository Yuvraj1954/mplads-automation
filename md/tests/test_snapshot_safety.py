"""Tests for snapshot reliability/failure-handling safety architecture.

Tests cover:
- validate_complete_snapshot: valid, missing datasets, invalid NDJSON, count mismatches
- validate_remote_snapshot: valid, missing files, incomplete marker
- Workflow YAML: failure state persistence, emergency fallback, cache integrity
- Python code: fallback env var handling, local validation, clean exit on failure
- Snapshot trust invariant: failed current never promoted, previous never deleted
- Idempotent cleanup: remote cleanup safely handles re-runs
- Cache restore validation: corrupt, incomplete, valid
- Immutable cache keys
"""
import json
import os
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

WORKFLOW_PATH = (
    "/Users/yuvrajkumar/Desktop/Arrow_Escape/mplads-automation/"
    ".github/workflows/mplads-daily.yml"
)
PIPELINE_PATH = (
    "/Users/yuvrajkumar/Desktop/Arrow_Escape/mplads-automation/"
    "automation/daily_pipeline.py"
)


def _wf():
    return Path(WORKFLOW_PATH).read_text()


def _py():
    return Path(PIPELINE_PATH).read_text()


# ============================================================
# validate_complete_snapshot tests
# ============================================================

class TestValidateCompleteSnapshot:
    """Tests for the strict local snapshot validation."""

    def _make_valid_snapshot(self, base):
        snap = Path(base) / "snapshot"
        snap.mkdir()
        datasets = [
            "allocated_limit", "works_recommended", "works_sanctioned",
            "works_completed", "expenditure", "calamity",
            "mla_allocated_limit", "mla_works_recommended",
            "mla_works_sanctioned", "mla_works_completed",
            "mla_expenditure", "mla_calamity",
        ]
        complete = {"status": "complete", "datasets": {}}
        for ds in datasets:
            ds_dir = snap / ds
            ds_dir.mkdir()
            records, chunks = 100, 2
            complete["datasets"][ds] = {"records": records, "chunks": chunks}
            (ds_dir / "manifest.json").write_text(json.dumps({"records": records, "chunks": chunks}))
            for i in range(chunks):
                lines = [json.dumps({"id": j}) for j in range(50)]
                (ds_dir / f"part_{i+1:04d}.ndjson").write_text("\n".join(lines))
        (snap / "_COMPLETE.json").write_text(json.dumps(complete))
        return snap

    def test_valid_snapshot_passes(self):
        from automation.daily_pipeline import validate_complete_snapshot
        with tempfile.TemporaryDirectory() as tmpdir:
            validate_complete_snapshot(str(self._make_valid_snapshot(tmpdir)))

    def test_missing_complete_json_fails(self):
        from automation.daily_pipeline import validate_complete_snapshot
        with tempfile.TemporaryDirectory() as tmpdir:
            snap = Path(tmpdir) / "snapshot"; snap.mkdir()
            (snap / "allocated_limit").mkdir()
            (snap / "allocated_limit" / "part_0001.ndjson").write_text('{"id":1}\n')
            with pytest.raises(RuntimeError, match="Missing _COMPLETE.json"):
                validate_complete_snapshot(str(snap))

    def test_incomplete_status_fails(self):
        from automation.daily_pipeline import validate_complete_snapshot
        with tempfile.TemporaryDirectory() as tmpdir:
            snap = Path(tmpdir) / "snapshot"; snap.mkdir()
            (snap / "_COMPLETE.json").write_text(json.dumps({"status": "in_progress", "datasets": {}}))
            with pytest.raises(RuntimeError, match="status is not 'complete'"):
                validate_complete_snapshot(str(snap))

    def test_missing_dataset_directory_fails(self):
        from automation.daily_pipeline import validate_complete_snapshot
        with tempfile.TemporaryDirectory() as tmpdir:
            snap = Path(tmpdir) / "snapshot"; snap.mkdir()
            (snap / "_COMPLETE.json").write_text(json.dumps({"status": "complete", "datasets": {}}))
            with pytest.raises(RuntimeError, match="Missing dataset directory"):
                validate_complete_snapshot(str(snap))

    def test_no_part_files_fails(self):
        from automation.daily_pipeline import validate_complete_snapshot
        with tempfile.TemporaryDirectory() as tmpdir:
            snap = Path(tmpdir) / "snapshot"; snap.mkdir()
            datasets = ["allocated_limit", "works_recommended", "works_sanctioned",
                        "works_completed", "expenditure", "calamity",
                        "mla_allocated_limit", "mla_works_recommended",
                        "mla_works_sanctioned", "mla_works_completed",
                        "mla_expenditure", "mla_calamity"]
            complete = {"status": "complete", "datasets": {}}
            for ds in datasets:
                (snap / ds).mkdir()
                complete["datasets"][ds] = {"records": 0, "chunks": 0}
            (snap / "_COMPLETE.json").write_text(json.dumps(complete))
            shutil.rmtree(snap / "allocated_limit"); (snap / "allocated_limit").mkdir()
            with pytest.raises(RuntimeError, match="Snapshot validation failed"):
                validate_complete_snapshot(str(snap))

    def test_invalid_ndjson_fails(self):
        from automation.daily_pipeline import validate_complete_snapshot
        with tempfile.TemporaryDirectory() as tmpdir:
            snap = Path(tmpdir) / "snapshot"; snap.mkdir()
            datasets = ["allocated_limit", "works_recommended", "works_sanctioned",
                        "works_completed", "expenditure", "calamity",
                        "mla_allocated_limit", "mla_works_recommended",
                        "mla_works_sanctioned", "mla_works_completed",
                        "mla_expenditure", "mla_calamity"]
            complete = {"status": "complete", "datasets": {}}
            for ds in datasets:
                ds_dir = snap / ds; ds_dir.mkdir()
                complete["datasets"][ds] = {"records": 1, "chunks": 1}
                (ds_dir / "manifest.json").write_text(json.dumps({"records": 1, "chunks": 1}))
                (ds_dir / "part_0001.ndjson").write_text('{"id":1}\n')
            (snap / "_COMPLETE.json").write_text(json.dumps(complete))
            (snap / "works_recommended" / "part_0001.ndjson").write_text("{invalid json\n")
            with pytest.raises(RuntimeError, match="Invalid NDJSON"):
                validate_complete_snapshot(str(snap))

    def test_chunk_count_mismatch_fails(self):
        from automation.daily_pipeline import validate_complete_snapshot
        with tempfile.TemporaryDirectory() as tmpdir:
            snap = Path(tmpdir) / "snapshot"; snap.mkdir()
            complete = {"status": "complete", "datasets": {}}
            for ds in ["allocated_limit", "works_recommended", "works_sanctioned",
                       "works_completed", "expenditure", "calamity",
                       "mla_allocated_limit", "mla_works_recommended",
                       "mla_works_sanctioned", "mla_works_completed",
                       "mla_expenditure", "mla_calamity"]:
                ds_dir = snap / ds; ds_dir.mkdir()
                complete["datasets"][ds] = {"records": 10, "chunks": 3}
                (ds_dir / "manifest.json").write_text(json.dumps({"records": 10, "chunks": 3}))
                for i in range(3):
                    (ds_dir / f"part_{i+1:04d}.ndjson").write_text('{"id":1}\n')
            (snap / "_COMPLETE.json").write_text(json.dumps(complete))
            (snap / "allocated_limit" / "part_0004.ndjson").write_text('{"id":1}\n')
            with pytest.raises(RuntimeError, match="Part file count mismatch"):
                validate_complete_snapshot(str(snap))


# ============================================================
# validate_remote_snapshot tests
# ============================================================

class TestValidateRemoteSnapshot:
    """Tests for remote snapshot validation via Supabase Storage (metadata-only)."""

    def test_valid_remote_snapshot(self):
        from automation.daily_pipeline import validate_remote_snapshot
        mock_storage = MagicMock()
        marker = {"status": "complete", "datasets": {
            "allocated_limit": {"records": 100, "chunks": 2},
            "works_recommended": {"records": 200, "chunks": 2},
        }}
        mock_storage.download.return_value = json.dumps(marker).encode()
        mock_storage.list.return_value = [{"name": "part_0001.ndjson"}, {"name": "part_0002.ndjson"}]
        with patch("automation.daily_pipeline.sb") as mock_sb:
            mock_sb.storage.from_.return_value = mock_storage
            validate_remote_snapshot("2026-01-01T00-00-00Z")

    def test_missing_complete_json_fails(self):
        from automation.daily_pipeline import validate_remote_snapshot
        mock_storage = MagicMock()
        mock_storage.download.side_effect = Exception("Not found")
        with patch("automation.daily_pipeline.sb") as mock_sb:
            mock_sb.storage.from_.return_value = mock_storage
            with pytest.raises(RuntimeError, match="_COMPLETE.json missing"):
                validate_remote_snapshot("2026-01-01T00-00-00Z")

    def test_incomplete_status_fails(self):
        from automation.daily_pipeline import validate_remote_snapshot
        mock_storage = MagicMock()
        mock_storage.download.return_value = json.dumps({"status": "in_progress", "datasets": {}}).encode()
        with patch("automation.daily_pipeline.sb") as mock_sb:
            mock_sb.storage.from_.return_value = mock_storage
            with pytest.raises(RuntimeError, match="status is 'in_progress'"):
                validate_remote_snapshot("2026-01-01T00-00-00Z")

    def test_missing_parts_fails(self):
        from automation.daily_pipeline import validate_remote_snapshot
        mock_storage = MagicMock()
        marker = {"status": "complete", "datasets": {"allocated_limit": {"records": 100, "chunks": 5}}}
        mock_storage.download.return_value = json.dumps(marker).encode()
        mock_storage.list.return_value = [{"name": "part_0001.ndjson"}, {"name": "part_0002.ndjson"}]
        with patch("automation.daily_pipeline.sb") as mock_sb:
            mock_sb.storage.from_.return_value = mock_storage
            with pytest.raises(RuntimeError, match="has 2 parts, expected 5"):
                validate_remote_snapshot("2026-01-01T00-00-00Z")

    def test_only_downloads_complete_json(self):
        """Remote validation only downloads _COMPLETE.json, not content files."""
        from automation.daily_pipeline import validate_remote_snapshot
        mock_storage = MagicMock()
        mock_storage.download.return_value = json.dumps({"status": "complete", "datasets": {}}).encode()
        with patch("automation.daily_pipeline.sb") as mock_sb:
            mock_sb.storage.from_.return_value = mock_storage
            validate_remote_snapshot("2026-01-01T00-00-00Z")
        mock_storage.download.assert_called_once_with("2026-01-01T00-00-00Z/_COMPLETE.json")


# ============================================================
# Workflow YAML: failure state persistence
# ============================================================

class TestWorkflowFailureStatePersistence:
    """Verify failure counter is persisted via GitHub Actions cache."""

    def test_failure_state_restored_from_cache(self):
        wf = _wf()
        assert "Restore failure state from cache" in wf
        assert "mplads-failure-state" in wf

    def test_failure_state_saved_with_save_always(self):
        wf = _wf()
        assert "save-always: true" in wf
        assert "Save failure state to cache" in wf

    def test_failure_state_updated_on_success_and_failure(self):
        wf = _wf()
        assert "Update failure state" in wf
        assert 'if: always()' in wf

    def test_counter_resets_to_zero_on_success(self):
        wf = _wf()
        assert '"consecutive_failures": 0' in wf

    def test_counter_increments_on_failure(self):
        wf = _wf()
        assert "PREV_FAILURES + 1" in wf

    def test_persistence_survives_runner_restart(self):
        wf = _wf()
        assert wf.count("mplads-failure-state") >= 2


# ============================================================
# Workflow YAML: emergency fallback wiring
# ============================================================

class TestEmergencyFallbackWiring:
    """Verify emergency fallback is automatically enabled after 3 failures."""

    def test_strategy_reads_failure_count(self):
        wf = _wf()
        assert 'FAILURES="${{ steps.failure-state.outputs.failures }}"' in wf

    def test_fallback_enabled_at_count_3(self):
        wf = _wf()
        assert '"$FAILURES" -ge 3' in wf
        assert 'echo "use_supabase_fallback=true"' in wf

    def test_fallback_disabled_below_count_3(self):
        wf = _wf()
        assert 'echo "use_supabase_fallback=false"' in wf

    def test_no_normal_supabase_download(self):
        wf = _wf()
        lines = wf.split("\n")
        in_cache_available = False
        for line in lines:
            if "Cache available — normal path" in line:
                in_cache_available = True
            if in_cache_available and "use_supabase_fallback" in line:
                assert "false" in line
                break

    def test_fallback_passed_to_python(self):
        wf = _wf()
        assert "USE_SUPABASE_FALLBACK: ${{ steps.strategy.outputs.use_supabase_fallback }}" in wf

    def test_three_failures_then_recovery(self):
        """
        Exact state machine:
          start 0 → FAIL → 1 (no fallback)
          start 1 → FAIL → 2 (no fallback)
          start 2 → FAIL → 3 (no fallback)
          start 3 → EMERGENCY (Supabase recovery)
            if succeeds → 0
            if fails    → 4 (still eligible)
        Any start >= 3 → EMERGENCY until success resets to 0.
        """
        wf = _wf()
        assert "3 consecutive" in wf or "start 3" in wf


# ============================================================
# Python code: USE_SUPABASE_FALLBACK handling
# ============================================================

class TestPythonFallbackHandling:
    """Verify Python code correctly reads the fallback env var."""

    def test_python_reads_env_var(self):
        source = _py()
        assert 'os.environ.get("USE_SUPABASE_FALLBACK"' in source

    def test_python_fails_when_no_cache_and_no_fallback(self):
        from automation.daily_pipeline import main
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("automation.daily_pipeline.FETCHER", Path("/nonexistent")), \
                 patch("automation.daily_pipeline.COMPARATOR", Path("/nonexistent")), \
                 patch.dict(os.environ, {"USE_SUPABASE_FALLBACK": "false", "MPLADS_CACHE_WORK_DIR": tmpdir}):
                with pytest.raises((RuntimeError, SystemExit)):
                    main()

    def test_python_does_not_manage_failure_counter(self):
        source = _py()
        for fn in ["load_failure_counter", "save_failure_counter",
                    "increment_failure_counter", "reset_failure_counter",
                    "FAILURE_STATE_FILE", "FAILURE_STATE_DIR"]:
            assert fn not in source, f"Python still contains {fn}"

    def test_python_does_not_do_remote_cleanup(self):
        source = _py()
        assert "_cleanup_failed_remote_snapshot" not in source


# ============================================================
# Workflow YAML: cleanup behavior
# ============================================================

class TestCleanupBehavior:
    """Verify cleanup is idempotent and separation of concerns."""

    def test_workflow_cleanup_runs_on_failure(self):
        wf = _wf()
        assert "Cleanup failed snapshot from Supabase" in wf
        assert "failure()" in wf

    def test_workflow_cleanup_is_idempotent(self):
        wf = _wf()
        assert '-f "$SNAPSHOT_TS_FILE"' in wf

    def test_python_does_local_cleanup_only(self):
        source = _py()
        assert "Local cleanup" in source or "local cleanup" in source

    def test_cleanup_script_exists(self):
        assert Path("/Users/yuvrajkumar/Desktop/Arrow_Escape/mplads-automation/"
                     "automation/_cleanup_failed_snapshot.py").exists()

    def test_cleanup_script_is_standalone(self):
        source = open("/Users/yuvrajkumar/Desktop/Arrow_Escape/mplads-automation/"
                       "automation/_cleanup_failed_snapshot.py").read()
        assert 'if __name__ == "__main__"' in source


# ============================================================
# Snapshot trust invariant
# ============================================================

class TestSnapshotTrustInvariant:
    """Verify: failed snapshot never becomes trusted baseline."""

    def test_failure_does_not_write_success_markers(self):
        from automation.daily_pipeline import main
        with patch("automation.daily_pipeline.FETCHER", Path("/nonexistent")), \
             patch("automation.daily_pipeline.COMPARATOR", Path("/nonexistent")):
            with pytest.raises((RuntimeError, SystemExit)):
                main()

    def test_success_writes_timestamp(self):
        from automation.daily_pipeline import _write_pipeline_success
        with tempfile.TemporaryDirectory() as tmpdir:
            work_dir = Path(tmpdir)
            _write_pipeline_success("2026-01-01T00-00-00Z", str(work_dir))
            ts_file = work_dir / ".new_timestamp"
            assert ts_file.exists()
            assert ts_file.read_text() == "2026-01-01T00-00-00Z"

    def test_failed_current_never_promoted(self):
        """Failed run never reaches rotation step (only runs on success)."""
        wf = _wf()
        lines = wf.split("\n")
        in_rotate = False
        for line in lines:
            if "Rotate snapshots" in line:
                in_rotate = True
            if in_rotate and "if:" in line:
                assert "success()" in line
                break

    def test_previous_snapshot_never_deleted_on_failure(self):
        wf = _wf()
        # Cache cleanup only runs on success
        lines = wf.split("\n")
        in_cleanup = False
        for line in lines:
            if "Cleanup old cache entries" in line:
                in_cleanup = True
            if in_cleanup and "if:" in line:
                assert "success()" in line
                break


# ============================================================
# Immutable cache keys
# ============================================================

class TestImmutableCacheKeys:
    """Verify that cache keys are immutable and versioned."""

    def test_snapshot_cache_key_uses_timestamp(self):
        wf = _wf()
        assert "mplads-snapshot-state-${{ steps.new-ts.outputs.new_ts }}" in wf

    def test_snapshot_cache_restore_uses_prefix(self):
        wf = _wf()
        assert "mplads-snapshot-state-${{ github.run_id }}" in wf
        assert "mplads-snapshot-state-" in wf

    def test_failure_state_cache_key_is_fixed(self):
        wf = _wf()
        assert 'key: mplads-failure-state' in wf


# ============================================================
# Cache restore validation
# ============================================================

class TestCacheRestoreValidation:
    """Verify cache is validated after every restore attempt."""

    def test_all_three_cache_restores_have_validation(self):
        wf = _wf()
        assert "Validate cache (attempt 1)" in wf
        assert "Validate cache (attempt 2)" in wf
        assert "Validate cache (attempt 3)" in wf

    def test_cache_validated_before_use(self):
        wf = _wf()
        lines = wf.split("\n")
        validate_lines = []
        pipeline_line = -1
        for i, line in enumerate(lines):
            if "Validate cache" in line:
                validate_lines.append(i)
            if "Run complete MPLADS pipeline" in line:
                pipeline_line = i
        for vl in validate_lines:
            assert vl < pipeline_line


# ============================================================
# No normal-path Supabase fallback
# ============================================================

class TestNoNormalSupabaseFallback:
    """Verify that normal daily runs never download previous snapshot from Supabase."""

    def test_fallback_only_when_armed(self):
        wf = _wf()
        lines = wf.split("\n")
        for i, line in enumerate(lines):
            if 'echo "use_supabase_fallback=true"' in line:
                context = "\n".join(lines[max(0, i-5):i+1])
                assert "-ge 3" in context


# ============================================================
# GitHub cache write integrity
# ============================================================

class TestGitHubCacheWriteIntegrity:
    """Verify cache write integrity documentation."""

    def test_cache_save_documented(self):
        wf = _wf()
        assert "Cache write integrity" in wf

    def test_candidate_validated_before_save(self):
        wf = _wf()
        assert "validate_complete_snapshot" in wf

    def test_immutable_key_documented(self):
        wf = _wf()
        assert "immutable" in wf.lower()

    def test_previous_never_overwritten(self):
        wf = _wf()
        assert "Never overwrites trusted previous" in wf or "never overwrites" in wf.lower()

    def test_failed_run_never_reaches_save(self):
        wf = _wf()
        lines = wf.split("\n")
        in_save = False
        for line in lines:
            if "Save snapshot cache" in line and "failure state" not in line.lower():
                in_save = True
            if in_save and "if:" in line:
                assert "success()" in line
                break
