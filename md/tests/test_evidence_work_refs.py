"""Tests for Stage 8 evidence_work_refs — sequential writes.

Verifies that evidence_work_refs batches execute sequentially,
not in parallel, to avoid overwhelming the server.
"""
import ast
import unittest
from pathlib import Path


PIPELINE_FILE = Path(__file__).resolve().parent.parent.parent / "automation" / "pipeline_controller.py"


class TestEvidenceWorkRefsSequential:
    """Tests for evidence_work_refs sequential execution."""

    def test_full_mode_no_threadpool(self):
        """stage_evidence_work_refs does not use ThreadPoolExecutor."""
        src = PIPELINE_FILE.read_text()
        lines = src.split("\n")

        in_func = False
        for i, line in enumerate(lines):
            if "def stage_evidence_work_refs(" in line:
                in_func = True
            elif in_func and line.strip().startswith("def ") and "stage_evidence_work_refs" not in line:
                break
            if in_func and "ThreadPoolExecutor" in line:
                raise AssertionError(
                    f"stage_evidence_work_refs still uses ThreadPoolExecutor at line {i+1}"
                )

    def test_affected_mode_no_threadpool(self):
        """stage_evidence_work_refs_affected does not use ThreadPoolExecutor."""
        src = PIPELINE_FILE.read_text()
        lines = src.split("\n")

        in_func = False
        for i, line in enumerate(lines):
            if "def stage_evidence_work_refs_affected(" in line:
                in_func = True
            elif in_func and line.strip().startswith("def ") and "stage_evidence_work_refs_affected" not in line:
                break
            if in_func and "ThreadPoolExecutor" in line:
                raise AssertionError(
                    f"stage_evidence_work_refs_affected still uses ThreadPoolExecutor at line {i+1}"
                )

    def test_full_mode_uses_sequential_loop(self):
        """stage_evidence_work_refs writes batches in a sequential for-loop."""
        src = PIPELINE_FILE.read_text()
        lines = src.split("\n")

        in_func = False
        found_sequential = False
        for i, line in enumerate(lines):
            if "def stage_evidence_work_refs(" in line:
                in_func = True
            elif in_func and line.strip().startswith("def ") and "stage_evidence_work_refs" not in line:
                break
            if in_func and "for batch in batches:" in line:
                found_sequential = True
                break

        assert found_sequential, \
            "stage_evidence_work_refs does not use sequential 'for batch in batches:' pattern"

    def test_affected_mode_uses_sequential_loop(self):
        """stage_evidence_work_refs_affected writes batches in a sequential for-loop."""
        src = PIPELINE_FILE.read_text()
        lines = src.split("\n")

        in_func = False
        found_sequential = False
        for i, line in enumerate(lines):
            if "def stage_evidence_work_refs_affected(" in line:
                in_func = True
            elif in_func and line.strip().startswith("def ") and "stage_evidence_work_refs_affected" not in line:
                break
            if in_func and "for batch in batches:" in line:
                found_sequential = True
                break

        assert found_sequential, \
            "stage_evidence_work_refs_affected does not use sequential 'for batch in batches:' pattern"


if __name__ == "__main__":
    unittest.main()
