"""Tests for Stage 7 national_statistics persistence — idempotent upsert.

Verifies that both stage_analytics_persist and stage_analytics_persist_affected
pass conflict_cols=["metric_name", "scope"] to sb_upsert for national_statistics.
"""
import ast
import re
import unittest
from pathlib import Path


PIPELINE_FILE = Path(__file__).resolve().parent.parent.parent / "automation" / "pipeline_controller.py"


class TestNationalStatisticsUpsert:
    """Tests for national_statistics upsert with conflict_cols."""

    def test_full_mode_has_conflict_cols(self):
        """stage_analytics_persist calls sb_upsert with conflict_cols for national_statistics."""
        src = PIPELINE_FILE.read_text()

        # Find the national_statistics section in stage_analytics_persist
        # Look for the pattern: sb_upsert(... "national_statistics" ...
        # and verify it has conflict_cols=["metric_name", "scope"]
        lines = src.split("\n")

        # Find stage_analytics_persist function
        in_func = False
        found_ns_upsert = False
        for i, line in enumerate(lines):
            if "def stage_analytics_persist(" in line:
                in_func = True
            elif in_func and line.strip().startswith("def ") and "stage_analytics_persist" not in line:
                break
            if in_func and "national_statistics" in line and "sb_upsert" in line:
                # Check this line and the next few for conflict_cols
                context = "\n".join(lines[i:i+3])
                assert 'conflict_cols=["metric_name", "scope"]' in context, \
                    f"Missing conflict_cols at line {i+1}: {context}"
                found_ns_upsert = True
                break

        assert found_ns_upsert, "sb_upsert for national_statistics not found in stage_analytics_persist"

    def test_affected_mode_has_conflict_cols(self):
        """stage_analytics_persist_affected calls sb_upsert with conflict_cols for national_statistics."""
        src = PIPELINE_FILE.read_text()
        lines = src.split("\n")

        in_func = False
        found_ns_upsert = False
        for i, line in enumerate(lines):
            if "def stage_analytics_persist_affected(" in line:
                in_func = True
            elif in_func and line.strip().startswith("def ") and "stage_analytics_persist_affected" not in line:
                break
            if in_func and "national_statistics" in line and "sb_upsert" in line:
                context = "\n".join(lines[i:i+3])
                assert 'conflict_cols=["metric_name", "scope"]' in context, \
                    f"Missing conflict_cols at line {i+1}: {context}"
                found_ns_upsert = True
                break

        assert found_ns_upsert, "sb_upsert for national_statistics not found in stage_analytics_persist_affected"


if __name__ == "__main__":
    unittest.main()
