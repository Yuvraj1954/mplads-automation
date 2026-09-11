"""Tests for analysis/affected.py — expand_time_sensitive scoping.

Tests cover:
- Time-sensitive works for delta-affected members are included
- Time-sensitive works for unaffected members are excluded
- All three time-sensitive conditions still work
- Without affected_members, all qualifying works are returned (backward compat)
"""
import unittest


class TestExpandTimeSensitive:
    """Tests for expand_time_sensitive with affected_members scoping."""

    def _make_work(self, work_id, member_type="MP", member_id=100,
                   sanction_date=None, completion_date=None,
                   recommendation_date=None, last_expenditure_date=None):
        return {
            "work_id": work_id,
            "member_type": member_type,
            "member_id": member_id,
            "sanction_date": sanction_date,
            "completion_date": completion_date,
            "recommendation_date": recommendation_date,
            "last_expenditure_date": last_expenditure_date,
        }

    def test_in_progress_work_included_for_affected_member(self):
        """Sanctioned + incomplete work for an affected member is included."""
        from analysis.affected import expand_time_sensitive
        works = {
            1: self._make_work(1, member_id=100, sanction_date="2026-01-01"),
        }
        affected = {("MP", 100)}
        result = expand_time_sensitive(works, None, None, affected_members=affected)
        assert 1 in result

    def test_in_progress_work_excluded_for_unaffected_member(self):
        """Sanctioned + incomplete work for a non-affected member is excluded."""
        from analysis.affected import expand_time_sensitive
        works = {
            1: self._make_work(1, member_id=200, sanction_date="2026-01-01"),
        }
        affected = {("MP", 100)}  # member 200 not in affected set
        result = expand_time_sensitive(works, None, None, affected_members=affected)
        assert 1 not in result

    def test_recommended_work_included_for_affected_member(self):
        """Recommended + unsanctioned work for an affected member is included."""
        from analysis.affected import expand_time_sensitive
        works = {
            2: self._make_work(2, member_id=100, recommendation_date="2026-06-01"),
        }
        affected = {("MP", 100)}
        result = expand_time_sensitive(works, None, None, affected_members=affected)
        assert 2 in result

    def test_recommended_work_excluded_for_unaffected_member(self):
        """Recommended + unsanctioned work for a non-affected member is excluded."""
        from analysis.affected import expand_time_sensitive
        works = {
            2: self._make_work(2, member_id=200, recommendation_date="2026-06-01"),
        }
        affected = {("MP", 100)}
        result = expand_time_sensitive(works, None, None, affected_members=affected)
        assert 2 not in result

    def test_expenditure_work_included_for_affected_member(self):
        """Work with expenditure date for an affected member is included."""
        from analysis.affected import expand_time_sensitive
        works = {
            3: self._make_work(3, member_id=100, last_expenditure_date="2026-08-01"),
        }
        affected = {("MP", 100)}
        result = expand_time_sensitive(works, None, None, affected_members=affected)
        assert 3 in result

    def test_expenditure_work_excluded_for_unaffected_member(self):
        """Work with expenditure date for a non-affected member is excluded."""
        from analysis.affected import expand_time_sensitive
        works = {
            3: self._make_work(3, member_id=200, last_expenditure_date="2026-08-01"),
        }
        affected = {("MP", 100)}
        result = expand_time_sensitive(works, None, None, affected_members=affected)
        assert 3 not in result

    def test_completed_work_not_time_sensitive(self):
        """Completed work (with completion_date) is not time-sensitive."""
        from analysis.affected import expand_time_sensitive
        works = {
            4: self._make_work(4, member_id=100,
                               sanction_date="2026-01-01",
                               completion_date="2026-06-01"),
        }
        affected = {("MP", 100)}
        result = expand_time_sensitive(works, None, None, affected_members=affected)
        assert 4 not in result

    def test_without_affected_members_returns_all_qualifying(self):
        """Without affected_members filter, all qualifying works are returned."""
        from analysis.affected import expand_time_sensitive
        works = {
            1: self._make_work(1, member_id=100, sanction_date="2026-01-01"),
            2: self._make_work(2, member_id=200, sanction_date="2026-01-01"),
        }
        result = expand_time_sensitive(works, None, None, affected_members=None)
        assert 1 in result
        assert 2 in result

    def test_mixed_members_only_affected_included(self):
        """Mix of affected and unaffected members — only affected included."""
        from analysis.affected import expand_time_sensitive
        works = {
            1: self._make_work(1, member_id=100, sanction_date="2026-01-01"),
            2: self._make_work(2, member_id=200, sanction_date="2026-01-01"),
            3: self._make_work(3, member_id=100, last_expenditure_date="2026-08-01"),
            4: self._make_work(4, member_id=300, last_expenditure_date="2026-08-01"),
        }
        affected = {("MP", 100), ("MP", 200)}
        result = expand_time_sensitive(works, None, None, affected_members=affected)
        assert 1 in result   # member 100, affected
        assert 2 in result   # member 200, affected
        assert 3 in result   # member 100, affected
        assert 4 not in result  # member 300, NOT affected

    def test_empty_affected_set_returns_nothing(self):
        """Empty affected set means no time-sensitive works included."""
        from analysis.affected import expand_time_sensitive
        works = {
            1: self._make_work(1, member_id=100, sanction_date="2026-01-01"),
        }
        affected = set()
        result = expand_time_sensitive(works, None, None, affected_members=affected)
        assert len(result) == 0


if __name__ == "__main__":
    unittest.main()
