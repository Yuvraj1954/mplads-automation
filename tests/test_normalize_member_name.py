"""Tests for normalize_member_name() — canonical identity normalization.

These tests validate the Python normalization function AND serve as the
specification for the TypeScript mirror in ingest-mplads-part/source/index.ts.

The edge function's normalizeMemberName() must produce identical results
for all test cases listed here.
"""

import pytest

from analysis.snapshot_loader import normalize_member_name


# ============================================================
# HONORIFIC PREFIX STRIPPING
# ============================================================

class TestHonorificStripping:
    """Verify that common Indian honorific prefixes are stripped."""

    def test_shri_prefix(self):
        assert normalize_member_name("Shri Parimal Nathwani") == "PARIMAL NATHWANI"

    def test_shri_prefix_with_dot(self):
        # "Shri." with a dot is non-standard and NOT stripped by the regex.
        # Only "Shri" (without dot) is stripped. This matches real-world usage.
        assert normalize_member_name("Shri. Parimal Nathwani") == "SHRI. PARIMAL NATHWANI"

    def test_smt_prefix(self):
        assert normalize_member_name("Smt. Sunita Singh") == "SUNITA SINGH"

    def test_smt_prefix_no_dot(self):
        assert normalize_member_name("Smt Sunita Singh") == "SUNITA SINGH"

    def test_dr_prefix(self):
        assert normalize_member_name("Dr. Rajesh Kumar") == "RAJESH KUMAR"

    def test_dr_prefix_no_dot(self):
        assert normalize_member_name("Dr Rajesh Kumar") == "RAJESH KUMAR"

    def test_mrs_prefix(self):
        assert normalize_member_name("Mrs. Priya Verma") == "PRIYA VERMA"

    def test_ms_prefix(self):
        assert normalize_member_name("Ms. Anita Desai") == "ANITA DESAI"

    def test_late_prefix(self):
        assert normalize_member_name("Late Ram Nath Kovind") == "RAM NATH KOVIND"


# ============================================================
# PARENTHETICAL SUFFIX REMOVAL
# ============================================================

class TestParentheticalSuffixRemoval:
    """Verify that parenthetical suffixes like (2024-30) are stripped."""

    def test_tenure_suffix(self):
        assert normalize_member_name("Parimal Nathwani (2026-32)") == "PARIMAL NATHWANI"

    def test_shri_with_tenure_suffix(self):
        assert normalize_member_name("Shri Parimal Nathwani (2026-32)") == "PARIMAL NATHWANI"

    def test_smt_with_tenure_suffix(self):
        assert normalize_member_name("Smt. Sunita Singh (2024-30)") == "SUNITA SINGH"

    def test_dr_with_suffix(self):
        assert normalize_member_name("Dr. Rajesh Kumar (2023-29)") == "RAJESH KUMAR"

    def test_no_suffix(self):
        assert normalize_member_name("Parimal Nathwani") == "PARIMAL NATHWANI"

    def test_suffix_with_spaces(self):
        assert normalize_member_name("Name (2024-30)  ") == "NAME"

    def test_suffix_no_space_before_paren(self):
        assert normalize_member_name("Name(2024-30)") == "NAME"


# ============================================================
# THE CRITICAL BUG CASE (Shri Parimal Nathwani)
# ============================================================

class TestCriticalBugCase:
    """The exact case that caused mla_id=233 vs 234 duplicate."""

    def test_parimal_nathwani_without_shri(self):
        """The original name that created mla_id=233."""
        result = normalize_member_name("Parimal Nathwani (2026-32)")
        assert result == "PARIMAL NATHWANI"

    def test_parimal_nathwani_with_shri(self):
        """The variant name that created mla_id=234."""
        result = normalize_member_name("Shri Parimal Nathwani (2026-32)")
        assert result == "PARIMAL NATHWANI"

    def test_both_variants_resolve_to_same_identity(self):
        """The fix: both variants MUST produce the same normalized name."""
        name_a = normalize_member_name("Parimal Nathwani (2026-32)")
        name_b = normalize_member_name("Shri Parimal Nathwani (2026-32)")
        assert name_a == name_b
        assert name_a == "PARIMAL NATHWANI"


# ============================================================
# MULTI-TENURE IDENTITY
# ============================================================

class TestMultiTenureIdentity:
    """Verify that the same person across different tenures normalizes
    to the same identity. The resolver uses name + constituency_id
    (NOT tenure) as the identity key. This is correct because the
    DB enforces one mla_id per person (UNIQUE constraint on mla_id
    in mla_allocations).

    IMPORTANT: These tests verify normalization only. The resolver's
    memberKey() function uses this normalized name, and the DB schema
    enforces one allocation per mla_id. Do NOT add tenure to the
    identity key — it would break the UNIQUE constraint.
    """

    def test_same_person_different_tenure_normalizes_identically(self):
        """Same person, different Rajya Sabha tenures → same identity."""
        a = normalize_member_name("Parimal Nathwani (2020-26)")
        b = normalize_member_name("Parimal Nathwani (2026-32)")
        assert a == b == "PARIMAL NATHWANI"

    def test_shri_variant_different_tenure(self):
        """Shri prefix + different tenure → same identity."""
        a = normalize_member_name("Shri Parimal Nathwani (2020-26)")
        b = normalize_member_name("Parimal Nathwani (2026-32)")
        assert a == b == "PARIMAL NATHWANI"

    def test_tenure_suffix_is_always_stripped(self):
        """Any parenthetical suffix is removed regardless of content."""
        assert normalize_member_name("Name (2020-26)") == "NAME"
        assert normalize_member_name("Name (2026-32)") == "NAME"
        assert normalize_member_name("Name (any)") == "NAME"
        assert normalize_member_name("Name") == "NAME"


# ============================================================
# CASE AND WHITESPACE VARIATIONS
# ============================================================

class TestCaseAndWhitespace:
    """Verify case-insensitive and whitespace-normalized matching."""

    def test_lowercase_input(self):
        assert normalize_member_name("parimal nathwani") == "PARIMAL NATHWANI"

    def test_uppercase_input(self):
        assert normalize_member_name("PARIMAL NATHWANI") == "PARIMAL NATHWANI"

    def test_mixed_case_input(self):
        assert normalize_member_name("PaRiMaL nAtHwAnI") == "PARIMAL NATHWANI"

    def test_extra_whitespace(self):
        assert normalize_member_name("  Parimal   Nathwani  ") == "PARIMAL NATHWANI"

    def test_tab_characters(self):
        assert normalize_member_name("Parimal\tNathwani") == "PARIMAL NATHWANI"

    def test_newline_characters(self):
        assert normalize_member_name("Parimal\nNathwani") == "PARIMAL NATHWANI"


# ============================================================
# EDGE CASES
# ============================================================

class TestEdgeCases:
    """Verify edge cases are handled safely."""

    def test_empty_string(self):
        assert normalize_member_name("") == ""

    def test_none_input(self):
        assert normalize_member_name(None) == ""

    def test_whitespace_only(self):
        assert normalize_member_name("   ") == ""

    def test_single_name(self):
        assert normalize_member_name("Modi") == "MODI"

    def test_name_with_initial(self):
        assert normalize_member_name("A. Raja") == "A. RAJA"

    def test_name_with_multiple_parts(self):
        assert normalize_member_name("Rahul Rajesh Gandhi") == "RAHUL RAJESH GANDHI"


# ============================================================
# LEGITIMATE DIFFERENT MEMBERS (must NOT collapse)
# ============================================================

class TestLegitimateDifferentMembers:
    """Verify that genuinely different people remain distinct."""

    def test_different_first_names(self):
        a = normalize_member_name("Parimal Nathwani (2026-32)")
        b = normalize_member_name("Suresh Nathwani (2026-32)")
        assert a != b

    def test_different_last_names(self):
        a = normalize_member_name("Parimal Nathwani (2026-32)")
        b = normalize_member_name("Parimal Patel (2026-32)")
        assert a != b

    def test_same_name_different_tenure(self):
        """Same name, different tenures — normalization strips tenure,
        so they collapse. This is expected: the resolver uses
        constituency_id as the disambiguating dimension, not tenure."""
        a = normalize_member_name("Parimal Nathwani (2024-30)")
        b = normalize_member_name("Parimal Nathwani (2026-32)")
        # Both normalize to PARIMAL NATHWANI — the resolver must use
        # constituency_id to distinguish (same person, different tenure
        # is legitimate for Rajya Sabha).
        assert a == b

    def test_similar_names_different_people(self):
        a = normalize_member_name("Ram Kumar (2024-30)")
        b = normalize_member_name("Raj Kumar (2024-30)")
        assert a != b


# ============================================================
# CONSISTENCY WITH EDGE FUNCTION SPEC
# ============================================================

class TestEdgeFunctionConsistency:
    """Verify these results match what the TypeScript normalizeMemberName()
    must produce. If you change normalize_member_name() here, update the
    TypeScript mirror in ingest-mplads-part/source/index.ts."""

    @pytest.mark.parametrize("raw,expected", [
        ("Shri Parimal Nathwani (2026-32)", "PARIMAL NATHWANI"),
        ("Parimal Nathwani (2026-32)", "PARIMAL NATHWANI"),
        ("Smt. Sunita Singh (2024-30)", "SUNITA SINGH"),
        ("Dr. Rajesh Kumar (2023-29)", "RAJESH KUMAR"),
        ("Late Ram Nath Kovind", "RAM NATH KOVIND"),
        ("Mrs. Priya Verma", "PRIYA VERMA"),
        ("Ms. Anita Desai", "ANITA DESAI"),
        ("Modi", "MODI"),
        ("", ""),
        (None, ""),
    ])
    def test_parameterized(self, raw, expected):
        assert normalize_member_name(raw) == expected
