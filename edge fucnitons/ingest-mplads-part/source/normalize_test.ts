/**
 * Tests for normalizeMemberName() and memberKey() — edge function identity normalization.
 *
 * These mirror the Python tests in tests/test_normalize_member_name.py.
 * Run with: deno test edge\ fucnitons/ingest-mplads-part/source/normalize_test.ts
 *
 * The functions under test are defined inline here to match the edge function
 * exactly. When the edge function is updated, update these copies too.
 */

import { assertEquals } from "https://deno.land/std@0.224.0/assert/mod.ts";

// ============================================================
// COPY OF FUNCTIONS UNDER TEST (must match edge function exactly)
// ============================================================

function normalizeMemberName(name: string | null): string {
  if (!name) {
    return "";
  }
  let n = name.trim();
  n = n.replace(/\s*\(.*?\)\s*$/, "");
  n = n.replace(
    /^(Shri|Smt\.?|Dr\.?|Mrs\.?|Ms\.?|Late)\s+/i,
    ""
  );
  n = n.toUpperCase();
  n = n.replace(/\s+/g, " ").trim();
  return n;
}

function memberKey(
  name: string | null,
  constituencyId: number | null
): string {
  return `${normalizeMemberName(name)}|${
    constituencyId ?? ""
  }`;
}

// ============================================================
// HONORIFIC PREFIX STRIPPING
// ============================================================

Deno.test("normalizeMemberName: Shri prefix", () => {
  assertEquals(
    normalizeMemberName("Shri Parimal Nathwani"),
    "PARIMAL NATHWANI"
  );
});

Deno.test("normalizeMemberName: Shri. prefix with dot is NOT stripped (non-standard)", () => {
  assertEquals(
    normalizeMemberName("Shri. Parimal Nathwani"),
    "SHRI. PARIMAL NATHWANI"
  );
});

Deno.test("normalizeMemberName: Smt. prefix", () => {
  assertEquals(
    normalizeMemberName("Smt. Sunita Singh"),
    "SUNITA SINGH"
  );
});

Deno.test("normalizeMemberName: Smt prefix without dot", () => {
  assertEquals(
    normalizeMemberName("Smt Sunita Singh"),
    "SUNITA SINGH"
  );
});

Deno.test("normalizeMemberName: Dr. prefix", () => {
  assertEquals(
    normalizeMemberName("Dr. Rajesh Kumar"),
    "RAJESH KUMAR"
  );
});

Deno.test("normalizeMemberName: Dr prefix without dot", () => {
  assertEquals(
    normalizeMemberName("Dr Rajesh Kumar"),
    "RAJESH KUMAR"
  );
});

Deno.test("normalizeMemberName: Mrs. prefix", () => {
  assertEquals(
    normalizeMemberName("Mrs. Priya Verma"),
    "PRIYA VERMA"
  );
});

Deno.test("normalizeMemberName: Ms. prefix", () => {
  assertEquals(
    normalizeMemberName("Ms. Anita Desai"),
    "ANITA DESAI"
  );
});

Deno.test("normalizeMemberName: Late prefix", () => {
  assertEquals(
    normalizeMemberName("Late Ram Nath Kovind"),
    "RAM NATH KOVIND"
  );
});

// ============================================================
// PARENTHETICAL SUFFIX REMOVAL
// ============================================================

Deno.test("normalizeMemberName: tenure suffix", () => {
  assertEquals(
    normalizeMemberName("Parimal Nathwani (2026-32)"),
    "PARIMAL NATHWANI"
  );
});

Deno.test("normalizeMemberName: Shri with tenure suffix", () => {
  assertEquals(
    normalizeMemberName("Shri Parimal Nathwani (2026-32)"),
    "PARIMAL NATHWANI"
  );
});

Deno.test("normalizeMemberName: no suffix", () => {
  assertEquals(
    normalizeMemberName("Parimal Nathwani"),
    "PARIMAL NATHWANI"
  );
});

// ============================================================
// THE CRITICAL BUG CASE
// ============================================================

Deno.test("CRITICAL: Parimal Nathwani with and without Shri resolve identically", () => {
  const nameA = normalizeMemberName("Parimal Nathwani (2026-32)");
  const nameB = normalizeMemberName("Shri Parimal Nathwani (2026-32)");
  assertEquals(nameA, nameB);
  assertEquals(nameA, "PARIMAL NATHWANI");
});

// ============================================================
// CASE AND WHITESPACE
// ============================================================

Deno.test("normalizeMemberName: lowercase input", () => {
  assertEquals(
    normalizeMemberName("parimal nathwani"),
    "PARIMAL NATHWANI"
  );
});

Deno.test("normalizeMemberName: mixed case input", () => {
  assertEquals(
    normalizeMemberName("PaRiMaL nAtHwAnI"),
    "PARIMAL NATHWANI"
  );
});

Deno.test("normalizeMemberName: extra whitespace", () => {
  assertEquals(
    normalizeMemberName("  Parimal   Nathwani  "),
    "PARIMAL NATHWANI"
  );
});

// ============================================================
// EDGE CASES
// ============================================================

Deno.test("normalizeMemberName: empty string", () => {
  assertEquals(normalizeMemberName(""), "");
});

Deno.test("normalizeMemberName: null", () => {
  assertEquals(normalizeMemberName(null), "");
});

Deno.test("normalizeMemberName: whitespace only", () => {
  assertEquals(normalizeMemberName("   "), "");
});

// ============================================================
// memberKey() INTEGRATION
// ============================================================

Deno.test("memberKey: resolves identical key for Shri variant", () => {
  const keyA = memberKey("Parimal Nathwani (2026-32)", 557);
  const keyB = memberKey("Shri Parimal Nathwani (2026-32)", 557);
  assertEquals(keyA, keyB);
  assertEquals(keyA, "PARIMAL NATHWANI|557");
});

Deno.test("memberKey: different constituency produces different key", () => {
  const keyA = memberKey("Parimal Nathwani (2026-32)", 557);
  const keyB = memberKey("Parimal Nathwani (2026-32)", 999);
  assertEquals(keyA !== keyB, true);
});

Deno.test("memberKey: different people remain distinct", () => {
  const keyA = memberKey("Parimal Nathwani (2026-32)", 557);
  const keyB = memberKey("Suresh Nathwani (2026-32)", 557);
  assertEquals(keyA !== keyB, true);
});

Deno.test("memberKey: null constituency", () => {
  assertEquals(
    memberKey("Parimal Nathwani", null),
    "PARIMAL NATHWANI|"
  );
});

// ============================================================
// LEGITIMATE DIFFERENT MEMBERS (must NOT collapse)
// ============================================================

Deno.test("different first names stay distinct", () => {
  const a = normalizeMemberName("Parimal Nathwani (2026-32)");
  const b = normalizeMemberName("Suresh Nathwani (2026-32)");
  assertEquals(a !== b, true);
});

Deno.test("different last names stay distinct", () => {
  const a = normalizeMemberName("Parimal Nathwani (2026-32)");
  const b = normalizeMemberName("Parimal Patel (2026-32)");
  assertEquals(a !== b, true);
});

// ============================================================
// MULTI-TENURE IDENTITY
// ============================================================

Deno.test("same person different tenure normalizes identically", () => {
  const a = normalizeMemberName("Parimal Nathwani (2020-26)");
  const b = normalizeMemberName("Parimal Nathwani (2026-32)");
  assertEquals(a, b);
  assertEquals(a, "PARIMAL NATHWANI");
});

Deno.test("Shri variant different tenure → same identity", () => {
  const a = normalizeMemberName("Shri Parimal Nathwani (2020-26)");
  const b = normalizeMemberName("Parimal Nathwani (2026-32)");
  assertEquals(a, b);
  assertEquals(a, "PARIMAL NATHWANI");
});

Deno.test("tenure suffix is always stripped", () => {
  assertEquals(normalizeMemberName("Name (2020-26)"), "NAME");
  assertEquals(normalizeMemberName("Name (2026-32)"), "NAME");
  assertEquals(normalizeMemberName("Name (any)"), "NAME");
  assertEquals(normalizeMemberName("Name"), "NAME");
});
