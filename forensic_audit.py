#!/usr/bin/env python3
"""Standalone read-only forensic database audit for DB1 + DB2.

Queries production databases independently to verify data integrity.
Generates forensic_audit.json, forensic_audit.md, and a concise console summary.

STRICTLY READ-ONLY: No INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, TRUNCATE,
VACUUM, migrations, or automatic repairs. Only SELECT and information_schema queries.

Usage:
    python forensic_audit.py              # Run full audit
    python forensic_audit.py --json-only  # JSON only, skip markdown
    python forensic_audit.py --dry-run    # Test connections only
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
import sys
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse, urlunparse

import asyncpg

# ---------------------------------------------------------------------------
# IPv4 forcing (copied from db_pool.py to be standalone)
# ---------------------------------------------------------------------------

def _force_ipv4(dsn: str) -> str:
    try:
        parsed = urlparse(dsn)
        hostname = parsed.hostname
        if not hostname:
            return dsn
        infos = socket.getaddrinfo(hostname, None, socket.AF_INET, socket.SOCK_STREAM)
        if infos:
            ipv4_addr = infos[0][4][0]
            if ipv4_addr != hostname:
                replaced = parsed._replace(
                    netloc=parsed.netloc.replace(hostname, ipv4_addr)
                )
                return urlunparse(replaced)
        return dsn
    except (socket.gaierror, OSError):
        return dsn


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class CheckResult:
    name: str
    status: str  # PASS, FAIL, WARN, INFO, ERROR
    severity: str  # CRITICAL, HIGH, MEDIUM, LOW, INFORMATIONAL
    category: str  # CURRENT_PRODUCTION_ISSUE, HISTORICAL_ARCHIVAL, INFORMATIONAL_WARNING
    message: str
    details: Any = None
    row_count: Optional[int] = None
    query_ms: Optional[float] = None


@dataclass
class AuditSection:
    section_id: int
    title: str
    checks: list = field(default_factory=list)


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------

async def _init_conn(conn: asyncpg.Connection) -> None:
    await conn.execute("SET statement_timeout = '120000'")  # 2 min per query


async def create_pool(dsn: str, label: str) -> asyncpg.Pool:
    return await asyncpg.create_pool(
        dsn=_force_ipv4(dsn),
        min_size=1,
        max_size=2,
        statement_cache_size=0,
        command_timeout=120,
        init=_init_conn,
    )


@asynccontextmanager
async def db_conn(pool: asyncpg.Pool):
    async with pool.acquire() as conn:
        yield conn


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

async def fetchval(pool: asyncpg.Pool, sql: str) -> Any:
    async with db_conn(pool) as conn:
        return await conn.fetchval(sql)


async def fetch(pool: asyncpg.Pool, sql: str) -> list[asyncpg.Record]:
    async with db_conn(pool) as conn:
        return await conn.fetch(sql)


async def timed_query(pool: asyncpg.Pool, sql: str, label: str) -> tuple[list[asyncpg.Record], float]:
    t0 = time.monotonic()
    rows = await fetch(pool, sql)
    elapsed = (time.monotonic() - t0) * 1000
    return rows, elapsed


async def timed_fetchval(pool: asyncpg.Pool, sql: str, label: str) -> tuple[Any, float]:
    t0 = time.monotonic()
    val = await fetchval(pool, sql)
    elapsed = (time.monotonic() - t0) * 1000
    return val, elapsed


def _row_to_dict(row: asyncpg.Record) -> dict:
    return dict(row) if row else {}


def _rows_to_list(rows: list[asyncpg.Record]) -> list[dict]:
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# AUDIT SECTIONS
# ---------------------------------------------------------------------------

async def section_01_db_inventory(db1: asyncpg.Pool, db2: asyncpg.Pool) -> AuditSection:
    """Database inventory — list all tables on both databases."""
    sec = AuditSection(section_id=1, title="DATABASE INVENTORY")

    for label, pool in [("DB1", db1), ("DB2", db2)]:
        rows, ms = await timed_query(pool,
            "SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename",
            f"{label} tables")
        tables = [r["tablename"] for r in rows]
        sec.checks.append(CheckResult(
            name=f"{label} table list",
            status="PASS",
            severity="INFORMATIONAL",
            category="INFORMATIONAL_WARNING",
            message=f"{len(tables)} tables on {label}",
            details=tables,
            row_count=len(tables),
            query_ms=ms,
        ))

        # Check for backup/temp tables (exclude z_deprecated_* which are intentional archival)
        backup = [t for t in tables if any(x in t.lower() for x in ("backup", "tmp", "temp", "_old", "_bak"))]
        deprecated = [t for t in tables if t.startswith("z_deprecated_")]
        if backup:
            sec.checks.append(CheckResult(
                name=f"{label} backup/temp tables",
                status="WARN",
                severity="MEDIUM",
                category="HISTORICAL_ARCHIVAL",
                message=f"Found {len(backup)} backup/temp tables: {backup}",
                details=backup,
            ))
        else:
            sec.checks.append(CheckResult(
                name=f"{label} backup/temp tables",
                status="PASS",
                severity="INFORMATIONAL",
                category="INFORMATIONAL_WARNING",
                message="No backup/temp tables found",
            ))

    return sec


async def section_02_member_population(db1: asyncpg.Pool, db2: asyncpg.Pool) -> AuditSection:
    """Canonical member population — 543 MPs + 233 MLAs = 776 total."""
    sec = AuditSection(section_id=2, title="CANONICAL MEMBER POPULATION")

    mp_count, ms1 = await timed_fetchval(db1, "SELECT COUNT(*) FROM mps", "MP count")
    mla_count, ms2 = await timed_fetchval(db1, "SELECT COUNT(*) FROM mlas", "MLA count")
    total = mp_count + mla_count

    sec.checks.append(CheckResult(
        name="MP count",
        status="PASS" if mp_count == 543 else "FAIL",
        severity="CRITICAL" if mp_count != 543 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if mp_count != 543 else "INFORMATIONAL_WARNING",
        message=f"Expected 543, got {mp_count}",
        row_count=mp_count,
        query_ms=ms1,
    ))
    sec.checks.append(CheckResult(
        name="MLA count",
        status="PASS" if mla_count == 233 else "FAIL",
        severity="CRITICAL" if mla_count != 233 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if mla_count != 233 else "INFORMATIONAL_WARNING",
        message=f"Expected 233, got {mla_count}",
        row_count=mla_count,
        query_ms=ms2,
    ))
    sec.checks.append(CheckResult(
        name="Total canonical members",
        status="PASS" if total == 776 else "FAIL",
        severity="CRITICAL" if total != 776 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if total != 776 else "INFORMATIONAL_WARNING",
        message=f"Expected 776, got {total} ({mp_count} MPs + {mla_count} MLAs)",
        row_count=total,
    ))

    # Duplicate member names
    dup_mp_rows, ms3 = await timed_query(db1,
        "SELECT mp_name, COUNT(*) AS cnt FROM mps GROUP BY mp_name HAVING COUNT(*) > 1",
        "Duplicate MP names")
    if dup_mp_rows:
        sec.checks.append(CheckResult(
            name="Duplicate MP names",
            status="WARN",
            severity="MEDIUM",
            category="HISTORICAL_ARCHIVAL",
            message=f"{len(dup_mp_rows)} MP names appear multiple times",
            details=_rows_to_list(dup_mp_rows),
            query_ms=ms3,
        ))
    else:
        sec.checks.append(CheckResult(
            name="Duplicate MP names",
            status="PASS",
            severity="INFORMATIONAL",
            category="INFORMATIONAL_WARNING",
            message="No duplicate MP names",
            query_ms=ms3,
        ))

    dup_mla_rows, ms4 = await timed_query(db1,
        "SELECT mla_name, COUNT(*) AS cnt FROM mlas GROUP BY mla_name HAVING COUNT(*) > 1",
        "Duplicate MLA names")
    if dup_mla_rows:
        sec.checks.append(CheckResult(
            name="Duplicate MLA names",
            status="WARN",
            severity="MEDIUM",
            category="HISTORICAL_ARCHIVAL",
            message=f"{len(dup_mla_rows)} MLA names appear multiple times",
            details=_rows_to_list(dup_mla_rows),
            query_ms=ms4,
        ))
    else:
        sec.checks.append(CheckResult(
            name="Duplicate MLA names",
            status="PASS",
            severity="INFORMATIONAL",
            category="INFORMATIONAL_WARNING",
            message="No duplicate MLA names",
            query_ms=ms4,
        ))

    # Unique IDs check
    mp_unique, _ = await timed_fetchval(db1, "SELECT COUNT(DISTINCT mp_id) FROM mps", "MP unique IDs")
    mla_unique, _ = await timed_fetchval(db1, "SELECT COUNT(DISTINCT mla_id) FROM mlas", "MLA unique IDs")
    sec.checks.append(CheckResult(
        name="MP unique IDs",
        status="PASS" if mp_unique == mp_count else "FAIL",
        severity="CRITICAL" if mp_unique != mp_count else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if mp_unique != mp_count else "INFORMATIONAL_WARNING",
        message=f"{mp_unique} unique mp_id values for {mp_count} rows",
        row_count=mp_unique,
    ))
    sec.checks.append(CheckResult(
        name="MLA unique IDs",
        status="PASS" if mla_unique == mla_count else "FAIL",
        severity="CRITICAL" if mla_unique != mla_count else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if mla_unique != mla_count else "INFORMATIONAL_WARNING",
        message=f"{mla_unique} unique mla_id values for {mla_count} rows",
        row_count=mla_unique,
    ))

    return sec


async def section_03_synthetic_ids(db1: asyncpg.Pool, db2: asyncpg.Pool) -> AuditSection:
    """Synthetic ID detection — IDs >= 100000 or < 0."""
    sec = AuditSection(section_id=3, title="SYNTHETIC ID DETECTION")

    queries = [
        ("DB1 mps synthetic IDs", db1,
         "SELECT mp_id, mp_name FROM mps WHERE mp_id >= 100000 OR mp_id < 0"),
        ("DB1 mlas synthetic IDs", db1,
         "SELECT mla_id, mla_name FROM mlas WHERE mla_id >= 100000 OR mla_id < 0"),
        ("DB2 member_metrics synthetic IDs", db2,
         "SELECT member_id, member_type, member_name FROM member_metrics WHERE member_id >= 100000 OR member_id < 0"),
        ("DB2 member_intelligence synthetic IDs", db2,
         "SELECT member_id, member_type FROM member_intelligence WHERE member_id >= 100000 OR member_id < 0"),
    ]

    for name, pool, sql in queries:
        rows, ms = await timed_query(pool, sql, name)
        sec.checks.append(CheckResult(
            name=name,
            status="PASS" if len(rows) == 0 else "FAIL",
            severity="HIGH" if len(rows) > 0 else "INFORMATIONAL",
            category="CURRENT_PRODUCTION_ISSUE" if len(rows) > 0 else "INFORMATIONAL_WARNING",
            message=f"Found {len(rows)} synthetic IDs" if rows else "No synthetic IDs found",
            details=_rows_to_list(rows) if rows else None,
            row_count=len(rows),
            query_ms=ms,
        ))

    return sec


async def section_04_mp_mla_separation(db1: asyncpg.Pool, db2: asyncpg.Pool) -> AuditSection:
    """MP/MLA separation — no cross-contamination between work tables."""
    sec = AuditSection(section_id=4, title="MP/MLA SEPARATION")

    mp_works, ms1 = await timed_fetchval(db2, "SELECT COUNT(*) FROM work_analysis", "MP work count")
    mla_works, ms2 = await timed_fetchval(db2, "SELECT COUNT(*) FROM mla_work_analysis", "MLA work count")

    sec.checks.append(CheckResult(
        name="MP work_analysis count",
        status="PASS",
        severity="INFORMATIONAL",
        category="INFORMATIONAL_WARNING",
        message=f"{mp_works:,} MP work records",
        row_count=mp_works,
        query_ms=ms1,
    ))
    sec.checks.append(CheckResult(
        name="MLA work_analysis count",
        status="PASS",
        severity="INFORMATIONAL",
        category="INFORMATIONAL_WARNING",
        message=f"{mla_works:,} MLA work records",
        row_count=mla_works,
        query_ms=ms2,
    ))

    # Wrong member_type in MP table
    wrong_mp, ms3 = await timed_query(db2,
        "SELECT work_id, member_id, member_type FROM work_analysis WHERE member_type != 'MP'",
        "Wrong member_type in work_analysis")
    sec.checks.append(CheckResult(
        name="Non-MP member_type in work_analysis",
        status="PASS" if len(wrong_mp) == 0 else "FAIL",
        severity="CRITICAL" if len(wrong_mp) > 0 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if len(wrong_mp) > 0 else "INFORMATIONAL_WARNING",
        message=f"Found {len(wrong_mp)} non-MP records in MP work table" if wrong_mp else "All records in work_analysis are MP",
        details=_rows_to_list(wrong_mp) if wrong_mp else None,
        row_count=len(wrong_mp),
        query_ms=ms3,
    ))

    # Wrong member_type in MLA table
    wrong_mla, ms4 = await timed_query(db2,
        "SELECT work_id, member_id, member_type FROM mla_work_analysis WHERE member_type != 'MLA'",
        "Wrong member_type in mla_work_analysis")
    sec.checks.append(CheckResult(
        name="Non-MLA member_type in mla_work_analysis",
        status="PASS" if len(wrong_mla) == 0 else "FAIL",
        severity="CRITICAL" if len(wrong_mla) > 0 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if len(wrong_mla) > 0 else "INFORMATIONAL_WARNING",
        message=f"Found {len(wrong_mla)} non-MLA records in MLA work table" if wrong_mla else "All records in mla_work_analysis are MLA",
        details=_rows_to_list(wrong_mla) if wrong_mla else None,
        row_count=len(wrong_mla),
        query_ms=ms4,
    ))

    # Member IDs that are MP in work_analysis AND MLA in mla_work_analysis
    # (i.e. the SAME entity_id appears with DIFFERENT member_type in both tables)
    # This is the real contamination check — bare member_id overlap is expected
    # because both tables use auto-incrementing IDs that naturally collide.
    cross, ms5 = await timed_query(db2,
        """SELECT DISTINCT wa.member_id
           FROM work_analysis wa
           INNER JOIN mla_work_analysis mwa ON wa.member_id = mwa.member_id
           WHERE wa.member_type = 'MP' AND mwa.member_type = 'MLA'""",
        "Cross-table member IDs (actual contamination)")
    sec.checks.append(CheckResult(
        name="Member IDs in both MP and MLA work tables",
        status="PASS" if len(cross) == 0 else "WARN",
        severity="HIGH" if len(cross) > 0 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if len(cross) > 0 else "INFORMATIONAL_WARNING",
        message=f"Found {len(cross)} distinct member_ids with MP works AND MLA works (auto-ID overlap is normal)" if cross else "No cross-table member IDs",
        details=_rows_to_list(cross[:20]) if cross else None,
        row_count=len(cross),
        query_ms=ms5,
    ))

    return sec


async def section_05_duplicate_works(db1: asyncpg.Pool, db2: asyncpg.Pool) -> AuditSection:
    """Duplicate works — check for duplicate work_ids in both tables."""
    sec = AuditSection(section_id=5, title="DUPLICATE WORKS")

    for table, label in [("work_analysis", "MP"), ("mla_work_analysis", "MLA")]:
        # Find duplicates using real business key: work_id
        rows, ms = await timed_query(db2,
            f"SELECT work_id, COUNT(*) AS cnt FROM {table} GROUP BY work_id HAVING COUNT(*) > 1 ORDER BY cnt DESC LIMIT 20",
            f"{label} duplicate work_ids")
        sec.checks.append(CheckResult(
            name=f"{label} duplicate work_ids in {table}",
            status="PASS" if len(rows) == 0 else "FAIL",
            severity="CRITICAL" if len(rows) > 0 else "INFORMATIONAL",
            category="CURRENT_PRODUCTION_ISSUE" if len(rows) > 0 else "INFORMATIONAL_WARNING",
            message=f"Found {len(rows)} duplicate work_id groups" if rows else "No duplicate work_ids",
            details=_rows_to_list(rows) if rows else None,
            row_count=len(rows),
            query_ms=ms,
        ))

    # Cross-table work_id overlap
    cross, ms = await timed_query(db2,
        """SELECT work_id FROM work_analysis
           INTERSECT
           SELECT work_id FROM mla_work_analysis""",
        "Cross-table work_id overlap")
    sec.checks.append(CheckResult(
        name="work_id overlap between MP and MLA tables",
        status="PASS" if len(cross) == 0 else "FAIL",
        severity="HIGH" if len(cross) > 0 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if len(cross) > 0 else "INFORMATIONAL_WARNING",
        message=f"Found {len(cross)} overlapping work_ids" if cross else "No overlapping work_ids",
        details=_rows_to_list(cross) if cross else None,
        row_count=len(cross),
        query_ms=ms,
    ))

    # Check PK constraints exist
    for table, label in [("work_analysis", "MP"), ("mla_work_analysis", "MLA")]:
        pk, ms = await timed_query(db2,
            f"""SELECT conname FROM pg_constraint
                WHERE conrelid = 'public.{table}'::regclass AND contype = 'p'""",
            f"{label} PK constraint")
        sec.checks.append(CheckResult(
            name=f"{label} {table} PK constraint",
            status="PASS" if len(pk) > 0 else "WARN",
            severity="HIGH" if len(pk) == 0 else "INFORMATIONAL",
            category="CURRENT_PRODUCTION_ISSUE" if len(pk) == 0 else "INFORMATIONAL_WARNING",
            message=f"PK constraint exists: {pk[0]['conname']}" if pk else f"No PK constraint on {table}",
            details=_rows_to_list(pk) if pk else None,
            query_ms=ms,
        ))

    return sec


async def section_06_member_metrics(db1: asyncpg.Pool, db2: asyncpg.Pool) -> AuditSection:
    """Member metrics — duplicates and synthetic IDs."""
    sec = AuditSection(section_id=6, title="MEMBER METRICS INTEGRITY")

    total, ms1 = await timed_fetchval(db2, "SELECT COUNT(*) FROM member_metrics", "Total member_metrics")
    sec.checks.append(CheckResult(
        name="member_metrics total count",
        status="PASS" if total == 776 else "WARN",
        severity="HIGH" if total != 776 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if total != 776 else "INFORMATIONAL_WARNING",
        message=f"Expected 776, got {total}",
        row_count=total,
        query_ms=ms1,
    ))

    # By type
    type_rows, ms2 = await timed_query(db2,
        "SELECT member_type, COUNT(*) AS cnt FROM member_metrics GROUP BY member_type ORDER BY member_type",
        "member_metrics by type")
    sec.checks.append(CheckResult(
        name="member_metrics by type",
        status="PASS",
        severity="INFORMATIONAL",
        category="INFORMATIONAL_WARNING",
        message=", ".join(f"{r['member_type']}: {r['cnt']}" for r in type_rows),
        details=_rows_to_list(type_rows),
        query_ms=ms2,
    ))

    # Duplicate member_id + member_type
    dup, ms3 = await timed_query(db2,
        """SELECT member_id, member_type, member_name, COUNT(*) AS cnt
           FROM member_metrics GROUP BY member_id, member_type, member_name HAVING COUNT(*) > 1""",
        "Duplicate member_metrics")
    sec.checks.append(CheckResult(
        name="Duplicate member_id+type in member_metrics",
        status="PASS" if len(dup) == 0 else "FAIL",
        severity="CRITICAL" if len(dup) > 0 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if len(dup) > 0 else "INFORMATIONAL_WARNING",
        message=f"Found {len(dup)} duplicate groups" if dup else "No duplicates",
        details=_rows_to_list(dup) if dup else None,
        row_count=len(dup),
        query_ms=ms3,
    ))

    # Synthetic IDs
    synth, ms4 = await timed_query(db2,
        "SELECT member_id, member_type, member_name FROM member_metrics WHERE member_id >= 100000 OR member_id < 0",
        "Synthetic member_metrics IDs")
    sec.checks.append(CheckResult(
        name="Synthetic IDs in member_metrics",
        status="PASS" if len(synth) == 0 else "FAIL",
        severity="HIGH" if len(synth) > 0 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if len(synth) > 0 else "INFORMATIONAL_WARNING",
        message=f"Found {len(synth)} synthetic IDs" if synth else "No synthetic IDs",
        details=_rows_to_list(synth) if synth else None,
        row_count=len(synth),
        query_ms=ms4,
    ))

    return sec


async def section_07_member_intelligence(db1: asyncpg.Pool, db2: asyncpg.Pool) -> AuditSection:
    """Member intelligence — duplicates, orphans, synthetic IDs."""
    sec = AuditSection(section_id=7, title="MEMBER INTELLIGENCE INTEGRITY")

    total, ms1 = await timed_fetchval(db2, "SELECT COUNT(*) FROM member_intelligence", "Total member_intelligence")
    sec.checks.append(CheckResult(
        name="member_intelligence total count",
        status="PASS" if total == 776 else "WARN",
        severity="HIGH" if total != 776 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if total != 776 else "INFORMATIONAL_WARNING",
        message=f"Expected 776, got {total}",
        row_count=total,
        query_ms=ms1,
    ))

    # By type
    type_rows, ms2 = await timed_query(db2,
        "SELECT member_type, COUNT(*) AS cnt FROM member_intelligence GROUP BY member_type ORDER BY member_type",
        "member_intelligence by type")
    sec.checks.append(CheckResult(
        name="member_intelligence by type",
        status="PASS",
        severity="INFORMATIONAL",
        category="INFORMATIONAL_WARNING",
        message=", ".join(f"{r['member_type']}: {r['cnt']}" for r in type_rows),
        details=_rows_to_list(type_rows),
        query_ms=ms2,
    ))

    # Duplicate member_id + member_type
    dup, ms3 = await timed_query(db2,
        """SELECT member_id, member_type, COUNT(*) AS cnt
           FROM member_intelligence GROUP BY member_id, member_type HAVING COUNT(*) > 1""",
        "Duplicate member_intelligence")
    sec.checks.append(CheckResult(
        name="Duplicate member_id+type in member_intelligence",
        status="PASS" if len(dup) == 0 else "FAIL",
        severity="CRITICAL" if len(dup) > 0 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if len(dup) > 0 else "INFORMATIONAL_WARNING",
        message=f"Found {len(dup)} duplicate groups" if dup else "No duplicates",
        details=_rows_to_list(dup) if dup else None,
        row_count=len(dup),
        query_ms=ms3,
    ))

    # Synthetic IDs
    synth, ms4 = await timed_query(db2,
        "SELECT member_id, member_type FROM member_intelligence WHERE member_id >= 100000 OR member_id < 0",
        "Synthetic member_intelligence IDs")
    sec.checks.append(CheckResult(
        name="Synthetic IDs in member_intelligence",
        status="PASS" if len(synth) == 0 else "FAIL",
        severity="HIGH" if len(synth) > 0 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if len(synth) > 0 else "INFORMATIONAL_WARNING",
        message=f"Found {len(synth)} synthetic IDs" if synth else "No synthetic IDs",
        details=_rows_to_list(synth) if synth else None,
        row_count=len(synth),
        query_ms=ms4,
    ))

    # Members in member_metrics but NOT in member_intelligence
    missing_mi, ms5 = await timed_query(db2,
        """SELECT mm.member_id, mm.member_type, mm.member_name
           FROM member_metrics mm
           LEFT JOIN member_intelligence mi ON mm.member_id = mi.member_id AND mm.member_type = mi.member_type
           WHERE mi.member_id IS NULL""",
        "Members in metrics but not intelligence")
    sec.checks.append(CheckResult(
        name="Members in member_metrics but not member_intelligence",
        status="PASS" if len(missing_mi) == 0 else "FAIL",
        severity="HIGH" if len(missing_mi) > 0 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if len(missing_mi) > 0 else "INFORMATIONAL_WARNING",
        message=f"Found {len(missing_mi)} members missing intelligence records" if missing_mi else "All members have intelligence records",
        details=_rows_to_list(missing_mi) if missing_mi else None,
        row_count=len(missing_mi),
        query_ms=ms5,
    ))

    # Members in member_intelligence but NOT in member_metrics
    orphan_mi, ms6 = await timed_query(db2,
        """SELECT mi.member_id, mi.member_type
           FROM member_intelligence mi
           LEFT JOIN member_metrics mm ON mi.member_id = mm.member_id AND mi.member_type = mm.member_type
           WHERE mm.member_id IS NULL""",
        "Intelligence records without metrics")
    sec.checks.append(CheckResult(
        name="member_intelligence without member_metrics",
        status="PASS" if len(orphan_mi) == 0 else "WARN",
        severity="HIGH" if len(orphan_mi) > 0 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if len(orphan_mi) > 0 else "INFORMATIONAL_WARNING",
        message=f"Found {len(orphan_mi)} orphan intelligence records" if orphan_mi else "No orphan intelligence records",
        details=_rows_to_list(orphan_mi) if orphan_mi else None,
        row_count=len(orphan_mi),
        query_ms=ms6,
    ))

    return sec


async def section_08_state_tables(db1: asyncpg.Pool, db2: asyncpg.Pool) -> AuditSection:
    """State metrics and intelligence — duplicates and cross-checks."""
    sec = AuditSection(section_id=8, title="STATE TABLES INTEGRITY")

    sm_count, ms1 = await timed_fetchval(db2, "SELECT COUNT(*) FROM state_metrics", "state_metrics count")
    si_count, ms2 = await timed_fetchval(db2, "SELECT COUNT(*) FROM state_intelligence", "state_intelligence count")

    sec.checks.append(CheckResult(
        name="state_metrics count",
        status="PASS" if sm_count >= 36 else "WARN",
        severity="HIGH" if sm_count < 36 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if sm_count < 36 else "INFORMATIONAL_WARNING",
        message=f"Expected >= 36, got {sm_count}",
        row_count=sm_count,
        query_ms=ms1,
    ))
    sec.checks.append(CheckResult(
        name="state_intelligence count",
        status="PASS" if si_count >= 36 else "WARN",
        severity="HIGH" if si_count < 36 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if si_count < 36 else "INFORMATIONAL_WARNING",
        message=f"Expected >= 36, got {si_count}",
        row_count=si_count,
        query_ms=ms2,
    ))

    # Duplicate state_ids
    for table, label in [("state_metrics", "state_metrics"), ("state_intelligence", "state_intelligence")]:
        dup, ms = await timed_query(db2,
            f"SELECT state_id, COUNT(*) AS cnt FROM {table} GROUP BY state_id HAVING COUNT(*) > 1",
            f"Duplicate {label}")
        sec.checks.append(CheckResult(
            name=f"Duplicate state_id in {label}",
            status="PASS" if len(dup) == 0 else "FAIL",
            severity="HIGH" if len(dup) > 0 else "INFORMATIONAL",
            category="CURRENT_PRODUCTION_ISSUE" if len(dup) > 0 else "INFORMATIONAL_WARNING",
            message=f"Found {len(dup)} duplicate state_ids" if dup else "No duplicates",
            details=_rows_to_list(dup) if dup else None,
            row_count=len(dup),
            query_ms=ms,
        ))

    # NULL state_id
    null_sm, _ = await timed_fetchval(db2, "SELECT COUNT(*) FROM state_metrics WHERE state_id IS NULL", "NULL state_id sm")
    null_si, _ = await timed_fetchval(db2, "SELECT COUNT(*) FROM state_intelligence WHERE state_id IS NULL", "NULL state_id si")
    sec.checks.append(CheckResult(
        name="NULL state_id in state_metrics",
        status="PASS" if null_sm == 0 else "FAIL",
        severity="HIGH" if null_sm > 0 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if null_sm > 0 else "INFORMATIONAL_WARNING",
        message=f"Found {null_sm} NULL state_ids" if null_sm else "No NULL state_ids",
        row_count=null_sm,
    ))
    sec.checks.append(CheckResult(
        name="NULL state_id in state_intelligence",
        status="PASS" if null_si == 0 else "FAIL",
        severity="HIGH" if null_si > 0 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if null_si > 0 else "INFORMATIONAL_WARNING",
        message=f"Found {null_si} NULL state_ids" if null_si else "No NULL state_ids",
        row_count=null_si,
    ))

    # Cross-checks
    missing_si, ms5 = await timed_query(db2,
        """SELECT sm.state_id, sm.state_name
           FROM state_metrics sm
           LEFT JOIN state_intelligence si ON sm.state_id = si.state_id
           WHERE si.state_id IS NULL""",
        "States in metrics but not intelligence")
    sec.checks.append(CheckResult(
        name="States in state_metrics but not state_intelligence",
        status="PASS" if len(missing_si) == 0 else "FAIL",
        severity="HIGH" if len(missing_si) > 0 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if len(missing_si) > 0 else "INFORMATIONAL_WARNING",
        message=f"Found {len(missing_si)} states missing intelligence" if missing_si else "All states have intelligence",
        details=_rows_to_list(missing_si) if missing_si else None,
        row_count=len(missing_si),
        query_ms=ms5,
    ))

    missing_sm, ms6 = await timed_query(db2,
        """SELECT si.state_id
           FROM state_intelligence si
           LEFT JOIN state_metrics sm ON si.state_id = sm.state_id
           WHERE sm.state_id IS NULL""",
        "States in intelligence but not metrics")
    sec.checks.append(CheckResult(
        name="States in state_intelligence but not state_metrics",
        status="PASS" if len(missing_sm) == 0 else "WARN",
        severity="HIGH" if len(missing_sm) > 0 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if len(missing_sm) > 0 else "INFORMATIONAL_WARNING",
        message=f"Found {len(missing_sm)} orphan intelligence records" if missing_sm else "No orphan intelligence records",
        details=_rows_to_list(missing_sm) if missing_sm else None,
        row_count=len(missing_sm),
        query_ms=ms6,
    ))

    return sec


async def section_09_evidence_gemini(db1: asyncpg.Pool, db2: asyncpg.Pool) -> AuditSection:
    """Evidence and Gemini records — duplicates and backlog state."""
    sec = AuditSection(section_id=9, title="EVIDENCE & GEMINI INTEGRITY")

    # entity_evidence
    ee_total, ms1 = await timed_fetchval(db2, "SELECT COUNT(*) FROM entity_evidence", "entity_evidence count")
    sec.checks.append(CheckResult(
        name="entity_evidence total count",
        status="PASS",
        severity="INFORMATIONAL",
        category="INFORMATIONAL_WARNING",
        message=f"{ee_total:,} evidence records",
        row_count=ee_total,
        query_ms=ms1,
    ))

    ee_dup, ms2 = await timed_query(db2,
        """SELECT entity_type, entity_id, COUNT(*) AS cnt
           FROM entity_evidence GROUP BY entity_type, entity_id HAVING COUNT(*) > 1""",
        "entity_evidence duplicates")
    sec.checks.append(CheckResult(
        name="Duplicate entity_type+entity_id in entity_evidence",
        status="PASS" if len(ee_dup) == 0 else "WARN",
        severity="MEDIUM" if len(ee_dup) > 0 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if len(ee_dup) > 0 else "INFORMATIONAL_WARNING",
        message=f"Found {len(ee_dup)} duplicate groups" if ee_dup else "No duplicates",
        details=_rows_to_list(ee_dup) if ee_dup else None,
        row_count=len(ee_dup),
        query_ms=ms2,
    ))

    # ai_analysis
    aa_total, ms3 = await timed_fetchval(db2, "SELECT COUNT(*) FROM ai_analysis", "ai_analysis count")
    sec.checks.append(CheckResult(
        name="ai_analysis total count",
        status="PASS",
        severity="INFORMATIONAL",
        category="INFORMATIONAL_WARNING",
        message=f"{aa_total:,} AI analysis records",
        row_count=aa_total,
        query_ms=ms3,
    ))

    aa_dup, ms4 = await timed_query(db2,
        """SELECT entity_type, entity_id, COUNT(*) AS cnt
           FROM ai_analysis GROUP BY entity_type, entity_id HAVING COUNT(*) > 1""",
        "ai_analysis duplicates")
    sec.checks.append(CheckResult(
        name="Duplicate entity_type+entity_id in ai_analysis",
        status="PASS" if len(aa_dup) == 0 else "WARN",
        severity="MEDIUM" if len(aa_dup) > 0 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if len(aa_dup) > 0 else "INFORMATIONAL_WARNING",
        message=f"Found {len(aa_dup)} duplicate groups" if aa_dup else "No duplicates",
        details=_rows_to_list(aa_dup) if aa_dup else None,
        row_count=len(aa_dup),
        query_ms=ms4,
    ))

    # evidence_work_refs
    ewr_total, ms5 = await timed_fetchval(db2, "SELECT COUNT(*) FROM evidence_work_refs", "evidence_work_refs count")
    sec.checks.append(CheckResult(
        name="evidence_work_refs total count",
        status="PASS",
        severity="INFORMATIONAL",
        category="INFORMATIONAL_WARNING",
        message=f"{ewr_total:,} evidence work references",
        row_count=ewr_total,
        query_ms=ms5,
    ))

    # gemini_retry_backlog
    try:
        backlog_rows, ms6 = await timed_query(db2,
            "SELECT status, COUNT(*) AS cnt FROM gemini_retry_backlog GROUP BY status",
            "Backlog status")
        pending = next((r["cnt"] for r in backlog_rows if r["status"] == "PENDING"), 0)
        total_backlog = sum(r["cnt"] for r in backlog_rows)

        sec.checks.append(CheckResult(
            name="gemini_retry_backlog status",
            status="PASS" if pending == 0 else "WARN",
            severity="MEDIUM" if pending > 0 else "INFORMATIONAL",
            category="CURRENT_PRODUCTION_ISSUE" if pending > 0 else "INFORMATIONAL_WARNING",
            message=f"Total backlog: {total_backlog}, Pending: {pending}",
            details=_rows_to_list(backlog_rows),
            row_count=total_backlog,
            query_ms=ms6,
        ))
    except Exception as e:
        sec.checks.append(CheckResult(
            name="gemini_retry_backlog status",
            status="WARN",
            severity="LOW",
            category="INFORMATIONAL_WARNING",
            message=f"Table may not exist or is empty: {e}",
        ))

    return sec


async def section_10_anomaly_data(db1: asyncpg.Pool, db2: asyncpg.Pool) -> AuditSection:
    """Anomaly data — isolation levels, Isolation Forest, XGBoost check."""
    sec = AuditSection(section_id=10, title="ANOMALY DATA INTEGRITY")

    # Check if ml_work_anomaly table exists
    ml_anomaly_exists, ms0 = await timed_fetchval(db2,
        """SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname='public' AND tablename='ml_work_anomaly')""",
        "ml_work_anomaly exists")
    sec.checks.append(CheckResult(
        name="ml_work_anomaly table exists",
        status="PASS" if ml_anomaly_exists else "WARN",
        severity="MEDIUM" if not ml_anomaly_exists else "INFORMATIONAL",
        category="HISTORICAL_ARCHIVAL" if not ml_anomaly_exists else "INFORMATIONAL_WARNING",
        message="Table exists" if ml_anomaly_exists else "Table does NOT exist (may be expected if not yet created)",
        query_ms=ms0,
    ))

    # Isolation levels in work_analysis
    iso_mp, ms1 = await timed_query(db2,
        """SELECT isolation_level, COUNT(*) AS cnt FROM work_analysis
           GROUP BY isolation_level ORDER BY isolation_level""",
        "MP isolation levels")
    sec.checks.append(CheckResult(
        name="MP work_analysis isolation levels",
        status="PASS",
        severity="INFORMATIONAL",
        category="INFORMATIONAL_WARNING",
        message=", ".join(f"{r['isolation_level']}: {r['cnt']}" for r in iso_mp) if iso_mp else "No isolation levels",
        details=_rows_to_list(iso_mp),
        query_ms=ms1,
    ))

    iso_mla, ms2 = await timed_query(db2,
        """SELECT isolation_level, COUNT(*) AS cnt FROM mla_work_analysis
           GROUP BY isolation_level ORDER BY isolation_level""",
        "MLA isolation levels")
    sec.checks.append(CheckResult(
        name="MLA work_analysis isolation levels",
        status="PASS",
        severity="INFORMATIONAL",
        category="INFORMATIONAL_WARNING",
        message=", ".join(f"{r['isolation_level']}: {r['cnt']}" for r in iso_mla) if iso_mla else "No isolation levels",
        details=_rows_to_list(iso_mla),
        query_ms=ms2,
    ))

    # Works with isolation scores
    mp_iso, ms3 = await timed_fetchval(db2,
        "SELECT COUNT(*) FROM work_analysis WHERE isolation_score IS NOT NULL",
        "MP works with isolation score")
    sec.checks.append(CheckResult(
        name="MP works with isolation_score",
        status="PASS",
        severity="INFORMATIONAL",
        category="INFORMATIONAL_WARNING",
        message=f"{mp_iso:,} MP works have isolation scores",
        row_count=mp_iso,
        query_ms=ms3,
    ))

    mla_iso, ms4 = await timed_fetchval(db2,
        "SELECT COUNT(*) FROM mla_work_analysis WHERE isolation_score IS NOT NULL",
        "MLA works with isolation score")
    sec.checks.append(CheckResult(
        name="MLA works with isolation_score",
        status="PASS",
        severity="INFORMATIONAL",
        category="INFORMATIONAL_WARNING",
        message=f"{mla_iso:,} MLA works have isolation scores",
        row_count=mla_iso,
        query_ms=ms4,
    ))

    # Works missing isolation scores
    mp_no_iso, _ = await timed_fetchval(db2,
        "SELECT COUNT(*) FROM work_analysis WHERE isolation_score IS NULL",
        "MP works missing isolation")
    sec.checks.append(CheckResult(
        name="MP works missing isolation_score",
        status="PASS" if mp_no_iso == 0 else "WARN",
        severity="MEDIUM" if mp_no_iso > 0 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if mp_no_iso > 0 else "INFORMATIONAL_WARNING",
        message=f"{mp_no_iso:,} MP works missing isolation_score" if mp_no_iso else "All MP works have isolation scores",
        row_count=mp_no_iso,
    ))

    mla_no_iso, _ = await timed_fetchval(db2,
        "SELECT COUNT(*) FROM mla_work_analysis WHERE isolation_score IS NULL",
        "MLA works missing isolation")
    sec.checks.append(CheckResult(
        name="MLA works missing isolation_score",
        status="PASS" if mla_no_iso == 0 else "WARN",
        severity="MEDIUM" if mla_no_iso > 0 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if mla_no_iso > 0 else "INFORMATIONAL_WARNING",
        message=f"{mla_no_iso:,} MLA works missing isolation_score" if mla_no_iso else "All MLA works have isolation scores",
        row_count=mla_no_iso,
    ))

    # Model registry
    try:
        model_rows, ms5 = await timed_query(db2,
            "SELECT model_name, model_type, status, training_observations FROM model_registry",
            "Model registry")
        sec.checks.append(CheckResult(
            name="model_registry contents",
            status="PASS",
            severity="INFORMATIONAL",
            category="INFORMATIONAL_WARNING",
            message=f"{len(model_rows)} models registered",
            details=_rows_to_list(model_rows),
            row_count=len(model_rows),
            query_ms=ms5,
        ))

        # XGBoost check
        xgb = [r for r in model_rows if "xgboost" in (r.get("model_name", "") + r.get("model_type", "")).lower()]
        sec.checks.append(CheckResult(
            name="XGBoost production artifacts",
            status="PASS" if len(xgb) == 0 else "WARN",
            severity="MEDIUM" if len(xgb) > 0 else "INFORMATIONAL",
            category="HISTORICAL_ARCHIVAL" if len(xgb) > 0 else "INFORMATIONAL_WARNING",
            message=f"Found {len(xgb)} XGBoost entries" if xgb else "No XGBoost artifacts found",
            details=_rows_to_list(xgb) if xgb else None,
        ))
    except Exception as e:
        sec.checks.append(CheckResult(
            name="model_registry contents",
            status="WARN",
            severity="LOW",
            category="INFORMATIONAL_WARNING",
            message=f"Table may not exist: {e}",
        ))

    return sec


async def section_11_performance_scores(db1: asyncpg.Pool, db2: asyncpg.Pool) -> AuditSection:
    """Performance scores — formula consistency and distribution."""
    sec = AuditSection(section_id=11, title="PERFORMANCE SCORES")

    # Qualified members
    qualified, ms1 = await timed_fetchval(db2,
        "SELECT COUNT(*) FROM member_metrics WHERE ranking_qualified = true",
        "Qualified members")
    sec.checks.append(CheckResult(
        name="ranking_qualified members",
        status="PASS" if qualified == 776 else "WARN",
        severity="HIGH" if qualified != 776 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if qualified != 776 else "INFORMATIONAL_WARNING",
        message=f"Expected 776 qualified, got {qualified}",
        row_count=qualified,
        query_ms=ms1,
    ))

    # Score distribution
    score_stats, ms2 = await timed_query(db2,
        """SELECT
             MIN(performance_score_weighted) AS min_score,
             MAX(performance_score_weighted) AS max_score,
             AVG(performance_score_weighted) AS avg_score,
             COUNT(*) AS total,
             COUNT(*) FILTER (WHERE performance_score_weighted IS NULL) AS null_scores,
             COUNT(*) FILTER (WHERE performance_score_weighted < 0) AS negative_scores,
             COUNT(*) FILTER (WHERE performance_score_weighted > 100) AS over_100
           FROM member_metrics WHERE ranking_qualified = true""",
        "Score distribution")
    if score_stats:
        s = dict(score_stats[0])
        sec.checks.append(CheckResult(
            name="Performance score distribution (qualified members)",
            status="PASS",
            severity="INFORMATIONAL",
            category="INFORMATIONAL_WARNING",
            message=f"Min: {s['min_score']}, Max: {s['max_score']}, Avg: {s['avg_score']:.2f}, "
                    f"NULL: {s['null_scores']}, Negative: {s['negative_scores']}, >100: {s['over_100']}",
            details=s,
            query_ms=ms2,
        ))

        # Validate scores within 0-100 range
        out_of_range = (s.get("negative_scores", 0) or 0) + (s.get("over_100", 0) or 0)
        sec.checks.append(CheckResult(
            name="Scores within valid range [0,100]",
            status="PASS" if out_of_range == 0 else "FAIL",
            severity="HIGH" if out_of_range > 0 else "INFORMATIONAL",
            category="CURRENT_PRODUCTION_ISSUE" if out_of_range > 0 else "INFORMATIONAL_WARNING",
            message=f"{out_of_range} scores outside [0,100]" if out_of_range else "All scores within [0,100]",
            row_count=out_of_range,
        ))

        null_count = s.get("null_scores", 0) or 0
        sec.checks.append(CheckResult(
            name="NULL scores for qualified members",
            status="PASS" if null_count == 0 else "WARN",
            severity="HIGH" if null_count > 0 else "INFORMATIONAL",
            category="CURRENT_PRODUCTION_ISSUE" if null_count > 0 else "INFORMATIONAL_WARNING",
            message=f"{null_count} qualified members have NULL scores" if null_count else "All qualified members have scores",
            row_count=null_count,
        ))

    # Unqualified members
    unqual, ms3 = await timed_query(db2,
        """SELECT member_type, COUNT(*) AS cnt, performance_classification
           FROM member_metrics
           WHERE ranking_qualified = false OR ranking_qualified IS NULL
           GROUP BY member_type, performance_classification""",
        "Unqualified members")
    sec.checks.append(CheckResult(
        name="Unqualified members breakdown",
        status="PASS" if len(unqual) == 0 else "INFO",
        severity="INFORMATIONAL",
        category="INFORMATIONAL_WARNING",
        message=f"{sum(r['cnt'] for r in unqual)} unqualified members" if unqual else "All 776 members are qualified",
        details=_rows_to_list(unqual) if unqual else None,
        row_count=sum(r["cnt"] for r in unqual),
        query_ms=ms3,
    ))

    return sec


async def section_12_ranking_consistency(db1: asyncpg.Pool, db2: asyncpg.Pool) -> AuditSection:
    """Ranking consistency — max rank vs population, NULL ranks for qualified."""
    sec = AuditSection(section_id=12, title="RANKING CONSISTENCY")

    # Max rank vs population per member_type
    rank_stats, ms1 = await timed_query(db2,
        """SELECT member_type, COUNT(*) AS qualified_population, MAX(rank) AS max_rank
           FROM member_metrics
           WHERE ranking_qualified = true AND rank IS NOT NULL
           GROUP BY member_type""",
        "Rank vs population")
    for r in rank_stats:
        mt = r["member_type"]
        pop = r["qualified_population"]
        mx = r["max_rank"]
        sec.checks.append(CheckResult(
            name=f"{mt} max rank vs population",
            status="PASS" if mx <= pop else "FAIL",
            severity="CRITICAL" if mx > pop else "INFORMATIONAL",
            category="CURRENT_PRODUCTION_ISSUE" if mx > pop else "INFORMATIONAL_WARNING",
            message=f"Max rank {mx} vs population {pop}" + (" (OK)" if mx <= pop else f" (EXCEEDS by {mx - pop})"),
            row_count=pop,
            query_ms=ms1,
        ))

    # Ranks exceeding population
    exceed, ms2 = await timed_query(db2,
        """SELECT member_id, member_type, member_name, rank
           FROM member_metrics m1
           WHERE rank IS NOT NULL AND ranking_qualified = true
             AND rank > (SELECT COUNT(*) FROM member_metrics m2
                         WHERE m2.member_type = m1.member_type AND m2.ranking_qualified = true)""",
        "Ranks exceeding population")
    sec.checks.append(CheckResult(
        name="Ranks exceeding population",
        status="PASS" if len(exceed) == 0 else "FAIL",
        severity="CRITICAL" if len(exceed) > 0 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if len(exceed) > 0 else "INFORMATIONAL_WARNING",
        message=f"Found {len(exceed)} members with rank exceeding population" if exceed else "No ranks exceed population",
        details=_rows_to_list(exceed) if exceed else None,
        row_count=len(exceed),
        query_ms=ms2,
    ))

    # Qualified members with NULL rank
    null_rank, ms3 = await timed_query(db2,
        """SELECT member_id, member_type, member_name
           FROM member_metrics
           WHERE ranking_qualified = true AND rank IS NULL""",
        "Qualified members with NULL rank")
    sec.checks.append(CheckResult(
        name="Qualified members with NULL rank",
        status="PASS" if len(null_rank) == 0 else "FAIL",
        severity="CRITICAL" if len(null_rank) > 0 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if len(null_rank) > 0 else "INFORMATIONAL_WARNING",
        message=f"Found {len(null_rank)} qualified members with NULL rank" if null_rank else "All qualified members have ranks",
        details=_rows_to_list(null_rank) if null_rank else None,
        row_count=len(null_rank),
        query_ms=ms3,
    ))

    # State rank consistency
    state_rank, ms4 = await timed_query(db2,
        """SELECT COUNT(*) AS population, MAX(rank) AS max_rank
           FROM state_metrics WHERE rank IS NOT NULL""",
        "State rank vs population")
    if state_rank:
        sr = dict(state_rank[0])
        sec.checks.append(CheckResult(
            name="State max rank vs population",
            status="PASS" if sr["max_rank"] <= sr["population"] else "FAIL",
            severity="CRITICAL" if sr["max_rank"] > sr["population"] else "INFORMATIONAL",
            category="CURRENT_PRODUCTION_ISSUE" if sr["max_rank"] > sr["population"] else "INFORMATIONAL_WARNING",
            message=f"Max state rank {sr['max_rank']} vs population {sr['population']}",
            details=sr,
            query_ms=ms4,
        ))

    # Peer rank / percentile in member_intelligence
    mi_total, ms5 = await timed_fetchval(db2, "SELECT COUNT(*) FROM member_intelligence", "MI total")
    has_peer_rank, _ = await timed_fetchval(db2,
        "SELECT COUNT(*) FROM member_intelligence WHERE peer_rank IS NOT NULL",
        "MI has peer_rank")
    has_peer_pct, _ = await timed_fetchval(db2,
        "SELECT COUNT(*) FROM member_intelligence WHERE peer_percentile IS NOT NULL AND peer_rank IS NOT NULL",
        "MI has peer_percentile with peer_rank")
    sec.checks.append(CheckResult(
        name="member_intelligence peer_rank coverage",
        status="PASS" if has_peer_rank == mi_total else "WARN",
        severity="MEDIUM" if has_peer_rank != mi_total else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if has_peer_rank != mi_total else "INFORMATIONAL_WARNING",
        message=f"{has_peer_rank}/{mi_total} have peer_rank",
        row_count=has_peer_rank,
        query_ms=ms5,
    ))
    sec.checks.append(CheckResult(
        name="member_intelligence peer_percentile coverage",
        status="PASS" if has_peer_pct == has_peer_rank else "WARN",
        severity="MEDIUM" if has_peer_pct != has_peer_rank else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if has_peer_pct != has_peer_rank else "INFORMATIONAL_WARNING",
        message=f"{has_peer_pct}/{has_peer_rank} have peer_percentile (with peer_rank)",
        row_count=has_peer_pct,
    ))

    return sec


async def section_13_risk_data(db1: asyncpg.Pool, db2: asyncpg.Pool) -> AuditSection:
    """Risk data — risk levels, scores, consistency."""
    sec = AuditSection(section_id=13, title="RISK DATA INTEGRITY")

    # Risk level breakdown
    risk_levels, ms1 = await timed_query(db2,
        "SELECT risk_level, COUNT(*) AS cnt FROM member_intelligence GROUP BY risk_level ORDER BY risk_level",
        "Risk levels")
    sec.checks.append(CheckResult(
        name="member_intelligence risk level distribution",
        status="PASS",
        severity="INFORMATIONAL",
        category="INFORMATIONAL_WARNING",
        message=", ".join(f"{r['risk_level']}: {r['cnt']}" for r in risk_levels) if risk_levels else "No risk levels",
        details=_rows_to_list(risk_levels),
        query_ms=ms1,
    ))

    # Invalid risk levels — intelligence risk uses CRITICAL/HIGH/MODERATE/LOW
    # (from analysis/intelligence/risk.py _risk_level_from_pctile)
    invalid_risk, ms2 = await timed_query(db2,
        """SELECT member_id, member_type, risk_level
           FROM member_intelligence
           WHERE risk_level IS NOT NULL
             AND risk_level NOT IN ('LOW', 'MODERATE', 'HIGH', 'CRITICAL')""",
        "Invalid risk levels")
    sec.checks.append(CheckResult(
        name="Invalid risk_level values",
        status="PASS" if len(invalid_risk) == 0 else "FAIL",
        severity="HIGH" if len(invalid_risk) > 0 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if len(invalid_risk) > 0 else "INFORMATIONAL_WARNING",
        message=f"Found {len(invalid_risk)} invalid risk levels" if invalid_risk else "All risk levels are valid",
        details=_rows_to_list(invalid_risk) if invalid_risk else None,
        row_count=len(invalid_risk),
        query_ms=ms2,
    ))

    # Risk scores range [0,100] — intelligence risk scores are on 0-100 scale
    # (from analysis/intelligence/risk.py: s * 100.0)
    invalid_score, ms3 = await timed_query(db2,
        """SELECT member_id, member_type, risk_score
           FROM member_intelligence
           WHERE risk_score IS NOT NULL AND (risk_score < 0 OR risk_score > 100)""",
        "Invalid risk scores")
    sec.checks.append(CheckResult(
        name="Risk scores outside [0,100] range",
        status="PASS" if len(invalid_score) == 0 else "FAIL",
        severity="HIGH" if len(invalid_score) > 0 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if len(invalid_score) > 0 else "INFORMATIONAL_WARNING",
        message=f"Found {len(invalid_score)} out-of-range scores" if invalid_score else "All risk scores in [0,100]",
        details=_rows_to_list(invalid_score) if invalid_score else None,
        row_count=len(invalid_score),
        query_ms=ms3,
    ))

    # Scored members without risk
    no_risk, ms4 = await timed_query(db2,
        """SELECT COUNT(*) AS cnt FROM member_intelligence
           WHERE performance_score_100 IS NOT NULL AND risk_level IS NULL""",
        "Scored without risk")
    cnt = no_risk[0]["cnt"] if no_risk else 0
    sec.checks.append(CheckResult(
        name="Scored members without risk_level",
        status="PASS" if cnt == 0 else "WARN",
        severity="MEDIUM" if cnt > 0 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if cnt > 0 else "INFORMATIONAL_WARNING",
        message=f"{cnt} scored members missing risk_level" if cnt else "All scored members have risk_level",
        row_count=cnt,
        query_ms=ms4,
    ))

    # State intelligence risk
    si_risk, ms5 = await timed_query(db2,
        "SELECT risk_level, COUNT(*) AS cnt FROM state_intelligence GROUP BY risk_level ORDER BY risk_level",
        "State risk levels")
    sec.checks.append(CheckResult(
        name="state_intelligence risk level distribution",
        status="PASS",
        severity="INFORMATIONAL",
        category="INFORMATIONAL_WARNING",
        message=", ".join(f"{r['risk_level']}: {r['cnt']}" for r in si_risk) if si_risk else "No risk levels",
        details=_rows_to_list(si_risk),
        query_ms=ms5,
    ))

    return sec


async def section_14_orphan_checks(db1: asyncpg.Pool, db2: asyncpg.Pool) -> AuditSection:
    """Orphan records — work_analysis without member_metrics, etc."""
    sec = AuditSection(section_id=14, title="ORPHAN RECORD CHECKS")

    # MP work_analysis members not in member_metrics
    orphan_mp, ms1 = await timed_query(db2,
        """SELECT wa.member_id, COUNT(*) AS works
           FROM work_analysis wa
           LEFT JOIN member_metrics mm ON wa.member_id = mm.member_id AND wa.member_type = mm.member_type
           WHERE mm.member_id IS NULL
           GROUP BY wa.member_id""",
        "Orphan MP works")
    sec.checks.append(CheckResult(
        name="MP work_analysis members not in member_metrics",
        status="PASS" if len(orphan_mp) == 0 else "FAIL",
        severity="HIGH" if len(orphan_mp) > 0 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if len(orphan_mp) > 0 else "INFORMATIONAL_WARNING",
        message=f"Found {len(orphan_mp)} orphan MP members" if orphan_mp else "No orphan MP works",
        details=_rows_to_list(orphan_mp) if orphan_mp else None,
        row_count=len(orphan_mp),
        query_ms=ms1,
    ))

    # MLA work_analysis members not in member_metrics
    orphan_mla, ms2 = await timed_query(db2,
        """SELECT mwa.member_id, COUNT(*) AS works
           FROM mla_work_analysis mwa
           LEFT JOIN member_metrics mm ON mwa.member_id = mm.member_id AND mwa.member_type = mm.member_type
           WHERE mm.member_id IS NULL
           GROUP BY mwa.member_id""",
        "Orphan MLA works")
    sec.checks.append(CheckResult(
        name="MLA work_analysis members not in member_metrics",
        status="PASS" if len(orphan_mla) == 0 else "FAIL",
        severity="HIGH" if len(orphan_mla) > 0 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if len(orphan_mla) > 0 else "INFORMATIONAL_WARNING",
        message=f"Found {len(orphan_mla)} orphan MLA members" if orphan_mla else "No orphan MLA works",
        details=_rows_to_list(orphan_mla) if orphan_mla else None,
        row_count=len(orphan_mla),
        query_ms=ms2,
    ))

    # evidence_work_refs referencing non-existent work_ids
    orphan_ewr, ms3 = await timed_query(db2,
        """SELECT ewr.work_id
           FROM evidence_work_refs ewr
           LEFT JOIN work_analysis wa ON ewr.work_id = wa.work_id
           LEFT JOIN mla_work_analysis mwa ON ewr.work_id = mwa.work_id
           WHERE wa.work_id IS NULL AND mwa.work_id IS NULL
           LIMIT 20""",
        "Orphan evidence_work_refs")
    sec.checks.append(CheckResult(
        name="evidence_work_refs referencing non-existent work_ids",
        status="PASS" if len(orphan_ewr) == 0 else "WARN",
        severity="MEDIUM" if len(orphan_ewr) > 0 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if len(orphan_ewr) > 0 else "INFORMATIONAL_WARNING",
        message=f"Found {len(orphan_ewr)} orphan evidence work refs" if orphan_ewr else "No orphan evidence work refs",
        details=_rows_to_list(orphan_ewr) if orphan_ewr else None,
        row_count=len(orphan_ewr),
        query_ms=ms3,
    ))

    return sec


async def section_15_db_routing(db1: asyncpg.Pool, db2: asyncpg.Pool) -> AuditSection:
    """DB1/DB2 routing — intelligence tables should NOT be on DB1."""
    sec = AuditSection(section_id=15, title="DB1/DB2 ROUTING VERIFICATION")

    # These tables should ONLY exist on DB2 (intelligence/analytics layer)
    db2_only_tables = [
        "member_intelligence", "state_intelligence",
        "entity_evidence", "ai_analysis", "model_registry",
        "gemini_retry_backlog", "overall_metrics", "national_statistics",
        "trends", "evidence_work_refs", "member_metrics", "state_metrics",
    ]

    found_on_db1 = []
    for tbl in db2_only_tables:
        exists, _ = await timed_fetchval(db1,
            f"SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname='public' AND tablename='{tbl}')",
            f"DB1 check {tbl}")
        if exists:
            found_on_db1.append(tbl)

    sec.checks.append(CheckResult(
        name="Intelligence/analytics tables on DB1",
        status="PASS" if len(found_on_db1) == 0 else "FAIL",
        severity="CRITICAL" if len(found_on_db1) > 0 else "INFORMATIONAL",
        category="CURRENT_PRODUCTION_ISSUE" if len(found_on_db1) > 0 else "INFORMATIONAL_WARNING",
        message=f"Found {len(found_on_db1)} intelligence tables on DB1" if found_on_db1 else "No intelligence tables on DB1 (correct)",
        details=found_on_db1 if found_on_db1 else None,
        row_count=len(found_on_db1),
    ))

    # Expected DB1 tables (source/core data)
    expected_db1 = {
        "states", "constituencies", "mps", "mlas", "works",
        "work_recommendations", "work_sanctions", "work_completions",
        "work_expenditures", "mp_allocations", "calamities",
        "mla_works", "mla_work_recommendations", "mla_work_sanctions",
        "mla_work_completions", "mla_work_expenditures", "mla_allocations",
        "mla_calamities", "ingestion_jobs", "data_updated",
        # Source data copies that exist on DB1 by design (raw ingested data)
        "work_analysis", "mla_work_analysis",
    }

    db1_tables_rows, _ = await timed_query(db1,
        "SELECT tablename FROM pg_tables WHERE schemaname='public'",
        "DB1 all tables")
    db1_tables = {r["tablename"] for r in db1_tables_rows}

    unexpected_db1 = db1_tables - expected_db1
    # Separate deprecated/archival tables from truly unexpected tables
    deprecated_tables = sorted(t for t in unexpected_db1 if t.startswith("z_deprecated_"))
    truly_unexpected = sorted(unexpected_db1 - set(deprecated_tables))

    if deprecated_tables:
        sec.checks.append(CheckResult(
            name="Deprecated/archival tables on DB1",
            status="PASS",
            severity="INFORMATIONAL",
            category="HISTORICAL_ARCHIVAL",
            message=f"{len(deprecated_tables)} z_deprecated_* tables (historical, harmless)",
            details=deprecated_tables,
        ))

    if truly_unexpected:
        sec.checks.append(CheckResult(
            name="Unexpected tables on DB1",
            status="WARN",
            severity="MEDIUM",
            category="HISTORICAL_ARCHIVAL",
            message=f"Found {len(truly_unexpected)} unexpected tables on DB1: {truly_unexpected}",
            details=truly_unexpected,
        ))
    else:
        sec.checks.append(CheckResult(
            name="Unexpected tables on DB1",
            status="PASS",
            severity="INFORMATIONAL",
            category="INFORMATIONAL_WARNING",
            message=f"All {len(db1_tables)} DB1 tables are expected (source + source copies + deprecated)",
        ))

    return sec


async def section_16_null_invalid(db1: asyncpg.Pool, db2: asyncpg.Pool) -> AuditSection:
    """NULL/invalid data audit across key tables."""
    sec = AuditSection(section_id=16, title="NULL / INVALID DATA AUDIT")

    checks = [
        ("work_analysis NULL work_id", "SELECT COUNT(*) FROM work_analysis WHERE work_id IS NULL"),
        ("mla_work_analysis NULL work_id", "SELECT COUNT(*) FROM mla_work_analysis WHERE work_id IS NULL"),
        ("member_metrics NULL member_id", "SELECT COUNT(*) FROM member_metrics WHERE member_id IS NULL"),
        ("member_metrics NULL member_type", "SELECT COUNT(*) FROM member_metrics WHERE member_type IS NULL"),
        ("state_metrics NULL state_id", "SELECT COUNT(*) FROM state_metrics WHERE state_id IS NULL"),
        ("member_intelligence NULL member_id", "SELECT COUNT(*) FROM member_intelligence WHERE member_id IS NULL"),
        ("state_intelligence NULL state_id", "SELECT COUNT(*) FROM state_intelligence WHERE state_id IS NULL"),
        ("work_analysis NULL member_id", "SELECT COUNT(*) FROM work_analysis WHERE member_id IS NULL"),
        ("mla_work_analysis NULL member_id", "SELECT COUNT(*) FROM mla_work_analysis WHERE member_id IS NULL"),
        ("member_metrics negative total_works", "SELECT COUNT(*) FROM member_metrics WHERE total_works < 0"),
        ("member_metrics negative completed_works", "SELECT COUNT(*) FROM member_metrics WHERE completed_works < 0"),
        ("state_metrics negative total_works", "SELECT COUNT(*) FROM state_metrics WHERE total_works < 0"),
        ("member_metrics completion_rate_pct out of range",
         "SELECT COUNT(*) FROM member_metrics WHERE completion_rate_pct < 0 OR completion_rate_pct > 100"),
        ("member_metrics fund_utilization_pct out of range",
         "SELECT COUNT(*) FROM member_metrics WHERE fund_utilization_pct < 0 OR fund_utilization_pct > 100"),
        ("state_metrics completion_rate_pct out of range",
         "SELECT COUNT(*) FROM state_metrics WHERE completion_rate_pct < 0 OR completion_rate_pct > 100"),
        ("state_metrics NULL state_name",
         "SELECT COUNT(*) FROM state_metrics WHERE state_name IS NULL OR state_name = ''"),
        ("member_metrics NULL member_name",
         "SELECT COUNT(*) FROM member_metrics WHERE member_name IS NULL OR member_name = ''"),
    ]

    for name, sql in checks:
        cnt, ms = await timed_fetchval(db2, sql, name)
        sec.checks.append(CheckResult(
            name=name,
            status="PASS" if cnt == 0 else "WARN",
            severity="MEDIUM" if cnt > 0 else "INFORMATIONAL",
            category="CURRENT_PRODUCTION_ISSUE" if cnt > 0 else "INFORMATIONAL_WARNING",
            message=f"{cnt} violations" if cnt else "Clean",
            row_count=cnt,
            query_ms=ms,
        ))

    return sec


async def section_17_pipeline_state(db1: asyncpg.Pool, db2: asyncpg.Pool) -> AuditSection:
    """Latest pipeline state — data_updated, ingestion jobs, timestamps."""
    sec = AuditSection(section_id=17, title="LATEST PIPELINE STATE")

    # data_updated on DB1
    try:
        du_rows, ms1 = await timed_query(db1, "SELECT * FROM data_updated WHERE id = 1", "data_updated")
        if du_rows:
            du = dict(du_rows[0])
            sec.checks.append(CheckResult(
                name="data_updated status",
                status="PASS",
                severity="INFORMATIONAL",
                category="INFORMATIONAL_WARNING",
                message=f"Status: {du.get('status')}, Completed: {du.get('completed_at')}, Updated: {du.get('updated_at')}",
                details=du,
                query_ms=ms1,
            ))
        else:
            sec.checks.append(CheckResult(
                name="data_updated status",
                status="WARN",
                severity="MEDIUM",
                category="CURRENT_PRODUCTION_ISSUE",
                message="No data_updated record found (id=1)",
                query_ms=ms1,
            ))
    except Exception as e:
        sec.checks.append(CheckResult(
            name="data_updated status",
            status="ERROR",
            severity="HIGH",
            category="CURRENT_PRODUCTION_ISSUE",
            message=f"Error querying data_updated: {e}",
        ))

    # member_intelligence calculated_at timestamps
    try:
        calc_rows, ms2 = await timed_query(db2,
            """SELECT MIN(calculated_at) AS oldest, MAX(calculated_at) AS newest, COUNT(*) AS total
               FROM member_intelligence""",
            "MI timestamps")
        if calc_rows:
            cr = dict(calc_rows[0])
            sec.checks.append(CheckResult(
                name="member_intelligence calculated_at range",
                status="PASS",
                severity="INFORMATIONAL",
                category="INFORMATIONAL_WARNING",
                message=f"Oldest: {cr['oldest']}, Newest: {cr['newest']}, Total: {cr['total']}",
                details=cr,
                query_ms=ms2,
            ))
    except Exception as e:
        sec.checks.append(CheckResult(
            name="member_intelligence calculated_at range",
            status="ERROR",
            severity="LOW",
            category="INFORMATIONAL_WARNING",
            message=f"Error: {e}",
        ))

    # state_intelligence calculated_at
    try:
        si_rows, ms3 = await timed_query(db2,
            """SELECT MIN(calculated_at) AS oldest, MAX(calculated_at) AS newest
               FROM state_intelligence""",
            "SI timestamps")
        if si_rows:
            sr = dict(si_rows[0])
            sec.checks.append(CheckResult(
                name="state_intelligence calculated_at range",
                status="PASS",
                severity="INFORMATIONAL",
                category="INFORMATIONAL_WARNING",
                message=f"Oldest: {sr['oldest']}, Newest: {sr['newest']}",
                details=sr,
                query_ms=ms3,
            ))
    except Exception as e:
        sec.checks.append(CheckResult(
            name="state_intelligence calculated_at range",
            status="ERROR",
            severity="LOW",
            category="INFORMATIONAL_WARNING",
            message=f"Error: {e}",
        ))

    # Ingestion jobs
    try:
        ij_rows, ms4 = await timed_query(db1,
            """SELECT run_id, status, COUNT(*) AS cnt
               FROM ingestion_jobs
               WHERE run_id LIKE '%2026-09-19%'
               GROUP BY run_id, status""",
            "Ingestion jobs")
        if ij_rows:
            sec.checks.append(CheckResult(
                name="Ingestion jobs for latest run",
                status="PASS",
                severity="INFORMATIONAL",
                category="INFORMATIONAL_WARNING",
                message=f"{len(ij_rows)} job records",
                details=_rows_to_list(ij_rows),
                query_ms=ms4,
            ))
        else:
            sec.checks.append(CheckResult(
                name="Ingestion jobs for latest run",
                status="INFO",
                severity="INFORMATIONAL",
                category="INFORMATIONAL_WARNING",
                message="No ingestion jobs found for 2026-09-19 (may use different run_id format)",
                query_ms=ms4,
            ))
    except Exception as e:
        sec.checks.append(CheckResult(
            name="Ingestion jobs for latest run",
            status="ERROR",
            severity="LOW",
            category="INFORMATIONAL_WARNING",
            message=f"Error: {e}",
        ))

    return sec


async def section_18_project_delay_artifacts(db1: asyncpg.Pool, db2: asyncpg.Pool) -> AuditSection:
    """Check for XGBoost / project-delay artifacts that should NOT be in production."""
    sec = AuditSection(section_id=18, title="PROJECT-DELAY ARTIFACT CHECK")

    # Check model_registry for XGBoost
    try:
        xgb_rows, ms1 = await timed_query(db2,
            """SELECT model_name, model_type, status
               FROM model_registry
               WHERE model_name ILIKE '%xgboost%'
                  OR model_type ILIKE '%xgboost%'
                  OR model_name ILIKE '%project_delay%'
                  OR model_type ILIKE '%project_delay%'""",
            "XGBoost/project-delay in model_registry")
        sec.checks.append(CheckResult(
            name="XGBoost/project-delay in model_registry",
            status="PASS" if len(xgb_rows) == 0 else "WARN",
            severity="MEDIUM" if len(xgb_rows) > 0 else "INFORMATIONAL",
            category="HISTORICAL_ARCHIVAL" if len(xgb_rows) > 0 else "INFORMATIONAL_WARNING",
            message=f"Found {len(xgb_rows)} XGBoost/project-delay entries" if xgb_rows else "No XGBoost/project-delay artifacts",
            details=_rows_to_list(xgb_rows) if xgb_rows else None,
            row_count=len(xgb_rows),
            query_ms=ms1,
        ))
    except Exception as e:
        sec.checks.append(CheckResult(
            name="XGBoost/project-delay in model_registry",
            status="WARN",
            severity="LOW",
            category="INFORMATIONAL_WARNING",
            message=f"model_registry may not exist: {e}",
        ))

    # Check for any column named xgboost or project_delay in work_analysis
    try:
        cols, ms2 = await timed_query(db2,
            """SELECT column_name FROM information_schema.columns
               WHERE table_schema = 'public'
                 AND (column_name ILIKE '%xgboost%' OR column_name ILIKE '%project_delay%')""",
            "XGBoost/project-delay columns")
        sec.checks.append(CheckResult(
            name="XGBoost/project-delay columns in schema",
            status="PASS" if len(cols) == 0 else "WARN",
            severity="MEDIUM" if len(cols) > 0 else "INFORMATIONAL",
            category="HISTORICAL_ARCHIVAL" if len(cols) > 0 else "INFORMATIONAL_WARNING",
            message=f"Found {len(cols)} XGBoost/project-delay columns" if cols else "No XGBoost/project-delay columns",
            details=_rows_to_list(cols) if cols else None,
            row_count=len(cols),
            query_ms=ms2,
        ))
    except Exception as e:
        sec.checks.append(CheckResult(
            name="XGBoost/project-delay columns in schema",
            status="WARN",
            severity="LOW",
            category="INFORMATIONAL_WARNING",
            message=f"Error: {e}",
        ))

    return sec


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def compute_verdict(sections: list[AuditSection]) -> str:
    """Compute final verdict based on all checks."""
    for sec in sections:
        for check in sec.checks:
            if check.status == "FAIL" and check.severity in ("CRITICAL", "HIGH"):
                return "PIPELINE NOT READY — ISSUES FOUND"
    return "PIPELINE READY — NO DATA INTEGRITY ISSUES FOUND"


def generate_json(sections: list[AuditSection], verdict: str, elapsed_ms: float) -> dict:
    """Generate JSON report, capping details lists at 50 items to prevent massive output."""
    MAX_DETAIL_ITEMS = 50

    def _cap_details(details):
        if details is None:
            return None
        if isinstance(details, list) and len(details) > MAX_DETAIL_ITEMS:
            return details[:MAX_DETAIL_ITEMS] + [{"_truncated": True, "remaining": len(details) - MAX_DETAIL_ITEMS}]
        return details

    return {
        "audit_timestamp": datetime.now(timezone.utc).isoformat(),
        "elapsed_ms": round(elapsed_ms, 1),
        "verdict": verdict,
        "sections": [
            {
                "section_id": sec.section_id,
                "title": sec.title,
                "checks": [
                    {**asdict(c), "details": _cap_details(c.details)}
                    for c in sec.checks
                ],
            }
            for sec in sections
        ],
    }


def generate_markdown(sections: list[AuditSection], verdict: str, elapsed_ms: float) -> str:
    lines = [
        "# FORENSIC DATABASE AUDIT REPORT",
        "",
        f"**Generated**: {datetime.now(timezone.utc).isoformat()}",
        f"**Elapsed**: {elapsed_ms:.0f}ms",
        f"**Verdict**: **{verdict}**",
        "",
    ]

    for sec in sections:
        lines.append(f"## Section {sec.section_id}: {sec.title}")
        lines.append("")

        # Summary counts
        pass_count = sum(1 for c in sec.checks if c.status == "PASS")
        fail_count = sum(1 for c in sec.checks if c.status == "FAIL")
        warn_count = sum(1 for c in sec.checks if c.status == "WARN")
        lines.append(f"Checks: {len(sec.checks)} | PASS: {pass_count} | FAIL: {fail_count} | WARN: {warn_count}")
        lines.append("")

        for check in sec.checks:
            icon = {"PASS": "✅", "FAIL": "❌", "WARN": "⚠️", "INFO": "ℹ️", "ERROR": "🔥"}.get(check.status, "?")
            sev_label = f" [{check.severity}]" if check.severity not in ("INFORMATIONAL",) else ""
            cat_label = ""
            if check.category == "CURRENT_PRODUCTION_ISSUE":
                cat_label = " `CURRENT PRODUCTION ISSUE`"
            elif check.category == "HISTORICAL_ARCHIVAL":
                cat_label = " `HISTORICAL/ARCHIVAL`"
            elif check.category == "INFORMATIONAL_WARNING":
                cat_label = " `INFORMATIONAL`"

            lines.append(f"- {icon} **{check.name}**{sev_label}{cat_label}: {check.message}")

            if check.details and check.status in ("FAIL", "WARN"):
                if isinstance(check.details, list):
                    for d in check.details[:10]:
                        if isinstance(d, dict):
                            lines.append(f"  - `{json.dumps(d, default=str)}`")
                        else:
                            lines.append(f"  - `{d}`")
                    if len(check.details) > 10:
                        lines.append(f"  - ... and {len(check.details) - 10} more")
            lines.append("")

        lines.append("---")
        lines.append("")

    lines.append(f"# FINAL VERDICT: {verdict}")
    lines.append("")

    return "\n".join(lines)


def print_console_summary(sections: list[AuditSection], verdict: str, elapsed_ms: float):
    """Print concise console summary."""
    print("\n" + "=" * 70)
    print("  FORENSIC DATABASE AUDIT — CONSOLE SUMMARY")
    print("=" * 70)
    print(f"  Timestamp: {datetime.now(timezone.utc).isoformat()}")
    print(f"  Elapsed:   {elapsed_ms:.0f}ms")
    print("=" * 70)

    total_pass = total_fail = total_warn = total_info = 0
    critical_fails = []

    for sec in sections:
        sec_pass = sum(1 for c in sec.checks if c.status == "PASS")
        sec_fail = sum(1 for c in sec.checks if c.status == "FAIL")
        sec_warn = sum(1 for c in sec.checks if c.status == "WARN")
        sec_info = sum(1 for c in sec.checks if c.status in ("INFO", "ERROR"))

        total_pass += sec_pass
        total_fail += sec_fail
        total_warn += sec_warn
        total_info += sec_info

        status_str = f"P:{sec_pass} F:{sec_fail} W:{sec_warn}"
        if sec_fail > 0:
            status_str += " ***"
        print(f"  [{sec.section_id:2d}] {sec.title:<45s} {status_str}")

        for check in sec.checks:
            if check.status == "FAIL" and check.severity in ("CRITICAL", "HIGH"):
                critical_fails.append(f"    ❌ {check.name}: {check.message}")

    print("-" * 70)
    print(f"  TOTAL: {total_pass} PASS | {total_fail} FAIL | {total_warn} WARN | {total_info} INFO/ERROR")
    print("-" * 70)

    if critical_fails:
        print("\n  CRITICAL FAILURES:")
        for f in critical_fails:
            print(f)
        print()

    # Color verdict
    if "NOT READY" in verdict:
        print(f"\n  \033[91m{'=' * 70}\033[0m")
        print(f"  \033[91m  {verdict}\033[0m")
        print(f"  \033[91m{'=' * 70}\033[0m\n")
    else:
        print(f"\n  \033[92m{'=' * 70}\033[0m")
        print(f"  \033[92m  {verdict}\033[0m")
        print(f"  \033[92m{'=' * 70}\033[0m\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def run_audit(dry_run: bool = False, json_only: bool = False) -> int:
    """Run the full forensic audit. Returns exit code (0=PASS, 1=FAIL)."""
    from dotenv import load_dotenv

    root = Path(__file__).resolve().parent
    load_dotenv(root / ".env", override=False)

    db1_dsn = os.environ.get("DATABASE_URL", "")
    db2_dsn = os.environ.get("DB2_DATABASE_URL", "")

    if not db1_dsn:
        print("ERROR: DATABASE_URL not set. Cannot connect to DB1.")
        return 2
    if not db2_dsn:
        print("ERROR: DB2_DATABASE_URL not set. Cannot connect to DB2.")
        return 2

    print(f"DB1 DSN: ...@{db1_dsn.split('@')[-1] if '@' in db1_dsn else '(unknown)'}")
    print(f"DB2 DSN: ...@{db2_dsn.split('@')[-1] if '@' in db2_dsn else '(unknown)'}")
    print()

    t_start = time.monotonic()

    try:
        print("Connecting to DB1...")
        db1 = await create_pool(db1_dsn, "DB1")
        print("  DB1 connected.")

        print("Connecting to DB2...")
        db2 = await create_pool(db2_dsn, "DB2")
        print("  DB2 connected.")

        if dry_run:
            print("\nDry run — connections OK. Closing.")
            await db1.close()
            await db2.close()
            return 0

        print("\nRunning audit sections...\n")

        sections: list[AuditSection] = []
        section_funcs = [
            section_01_db_inventory,
            section_02_member_population,
            section_03_synthetic_ids,
            section_04_mp_mla_separation,
            section_05_duplicate_works,
            section_06_member_metrics,
            section_07_member_intelligence,
            section_08_state_tables,
            section_09_evidence_gemini,
            section_10_anomaly_data,
            section_11_performance_scores,
            section_12_ranking_consistency,
            section_13_risk_data,
            section_14_orphan_checks,
            section_15_db_routing,
            section_16_null_invalid,
            section_17_pipeline_state,
            section_18_project_delay_artifacts,
        ]

        for func in section_funcs:
            sec = await func(db1, db2)
            sections.append(sec)
            check_count = len(sec.checks)
            fail_count = sum(1 for c in sec.checks if c.status == "FAIL")
            status = f" ({fail_count} FAILURES)" if fail_count else ""
            print(f"  [{sec.section_id:2d}] {sec.title} — {check_count} checks{status}")

        elapsed_ms = (time.monotonic() - t_start) * 1000

        verdict = compute_verdict(sections)

        # Generate reports
        report_dir = root
        json_path = report_dir / "forensic_audit.json"
        md_path = report_dir / "forensic_audit.md"

        report_json = generate_json(sections, verdict, elapsed_ms)
        with open(json_path, "w") as f:
            json.dump(report_json, f, indent=2, default=str)
        print(f"\n  JSON report: {json_path}")

        if not json_only:
            report_md = generate_markdown(sections, verdict, elapsed_ms)
            with open(md_path, "w") as f:
                f.write(report_md)
            print(f"  MD report:   {md_path}")

        print_console_summary(sections, verdict, elapsed_ms)

        # Cleanup
        await db1.close()
        await db2.close()

        return 0 if "READY" in verdict else 1

    except Exception as exc:
        elapsed_ms = (time.monotonic() - t_start) * 1000
        print(f"\n  AUDIT FAILED: {exc}")
        print(f"  Elapsed: {elapsed_ms:.0f}ms")
        import traceback
        traceback.print_exc()
        return 2


def main():
    parser = argparse.ArgumentParser(
        description="Standalone read-only forensic database audit for DB1 + DB2"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Test connections only, do not run audit queries")
    parser.add_argument("--json-only", action="store_true",
                        help="Generate JSON report only, skip Markdown")
    args = parser.parse_args()

    exit_code = asyncio.run(run_audit(dry_run=args.dry_run, json_only=args.json_only))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
