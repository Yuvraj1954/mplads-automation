"""Persistent Gemini retry backlog for cross-run failure tracking.

When Gemini fails for an entity during a daily run, the failure is recorded
in the gemini_retry_backlog table in DB2. On subsequent runs, unresolved
backlog entries are merged with the current affected set, deduplicated,
and retried using the latest DB2 evidence.

Design:
- Natural key: (entity_type, entity_id) — same as ai_analysis
- Identity + retry state only — no stale evidence stored
- Latest evidence fetched from DB2 at retry time
- Upsert-safe: repeated failures update existing rows, never duplicate
"""

import json
import urllib.request
import urllib.parse
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Table name constant
# ---------------------------------------------------------------------------
BACKLOG_TABLE = "gemini_retry_backlog"

# Retry policy
MAX_ATTEMPTS = 10
BACKOFF_HOURS = {1: 0, 2: 1, 3: 2, 4: 4, 5: 8, 6: 12, 7: 24, 8: 24, 9: 24, 10: 48}


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _classify_error(exc):
    """Classify an exception into a safe, bounded error category string."""
    from analysis.gemini_client import ErrorClass, GeminiError
    if isinstance(exc, GeminiError):
        mapping = {
            ErrorClass.RPM_RATE_LIMITED: "RPM_RATE_LIMITED",
            ErrorClass.RPD_EXHAUSTED: "RPD_EXHAUSTED",
            ErrorClass.TEMPORARY_RATE_LIMIT: "TEMPORARY_RATE_LIMIT",
            ErrorClass.AUTH_ERROR: "AUTH_ERROR",
            ErrorClass.NOT_FOUND: "NOT_FOUND",
            ErrorClass.INVALID_REQUEST: "INVALID_REQUEST",
            ErrorClass.SERVER_ERROR: "SERVER_ERROR",
            ErrorClass.UNKNOWN_ERROR: "UNKNOWN_ERROR",
        }
        return mapping.get(exc.error_class, "UNKNOWN_ERROR")
    msg = str(exc).lower()
    if "429" in msg or "rate limit" in msg:
        return "RATE_LIMITED"
    if "timeout" in msg or "timed out" in msg or "deadline" in msg:
        return "TIMEOUT"
    if any(x in msg for x in ("500", "502", "503")):
        return "SERVER_ERROR"
    return "UNKNOWN_ERROR"


def ensure_backlog_table(url, key):
    """Create the gemini_retry_backlog table in DB2 if it does not exist.

    Uses Supabase RPC to execute raw DDL. Safe to call on every run.
    """
    from automation.db_config import SSL_CTX
    ddl = f"""
    CREATE TABLE IF NOT EXISTS public.{BACKLOG_TABLE} (
        entity_type   text        NOT NULL,
        entity_id     integer     NOT NULL,
        status        text        NOT NULL DEFAULT 'PENDING',
        attempt_count integer     NOT NULL DEFAULT 0,
        last_error_class  text,
        last_error_message text,
        last_attempt_at   text,
        next_retry_at     text,
        created_at    text        NOT NULL DEFAULT (now()::text),
        updated_at    text        NOT NULL DEFAULT (now()::text),
        CONSTRAINT {BACKLOG_TABLE}_pkey PRIMARY KEY (entity_type, entity_id)
    );
    """
    try:
        body = json.dumps({"query": ddl}).encode()
        req = urllib.request.Request(
            f"{url}/rest/v1/rpc/exec_sql",
            data=body,
            headers={
                "apikey": key,
                "Authorization": f"Bearer {key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30, context=SSL_CTX) as resp:
            return resp.status
    except Exception:
        pass

    # Fallback: use Supabase client to check if table exists via select
    try:
        from supabase import create_client
        client = create_client(url, key)
        client.table(BACKLOG_TABLE).select("entity_type").limit(1).execute()
        return 200
    except Exception as e:
        if "does not exist" in str(e).lower() or "relation" in str(e).lower():
            print(f"  WARNING: Could not create {BACKLOG_TABLE} table: {e}")
            print(f"  Table must be created manually in Supabase dashboard.")
        return 200


def load_pending_backlog(url, key):
    """Load all PENDING backlog entries from DB2.

    Returns list of dicts with entity_type, entity_id, attempt_count.
    """
    from supabase import create_client
    client = create_client(url, key)
    rows = []
    offset = 0
    while True:
        result = (
            client.table(BACKLOG_TABLE)
            .select("entity_type,entity_id,attempt_count,last_error_class")
            .eq("status", "PENDING")
            .order("created_at", desc=False)
            .range(offset, offset + 999)
            .execute()
        )
        batch = result.data or []
        rows.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
    return rows


def record_failure(url, key, entity_type, entity_id, error_class, error_message=""):
    """Record or update a failed Gemini entity in the backlog.

    Uses upsert on (entity_type, entity_id) to prevent duplicates.
    Bounded error_message: truncated to 500 chars, no secrets.
    """
    from supabase import create_client
    client = create_client(url, key)

    safe_msg = str(error_message)[:500] if error_message else ""
    now = _now_iso()

    # Try to load existing entry to increment attempt_count
    existing = (
        client.table(BACKLOG_TABLE)
        .select("attempt_count")
        .eq("entity_type", entity_type)
        .eq("entity_id", entity_id)
        .execute()
    )

    attempt = 1
    if existing.data:
        attempt = (existing.data[0].get("attempt_count") or 0) + 1

    next_retry = None
    if attempt < MAX_ATTEMPTS:
        hours = BACKOFF_HOURS.get(attempt, 48)
        from datetime import timedelta
        next_dt = datetime.now(timezone.utc) + timedelta(hours=hours)
        next_retry = next_dt.isoformat()

    row = {
        "entity_type": entity_type,
        "entity_id": entity_id,
        "status": "PENDING" if attempt < MAX_ATTEMPTS else "EXHAUSTED",
        "attempt_count": attempt,
        "last_error_class": error_class,
        "last_error_message": safe_msg,
        "last_attempt_at": now,
        "next_retry_at": next_retry,
        "updated_at": now,
    }

    client.table(BACKLOG_TABLE).upsert(
        row, on_conflict="entity_type,entity_id"
    ).execute()


def record_success(url, key, entity_type, entity_id):
    """Mark a backlog entry as RESOLVED after successful Gemini generation."""
    from supabase import create_client
    client = create_client(url, key)
    now = _now_iso()
    client.table(BACKLOG_TABLE).upsert({
        "entity_type": entity_type,
        "entity_id": entity_id,
        "status": "RESOLVED",
        "updated_at": now,
    }, on_conflict="entity_type,entity_id").execute()


def resolve_failures(url, key, failures):
    """Record all failures from a Gemini batch result into the backlog.

    Args:
        failures: list of dicts from GeminiScheduler with keys:
                  entity_type, entity_id, reason (or similar)
    """
    if not failures:
        return
    for f in failures:
        etype = f.get("entity_type", "")
        eid = f.get("entity_id")
        reason = f.get("reason", "UNKNOWN")
        record_failure(url, key, etype, eid, reason, reason)


def merge_candidates(evidence_records, backlog_entries):
    """Merge current affected evidence records with pending backlog entries.

    Deduplicates by (entity_type, entity_id). For backlog entries not in the
    current evidence set, they will be fetched from DB2 at retry time.

    Args:
        evidence_records: list of evidence dicts from current run
        backlog_entries: list of backlog dicts from load_pending_backlog()

    Returns:
        (merged_evidence, backlog_only) where:
        - merged_evidence: evidence records to process (affected + backlog)
        - backlog_only: entities from backlog that need fresh evidence from DB2
    """
    seen = set()
    merged = []
    backlog_only = []

    # Current affected evidence (primary)
    for row in evidence_records:
        key = (row["entity_type"], row["entity_id"])
        seen.add(key)
        merged.append(row)

    # Backlog entries not already in current evidence
    for entry in backlog_entries:
        key = (entry["entity_type"], entry["entity_id"])
        if key not in seen:
            seen.add(key)
            backlog_only.append(entry)

    return merged, backlog_only


def fetch_evidence_for_backlog(url, key, backlog_entries, evidence_table="entity_evidence"):
    """Fetch current DB2 evidence for backlog entities not in the current run.

    Returns list of evidence dicts suitable for Gemini processing.
    """
    if not backlog_entries:
        return []

    from supabase import create_client
    client = create_client(url, key)
    evidence_rows = []

    for entry in backlog_entries:
        etype = entry["entity_type"]
        eid = entry["entity_id"]
        try:
            result = (
                client.table(evidence_table)
                .select("*")
                .eq("entity_type", etype)
                .eq("entity_id", eid)
                .limit(1)
                .execute()
            )
            if result.data:
                row = result.data[0]
                # Reconstruct evidence record format expected by Gemini
                evidence_rows.append({
                    "entity_type": etype,
                    "entity_id": eid,
                    "entity_name": row.get("entity_name", ""),
                    "member_type": row.get("member_type"),
                    "state_id": row.get("state_id", 0),
                    "constituency_id": row.get("constituency_id"),
                    "evidence_version": row.get("evidence_version", 2),
                    "evidence_hash": row.get("evidence_hash", ""),
                    "generated_at": row.get("generated_at", ""),
                    "evidence": row.get("evidence", {}),
                })
        except Exception as e:
            print(f"  WARNING: Could not fetch evidence for backlog {etype}:{eid}: {e}")

    return evidence_rows


def backlog_summary(pending):
    """Return a concise summary dict of pending backlog entries."""
    if not pending:
        return {"total": 0, "by_error": {}, "max_attempts": 0}
    by_error = {}
    max_att = 0
    for e in pending:
        ec = e.get("last_error_class", "UNKNOWN")
        by_error[ec] = by_error.get(ec, 0) + 1
        att = e.get("attempt_count", 0)
        if att > max_att:
            max_att = att
    return {"total": len(pending), "by_error": by_error, "max_attempts": max_att}
