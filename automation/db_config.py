"""Centralized two-database configuration for the MPLADS pipeline.

DB1 = CORE / SOURCE
    - Government source records (works, recommendations, sanctions, completions, expenditures)
    - MLA works/source data
    - MP/MLA allocations, calamity data
    - Member identity information
    - Ingestion/checkpoint/operational state
    - Storage bucket (mplads-raw)
    - All source data required by Python analysis

DB2 = INTELLIGENCE / RESULTS
    - Deterministic work analysis results
    - Member/entity analysis results
    - State analysis, national statistics, dashboard metrics, trends
    - Entity Anomaly / statistical intelligence
    - Evidence
    - Gemini outputs

Architecture:
    Government → Fetcher → Comparator → Delta → DB1 ingestion
    → Affected works/entities → Python analysis → Entity Anomaly
    → Evidence → Gemini → DB2

Env vars (user pastes into .env or GitHub Secrets):
    DB1_URL             — DB1 Supabase project URL
    DB1_SECRET_KEY      — DB1 anon/public key
    DB1_SERVICE_ROLE_KEY — DB1 service role key
    DB2_URL             — DB2 Supabase project URL
    DB2_SECRET_KEY      — DB2 anon/public key
    DB2_SERVICE_ROLE_KEY — DB2 service role key
    GEMINI_API_KEYS     — Comma-separated Gemini API keys
"""

import os
import ssl
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

try:
    import certifi
    SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    SSL_CTX = ssl.create_default_context()


class DBConfig:
    """Holds DB1 and DB2 connection parameters."""

    def __init__(self):
        self.db1_url = ""
        self.db1_key = ""
        self.db2_url = ""
        self.db2_key = ""
        self.gemini_keys = []

    @property
    def db1_ready(self):
        return bool(self.db1_url and self.db1_key)

    @property
    def db2_ready(self):
        return bool(self.db2_url and self.db2_key)

    def __repr__(self):
        return (
            f"DBConfig(db1={'READY' if self.db1_ready else 'MISSING'}, "
            f"db2={'READY' if self.db2_ready else 'MISSING'}, "
            f"gemini_keys={len(self.gemini_keys)})"
        )


_config = None


def load_config(require_db2=True):
    """Load configuration from environment variables.

    Reads from .env file (if present) and environment.

    Env vars:
        DB1_URL                    — DB1 project URL
        DB1_SERVICE_ROLE_KEY       — DB1 service role key
        DB2_URL                    — DB2 project URL
        DB2_SERVICE_ROLE_KEY       — DB2 service role key
        GEMINI_API_KEYS            — Comma-separated Gemini API keys

    Args:
        require_db2: if True, raise if DB2 credentials missing.
                     Set False for DB1-only operations (fetcher, comparator, ingestion).

    Returns:
        DBConfig instance
    """
    global _config

    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env", override=False)

    cfg = DBConfig()

    # DB1 — accept new or legacy env var names
    cfg.db1_url = (
        os.environ.get("DB1_URL", "")
        or os.environ.get("SUPABASE_URL", "")
    ).rstrip("/")

    cfg.db1_key = (
        os.environ.get("DB1_SERVICE_ROLE_KEY", "")
        or os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
        or os.environ.get("DB1_SECRET_KEY", "")
        or os.environ.get("SUPABASE_SECRET_KEY", "")
    )

    # DB2 — accept new or legacy env var names
    cfg.db2_url = (
        os.environ.get("DB2_URL", "")
        or os.environ.get("DB2_SUPABASE_URL", "")
    ).rstrip("/")

    cfg.db2_key = (
        os.environ.get("DB2_SERVICE_ROLE_KEY", "")
        or os.environ.get("DB2_SUPABASE_SERVICE_ROLE_KEY", "")
        or os.environ.get("DB2_SECRET_KEY", "")
    )

    # Gemini
    gemini_raw = os.environ.get("GEMINI_API_KEYS", "")
    if gemini_raw:
        cfg.gemini_keys = [k.strip() for k in gemini_raw.split(",") if k.strip()]

    # Validation
    if not cfg.db1_url or not cfg.db1_key:
        raise RuntimeError(
            "Missing DB1 credentials. Set DB1_URL and DB1_SERVICE_ROLE_KEY in .env"
        )

    if require_db2 and not cfg.db2_ready:
        raise RuntimeError(
            "Missing DB2 credentials. Set DB2_URL and DB2_SERVICE_ROLE_KEY in .env"
        )

    _config = cfg
    return cfg


def get_config():
    """Return the loaded config. Raises if not loaded yet."""
    if _config is None:
        raise RuntimeError("Config not loaded. Call load_config() first.")
    return _config


def get_db1():
    """Return (url, key) tuple for DB1."""
    cfg = get_config()
    return cfg.db1_url, cfg.db1_key


def get_db2():
    """Return (url, key) tuple for DB2."""
    cfg = get_config()
    if not cfg.db2_ready:
        raise RuntimeError("DB2 not configured")
    return cfg.db2_url, cfg.db2_key
