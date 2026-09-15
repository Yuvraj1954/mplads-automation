"""Pipeline observability — per-stage timing, affected counts, skip markers.

Provides lightweight instrumentation that every pipeline stage uses.
No external dependencies — writes to stdout only.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Dict, List, Optional


class StageMetrics:
    """Accumulates metrics for a single pipeline stage."""

    def __init__(self, name: str):
        self.name = name
        self.t0: Optional[float] = None
        self.elapsed: float = 0.0
        self.skipped: bool = False
        self.skip_reason: str = ""
        self.affected_count: int = 0
        self.total_count: int = 0
        self.db_writes: int = 0
        self.db_reads: int = 0
        self.errors: List[str] = []
        self.details: Dict = {}

    def start(self):
        self.t0 = time.monotonic()

    def finish(self):
        if self.t0 is not None:
            self.elapsed = time.monotonic() - self.t0
            self.t0 = None

    def mark_skip(self, reason: str):
        self.skipped = True
        self.skip_reason = reason

    def summary(self) -> str:
        status = "SKIP" if self.skipped else "OK"
        timing = f"{self.elapsed:.1f}s" if not self.skipped else "0.0s"
        affected = f"affected={self.affected_count}" if self.affected_count else ""
        total = f"total={self.total_count}" if self.total_count else ""
        writes = f"writes={self.db_writes}" if self.db_writes else ""
        errors = f"ERRORS={len(self.errors)}" if self.errors else ""
        parts = [p for p in [status, timing, affected, total, writes, errors] if p]
        return " | ".join(parts)


class PipelineTimer:
    """Top-level pipeline timing and metrics collection."""

    def __init__(self, run_id: str = "", mode: str = "affected"):
        self.run_id = run_id
        self.mode = mode
        self.pipeline_t0 = time.monotonic()
        self.stages: Dict[str, StageMetrics] = {}
        self.started_at = datetime.now(timezone.utc).isoformat()

    def stage(self, name: str) -> StageMetrics:
        """Get or create metrics for a stage."""
        if name not in self.stages:
            self.stages[name] = StageMetrics(name)
        return self.stages[name]

    def begin(self, name: str) -> StageMetrics:
        """Begin timing a stage."""
        m = self.stage(name)
        m.start()
        return m

    def end(self, name: str):
        """End timing a stage."""
        m = self.stage(name)
        m.finish()

    def skip(self, name: str, reason: str):
        """Mark a stage as skipped."""
        m = self.stage(name)
        m.mark_skip(reason)

    def total_elapsed(self) -> float:
        return time.monotonic() - self.pipeline_t0

    def print_summary(self):
        """Print a compact pipeline summary."""
        total = self.total_elapsed()
        print(f"\n--- PIPELINE TIMING ({self.mode}) ---")
        print(f"  Run ID: {self.run_id}")
        print(f"  Started: {self.started_at}")
        for name, m in self.stages.items():
            print(f"  {name:>30s}: {m.summary()}")
        print(f"  {'TOTAL':>30s}: {total:.1f}s")
        print("--- END TIMING ---\n")

    def to_dict(self) -> Dict:
        """Serialize timing to a dict for checkpoint/metadata."""
        return {
            "run_id": self.run_id,
            "mode": self.mode,
            "started_at": self.started_at,
            "total_elapsed_s": round(self.total_elapsed(), 2),
            "stages": {
                name: {
                    "elapsed_s": round(m.elapsed, 2),
                    "skipped": m.skipped,
                    "skip_reason": m.skip_reason,
                    "affected_count": m.affected_count,
                    "total_count": m.total_count,
                    "db_writes": m.db_writes,
                    "db_reads": m.db_reads,
                    "errors": m.errors,
                }
                for name, m in self.stages.items()
            },
        }
