-- Migration: state rank = 40% completion + 40% utilization + 20% scale.
--
-- Run once in the DB2 Supabase SQL editor.
-- Idempotent — safe to re-run.

ALTER TABLE public.state_metrics
  ADD COLUMN IF NOT EXISTS scale_score                NUMERIC,
  ADD COLUMN IF NOT EXISTS performance_score_weighted  NUMERIC;

-- Helpful for any future ranking queries.
CREATE INDEX IF NOT EXISTS idx_state_metrics_rank
  ON public.state_metrics (rank);