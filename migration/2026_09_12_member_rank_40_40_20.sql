-- Migration: member rank = 40% completion + 40% utilization + 20% scale.
--
-- Run once in the DB2 Supabase SQL editor.
-- Idempotent — safe to re-run.

ALTER TABLE public.member_metrics
  ADD COLUMN IF NOT EXISTS scale_score                NUMERIC,
  ADD COLUMN IF NOT EXISTS performance_score_weighted  NUMERIC;

CREATE INDEX IF NOT EXISTS idx_member_metrics_rank
  ON public.member_metrics (rank);