-- 2026-09-28: quality gate. Adds the quarantine count to pipeline_runs.
-- The new raw.taxi_trips_quarantine table is created by apply_schema
-- (CREATE TABLE IF NOT EXISTS), so only the ALTER is needed here.
-- ADD COLUMN IF NOT EXISTS keeps this safe to run twice.
ALTER TABLE datapulse.raw.pipeline_runs ADD COLUMN IF NOT EXISTS rows_quarantined INTEGER;
