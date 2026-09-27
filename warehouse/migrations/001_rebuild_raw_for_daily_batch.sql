-- One-time migration (2026-09-27): switch raw from the 60s polling loop to
-- the date-partitioned daily batch.
--
-- Why drop instead of ALTER: the old table holds only duplicated test rows
-- from the offset-0 bug (the same first N rows re-inserted every run), and
-- several column types change (payment_type VARCHAR -> INTEGER). There is no
-- data worth migrating. Run this once, then `python -m warehouse.apply_schema`.
DROP TABLE IF EXISTS datapulse.raw.taxi_trips;
DROP TABLE IF EXISTS datapulse.raw.pipeline_runs;
