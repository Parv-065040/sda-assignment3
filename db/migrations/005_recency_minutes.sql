-- Migration 005: renames customer_segments.recency_days to
-- recency_minutes and fixes its semantics (was DATEDIFF in days,
-- now TIMESTAMPDIFF in minutes).
--
-- Root cause this fixes: over a demo run spanning minutes/hours, almost
-- every customer's most recent purchase falls on "today", so day-level
-- recency collapsed to 0 for nearly everyone. With that many tied
-- values, NTILE(5) had no real signal to rank customers on and broke
-- ties using arbitrary internal row order -- meaning a customer who
-- purchased moments ago could randomly get assigned a BAD recency
-- score and land in "At Risk" / "Can't Lose Them" despite being
-- actively engaged right now. Minute-level granularity gives real,
-- distinguishable values to rank on instead.
--
-- Existing rows will be recomputed on segmentation's next cycle
-- (SEGMENTATION_INTERVAL_SECONDS, default 60s) -- no need to backfill.

ALTER TABLE customer_segments
    CHANGE COLUMN recency_days recency_minutes INT;
