-- Migration 001: add consumer_id to support multi-instance consumer scaling
-- (Phase 1). Safe to run against your EXISTING streaming_analytics database
-- -- it does not drop or wipe any data, it only adds a new column.
--
-- Run this once, manually, inside the mysql container:
--   docker exec -it <mysql-container-name> mysql -u root -p streaming_analytics
--   (paste the contents of this file)
--
-- Existing rows (written before this migration) will have consumer_id set
-- to 'unknown' -- expected, since they were written back when only a
-- single consumer instance existed and consumer_id didn't exist yet.

ALTER TABLE window_metrics
    ADD COLUMN consumer_id VARCHAR(150) NOT NULL DEFAULT 'unknown' AFTER window_end,
    ADD INDEX idx_consumer_id (consumer_id);

ALTER TABLE category_revenue
    ADD COLUMN consumer_id VARCHAR(150) NOT NULL DEFAULT 'unknown' AFTER window_start;

ALTER TABLE campaign_performance
    ADD COLUMN consumer_id VARCHAR(150) NOT NULL DEFAULT 'unknown' AFTER window_start;
