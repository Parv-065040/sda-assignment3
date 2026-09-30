-- Migration 002: add consumer_id to the alerts table (Phase 2 alerting).
-- Safe to run against your EXISTING streaming_analytics database.
--
-- Run the same way as migration 001:
--   docker cp db/migrations/002_add_consumer_id_to_alerts.sql <mysql-container>:/tmp/
--   docker exec -it <mysql-container> mysql -u root -p streaming_analytics -e "source /tmp/002_add_consumer_id_to_alerts.sql"

ALTER TABLE alerts
    ADD COLUMN consumer_id VARCHAR(150) NOT NULL DEFAULT 'unknown' AFTER triggered_at,
    ADD INDEX idx_severity (severity);
