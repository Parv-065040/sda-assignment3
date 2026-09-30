-- Migration 006: adds two new tables for Phase 6 dashboard additions --
-- purely additive, no existing data touched.
--
-- customer_ad_interactions: per-user click/email-click attribution,
-- needed to join against customer_segments for the Campaign -> Segment
-- chart (which segments does each campaign's clickers belong to).
--
-- segment_history: a snapshot of segment counts written every
-- segmentation cycle, needed for the Segment Trend Over Time chart.
-- customer_segments only ever holds each customer's LATEST state
-- (it's upserted), so without this there is no way to see whether
-- "At Risk" is growing or shrinking over time.

CREATE TABLE IF NOT EXISTS customer_ad_interactions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    campaign VARCHAR(100) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    interacted_at DATETIME NOT NULL,
    consumer_id VARCHAR(150) NOT NULL DEFAULT 'unknown',
    INDEX idx_user_id (user_id),
    INDEX idx_campaign (campaign)
);

CREATE TABLE IF NOT EXISTS segment_history (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    snapshot_at DATETIME NOT NULL,
    segment VARCHAR(50) NOT NULL,
    customer_count INT NOT NULL,
    INDEX idx_snapshot_at (snapshot_at)
);
