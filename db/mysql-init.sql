-- Auto-run by the MySQL container on first startup (mounted at
-- /docker-entrypoint-initdb.d/init.sql). Only runs against a fresh,
-- empty data volume -- if you need to re-run this, remove the mysql-data
-- volume first (docker-compose down -v) or run these statements manually.

CREATE TABLE IF NOT EXISTS window_metrics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    window_start DATETIME NOT NULL,
    window_end DATETIME NOT NULL,
    consumer_id VARCHAR(150) NOT NULL DEFAULT 'unknown',
    views INT DEFAULT 0,
    cart_adds INT DEFAULT 0,
    purchases INT DEFAULT 0,
    conversion_rate FLOAT DEFAULT 0,
    conversion_rate_ma FLOAT DEFAULT 0,
    cart_abandonment_rate FLOAT DEFAULT 0,
    cart_abandonment_rate_ma FLOAT DEFAULT 0,
    revenue FLOAT DEFAULT 0,
    orders INT DEFAULT 0,
    ad_clicks INT DEFAULT 0,
    ad_impressions INT DEFAULT 0,
    ad_spend FLOAT DEFAULT 0,
    INDEX idx_window_start (window_start),
    INDEX idx_consumer_id (consumer_id)
);

CREATE TABLE IF NOT EXISTS category_revenue (
    id INT AUTO_INCREMENT PRIMARY KEY,
    window_start DATETIME NOT NULL,
    consumer_id VARCHAR(150) NOT NULL DEFAULT 'unknown',
    category_code VARCHAR(100),
    revenue FLOAT DEFAULT 0,
    orders INT DEFAULT 0,
    INDEX idx_window_start (window_start)
);

CREATE TABLE IF NOT EXISTS campaign_performance (
    id INT AUTO_INCREMENT PRIMARY KEY,
    window_start DATETIME NOT NULL,
    consumer_id VARCHAR(150) NOT NULL DEFAULT 'unknown',
    campaign VARCHAR(100),
    utm_source VARCHAR(100),
    clicks INT DEFAULT 0,
    impressions INT DEFAULT 0,
    cost FLOAT DEFAULT 0,
    ctr FLOAT DEFAULT 0,
    INDEX idx_window_start (window_start)
);

-- Phase 2 (alerting): logs every anomaly the consumer detects.
CREATE TABLE IF NOT EXISTS alerts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    triggered_at DATETIME NOT NULL,
    consumer_id VARCHAR(150) NOT NULL DEFAULT 'unknown',
    metric_name VARCHAR(100) NOT NULL,
    metric_value FLOAT NOT NULL,
    expected_value FLOAT,
    deviation_score FLOAT,
    severity ENUM('info', 'warning', 'critical') DEFAULT 'warning',
    message VARCHAR(500),
    INDEX idx_triggered_at (triggered_at),
    INDEX idx_severity (severity)
);

-- Phase 3 (RFM segmentation): raw per-customer purchase history, read by
-- the standalone segmentation service to compute recency/frequency/monetary.
-- purchased_at is the REAL wall-clock time the consumer processed the
-- transaction, not the synthetic event_time baked into the sample data.
CREATE TABLE IF NOT EXISTS customer_transactions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    order_id VARCHAR(100),
    category_code VARCHAR(100),
    order_value FLOAT NOT NULL,
    purchased_at DATETIME NOT NULL,
    consumer_id VARCHAR(150) NOT NULL DEFAULT 'unknown',
    INDEX idx_user_id (user_id),
    INDEX idx_purchased_at (purchased_at)
);

-- Phase 5 (dashboard): raw search terms for the "Top Search Queries" chart.
CREATE TABLE IF NOT EXISTS search_queries (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT,
    query VARCHAR(255) NOT NULL,
    searched_at DATETIME NOT NULL,
    consumer_id VARCHAR(150) NOT NULL DEFAULT 'unknown',
    INDEX idx_query (query),
    INDEX idx_searched_at (searched_at)
);

-- Phase 6 (dashboard): per-user ad click / email-click attribution, used
-- to join against customer_segments for the Campaign -> Segment chart.
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

-- Phase 6 (dashboard): segment distribution snapshot per segmentation
-- cycle, for the Segment Trend Over Time chart. customer_segments only
-- ever holds each customer's LATEST state (upserted), so this is the
-- only place change-over-time is observable.
CREATE TABLE IF NOT EXISTS segment_history (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    snapshot_at DATETIME NOT NULL,
    segment VARCHAR(50) NOT NULL,
    customer_count INT NOT NULL,
    INDEX idx_snapshot_at (snapshot_at)
);

-- Phase 3 (RFM segmentation): one row per customer, refreshed periodically.
-- recency_minutes (not days) deliberately -- see rfm_segmentation.py for why.
CREATE TABLE IF NOT EXISTS customer_segments (
    user_id BIGINT PRIMARY KEY,
    recency_minutes INT,
    frequency INT,
    monetary FLOAT,
    r_score INT,
    f_score INT,
    m_score INT,
    segment VARCHAR(50),
    updated_at DATETIME,
    INDEX idx_segment (segment)
);