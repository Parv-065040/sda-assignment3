-- Migration 003: adds customer_transactions (new table, raw per-purchase
-- history) and upgrades customer_segments with r_score/f_score/m_score
-- columns needed for proper RFM classification (Phase 3).
--
-- Your customer_segments table was reported as EMPTY (0 rows) as of the
-- last progress report, so this migration drops and recreates it rather
-- than ALTERing -- simpler, and there's no data to lose. If you've since
-- written real data into customer_segments, back it up first instead of
-- running this as-is.

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

DROP TABLE IF EXISTS customer_segments;

CREATE TABLE customer_segments (
    user_id BIGINT PRIMARY KEY,
    recency_days INT,
    frequency INT,
    monetary FLOAT,
    r_score INT,
    f_score INT,
    m_score INT,
    segment VARCHAR(50),
    updated_at DATETIME,
    INDEX idx_segment (segment)
);
