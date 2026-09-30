-- Migration 004: adds search_queries (new table) for the "Top Search
-- Queries" dashboard chart (Phase 5). Purely additive -- no existing
-- data is touched.

CREATE TABLE IF NOT EXISTS search_queries (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT,
    query VARCHAR(255) NOT NULL,
    searched_at DATETIME NOT NULL,
    consumer_id VARCHAR(150) NOT NULL DEFAULT 'unknown',
    INDEX idx_query (query),
    INDEX idx_searched_at (searched_at)
);
