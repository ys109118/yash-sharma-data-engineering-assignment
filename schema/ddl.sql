-- ============================================================
-- E-Commerce Behavior Data Engineering Pipeline
-- DDL Script — 3NF Normalised Schema
-- Database: PostgreSQL (compatible with SQLite via minor tweaks)
-- ============================================================

-- Dimension: Users
CREATE TABLE IF NOT EXISTS dim_users (
    user_id     BIGINT      PRIMARY KEY
);

-- Dimension: Products (product_id → category, brand)
CREATE TABLE IF NOT EXISTS dim_products (
    product_id      BIGINT          PRIMARY KEY,
    category_id     BIGINT          NOT NULL,
    category_code   VARCHAR(255),           -- nullable: not all products have a code
    brand           VARCHAR(100),           -- nullable: brand sometimes missing
    price           NUMERIC(10,2)   NOT NULL
);

-- Dimension: Categories
CREATE TABLE IF NOT EXISTS dim_categories (
    category_id     BIGINT          PRIMARY KEY,
    category_code   VARCHAR(255)            -- nullable: some categories lack a code
);

-- Fact: Events
CREATE TABLE IF NOT EXISTS fact_events (
    event_id        BIGSERIAL       PRIMARY KEY,
    event_time      TIMESTAMP       NOT NULL,
    event_type      VARCHAR(20)     NOT NULL,   -- 'view' | 'cart' | 'purchase'
    product_id      BIGINT          NOT NULL REFERENCES dim_products(product_id),
    user_id         BIGINT          NOT NULL REFERENCES dim_users(user_id),
    user_session    VARCHAR(64)     NOT NULL,
    price           NUMERIC(10,2)   NOT NULL,
    source_month    CHAR(7)         NOT NULL,   -- 'YYYY-MM' for incremental tracking
    CONSTRAINT uq_event UNIQUE (event_time, event_type, product_id, user_id, user_session)
);

-- ============================================================
-- Indexes — justified by Section C query patterns
-- ============================================================

-- Funnel analysis (Q1): GROUP BY category, filter event_type
CREATE INDEX IF NOT EXISTS idx_events_product_id   ON fact_events(product_id);
CREATE INDEX IF NOT EXISTS idx_events_event_type   ON fact_events(event_type);

-- Session aggregation (Q2): GROUP BY user_session
CREATE INDEX IF NOT EXISTS idx_events_user_session ON fact_events(user_session);

-- Top brands by revenue (Q3): JOIN products, filter event_type='purchase'
CREATE INDEX IF NOT EXISTS idx_products_category   ON dim_products(category_id);

-- Users in Oct not in Nov (Q4): filter by user_id + source_month
CREATE INDEX IF NOT EXISTS idx_events_user_month   ON fact_events(user_id, source_month);

-- Hourly distribution (Q5): extract hour from event_time
CREATE INDEX IF NOT EXISTS idx_events_event_time   ON fact_events(event_time);

-- Pipeline idempotency / integrity validation
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          SERIAL          PRIMARY KEY,
    source_file     VARCHAR(255)    NOT NULL,
    started_at      TIMESTAMP       NOT NULL,
    finished_at     TIMESTAMP,
    rows_extracted  INTEGER,
    rows_transformed INTEGER,
    rows_loaded     INTEGER,
    rows_dropped    INTEGER,
    errors          TEXT,
    status          VARCHAR(20)     DEFAULT 'running'
);
