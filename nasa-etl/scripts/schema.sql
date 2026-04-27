-- ============================================================
-- NASA HTTP Log ETL – Database Schema
-- Compatible with PostgreSQL 12+ and MySQL 8+
-- ============================================================

-- ----------------------------------------
-- Run metadata (one row per query per run)
-- ----------------------------------------
CREATE TABLE IF NOT EXISTS run_metadata (
    id               SERIAL PRIMARY KEY,
    pipeline_name    VARCHAR(64),
    run_id           VARCHAR(64),
    query_name       VARCHAR(64),
    batch_size       INT,
    batch_count      BIGINT,
    avg_batch_size   DOUBLE PRECISION,
    total_records    BIGINT,
    malformed_count  BIGINT,
    runtime_ms       BIGINT,
    execution_time   VARCHAR(32)
);

-- ----------------------------------------
-- Q1 – Daily Traffic Summary
-- ----------------------------------------
CREATE TABLE IF NOT EXISTS q1_daily_traffic (
    id             SERIAL PRIMARY KEY,
    run_id         VARCHAR(64),
    pipeline_name  VARCHAR(64),
    batch_id       VARCHAR(64),
    log_date       DATE,
    status_code    INT,
    request_count  BIGINT,
    total_bytes    BIGINT,
    loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_q1_run  ON q1_daily_traffic(run_id);
CREATE INDEX IF NOT EXISTS idx_q1_date ON q1_daily_traffic(log_date);

-- ----------------------------------------
-- Q2 – Top Requested Resources
-- ----------------------------------------
CREATE TABLE IF NOT EXISTS q2_top_resources (
    id                  SERIAL PRIMARY KEY,
    run_id              VARCHAR(64),
    pipeline_name       VARCHAR(64),
    batch_id            VARCHAR(64),
    resource_path       TEXT,
    request_count       BIGINT,
    total_bytes         BIGINT,
    distinct_host_count BIGINT,
    loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_q2_run ON q2_top_resources(run_id);

-- ----------------------------------------
-- Q3 – Hourly Error Analysis
-- ----------------------------------------
CREATE TABLE IF NOT EXISTS q3_hourly_error (
    id                   SERIAL PRIMARY KEY,
    run_id               VARCHAR(64),
    pipeline_name        VARCHAR(64),
    batch_id             VARCHAR(64),
    log_date             DATE,
    log_hour             INT,
    error_request_count  BIGINT,
    total_request_count  BIGINT,
    error_rate           DOUBLE PRECISION,
    distinct_error_hosts BIGINT,
    loaded_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_q3_run  ON q3_hourly_error(run_id);
CREATE INDEX IF NOT EXISTS idx_q3_date ON q3_hourly_error(log_date);
