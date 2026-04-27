-- Schemas for each query output + metadata TABLE
-- We decided to go with seperate table per query

-- Run metadata

CREATE TABLE IF NOT EXISTS run_metadata (
    run_id           SERIAL PRIMARY KEY,
    pipeline_name    VARCHAR(64),
    batch_size       INT,
    total_batches    INT,
    avg_batch_size   DOUBLE PRECISION,
    total_records    BIGINT,
    malformed_count  BIGINT,
    runtime_ms       BIGINT,
    executed_at      TIMESTAMP
);

-- Query 1 – Daily Traffic Summary

CREATE TABLE IF NOT EXISTS q1_daily_traffic (
    id             SERIAL PRIMARY KEY,
    run_id         INT REFERENCES run_metadata(run_id),
    batch_id       INT NOT NULL,
    log_date       DATE,
    status_code    INT,
    request_count  BIGINT,
    total_bytes    BIGINT,
    query_runtime_ms BIGINT,
    loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Query 2 – Top Requested Resources

CREATE TABLE IF NOT EXISTS q2_top_resources (
    id                  SERIAL PRIMARY KEY,
    run_id              INT REFERENCES run_metadata(run_id),
    batch_id            INT NOT NULL,
    resource_path       TEXT,
    request_count       BIGINT,
    total_bytes         BIGINT,
    distinct_host_count BIGINT,
    query_runtime_ms    BIGINT,
    loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Query 3 – Hourly Error Analysis

CREATE TABLE IF NOT EXISTS q3_hourly_error (
    id                   SERIAL PRIMARY KEY,
    run_id               INT REFERENCES run_metadata(run_id),
    batch_id             INT NOT NULL,
    log_date             DATE,
    log_hour             INT,
    error_request_count  BIGINT,
    total_request_count  BIGINT,
    error_rate           DOUBLE PRECISION,
    distinct_error_hosts BIGINT,
    query_runtime_ms     BIGINT,
    loaded_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- creating indices
CREATE INDEX IF NOT EXISTS idx_q1_run  ON q1_daily_traffic(run_id);
CREATE INDEX IF NOT EXISTS idx_q2_run  ON q2_top_resources(run_id);
CREATE INDEX IF NOT EXISTS idx_q3_run  ON q3_hourly_error(run_id);