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
    q1_runtime_ms    BIGINT,
    q2_runtime_ms    BIGINT,
    q3_runtime_ms    BIGINT,
    runtime_ms       BIGINT,
    executed_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Batch metadata (per batch per query timing)

CREATE TABLE IF NOT EXISTS batch_metadata (
    id               SERIAL PRIMARY KEY,
    run_id           INT REFERENCES run_metadata(run_id),
    batch_id         INT NOT NULL,         
    query_name       VARCHAR(8) NOT NULL,  
    record_count     BIGINT,
    batch_runtime_ms BIGINT,
    loaded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    loaded_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- creating indices
CREATE INDEX IF NOT EXISTS idx_q1_run        ON q1_daily_traffic(run_id);
CREATE INDEX IF NOT EXISTS idx_q1_batch      ON q1_daily_traffic(batch_id);
CREATE INDEX IF NOT EXISTS idx_q2_run        ON q2_top_resources(run_id);
CREATE INDEX IF NOT EXISTS idx_q2_batch      ON q2_top_resources(batch_id);
CREATE INDEX IF NOT EXISTS idx_q3_run        ON q3_hourly_error(run_id);
CREATE INDEX IF NOT EXISTS idx_q3_batch      ON q3_hourly_error(batch_id);
CREATE INDEX IF NOT EXISTS idx_batch_run     ON batch_metadata(run_id);
CREATE INDEX IF NOT EXISTS idx_meta_pipeline ON run_metadata(pipeline_name);
CREATE INDEX IF NOT EXISTS idx_meta_batch    ON run_metadata(batch_size);