-- =============================================================================
-- query3_hourly_error.hql
-- NASA HTTP Log ETL – Hive Pipeline
--
-- Computes per-hour error statistics for each ISO-week batch:
--   • error_request_count  – requests with HTTP status 400–599
--   • total_request_count  – all requests in that hour
--   • error_rate           – error_request_count / total_request_count (6 dp)
--   • distinct_error_hosts – unique hosts that received an error response
--
-- Execution model:
--   • Stage A – GROUP BY aggregation   (one MR job per batch, partition pruning)
--   • Stage B – TSV export             (one MR job, lightweight)
--
-- Output TSV columns:
--   batch_id \t log_date \t log_hour \t error_request_count \t
--   total_request_count \t error_rate \t distinct_error_hosts
--
-- Parameters:
--   Q3_OUT     : base output directory
--   INPUT_TABLE: qualified table name
--   UDF_JAR    : path to fat JAR
-- =============================================================================

USE ${DB_NAME};

ADD JAR ${UDF_JAR};
CREATE TEMPORARY FUNCTION parse_log_line AS 'com.nasa.etl.hive.udf.LogParserUDF';
CREATE TEMPORARY FUNCTION week_batch_id  AS 'com.nasa.etl.hive.udf.WeekBatchUDF';

SET hive.exec.dynamic.partition      = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.mapred.mode                 = nonstrict;
SET mapreduce.job.reduces            = 4;
SET hive.optimize.ppd                = true;
SET hive.optimize.index.filter       = true;
SET hive.exec.parallel               = false;

-- ---------------------------------------------------------------------------
-- Stage A – Per-batch hourly error aggregation
--
-- Partition pruning on parsed_logs_stage (PARTITIONED BY batch_id) means
-- the planner emits one focused MapReduce job per batch_id value.
-- Each mapper reads only that week's ORC files; reducers aggregate by
-- (log_date, log_hour).
--
-- Error definition: status_code BETWEEN 400 AND 599  (4xx client + 5xx server)
-- ROUND(..., 6) matches the original pipeline's DOUBLE PRECISION contract.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS q3_result;

CREATE TABLE q3_result (
    log_date             STRING,
    log_hour             INT,
    error_request_count  BIGINT,
    total_request_count  BIGINT,
    error_rate           DOUBLE,
    distinct_error_hosts BIGINT
)
PARTITIONED BY (batch_id INT)
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE q3_result
PARTITION (batch_id)
SELECT
    log_date,
    log_hour,
    -- error request count: status 400–599
    COUNT(
        CASE
            WHEN status_code >= 400 AND status_code <= 599 THEN 1
        END
    )                                                        AS error_request_count,
    -- total requests in this hour
    COUNT(*)                                                 AS total_request_count,
    -- error rate rounded to 6 decimal places
    ROUND(
        CAST(
            COUNT(
                CASE
                    WHEN status_code >= 400 AND status_code <= 599 THEN 1
                END
            ) AS DOUBLE
        ) / COUNT(*),
        6
    )                                                        AS error_rate,
    -- distinct hosts that received an error response
    COUNT(
        DISTINCT CASE
            WHEN status_code >= 400 AND status_code <= 599 THEN host
        END
    )                                                        AS distinct_error_hosts,
    batch_id                                                 -- dynamic partition col last
FROM parsed_logs_stage
GROUP BY batch_id, log_date, log_hour;

-- ---------------------------------------------------------------------------
-- Stage B – TSV export for HiveDBLoader
-- ---------------------------------------------------------------------------
INSERT OVERWRITE DIRECTORY '${Q3_OUT}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT
    batch_id,
    log_date,
    log_hour,
    error_request_count,
    total_request_count,
    error_rate,
    distinct_error_hosts
FROM q3_result
ORDER BY batch_id, log_date, log_hour;

-- Cleanup
DROP TABLE IF EXISTS q3_result;
