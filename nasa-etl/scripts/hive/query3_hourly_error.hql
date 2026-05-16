-- =============================================================================
-- query3_hourly_error.hql
-- NASA HTTP Log ETL – Hive Pipeline
-- Query 3: Hourly Error Analysis
--
-- For each (batch_id, log_date, log_hour) group, compute:
--   error_request_count  – requests with status 400–599
--   total_request_count  – all requests in that hour
--   error_rate           – error_request_count / total_request_count
--   distinct_error_hosts – unique hosts that produced ≥1 error in that hour
--
-- Output schema (TSV, matches DBLoader / PigDBLoader contract):
--   batch_id \t log_date \t log_hour \t error_request_count \t
--   total_request_count \t error_rate \t distinct_error_hosts
--
-- Parameters (passed via --hiveconf on CLI or by HiveETLDriver):
--   INPUT_TABLE  : name of the external raw-log table  (default: nasa_raw_logs)
--   OUTPUT_DIR   : HDFS/local path for TSV output
--   UDF_JAR      : path to the fat JAR containing Hive UDFs
--   BATCH_SIZE   : records per logical batch (informational; batching is week-based)
-- =============================================================================

ADD JAR target/nasa-etl-1.0.0-shaded.jar;

CREATE TEMPORARY FUNCTION parse_log_line AS 'com.nasa.etl.hive.udf.LogParserUDF';

SET hive.exec.dynamic.partition      = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.mapred.mode                 = nonstrict;

DROP VIEW IF EXISTS parsed_logs_q3;

CREATE VIEW parsed_logs_q3 AS
SELECT
    parsed.batch_id    AS batch_id,
    parsed.log_date    AS log_date,
    parsed.log_hour    AS log_hour,
    parsed.status_code AS status_code,
    parsed.host        AS host
FROM (
    SELECT parse_log_line(line) AS parsed
    FROM default.nasa_raw_logs
) tmp
WHERE parsed.malformed = 0
  AND parsed.log_date IS NOT NULL
  AND parsed.log_date != '';

INSERT OVERWRITE DIRECTORY '/tmp/hive-output/q3'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT
    batch_id,
    log_date,
    log_hour,
    COUNT(CASE WHEN status_code >= 400 AND status_code <= 599 THEN 1 END)
        AS error_request_count,
    COUNT(*)
        AS total_request_count,
    CAST(
        COUNT(CASE WHEN status_code >= 400 AND status_code <= 599 THEN 1 END)
        AS DOUBLE
    ) / COUNT(*)
        AS error_rate,
    COUNT(DISTINCT CASE WHEN status_code >= 400 AND status_code <= 599 THEN host END)
        AS distinct_error_hosts
FROM parsed_logs_q3
GROUP BY batch_id, log_date, log_hour
ORDER BY batch_id, log_date, log_hour;

DROP VIEW IF EXISTS parsed_logs_q3;