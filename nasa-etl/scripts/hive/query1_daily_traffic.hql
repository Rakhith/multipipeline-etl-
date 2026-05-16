ADD JAR target/nasa-etl-1.0.0-shaded.jar;

CREATE TEMPORARY FUNCTION parse_log_line AS 'com.nasa.etl.hive.udf.LogParserUDF';

SET hive.exec.dynamic.partition      = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.mapred.mode                 = nonstrict;

DROP VIEW IF EXISTS parsed_logs_q1;

CREATE VIEW parsed_logs_q1 AS
SELECT
    parsed.batch_id          AS batch_id,
    parsed.log_date          AS log_date,
    parsed.status_code       AS status_code,
    parsed.bytes_transferred AS bytes_transferred
FROM (
    SELECT parse_log_line(line) AS parsed
    FROM default.nasa_raw_logs
) tmp
WHERE parsed.malformed = 0
  AND parsed.log_date IS NOT NULL
  AND parsed.log_date != '';

INSERT OVERWRITE DIRECTORY '/tmp/hive-output/q1'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT
    batch_id,
    log_date,
    status_code,
    COUNT(*)               AS request_count,
    SUM(bytes_transferred) AS total_bytes
FROM parsed_logs_q1
GROUP BY batch_id, log_date, status_code
ORDER BY batch_id, log_date, status_code;

DROP VIEW IF EXISTS parsed_logs_q1;