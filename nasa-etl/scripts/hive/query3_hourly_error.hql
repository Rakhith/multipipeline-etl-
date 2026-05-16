ADD JAR ${UDF_JAR};

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
    FROM ${INPUT_TABLE}
) tmp
WHERE parsed.malformed = 0
  AND parsed.log_date IS NOT NULL
  AND parsed.log_date != '';

INSERT OVERWRITE DIRECTORY '${OUTPUT_DIR}'
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
    ROUND(
        CAST(
            COUNT(CASE WHEN status_code >= 400 AND status_code <= 599 THEN 1 END)
            AS DOUBLE
        ) / COUNT(*),
        6
    )
        AS error_rate,
    COUNT(DISTINCT CASE WHEN status_code >= 400 AND status_code <= 599 THEN host END)
        AS distinct_error_hosts
FROM parsed_logs_q3
GROUP BY batch_id, log_date, log_hour
ORDER BY batch_id, log_date, log_hour;

DROP VIEW IF EXISTS parsed_logs_q3;
