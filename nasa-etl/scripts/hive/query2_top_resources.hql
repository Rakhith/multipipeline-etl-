ADD JAR target/nasa-etl-1.0.0-shaded.jar;

CREATE TEMPORARY FUNCTION parse_log_line AS 'com.nasa.etl.hive.udf.LogParserUDF';

SET hive.exec.dynamic.partition      = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.mapred.mode                 = nonstrict;

DROP VIEW IF EXISTS parsed_logs_q2;
DROP VIEW IF EXISTS resource_agg;
DROP VIEW IF EXISTS resource_ranked;

CREATE VIEW parsed_logs_q2 AS
SELECT
    parsed.batch_id          AS batch_id,
    parsed.resource_path     AS resource_path,
    parsed.host              AS host,
    parsed.bytes_transferred AS bytes_transferred
FROM (
    SELECT parse_log_line(line) AS parsed
    FROM default.nasa_raw_logs
) tmp
WHERE parsed.malformed = 0
  AND parsed.log_date IS NOT NULL
  AND parsed.log_date != '';

CREATE VIEW resource_agg AS
SELECT
    batch_id,
    CASE
        WHEN resource_path IS NULL OR resource_path = '' THEN '(empty)'
        ELSE resource_path
    END AS resource_path,
    COUNT(*)               AS request_count,
    SUM(bytes_transferred) AS total_bytes,
    COUNT(DISTINCT host)   AS distinct_host_count
FROM parsed_logs_q2
GROUP BY
    batch_id,
    CASE
        WHEN resource_path IS NULL OR resource_path = '' THEN '(empty)'
        ELSE resource_path
    END;

CREATE VIEW resource_ranked AS
SELECT
    batch_id,
    resource_path,
    request_count,
    total_bytes,
    distinct_host_count,
    RANK() OVER (
        PARTITION BY batch_id
        ORDER BY request_count DESC, total_bytes DESC
    ) AS rnk
FROM resource_agg;

INSERT OVERWRITE DIRECTORY '/tmp/hive-output/q2'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT
    batch_id,
    resource_path,
    request_count,
    total_bytes,
    distinct_host_count
FROM resource_ranked
WHERE rnk <= 20
ORDER BY batch_id, request_count DESC, resource_path;

DROP VIEW IF EXISTS resource_ranked;
DROP VIEW IF EXISTS resource_agg;
DROP VIEW IF EXISTS parsed_logs_q2;