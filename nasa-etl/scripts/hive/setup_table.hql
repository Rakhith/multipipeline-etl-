-- =============================================================================
-- setup_table.hql
-- NASA HTTP Log ETL – Hive Pipeline
--
-- Creates the external raw-log table, registers the UDF, and builds a
-- batch_ids helper table so the three query scripts can iterate one
-- MapReduce job per ISO-week batch rather than one monolithic job.
--
-- Parameters (substituted by HiveETLDriver before execution):
--   INPUT_DIR  : HDFS/local directory containing decompressed NASA log files
--   INPUT_TABLE: bare table name for raw logs  (e.g. nasa_raw_logs)
--   DB_NAME    : Hive database                 (e.g. default)
--   UDF_JAR    : full path to the nasa-etl fat JAR
--   BATCH_SIZE : records-per-batch hint (stored in metadata, not used here)
-- =============================================================================

CREATE DATABASE IF NOT EXISTS ${DB_NAME};
USE ${DB_NAME};

-- ---------------------------------------------------------------------------
-- 1. Raw-log external table
--    EXTERNAL → dropping the table never deletes the source files.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS ${INPUT_TABLE};

CREATE EXTERNAL TABLE ${INPUT_TABLE} (
    line STRING
)
STORED AS TEXTFILE
LOCATION '${INPUT_DIR}'
TBLPROPERTIES (
    'skip.header.line.count' = '0',
    'serialization.null.format' = ''
);

-- Sanity check: confirm lines were loaded
SELECT COUNT(*) AS total_raw_lines FROM ${INPUT_TABLE};

-- ---------------------------------------------------------------------------
-- 2. Register the shared parsing UDF
-- ---------------------------------------------------------------------------
ADD JAR ${UDF_JAR};

CREATE TEMPORARY FUNCTION parse_log_line
    AS 'com.nasa.etl.hive.udf.LogParserUDF';

CREATE TEMPORARY FUNCTION week_batch_id
    AS 'com.nasa.etl.hive.udf.WeekBatchUDF';

-- ---------------------------------------------------------------------------
-- 3. Global Hive settings for all subsequent jobs in this session
-- ---------------------------------------------------------------------------
SET hive.exec.dynamic.partition      = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.mapred.mode                 = nonstrict;
SET hive.variable.substitute         = true;

-- Enable per-query progress so the driver can track individual MR jobs
SET hive.log.explain.output = true;

-- ---------------------------------------------------------------------------
-- 4. Parsed-log staging table  (ORC + batch_id partition = one MR per batch)
--
--    Writing ALL parsed rows into a partitioned ORC table here means:
--      • The initial parse regex runs ONCE across the whole dataset.
--      • Each downstream query reads only the partition(s) it needs.
--      • Each INSERT … SELECT … WHERE batch_id = N is a single focused MR job.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS parsed_logs_stage;

CREATE TABLE parsed_logs_stage (
    host              STRING,
    log_date          STRING,
    log_hour          INT,
    http_method       STRING,
    resource_path     STRING,
    protocol_version  STRING,
    status_code       INT,
    bytes_transferred BIGINT,
    malformed         INT
)
PARTITIONED BY (batch_id INT)
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- Populate all partitions in one MapReduce pass over the raw table.
-- Each distinct batch_id value becomes a separate HDFS partition directory
-- that downstream queries can read independently.
SET hive.exec.max.dynamic.partitions        = 1000;
SET hive.exec.max.dynamic.partitions.pernode = 500;

INSERT OVERWRITE TABLE parsed_logs_stage
PARTITION (batch_id)
SELECT
    parsed.host,
    parsed.log_date,
    parsed.log_hour,
    parsed.http_method,
    parsed.resource_path,
    parsed.protocol_version,
    parsed.status_code,
    parsed.bytes_transferred,
    parsed.malformed,
    parsed.batch_id          -- dynamic partition column must be last
FROM (
    SELECT parse_log_line(line) AS parsed
    FROM   ${INPUT_TABLE}
) tmp
WHERE parsed.malformed = 0
  AND parsed.log_date IS NOT NULL
  AND parsed.log_date != '';

-- ---------------------------------------------------------------------------
-- 5. Distinct-batch catalogue
--    A tiny helper table (one row per ISO-week batch) that the three query
--    scripts join against so each can emit one MR job per batch.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS batch_ids;

CREATE TABLE batch_ids AS
SELECT DISTINCT batch_id
FROM   parsed_logs_stage
ORDER BY batch_id;

SELECT COUNT(*) AS total_batches FROM batch_ids;
SELECT * FROM batch_ids ORDER BY batch_id;
