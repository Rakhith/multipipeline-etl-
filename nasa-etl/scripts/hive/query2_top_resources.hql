-- =============================================================================
-- query2_top_resources.hql
-- NASA HTTP Log ETL – Hive Pipeline
--
-- Finds the top-20 most-requested resource paths per ISO-week batch,
-- reporting request count, total bytes transferred, and distinct requesting
-- host count.
--
-- Execution model:
--   • Stage A  – COUNT/SUM aggregation  (one MR job per batch via partition pruning)
--   • Stage B  – ROW_NUMBER window rank (one MR job per batch, shuffle by batch_id)
--   • Stage C  – TSV export             (one MR job, reads the ranked table)
--
-- Output TSV columns:
--   batch_id \t resource_path \t request_count \t total_bytes \t distinct_host_count
--
-- Parameters:
--   Q2_OUT     : base output directory
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
-- Stage A – Per-batch resource aggregation
--
-- Partition pruning on parsed_logs_stage ensures each batch_id group is
-- processed by a separate MapReduce wave.  The CASE expression normalises
-- empty resource paths to the literal '(empty)' consistent with the original
-- pipeline contract.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS q2_agg;

CREATE TABLE q2_agg (
    resource_path       STRING,
    request_count       BIGINT,
    total_bytes         BIGINT,
    distinct_host_count BIGINT
)
PARTITIONED BY (batch_id INT)
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE q2_agg
PARTITION (batch_id)
SELECT
    CASE
        WHEN resource_path IS NULL OR resource_path = '' THEN '(empty)'
        ELSE resource_path
    END                    AS resource_path,
    COUNT(*)               AS request_count,
    SUM(bytes_transferred) AS total_bytes,
    COUNT(DISTINCT host)   AS distinct_host_count,
    batch_id                                       -- dynamic partition col last
FROM parsed_logs_stage
GROUP BY
    batch_id,
    CASE
        WHEN resource_path IS NULL OR resource_path = '' THEN '(empty)'
        ELSE resource_path
    END;

-- ---------------------------------------------------------------------------
-- Stage B – Per-batch top-20 ranking
--
-- ROW_NUMBER() OVER (PARTITION BY batch_id ORDER BY request_count DESC)
-- shuffles rows by batch_id to a single reducer per batch, so this is also
-- one effective MR job per batch.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS q2_ranked;

CREATE TABLE q2_ranked (
    resource_path       STRING,
    request_count       BIGINT,
    total_bytes         BIGINT,
    distinct_host_count BIGINT,
    rn                  INT
)
PARTITIONED BY (batch_id INT)
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE q2_ranked
PARTITION (batch_id)
SELECT
    resource_path,
    request_count,
    total_bytes,
    distinct_host_count,
    ROW_NUMBER() OVER (
        PARTITION BY batch_id
        ORDER BY request_count DESC
    ) AS rn,
    batch_id                -- dynamic partition col last
FROM q2_agg;

-- ---------------------------------------------------------------------------
-- Stage C – TSV export (top-20 filter + sort for HiveDBLoader)
-- ---------------------------------------------------------------------------
INSERT OVERWRITE DIRECTORY '${Q2_OUT}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT
    batch_id,
    resource_path,
    request_count,
    total_bytes,
    distinct_host_count
FROM q2_ranked
WHERE rn <= 20
ORDER BY batch_id, request_count DESC;

-- Cleanup
DROP TABLE IF EXISTS q2_ranked;
DROP TABLE IF EXISTS q2_agg;
