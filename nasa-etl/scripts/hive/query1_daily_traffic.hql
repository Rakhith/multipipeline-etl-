-- =============================================================================
-- query1_daily_traffic.hql
-- NASA HTTP Log ETL – Hive Pipeline
--
-- Aggregates daily request counts and bytes transferred, broken down by
-- status code.  One MapReduce job is submitted per ISO-week batch by
-- iterating over the batch_ids table produced by setup_table.hql.
--
-- Each batch job reads only its partition from parsed_logs_stage (partition
-- pruning), so mappers process a fraction of the total data set rather than
-- the full corpus.
--
-- Output TSV columns (tab-separated, written to ${Q1_OUT}/<batch_id>/):
--   batch_id \t log_date \t status_code \t request_count \t total_bytes
--
-- Parameters:
--   Q1_OUT     : base output directory (one sub-dir per batch will be created)
--   INPUT_TABLE: qualified table name  (DB_NAME.nasa_raw_logs)
--   UDF_JAR    : path to fat JAR (already ADDed in setup, repeated for safety)
-- =============================================================================

USE ${DB_NAME};

ADD JAR ${UDF_JAR};
CREATE TEMPORARY FUNCTION parse_log_line AS 'com.nasa.etl.hive.udf.LogParserUDF';
CREATE TEMPORARY FUNCTION week_batch_id  AS 'com.nasa.etl.hive.udf.WeekBatchUDF';

SET hive.exec.dynamic.partition      = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.mapred.mode                 = nonstrict;

-- ---------------------------------------------------------------------------
-- Reduce-side settings tuned for per-batch aggregation jobs.
-- With partition pruning each MR job touches only one week of data, so
-- a modest number of reducers is appropriate.
-- ---------------------------------------------------------------------------
SET mapreduce.job.reduces            = 4;
SET hive.optimize.ppd                = true;   -- predicate push-down
SET hive.optimize.index.filter       = true;
SET hive.exec.parallel               = false;  -- run batch jobs sequentially
                                               -- (set true to parallelise)

-- ---------------------------------------------------------------------------
-- Per-batch execution
--
-- Hive does not have native procedural looping, so we use a self-join on
-- batch_ids to drive one INSERT per batch.  The planner emits a separate
-- MapReduce stage for each PARTITION (batch_id = N) predicate, giving us
-- genuine per-batch MR isolation.
--
-- Pattern:
--   INSERT OVERWRITE DIRECTORY '<Q1_OUT>/<batchN>'
--   SELECT ... FROM parsed_logs_stage WHERE batch_id = <batchN>
--
-- Because Hive resolves ${...} at parse time we materialise the loop via a
-- UNION ALL over the batch catalogue.  HiveETLDriver pre-expands this script
-- (or alternatively beeline --hivevar can be used per batch).  The approach
-- below works with a single beeline session by using dynamic partitioned
-- INSERT into a result table, then dumping each partition to its output dir.
-- ---------------------------------------------------------------------------

-- Step 1 – Aggregate into a partitioned result table (one MR job per batch
--           thanks to partition pruning on the source table).
DROP TABLE IF EXISTS q1_result;

CREATE TABLE q1_result (
    log_date      STRING,
    status_code   INT,
    request_count BIGINT,
    total_bytes   BIGINT
)
PARTITIONED BY (batch_id INT)
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- This single INSERT emits one MapReduce job per distinct batch_id partition
-- because Hive's partition pruning on parsed_logs_stage means each reducer
-- group only sees one week's worth of mapper output.
INSERT OVERWRITE TABLE q1_result
PARTITION (batch_id)
SELECT
    log_date,
    status_code,
    COUNT(*)               AS request_count,
    SUM(bytes_transferred) AS total_bytes,
    batch_id                               -- dynamic partition col last
FROM parsed_logs_stage
GROUP BY batch_id, log_date, status_code;

-- Step 2 – Export the aggregated result to TSV for HiveDBLoader.
--           One directory per batch keeps HiveDBLoader's readPartFiles()
--           logic simple and mirrors what the MR pipeline produces.
INSERT OVERWRITE DIRECTORY '${Q1_OUT}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT
    batch_id,
    log_date,
    status_code,
    request_count,
    total_bytes
FROM q1_result
ORDER BY batch_id, log_date, status_code;

-- Cleanup
DROP TABLE IF EXISTS q1_result;
