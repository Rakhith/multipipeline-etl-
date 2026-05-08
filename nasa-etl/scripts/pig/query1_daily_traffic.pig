-- =============================================================================
-- query1_daily_traffic.pig
-- NASA HTTP Log ETL – Apache Pig Pipeline
-- Query 1: Daily Traffic Summary
--
-- For each (batch_id, log_date, status_code) compute:
--   request_count  – total HTTP requests
--   total_bytes    – sum of bytes transferred
--
-- Output schema (TSV):
--   batch_id \t log_date \t status_code \t request_count \t total_bytes
--
-- batch_id is ISO week-based (YYYYWW), identical to the MapReduce pipeline.
--
-- Parameters (passed via -p on the CLI):
--   INPUT      – input file or directory of raw NASA log files
--   OUTPUT     – output directory for this query's results
--   UDF_JAR    – path to the fat JAR containing all UDFs
--   BATCH_SIZE – logical batch size (informational; batching is week-based)
--   RUN_ID     – current run identifier
-- =============================================================================

-- ---- register UDF jar ----
REGISTER '$UDF_JAR';

-- ---- define aliases for UDFs ----
DEFINE LogParser  com.nasa.etl.pig.udf.LogParser();

-- ============================================================
-- 1. Load raw log lines (one line per tuple)
-- ============================================================
raw_lines = LOAD '$INPUT'
    USING PigStorage('\n')
    AS (raw_line:chararray);

-- ============================================================
-- 2. Parse each line with the shared LogRecord parser
--    Output: host, log_date, log_hour, http_method, resource_path,
--            protocol_version, status_code, bytes_transferred,
--            batch_id, malformed
-- ============================================================
parsed = FOREACH raw_lines
    GENERATE FLATTEN(LogParser(raw_line))
    AS (host:chararray, log_date:chararray, log_hour:int,
        http_method:chararray, resource_path:chararray,
        protocol_version:chararray, status_code:int,
        bytes_transferred:long, batch_id:int, malformed:int);

-- ============================================================
-- 3. Keep only valid records (malformed == 0)
--    Malformed records are counted but excluded from aggregation,
--    matching the MapReduce Mapper behaviour exactly.
-- ============================================================
valid = FILTER parsed BY malformed == 0;

-- ============================================================
-- 4. Group by (batch_id, log_date, status_code)
--    This is the exact grouping key used by Query1DailyTraffic.
-- ============================================================
grouped = GROUP valid BY (batch_id, log_date, status_code);

-- ============================================================
-- 5. Aggregate: COUNT(*) = request_count, SUM(bytes) = total_bytes
-- ============================================================
q1_result = FOREACH grouped GENERATE
    group.batch_id         AS batch_id:int,
    group.log_date         AS log_date:chararray,
    group.status_code      AS status_code:int,
    COUNT(valid)           AS request_count:long,
    SUM(valid.bytes_transferred) AS total_bytes:long;

-- ============================================================
-- 6. Order by batch_id, log_date, status_code (deterministic output)
-- ============================================================
q1_ordered = ORDER q1_result BY batch_id ASC, log_date ASC, status_code ASC;

-- ============================================================
-- 7. Store as TSV
--    Format matches DBLoader.loadQuery1 column order:
--      batch_id \t log_date \t status_code \t request_count \t total_bytes
-- ============================================================
STORE q1_ordered INTO '$OUTPUT'
    USING PigStorage('\t');
