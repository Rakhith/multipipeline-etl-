-- =============================================================================
-- query2_top_resources.pig
-- NASA HTTP Log ETL – Apache Pig Pipeline
-- Query 2: Top-20 Requested Resources (per batch)
--
-- For the 20 most-requested resource paths within each batch, compute:
--   request_count       – total hits
--   total_bytes         – sum of bytes transferred
--   distinct_host_count – number of unique hosts
--
-- Output schema (TSV):
--   batch_id \t resource_path \t request_count \t total_bytes \t distinct_host_count
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
DEFINE Top20Filter com.nasa.etl.pig.udf.Top20Filter();

-- ============================================================
-- 1. Load raw log lines
-- ============================================================
raw_lines = LOAD '$INPUT'
    USING PigStorage('\n')
    AS (raw_line:chararray);

-- ============================================================
-- 2. Parse each line with the shared LogRecord parser
-- ============================================================
parsed = FOREACH raw_lines
    GENERATE FLATTEN(LogParser(raw_line))
    AS (host:chararray, log_date:chararray, log_hour:int,
        http_method:chararray, resource_path:chararray,
        protocol_version:chararray, status_code:int,
        bytes_transferred:long, batch_id:int, malformed:int);

-- ============================================================
-- 3. Keep only valid records
-- ============================================================
valid = FILTER parsed BY malformed == 0;

-- ============================================================
-- 4. Normalise resource_path: replace null/empty with "(empty)"
--    This matches the Mapper null-check in Query2TopResources.
-- ============================================================
valid_norm = FOREACH valid GENERATE
    batch_id,
    host,
    bytes_transferred,
    (resource_path IS NULL OR resource_path == ''
        ? '(empty)' : resource_path) AS resource_path:chararray;

-- ============================================================
-- 5. Group by (batch_id, resource_path) to aggregate per path
-- ============================================================
by_path = GROUP valid_norm BY (batch_id, resource_path);

-- ============================================================
-- 6. Aggregate request_count, total_bytes, distinct_host_count
--    COUNT()  = request_count  (each row is one request)
--    SUM()    = total_bytes
--    COUNT(DISTINCT) for hosts – Pig does not support COUNT(DISTINCT)
--    natively, so we use a nested DISTINCT on the host bag.
-- ============================================================
path_stats = FOREACH by_path {
    hosts_only  = FOREACH valid_norm GENERATE host;
    unique_hosts = DISTINCT hosts_only;
    GENERATE
        group.batch_id         AS batch_id:int,
        group.resource_path    AS resource_path:chararray,
        COUNT(valid_norm)      AS request_count:long,
        SUM(valid_norm.bytes_transferred) AS total_bytes:long,
        COUNT(unique_hosts)    AS distinct_host_count:long;
}

-- ============================================================
-- 7. Group by batch_id so we can pick the top 20 per batch
-- ============================================================
by_batch = GROUP path_stats BY batch_id;

-- ============================================================
-- 8. Project the columns for storage
--    We store all aggregates and let the database/reporter handle the top-20
--    selection for maximum stability in local mode.
-- ============================================================
q2_result = FOREACH path_stats GENERATE
    batch_id         AS batch_id:int,
    resource_path    AS resource_path:chararray,
    request_count    AS request_count:long,
    total_bytes      AS total_bytes:long,
    distinct_host_count AS distinct_host_count:long;

-- ============================================================
-- 10. Store as TSV
--    Format matches PigDBLoader.loadQuery2 column order:
--      batch_id \t resource_path \t request_count \t total_bytes \t distinct_host_count
-- ============================================================
STORE q2_result INTO '$OUTPUT'
    USING PigStorage('\t');
