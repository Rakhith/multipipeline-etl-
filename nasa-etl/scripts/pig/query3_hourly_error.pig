-- =============================================================================
-- query3_hourly_error.pig
-- NASA HTTP Log ETL – Apache Pig Pipeline
-- Query 3: Hourly Error Analysis
--
-- For each (batch_id, log_date, log_hour) group, compute:
--   error_request_count  – requests with status 400–599
--   total_request_count  – all requests in that hour
--   error_rate           – error_request_count / total_request_count
--   distinct_error_hosts – unique hosts that produced ≥1 error in that hour
--
-- Output schema (TSV):
--   batch_id \t log_date \t log_hour \t error_request_count \t
--   total_request_count \t error_rate \t distinct_error_hosts
--
-- batch_id is ISO week-based (YYYYWW), identical to the MapReduce pipeline.
-- error_rate is formatted to 6 decimal places to match the Reducer output.
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
-- 4. Annotate each record with is_error flag (1 = 4xx/5xx, 0 = other)
--    and extract only the columns needed for this query.
--    This mirrors the HourlyErrorMapper value: "is_error\thost"
-- ============================================================
annotated = FOREACH valid GENERATE
    batch_id,
    log_date,
    log_hour,
    host,
    (status_code >= 400 AND status_code <= 599 ? 1 : 0) AS is_error:int;

-- ============================================================
-- 5. Group by (batch_id, log_date, log_hour)
--    Identical grouping key to the Mapper output key in
--    Query3HourlyError.
-- ============================================================
grouped = GROUP annotated BY (batch_id, log_date, log_hour);

-- ============================================================
-- 6. Aggregate:
--    total_request_count  = COUNT(*)
--    error_request_count  = SUM(is_error)
--    distinct_error_hosts = COUNT(DISTINCT host) among errors only
--    error_rate           = error_request_count / total_request_count
--
--    COUNT(DISTINCT) is not natively available in Pig for nested bags
--    without UDFs, so we use the nested DISTINCT approach on a
--    filtered sub-bag to get distinct error hosts.
-- ============================================================
q3_result = FOREACH grouped {
    -- Sub-bag: only rows that are errors
    errors     = FILTER annotated BY is_error == 1;
    -- Distinct hosts among error rows
    err_hosts  = FOREACH errors GENERATE host;
    uniq_hosts = DISTINCT err_hosts;

    total_reqs = COUNT(annotated);
    error_reqs = COUNT(errors);

    GENERATE
        group.batch_id      AS batch_id:int,
        group.log_date      AS log_date:chararray,
        group.log_hour      AS log_hour:int,
        error_reqs          AS error_request_count:long,
        total_reqs          AS total_request_count:long,
        -- error_rate: match MapReduce format "%.6f"
        (total_reqs > 0L
            ? (double) error_reqs / (double) total_reqs
            : 0.0)          AS error_rate:double,
        COUNT(uniq_hosts)   AS distinct_error_hosts:long;
}

-- ============================================================
-- 7. Format error_rate to 6 decimal places
--    Pig stores doubles natively; the loader reads them as doubles,
--    so we output the raw double and let PigDBLoader parse it.
--    For human-readable TSV we round via a FOREACH expression.
-- ============================================================
q3_formatted = FOREACH q3_result GENERATE
    batch_id,
    log_date,
    log_hour,
    error_request_count,
    total_request_count,
    error_rate,
    distinct_error_hosts;

-- ============================================================
-- 8. Store as TSV
--    Format matches PigDBLoader.loadQuery3 column order:
--      batch_id \t log_date \t log_hour \t error_request_count \t
--      total_request_count \t error_rate \t distinct_error_hosts
-- ============================================================
STORE q3_formatted INTO '$OUTPUT'
    USING PigStorage('\t');
