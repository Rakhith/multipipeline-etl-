-- =============================================================================
-- setup_table.hql
-- NASA HTTP Log ETL – Hive Pipeline
--
-- Creates (or recreates) the external Hive table that the three query scripts
-- read from.  This script is run ONCE by HiveETLDriver before any query.
--
-- The table is EXTERNAL so that dropping it does not delete the raw log data.
-- Each row is exactly one raw log line stored in the 'line' column.
-- The three query scripts pass every line through the parse_log_line UDF.
--
-- Parameters:
--   INPUT_DIR  : HDFS/local directory containing decompressed NASA log files
--   INPUT_TABLE: name to give the Hive table (default: nasa_raw_logs)
--   DB_NAME    : Hive database to use (default: default)
-- =============================================================================

CREATE DATABASE IF NOT EXISTS ${DB_NAME};
USE ${DB_NAME};

-- Drop and recreate so that the LOCATION always points to the current input.
-- EXTERNAL tables are safe to drop (data on HDFS is not deleted).
DROP TABLE IF EXISTS ${INPUT_TABLE};

CREATE EXTERNAL TABLE ${INPUT_TABLE} (
    line STRING
)
STORED AS TEXTFILE
LOCATION 'file://${INPUT_DIR}'
TBLPROPERTIES (
    'skip.header.line.count' = '0',
    'serialization.null.format' = ''
);

-- Quick sanity count so the driver can see that lines were loaded
SELECT COUNT(*) AS total_lines FROM ${INPUT_TABLE};
