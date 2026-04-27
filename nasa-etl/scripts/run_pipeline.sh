#!/usr/bin/env bash
# =============================================================================
# run_pipeline.sh
# Complete end-to-end execution script for the NASA HTTP Log ETL pipeline
# using Hadoop MapReduce.
#
# Prerequisites:
#   - Hadoop 3.x installed and HADOOP_HOME set
#   - Maven 3.x installed
#   - PostgreSQL (or MySQL) running with a database created
#   - Datasets downloaded and decompressed
#
# Usage:
#   chmod +x run_pipeline.sh
#   ./run_pipeline.sh
# =============================================================================

set -e  # Exit on any error

# ---- CONFIGURATION (edit these) ----

HDFS_INPUT=/user/hadoop/nasa-logs
HDFS_OUTPUT=/user/hadoop/nasa-output
BATCH_SIZE=50000

# Database (PostgreSQL example)
DB_URL="jdbc:postgresql://localhost:5432/nasa_etl"
DB_USER="postgres"
DB_PASS="your_password_here"

# Paths to downloaded log files (after decompression)
LOG_FILE_JUL="/tmp/NASA_access_log_Jul95"
LOG_FILE_AUG="/tmp/NASA_access_log_Aug95"

# ---- END CONFIGURATION ----

echo "=============================================="
echo " NASA HTTP Log ETL – Hadoop MapReduce"
echo "=============================================="

# 1. Download and decompress datasets (if not already done)
echo ""
echo "[STEP 1] Downloading NASA log files..."
if [ ! -f "${LOG_FILE_JUL}" ]; then
    wget -O /tmp/NASA_access_log_Jul95.gz \
        https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
    gunzip /tmp/NASA_access_log_Jul95.gz
    echo "  Jul95 decompressed."
else
    echo "  Jul95 already present, skipping."
fi

if [ ! -f "${LOG_FILE_AUG}" ]; then
    wget -O /tmp/NASA_access_log_Aug95.gz \
        https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz
    gunzip /tmp/NASA_access_log_Aug95.gz
    echo "  Aug95 decompressed."
else
    echo "  Aug95 already present, skipping."
fi

# 2. Upload raw logs to HDFS
echo ""
echo "[STEP 2] Uploading raw log files to HDFS..."
hdfs dfs -mkdir -p ${HDFS_INPUT}
hdfs dfs -put -f ${LOG_FILE_JUL} ${HDFS_INPUT}/
hdfs dfs -put -f ${LOG_FILE_AUG} ${HDFS_INPUT}/
echo "  Files uploaded to ${HDFS_INPUT}"
hdfs dfs -ls ${HDFS_INPUT}

# 3. Build the fat JAR
echo ""
echo "[STEP 3] Building nasa-etl-shaded.jar with Maven..."
cd "$(dirname "$0")/.."
mvn clean package -q
JAR=$(ls target/nasa-etl-*-shaded.jar | head -1)
echo "  Built: ${JAR}"

# 4. Create the database and schema
echo ""
echo "[STEP 4] Initialising database schema..."
psql "${DB_URL}" -U "${DB_USER}" -f scripts/schema.sql || true
# (the Java code also creates tables on startup via CREATE TABLE IF NOT EXISTS)

# 5. Run the ETL pipeline
echo ""
echo "[STEP 5] Running ETL pipeline..."
echo "  Input      : ${HDFS_INPUT}"
echo "  Output     : ${HDFS_OUTPUT}"
echo "  Batch size : ${BATCH_SIZE}"
echo ""

hadoop jar "${JAR}" com.nasa.etl.ETLDriver \
    --input   ${HDFS_INPUT}  \
    --output  ${HDFS_OUTPUT} \
    --db-url  "${DB_URL}"    \
    --db-user "${DB_USER}"   \
    --db-pass "${DB_PASS}"   \
    --batch   ${BATCH_SIZE}

# 6. Print the report
echo ""
echo "[STEP 6] Generating report from database..."
# The reporter prints the most recent run automatically
java -cp "${JAR}" com.nasa.etl.report.Reporter \
    "${DB_URL}" "${DB_USER}" "${DB_PASS}"

echo ""
echo "=============================================="
echo " Pipeline complete."
echo "=============================================="
