#!/usr/bin/env bash
# =============================================================================
# framework_compare.sh
# NASA HTTP Log ETL – Multi-Framework Comparison Script
#
# Runs MapReduce, Pig, and Hive pipelines in sequence on the same dataset,
# then queries PostgreSQL to generate side-by-side comparison text files.
#
# Prerequisites (verify before running):
#   - Hadoop is running (start-all.sh or start-dfs.sh + start-yarn.sh)
#   - NASA log files are in HDFS: /user/hadoop/nasa-logs/
#   - PostgreSQL is running with database nasa_etl
#   - Java 8+ on PATH
#   - Pig is installed (pig on PATH)
#   - Hive/beeline is installed (beeline on PATH)
#   - Maven project built: cd nasa-etl && mvn clean package -DskipTests
#
# Usage:
#   chmod +x framework_compare.sh
#   ./framework_compare.sh [--skip-mr] [--skip-pig] [--skip-hive]
#
# The --skip-* flags let you re-run only specific pipelines.
# =============================================================================

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ---- CONFIGURATION -----------------------------------------------------------
HADOOP_INPUT="/user/hadoop/nasa-logs"
LOCAL_INPUT="/Users/rakshith/Desktop/multipipeline-etl/local-input"
HADOOP_OUTPUT_BASE="/user/hadoop/nasa-output"
PIG_OUTPUT_BASE="/tmp/nasa-pig-out"
HIVE_OUTPUT_BASE="/tmp/nasa-hive-out"

DB_URL="jdbc:postgresql://localhost:5432/nasa_etl"
DB_USER="rakshith"
DB_PASS=""

# Hive URL – use embedded HiveServer2 (no host:port means embedded/local mode)
# Change to jdbc:hive2://localhost:10000 if running HiveServer2 as a separate process
HIVE_URL="jdbc:hive2://"

ETL_DIR="${SCRIPT_DIR}/nasa-etl"
JAR="${ETL_DIR}/target/nasa-etl-1.0.0-shaded.jar"
RESULTS_DIR="${ETL_DIR}/framework-results"
LOG_FILE="${SCRIPT_DIR}/framework_compare_$(date +%Y%m%d_%H%M%S).log"

SKIP_MR=false
SKIP_PIG=false
SKIP_HIVE=false

# ---- parse flags -------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-mr)   SKIP_MR=true;   shift ;;
    --skip-pig)  SKIP_PIG=true;  shift ;;
    --skip-hive) SKIP_HIVE=true; shift ;;
    *) echo "Unknown flag: $1" >&2; exit 1 ;;
  esac
done

# ---- helpers -----------------------------------------------------------------
ts() { date "+%Y-%m-%d %H:%M:%S"; }
log() { echo "[$(ts)] $*" | tee -a "$LOG_FILE"; }

mkdir -p "$RESULTS_DIR"

# ---- validate JAR exists -----------------------------------------------------
if [[ ! -f "$JAR" ]]; then
  log "JAR not found at $JAR — building now..."
  (cd "$ETL_DIR" && mvn clean package -DskipTests -q)
  log "Build complete."
fi

log "=== NASA HTTP Log ETL Framework Comparison ==="
log "JAR       : $JAR"
log "DB URL    : $DB_URL"
log "Results   : $RESULTS_DIR"
log ""

# ============================================================================
# STEP 1 — Hadoop MapReduce
# ============================================================================
if [[ "$SKIP_MR" == "false" ]]; then
  log ">>> Running Hadoop MapReduce Pipeline..."
  hadoop jar "$JAR" com.nasa.etl.ETLDriver \
    --input   "$HADOOP_INPUT"       \
    --output  "${HADOOP_OUTPUT_BASE}" \
    --db-url  "$DB_URL"             \
    --db-user "$DB_USER"            \
    --db-pass "$DB_PASS"            \
    --pipeline-name "Hadoop-MapReduce" \
    --batch   50000 \
    2>&1 | tee -a "$LOG_FILE"
  log ">>> MapReduce complete."
else
  log ">>> Skipping MapReduce (--skip-mr)."
fi

# ============================================================================
# STEP 2 — Apache Pig
# ============================================================================
if [[ "$SKIP_PIG" == "false" ]]; then
  log ">>> Running Apache Pig Pipeline (Local)..."
  # CRITICAL: pass the HDFS input path, NOT a local directory.
  # A local dir with both .gz and decompressed files causes PigStorage to
  # read each file twice → doubled counts.
  (
    cd "$ETL_DIR"
    bash scripts/run_pig.sh \
      --input        "$LOCAL_INPUT"   \
      --output       "$PIG_OUTPUT_BASE" \
      --db-url       "$DB_URL"          \
      --db-user      "$DB_USER"         \
      --db-pass      "$DB_PASS"         \
      --exec-type    local              \
      --batch        10000              \
      --pipeline-name "Apache-Pig"
  ) 2>&1 | tee -a "$LOG_FILE"
  log ">>> Pig complete."
else
  log ">>> Skipping Pig (--skip-pig)."
fi

# ============================================================================
# STEP 3 — Apache Hive
# ============================================================================
if [[ "$SKIP_HIVE" == "false" ]]; then
  log ">>> Running Apache Hive Pipeline..."

  # Clean up stale Derby metastore locks before running
  for lockdir in "${ETL_DIR}/metastore_db" "${SCRIPT_DIR}/metastore_db"; do
    if [[ -d "$lockdir" ]]; then
      find "$lockdir" -name "*.lck" -delete 2>/dev/null || true
      log "Cleaned Derby locks in $lockdir"
    fi
  done

  (
    cd "$ETL_DIR"
    bash scripts/run_hive.sh \
      --input        "$LOCAL_INPUT"      \
      --output       "$HIVE_OUTPUT_BASE" \
      --db-url       "$DB_URL"           \
      --db-user      "$DB_USER"          \
      --db-pass      "$DB_PASS"          \
      --hive-url     "$HIVE_URL"         \
      --exec-type    mr                  \
      --batch        10000               \
      --pipeline-name "Apache-Hive"
  ) 2>&1 | tee -a "$LOG_FILE"
  log ">>> Hive complete."
else
  log ">>> Skipping Hive (--skip-hive)."
fi

# ============================================================================
# STEP 4 — Generate comparison text files from DB
# ============================================================================
log ""
log ">>> Generating comparison text files..."

generate_q1() {
  psql "$DB_URL" -U "$DB_USER" -t -A -F $'\t' <<'SQL'
SELECT
  m.pipeline_name,
  q.batch_id,
  q.log_date::text,
  q.status_code,
  q.request_count,
  q.total_bytes
FROM q1_daily_traffic q
JOIN run_metadata m ON q.run_id = m.run_id
WHERE m.run_id IN (
  -- Most recent run per pipeline
  SELECT DISTINCT ON (pipeline_name) run_id
  FROM run_metadata
  WHERE pipeline_name IN ('Hadoop-MapReduce','Apache-Pig','Apache-Hive')
    AND total_records > 0
  ORDER BY pipeline_name, run_id DESC
)
ORDER BY m.pipeline_name, q.batch_id, q.log_date, q.status_code;
SQL
}

generate_q2() {
  psql "$DB_URL" -U "$DB_USER" -t -A -F $'\t' <<'SQL'
SELECT
  m.pipeline_name,
  q.batch_id,
  q.resource_path,
  q.request_count,
  q.total_bytes,
  q.distinct_host_count
FROM q2_top_resources q
JOIN run_metadata m ON q.run_id = m.run_id
WHERE m.run_id IN (
  SELECT DISTINCT ON (pipeline_name) run_id
  FROM run_metadata
  WHERE pipeline_name IN ('Hadoop-MapReduce','Apache-Pig','Apache-Hive')
    AND total_records > 0
  ORDER BY pipeline_name, run_id DESC
)
ORDER BY m.pipeline_name, q.batch_id, q.request_count DESC;
SQL
}

generate_q3() {
  psql "$DB_URL" -U "$DB_USER" -t -A -F $'\t' <<'SQL'
SELECT
  m.pipeline_name,
  q.batch_id,
  q.log_date::text,
  q.log_hour,
  q.error_request_count,
  q.total_request_count,
  ROUND(q.error_rate::numeric, 6)::text,
  q.distinct_error_hosts
FROM q3_hourly_error q
JOIN run_metadata m ON q.run_id = m.run_id
WHERE m.run_id IN (
  SELECT DISTINCT ON (pipeline_name) run_id
  FROM run_metadata
  WHERE pipeline_name IN ('Hadoop-MapReduce','Apache-Pig','Apache-Hive')
    AND total_records > 0
  ORDER BY pipeline_name, run_id DESC
)
ORDER BY m.pipeline_name, q.batch_id, q.log_date, q.log_hour;
SQL
}

get_run_id() {
  local pipeline="$1"
  psql "$DB_URL" -U "$DB_USER" -t -A <<SQL
SELECT run_id FROM run_metadata
WHERE pipeline_name = '$pipeline' AND total_records > 0
ORDER BY run_id DESC LIMIT 1;
SQL
}

MR_RUN=$(get_run_id "Hadoop-MapReduce" 2>/dev/null || echo "NONE")
PIG_RUN=$(get_run_id "Apache-Pig" 2>/dev/null || echo "NONE")
HIVE_RUN=$(get_run_id "Apache-Hive" 2>/dev/null || echo "NONE")

GENERATED_AT="$(date '+%Y-%m-%d %H:%M:%S %Z')"

write_comparison() {
  local title="$1"
  local outfile="$2"
  local header="$3"
  local query_fn="$4"

  {
    echo "$title"
    echo "Generated at: $GENERATED_AT"
    echo "Hadoop run_id: ${MR_RUN:-NONE}"
    echo "Pig run_id: ${PIG_RUN:-NONE}"
    echo "Hive run_id: ${HIVE_RUN:-NONE}"
    echo ""

    for pipeline in "Hadoop-MapReduce" "Apache-Pig" "Apache-Hive"; do
      echo "===== $pipeline ====="
      echo "$header"
      mapfile -t rows < <($query_fn | grep "^${pipeline}" || true)
      if [[ ${#rows[@]} -eq 0 ]]; then
        echo "NO_ROWS"
      else
        for row in "${rows[@]}"; do
          # Strip the leading pipeline_name column
          echo "${row#*$'\t'}"
        done
      fi
      echo ""
    done
  } > "$outfile"
  log "  Written: $outfile"
}

write_comparison \
  "Query 1 - Daily Traffic Summary" \
  "${RESULTS_DIR}/query1_framework_results.txt" \
  "batch_id	log_date	status_code	request_count	total_bytes" \
  generate_q1

write_comparison \
  "Query 2 - Top 20 Requested Resources" \
  "${RESULTS_DIR}/query2_framework_results.txt" \
  "batch_id	resource_path	request_count	total_bytes	distinct_host_count" \
  generate_q2

write_comparison \
  "Query 3 - Hourly Error Analysis" \
  "${RESULTS_DIR}/query3_framework_results.txt" \
  "batch_id	log_date	log_hour	error_request_count	total_request_count	error_rate	distinct_error_hosts" \
  generate_q3

log ""
log "=== Comparison Complete ==="
log "Results directory: $RESULTS_DIR"
log "Log file         : $LOG_FILE"
log ""
log "Quick equivalence check (Q1 line counts):"
for f in "${RESULTS_DIR}/query1_framework_results.txt"; do
  echo "  Hadoop rows : $(grep -c $'^199' "$f" || echo 0) (in Q1 file)"
done
