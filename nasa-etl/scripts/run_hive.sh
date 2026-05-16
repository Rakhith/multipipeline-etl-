#!/usr/bin/env bash
# =============================================================================
# run_hive.sh
# NASA HTTP Log ETL – Hive Pipeline Runner
#
# Convenience wrapper around HiveETLDriver that sets sensible defaults and
# validates the environment before invoking the Java driver.
#
# Usage:
#   ./run_hive.sh [OPTIONS]
#
# Options:
#   --input        <path>      Raw NASA log directory (HDFS or local) [required]
#   --output       <path>      Output base directory (HDFS or local)  [required]
#   --db-url       <jdbc-url>  JDBC URL for PostgreSQL or MySQL       [required]
#   --db-user      <user>      DB username  (default: postgres)
#   --db-pass      <pass>      DB password  (default: "")
#   --hive-jar     <path>      Path to nasa-etl fat JAR (auto-detected)
#   --exec-type    <engine>    mr | tez | spark | local  (default: mr)
#   --hive-db      <database>  Hive database name  (default: default)
#   --input-table  <table>     Hive external table name (default: nasa_raw_logs)
#   --pipeline-name <name>     Pipeline label stored in DB (default: Apache-Hive)
#   --batch        <n>         Records per logical batch (default: 10000)
#
# Examples:
#   # Local mode – quick testing
#   ./run_hive.sh \
#     --input /data/nasa-logs \
#     --output /tmp/hive-output \
#     --db-url jdbc:postgresql://localhost:5432/nasa_etl \
#     --db-pass secret \
#     --exec-type local
#
#   # YARN/MR cluster
#   ./run_hive.sh \
#     --input hdfs:///user/hadoop/nasa-logs \
#     --output hdfs:///user/hadoop/nasa-hive-output \
#     --db-url jdbc:postgresql://dbhost:5432/nasa_etl \
#     --db-user etluser \
#     --db-pass secret \
#     --exec-type mr
#
#   # Tez (faster) – requires Tez installed
#   ./run_hive.sh \
#     --input hdfs:///user/hadoop/nasa-logs \
#     --output hdfs:///user/hadoop/nasa-hive-output \
#     --db-url jdbc:postgresql://localhost:5432/nasa_etl \
#     --db-pass secret \
#     --exec-type tez
# =============================================================================

set -euo pipefail

export HIVE_HOME=/usr/local/hive
export PATH=$PATH:$HIVE_HOME/bin
export DERBY_HOME=~/db-derby-10.11.1.1-bin
export PATH=$PATH:$DERBY_HOME/bin
export CLASSPATH=$CLASSPATH:$DERBY_HOME/lib/derby.jar:$DERBY_HOME/lib/derbytools.jar

# ---- defaults ----------------------------------------------------------------
INPUT=""
OUTPUT=""
DB_URL="jdbc:postgresql://localhost:5432/nosql"
HIVE_URL="jdbc:hive2://"
DB_USER="user"
DB_PASS="password"
HIVE_JAR=""
EXEC_TYPE="mr"
HIVE_DB="default"
INPUT_TABLE="nasa_raw_logs"
PIPELINE_NAME="Apache-Hive"
BATCH_SIZE=10000

# ---- parse args --------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --input)         INPUT="$2";         shift 2 ;;
    --output)        OUTPUT="$2";        shift 2 ;;
    --db-url)        DB_URL="$2";        shift 2 ;;
    --db-user)       DB_USER="$2";       shift 2 ;;
    --db-pass)       DB_PASS="$2";       shift 2 ;;
    --hive-jar)      HIVE_JAR="$2";      shift 2 ;;
    --hive-url)  HIVE_URL="$2"; shift 2 ;;
    --exec-type)     EXEC_TYPE="$2";     shift 2 ;;
    --hive-db)       HIVE_DB="$2";       shift 2 ;;
    --input-table)   INPUT_TABLE="$2";   shift 2 ;;
    --pipeline-name) PIPELINE_NAME="$2"; shift 2 ;;
    --batch)         BATCH_SIZE="$2";    shift 2 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

# ---- validate required args --------------------------------------------------
if [[ -z "$INPUT" || -z "$OUTPUT" || -z "$DB_URL" ]]; then
  echo "ERROR: --input, --output, and --db-url are required." >&2
  echo ""
  grep "^# Usage" "$0" -A 60 | head -60
  exit 1
fi

# ---- auto-detect fat JAR if not specified ------------------------------------
if [[ -z "$HIVE_JAR" ]]; then
  # Look for the fat jar produced by Maven Shade in common locations
  CANDIDATES=(
    "target/nasa-etl-*.jar"
    "../target/nasa-etl-*.jar"
    "nasa-etl.jar"
    "../nasa-etl.jar"
  )
  for pattern in "${CANDIDATES[@]}"; do
    match=$(ls $pattern 2>/dev/null | head -1 || true)
    if [[ -n "$match" ]]; then
      HIVE_JAR="$match"
      break
    fi
  done

  if [[ -z "$HIVE_JAR" ]]; then
    echo "ERROR: Could not find nasa-etl fat JAR. " >&2
    echo "  Build with: mvn -pl hive package -DskipTests" >&2
    echo "  Or specify: --hive-jar /path/to/nasa-etl.jar" >&2
    exit 1
  fi
  echo "[run_hive.sh] Auto-detected JAR: $HIVE_JAR"
fi

# ---- validate environment ----------------------------------------------------
if ! command -v hive &>/dev/null; then
  echo "ERROR: 'hive' command not found. Is Hive on PATH?" >&2
  echo "  export HIVE_HOME=/path/to/hive && export PATH=\$PATH:\$HIVE_HOME/bin" >&2
  exit 1
fi

if ! command -v java &>/dev/null; then
  echo "ERROR: 'java' command not found." >&2
  exit 1
fi

echo "========================================"
echo " NASA HTTP Log ETL – Hive Pipeline"
echo "========================================"
echo " Input        : $INPUT"
echo " Output base  : $OUTPUT"
echo " DB URL       : $DB_URL"
echo " Exec type    : $EXEC_TYPE"
echo " Hive DB      : $HIVE_DB"
echo " Input table  : $INPUT_TABLE"
echo " Batch size   : $BATCH_SIZE"
echo " JAR          : $HIVE_JAR"
echo "========================================"
echo ""

# ---- run the Java driver ------------------------------------------------------
exec java -cp "$HIVE_JAR" \
  com.nasa.etl.HiveETLDriver \
  --input        "$INPUT"        \
  --output       "$OUTPUT"       \
  --db-url       "$DB_URL"       \
  --db-user      "$DB_USER"      \
  --db-pass      "$DB_PASS"      \
  --hive-jar     "$HIVE_JAR"     \
  --exec-type    "$EXEC_TYPE"    \
  --hive-db      "$HIVE_DB"      \
  --hive-url   "$HIVE_URL" \
  --input-table  "$INPUT_TABLE"  \
  --pipeline-name "$PIPELINE_NAME" \
  --batch        "$BATCH_SIZE"