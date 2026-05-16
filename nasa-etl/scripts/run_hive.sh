#!/usr/bin/env bash
# =============================================================================
# run_hive.sh
# NASA HTTP Log ETL – Hive Pipeline Runner
#
# Convenience wrapper around HiveETLDriver that sets sensible defaults and
# validates the environment before invoking the Java driver.
#
# The Java driver invokes `beeline` (NOT `hive`) as a subprocess, so only
# beeline needs to be on PATH.  When using embedded HiveServer2 (local mode),
# use --hive-url "jdbc:hive2://" (two slashes, no host:port).
#
# Usage:
#   ./run_hive.sh [OPTIONS]
#
# Options:
#   --input        <path>      Raw NASA log directory (HDFS or local) [required]
#   --output       <path>      Output base directory (HDFS or local)  [required]
#   --db-url       <jdbc-url>  JDBC URL for PostgreSQL                [required]
#   --db-user      <user>      DB username  (default: rakshith)
#   --db-pass      <pass>      DB password  (default: "")
#   --hive-jar     <path>      Path to nasa-etl fat JAR (auto-detected)
#   --hive-url     <url>       Beeline JDBC URL (default: jdbc:hive2://)
#   --exec-type    <engine>    mr | tez | spark | local  (default: mr)
#   --hive-db      <database>  Hive database name  (default: default)
#   --input-table  <table>     Hive external table name (default: nasa_raw_logs)
#   --hive-user    <user>      Hive username (default: current OS user)
#   --hive-pass    <pass>      Hive password (default: empty)
#   --pipeline-name <name>     Pipeline label stored in DB (default: Apache-Hive)
#   --batch        <n>         Records per logical batch (default: 10000)
#
# Examples:
#   # Local embedded HiveServer2 (no running HiveServer2 needed)
#   ./run_hive.sh \
#     --input /user/hadoop/nasa-logs \
#     --output /tmp/hive-output \
#     --db-url jdbc:postgresql://localhost:5432/nasa_etl \
#     --hive-url "jdbc:hive2://" \
#     --exec-type local
#
#   # Running HiveServer2 on localhost:10000
#   ./run_hive.sh \
#     --input hdfs:///user/hadoop/nasa-logs \
#     --output hdfs:///user/hadoop/nasa-hive-output \
#     --db-url jdbc:postgresql://localhost:5432/nasa_etl \
#     --hive-url jdbc:hive2://localhost:10000 \
#     --exec-type mr
# =============================================================================

set -euo pipefail

# ---- Hive / Derby environment -----------------------------------------------
export HIVE_HOME="${HIVE_HOME:-/usr/local/hive}"
export PATH="$PATH:$HIVE_HOME/bin"

# Safe CLASSPATH update (handle unset CLASSPATH)
if [[ -n "${DERBY_HOME:-}" ]]; then
    export CLASSPATH="${CLASSPATH:+${CLASSPATH}:}${DERBY_HOME}/lib/derby.jar:${DERBY_HOME}/lib/derbytools.jar"
fi

# ---- defaults ----------------------------------------------------------------
INPUT=""
OUTPUT=""
DB_URL="jdbc:postgresql://localhost:5432/nasa_etl"
DB_USER="rakshith"
DB_PASS=""
HIVE_JAR=""
HIVE_URL="jdbc:hive2://"          # embedded HiveServer2 (no host:port needed)
EXEC_TYPE="mr"
HIVE_DB="default"
INPUT_TABLE="nasa_raw_logs"
HIVE_USER="${USER:-}"
HIVE_PASS=""
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
    --hive-url)      HIVE_URL="$2";      shift 2 ;;
    --exec-type)     EXEC_TYPE="$2";     shift 2 ;;
    --hive-db)       HIVE_DB="$2";       shift 2 ;;
    --input-table)   INPUT_TABLE="$2";   shift 2 ;;
    --hive-user)     HIVE_USER="$2";     shift 2 ;;
    --hive-pass)     HIVE_PASS="$2";     shift 2 ;;
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
  CANDIDATES=(
    "target/nasa-etl-*-shaded.jar"
    "../target/nasa-etl-*-shaded.jar"
    "nasa-etl/target/nasa-etl-*-shaded.jar"
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
    echo "ERROR: Could not find nasa-etl fat JAR." >&2
    echo "  Build with: cd nasa-etl && mvn clean package -DskipTests" >&2
    echo "  Or specify: --hive-jar /path/to/nasa-etl-shaded.jar" >&2
    exit 1
  fi
  echo "[run_hive.sh] Auto-detected JAR: $HIVE_JAR"
fi

# ---- validate environment ----------------------------------------------------
if ! command -v beeline &>/dev/null; then
  echo "ERROR: 'beeline' command not found. Is Hive on PATH?" >&2
  echo "  export HIVE_HOME=/path/to/hive && export PATH=\$PATH:\$HIVE_HOME/bin" >&2
  exit 1
fi

if ! command -v java &>/dev/null; then
  echo "ERROR: 'java' command not found." >&2
  exit 1
fi

# ---- clean up stale Derby metastore locks (prevents lock contention) ---------
# Derby locks become stale when a previous run crashed; remove them safely.
for derby_dir in metastore_db ../metastore_db; do
  if [[ -d "$derby_dir" ]]; then
    lock_file="$derby_dir/dbex.lck"
    service_file="$derby_dir/service.properties"
    if [[ -f "$lock_file" ]]; then
      echo "[run_hive.sh] Removing stale Derby lock: $lock_file"
      rm -f "$lock_file"
    fi
    # Also remove db-level lock files
    find "$derby_dir" -name "*.lck" -delete 2>/dev/null || true
  fi
done

echo "========================================"
echo " NASA HTTP Log ETL – Hive Pipeline"
echo "========================================"
echo " Input        : $INPUT"
echo " Output base  : $OUTPUT"
echo " DB URL       : $DB_URL"
echo " Hive URL     : $HIVE_URL"
echo " Exec type    : $EXEC_TYPE"
echo " Hive DB      : $HIVE_DB"
echo " Hive User    : $HIVE_USER"
echo " Input table  : $INPUT_TABLE"
echo " Batch size   : $BATCH_SIZE"
echo " JAR          : $HIVE_JAR"
echo "========================================"
echo ""

# ---- run the Java driver -----------------------------------------------------
exec java \
  -Dderby.system.home="$(pwd)/derby_home" \
  -cp "$HIVE_JAR" \
  com.nasa.etl.HiveETLDriver \
  --input        "$INPUT"        \
  --output       "$OUTPUT"       \
  --db-url       "$DB_URL"       \
  --db-user      "$DB_USER"      \
  --db-pass      "$DB_PASS"      \
  --hive-jar     "$HIVE_JAR"     \
  --hive-url     "$HIVE_URL"     \
  --exec-type    "$EXEC_TYPE"    \
  --hive-db      "$HIVE_DB"      \
  --hive-user    "$HIVE_USER"    \
  --hive-pass    "$HIVE_PASS"    \
  --input-table  "$INPUT_TABLE"  \
  --pipeline-name "$PIPELINE_NAME" \
  --batch        "$BATCH_SIZE"
