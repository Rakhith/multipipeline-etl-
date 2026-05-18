#!/usr/bin/env bash
# =============================================================================
# run_pig.sh
# NASA HTTP Log ETL – Apache Pig Pipeline launcher
#
# Usage:
#   ./run_pig.sh [options]
#
# Options (all have defaults; override as needed):
#   --input        PATH      Path to raw NASA log file(s)  [required]
#   --output       PATH      Output base directory          [default: /tmp/nasa-pig-out]
#   --db-url       URL       JDBC URL                       [default: jdbc:postgresql://localhost:5432/nasa_etl]
#   --db-user      USER      DB username                    [default: postgres]
#   --db-pass      PASS      DB password                    [default: ""]
#   --pig-jar      PATH      Fat JAR path                   [auto-detected]
#   --exec-type    TYPE      local | mapreduce               [default: local]
#   --batch        N         Records per batch               [default: 10000]
#   --pipeline-name NAME     Label stored in run_metadata   [default: Apache-Pig]
#
# Examples:
#   # Local mode (single machine / pseudo-distributed)
#   ./run_pig.sh --input /data/nasa-logs --db-pass secret
#
#   # YARN cluster mode
#   ./run_pig.sh \
#     --input hdfs:///user/hadoop/nasa-logs \
#     --output hdfs:///user/hadoop/nasa-pig-out \
#     --db-url jdbc:postgresql://db-host:5432/nasa_etl \
#     --db-user etl_user --db-pass secret \
#     --exec-type mapreduce --batch 50000
# =============================================================================
# CAUTION::: cp /home/mrtechtroid/pig/lib/spark/commons-collections-3.2.2.jar \
# $HADOOP_HOME/share/hadoop/common/lib/ Do this......
# Use Hadoop 3.4.3 (or later) and Pig 0.18.0 (or later)
set -euo pipefail

# ---- defaults ----
INPUT="/user/hadoop/nasa-logs"
OUTPUT="/user/hadoop/nasa-output-pig-6"
DB_URL="jdbc:postgresql://localhost:5432/nosql"
DB_USER="user"
DB_PASS="password"
EXEC_TYPE="mr"
BATCH_SIZE=10000
PIPELINE_NAME="Apache-Pig"
QUERY_ARGS=()
export PIG_HOME=$HOME/pig
export PATH=$PATH:$PIG_HOME/bin
export PIG_CLASSPATH=$HADOOP_HOME/etc/hadoop
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
echo $(pwd)
# Auto-detect fat JAR in target/ or current directory
PIG_JAR=$(find . -name "nasa-etl-*-shaded.jar" 2>/dev/null | head -1 || true)
if [ -z "$PIG_JAR" ]; then
    PIG_JAR="nasa-etl.jar"
fi

# ---- parse args ----
while [[ $# -gt 0 ]]; do
    case "$1" in
        --input)         INPUT="$2";         shift 2 ;;
        --output)        OUTPUT="$2";        shift 2 ;;
        --db-url)        DB_URL="$2";        shift 2 ;;
        --db-user)       DB_USER="$2";       shift 2 ;;
        --db-pass)       DB_PASS="$2";       shift 2 ;;
        --pig-jar)       PIG_JAR="$2";       shift 2 ;;
        --exec-type)     EXEC_TYPE="$2";     shift 2 ;;
        --batch)         BATCH_SIZE="$2";    shift 2 ;;
        --pipeline-name) PIPELINE_NAME="$2"; shift 2 ;;
        --query)         QUERY_ARGS=("--query" "$2"); shift 2 ;;
        *) echo "Unknown argument: $1" >&2; exit 1 ;;
    esac
done

# ---- validate ----
if [ -z "$INPUT" ]; then
    echo "ERROR: --input is required" >&2
    exit 1
fi
if [ ! -f "$PIG_JAR" ]; then
    echo "ERROR: JAR not found at '$PIG_JAR'. Build first with: mvn package -DskipTests" >&2
    exit 1
fi

echo "========================================"
echo " NASA HTTP Log ETL – Apache Pig"
echo "========================================"
echo " Input       : $INPUT"
echo " Output base : $OUTPUT"
echo " DB URL      : $DB_URL"
echo " Exec type   : $EXEC_TYPE"
echo " Batch size  : $BATCH_SIZE"
echo " Pipeline    : $PIPELINE_NAME"
echo " JAR         : $PIG_JAR"
echo "========================================"

# ---- run the Java driver (which invokes Pig internally) ----
java -cp "$PIG_JAR" com.nasa.etl.PigETLDriver \
    --input        "$INPUT"        \
    --output       "$OUTPUT"       \
    --db-url       "$DB_URL"       \
    --db-user      "$DB_USER"      \
    --db-pass      "$DB_PASS"      \
    --pig-jar      "$PIG_JAR"      \
    --exec-type    "$EXEC_TYPE"    \
    --batch        "$BATCH_SIZE"   \
    --pipeline-name "$PIPELINE_NAME"
    "${QUERY_ARGS[@]}"