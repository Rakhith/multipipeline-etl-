#!/usr/bin/env bash
# =============================================================================
# run_mongo.sh
# NASA HTTP Log ETL - MongoDB Pipeline launcher
#
# Usage:
#   ./run_mongo.sh [options]
#
# Options (all have defaults; override as needed):
#   --input          PATH    Local raw NASA log file or directory [required]
#   --mongo-uri      URI     MongoDB URI                          [default: mongodb://localhost:27017]
#   --database       NAME    Mongo database name                  [default: nasa_etl]
#   --collection     NAME    Mongo collection name                [default: logs]
#   --db-url         URL     JDBC URL                             [default: jdbc:postgresql://localhost:5432/nasa_etl]
#   --db-user        USER    DB username                          [default: postgres]
#   --db-pass        PASS    DB password                          [default: ""]
#   --mongo-jar      PATH    Fat JAR path                         [auto-detected]
#   --pipeline-name  NAME    Label stored in run_metadata         [default: MongoDB-MapReduce]
#   --drop                   Drop Mongo collection before ingestion
#
# Notes:
#   Mongo logical batching is week-based. There is no --batch option here.
# =============================================================================
set -euo pipefail

# ---- defaults ----
INPUT=""
MONGO_URI="mongodb://localhost:27017"
DATABASE="nasa_etl"
COLLECTION="logs"
DB_URL="jdbc:postgresql://localhost:5432/nasa_etl"
DB_USER="postgres"
DB_PASS=""
PIPELINE_NAME="MongoDB-MapReduce"
DROP=false

# Auto-detect fat JAR in target/ or current directory
MONGO_JAR=$(find . -name "nasa-etl-*-shaded.jar" 2>/dev/null | head -1 || true)
if [ -z "$MONGO_JAR" ]; then
    MONGO_JAR="nasa-etl.jar"
fi

# ---- parse args ----
while [[ $# -gt 0 ]]; do
    case "$1" in
        --input)         INPUT="$2";         shift 2 ;;
        --mongo-uri)     MONGO_URI="$2";     shift 2 ;;
        --database)      DATABASE="$2";      shift 2 ;;
        --collection)    COLLECTION="$2";    shift 2 ;;
        --db-url)        DB_URL="$2";        shift 2 ;;
        --db-user)       DB_USER="$2";       shift 2 ;;
        --db-pass)       DB_PASS="$2";       shift 2 ;;
        --mongo-jar)     MONGO_JAR="$2";     shift 2 ;;
        --pipeline-name) PIPELINE_NAME="$2"; shift 2 ;;
        --drop)          DROP=true;          shift ;;
        *) echo "Unknown argument: $1" >&2; exit 1 ;;
    esac
done

# ---- validate ----
if [ -z "$INPUT" ]; then
    echo "ERROR: --input is required" >&2
    exit 1
fi
if [ ! -f "$MONGO_JAR" ]; then
    echo "ERROR: JAR not found at '$MONGO_JAR'. Build first with: mvn package -DskipTests" >&2
    exit 1
fi

echo "========================================"
echo " NASA HTTP Log ETL - MongoDB"
echo "========================================"
echo " Input      : $INPUT"
echo " Mongo URI  : $MONGO_URI"
echo " Database   : $DATABASE"
echo " Collection : $COLLECTION"
echo " DB URL     : $DB_URL"
echo " DB User    : $DB_USER"
echo " Pipeline   : $PIPELINE_NAME"
echo " Drop first : $DROP"
echo " JAR        : $MONGO_JAR"
echo "========================================"

CMD=(
    java -cp "$MONGO_JAR" com.nasa.etl.MongoETLDriver
    --input "$INPUT"
    --mongo-uri "$MONGO_URI"
    --database "$DATABASE"
    --collection "$COLLECTION"
    --db-url "$DB_URL"
    --db-user "$DB_USER"
    --db-pass "$DB_PASS"
    --pipeline-name "$PIPELINE_NAME"
)

if [ "$DROP" = true ]; then
    CMD+=(--drop)
fi

"${CMD[@]}"
