#!/usr/bin/env bash
# =============================================================================
# DAS 839 – NoSQL Systems | Multi-Pipeline ETL & Reporting Framework
# =============================================================================
# Usage: ./etl_cli.sh [--pipeline <pig|mapreduce|mongodb|hive|all>]
#                     [--query <1|2|3|0>]
#                     [--report <runId>]
#        or just run ./etl_cli.sh for interactive mode
# =============================================================================

set -euo pipefail

# ── Colour codes ──────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

# ── Defaults ──────────────────────────────────────────────────────────────────
PIPELINE=""
QUERY=""
REPORT_RUN_ID=""

LOG_DIR="./logs"
RESULTS_DIR="./results"
mkdir -p "$LOG_DIR" "$RESULTS_DIR"

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

print_banner() {
  echo -e "${CYAN}${BOLD}"
  echo "╔══════════════════════════════════════════════════════╗"
  echo "║   DAS 839 – Multi-Pipeline ETL & Reporting CLI       ║"
  echo "║   Pipelines: Pig | MapReduce | MongoDB | Hive        ║"
  echo "╚══════════════════════════════════════════════════════╝"
  echo -e "${NC}"
}

print_section() { echo -e "\n${YELLOW}${BOLD}▶ $1${NC}"; }
print_ok()      { echo -e "  ${GREEN}✔ $1${NC}"; }
print_err()     { echo -e "  ${RED}✗  $1${NC}"; }
print_info()    { echo -e "  ${CYAN}ℹ  $1${NC}"; }

timestamp()     { date '+%Y-%m-%d %H:%M:%S'; }

# =============================================================================
# ARGUMENT PARSING  (supports both interactive and flag-based modes)
# =============================================================================

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --pipeline)   PIPELINE="$2";        shift 2 ;;
      --query)      QUERY="$2";           shift 2 ;;
      --report)     REPORT_RUN_ID="$2";   shift 2 ;;
      -h|--help)    usage; exit 0 ;;
      *) echo -e "${RED}Unknown option: $1${NC}"; usage; exit 1 ;;
    esac
  done
}

usage() {
  echo ""
  echo "Usage: $0 [options]"
  echo ""
  echo "  --pipeline  <pig|mapreduce|mongodb|hive|all>"
  echo "  --query     <1|2|3|0>   (0 = all queries)"
  echo "  --report    <runId>     View report for a specific run ID"
  echo ""
  echo "  Run without flags for interactive (menu-driven) mode."
  echo ""
}

# =============================================================================
# INTERACTIVE MENUS
# =============================================================================

select_mode() {
  print_section "What would you like to do?"
  echo "  1) Generate  – Run an ETL pipeline"
  echo "  2) Report    – View report for a run ID"
  echo ""
  read -rp "  Enter choice [1-2]: " choice
  case "$choice" in
    1) MODE="generate" ;;
    2) MODE="report"   ;;
    *) print_err "Invalid choice."; exit 1 ;;
  esac
  print_ok "Mode set to: ${BOLD}$MODE${NC}"
}

select_pipeline() {
  print_section "Select Execution Pipeline"
  echo "  1) Pig"
  echo "  2) MapReduce"
  echo "  3) MongoDB"
  echo "  4) Hive"
  echo "  5) All pipelines"
  echo ""
  read -rp "  Enter choice [1-5]: " choice
  case "$choice" in
    1) PIPELINE="pig"        ;;
    2) PIPELINE="mapreduce"  ;;
    3) PIPELINE="mongodb"    ;;
    4) PIPELINE="hive"       ;;
    5) PIPELINE="all"        ;;
    *) print_err "Invalid choice."; exit 1 ;;
  esac
  print_ok "Pipeline set to: ${BOLD}$PIPELINE${NC}"
}

select_query() {
  print_section "Select Query"
  echo "  1) Query 1"
  echo "  2) Query 2"
  echo "  3) Query 3"
  echo "  0) All queries"
  echo ""
  read -rp "  Enter choice [0-3]: " choice
  case "$choice" in
    1) QUERY="1" ;;
    2) QUERY="2" ;;
    3) QUERY="3" ;;
    0) QUERY="0" ;;
    *) print_err "Invalid choice."; exit 1 ;;
  esac
  print_ok "Query set to: ${BOLD}$QUERY${NC}"
}

select_report_run_id() {
  print_section "View Report"
  read -rp "  Enter Run ID: " REPORT_RUN_ID
  [[ -z "$REPORT_RUN_ID" ]] && { print_err "Run ID cannot be empty."; exit 1; }
  print_ok "Run ID set to: ${BOLD}$REPORT_RUN_ID${NC}"
}

# =============================================================================
# REPORT
# =============================================================================

run_report() {
  local run_id="$1"
  print_section "Generating Report for Run ID: $run_id"
  java -cp target/nasa-etl-1.0.0-shaded.jar com.nasa.etl.report.Reporter \
    jdbc:postgresql://localhost:5432/nosql user password "$run_id" > mongo.txt
  print_ok "Report written to mongo.txt"
}

# =============================================================================
# CORE EXECUTION DISPATCH
# =============================================================================

run_pig_query() {
  local query="$1"
  print_info "[Pig] Running Query $query"
  ./scripts/run_pig.sh --query "$query"
}

run_mapreduce_query() {
  local query="$1"
  print_info "[MapReduce] Running Query $query"
  hadoop jar target/nasa-etl-1.0.0-shaded.jar \
    --input /user/hadoop/nasa-logs/ \
    --output /user/hadoop/nasa-output \
    --db-url jdbc:postgresql://localhost:5432/nosql \
    --db-user user --db-pass password \
    --batch 10000 --query "$query"
}

run_mongodb_query() {
  local query="$1"
  print_info "[MongoDB] Running Query $query"
  java -cp target/nasa-etl-1.0.0-shaded.jar com.nasa.etl.MongoETLDriver \
    --input input/ \
    --mongo-uri mongodb://localhost:27018 \
    --database nasa_etl --collection logs \
    --db-url jdbc:postgresql://localhost:5432/nosql \
    --db-user user --db-pass password \
    --drop --query "$query"
}

run_hive_query() {
  local query="$1"
  print_info "[Hive] Running Query $query"
  ./scripts/run_hive.sh \
    --input hdfs:///user/hadoop/nasa-logs \
    --output hdfs:///user/hadoop/nasa-hive-output \
    --db-url jdbc:postgresql://localhost:5432/nosql \
    --query "$query"
}

dispatch_pipeline() {
  local pipeline="$1" query="$2"

  local pipelines=()
  local queries=()

  [[ "$pipeline" == "all" ]] && pipelines=(pig mapreduce mongodb hive) || pipelines=("$pipeline")
  [[ "$query"    == "0"   ]] && queries=(1 2 3)                        || queries=("$query")

  for p in "${pipelines[@]}"; do
    for q in "${queries[@]}"; do
      local start_ts; start_ts=$(date +%s)
      echo ""
      echo -e "  ${BOLD}[$(timestamp)] Pipeline=$p | Query=$q${NC}"

      case "$p" in
        pig)        run_pig_query       "$q" ;;
        mapreduce)  run_mapreduce_query "$q" ;;
        mongodb)    run_mongodb_query   "$q" ;;
        hive)       run_hive_query      "$q" ;;
      esac

      local end_ts; end_ts=$(date +%s)
      local runtime=$(( end_ts - start_ts ))
      print_ok "Done in ${runtime}s"
    done
  done
}

# =============================================================================
# MAIN
# =============================================================================

main() {
  print_banner
  parse_args "$@"

  # ── Flag-based shortcut: --report skips everything else ───────────────────
  if [[ -n "$REPORT_RUN_ID" ]]; then
    run_report "$REPORT_RUN_ID"
    exit 0
  fi

  # ── Interactive mode: ask what to do first ────────────────────────────────
  if [[ -z "$PIPELINE" && -z "$QUERY" ]]; then
    MODE=""
    select_mode

    if [[ "$MODE" == "report" ]]; then
      select_report_run_id
      run_report "$REPORT_RUN_ID"
      exit 0
    fi
  fi

  # ── ETL generate path ─────────────────────────────────────────────────────
  [[ -z "$PIPELINE" ]] && select_pipeline
  [[ -z "$QUERY"    ]] && select_query

  print_section "Execution Summary"
  echo "  Pipeline : $PIPELINE"
  echo "  Query    : $QUERY"
  echo ""
  read -rp "  Proceed? [Y/n]: " confirm
  [[ "${confirm,,}" == "n" ]] && { echo "Aborted."; exit 0; }

  print_section "Starting Execution"
  dispatch_pipeline "$PIPELINE" "$QUERY"

  echo ""
  print_ok "All done. Check $RESULTS_DIR/ for reports and $LOG_DIR/ for logs."
}

main "$@"