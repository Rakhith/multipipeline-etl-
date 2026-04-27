# NASA HTTP Log ETL – Hadoop MapReduce Pipeline
## DAS 839 – NoSQL Systems | End Semester Project

---

## Project Overview

This project implements a **multi-query ETL and reporting framework** for NASA
HTTP web server logs using **Hadoop MapReduce**. It:

1. Reads raw NASA log files directly from HDFS (no preprocessing).
2. Parses and validates every record, counting malformed lines.
3. Executes three analytical queries as separate MapReduce jobs.
4. Loads aggregated results into PostgreSQL (or MySQL) via JDBC.
5. Provides a standalone Reporter that reads from the DB and prints a
   formatted report with execution metadata.

---

## Project Structure

```
nasa-etl/
├── pom.xml                                    ← Maven build (fat JAR)
├── scripts/
│   ├── schema.sql                             ← DDL for all tables
│   └── run_pipeline.sh                        ← End-to-end run script
└── src/main/java/com/nasa/etl/
    ├── ETLDriver.java                         ← Main entry point
    ├── common/
    │   ├── LogRecord.java                     ← Parsed record POJO + parser
    │   ├── ETLCounters.java                   ← Shared Hadoop counters
    │   ├── RunMetadata.java                   ← Runtime metadata POJO
    │   └── BatchedLineInputFormat.java        ← Custom InputFormat
    ├── mapreduce/
    │   ├── Query1DailyTraffic.java            ← Q1 Mapper + Reducer + Job
    │   ├── Query2TopResources.java            ← Q2 Mapper + Reducer + Job
    │   └── Query3HourlyError.java             ← Q3 Mapper + Reducer + Job
    ├── loader/
    │   └── DBLoader.java                      ← HDFS→JDBC loader
    └── report/
        └── Reporter.java                      ← Standalone report printer
```

---

## Quick Start

### Prerequisites

| Tool | Version |
|------|---------|
| JDK | 8 or 11 |
| Maven | 3.6+ |
| Hadoop | 3.3.x (pseudo-distributed or cluster) |
| PostgreSQL | 12+ (or MySQL 8+) |

### Step 1 – Create the database

```sql
-- PostgreSQL
CREATE DATABASE nasa_etl;
```

### Step 2 – Download and decompress the datasets

```bash
wget https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
wget https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz
gunzip NASA_access_log_Jul95.gz
gunzip NASA_access_log_Aug95.gz
```

### Step 3 – Upload raw logs to HDFS

```bash
hdfs dfs -mkdir -p /user/hadoop/nasa-logs
hdfs dfs -put NASA_access_log_Jul95 /user/hadoop/nasa-logs/
hdfs dfs -put NASA_access_log_Aug95 /user/hadoop/nasa-logs/
```

### Step 4 – Build the fat JAR

```bash
mvn clean package
# Output: target/nasa-etl-1.0.0-shaded.jar
```

### Step 5 – Run the ETL pipeline

```bash
hadoop jar target/nasa-etl-1.0.0-shaded.jar com.nasa.etl.ETLDriver \
  --input   /user/hadoop/nasa-logs          \
  --output  /user/hadoop/nasa-output        \
  --db-url  jdbc:postgresql://localhost:5432/nasa_etl \
  --db-user postgres                        \
  --db-pass your_password                   \
  --batch   50000
```

#### For MySQL, change the `--db-url`:

```bash
--db-url jdbc:mysql://localhost:3306/nasa_etl?allowPublicKeyRetrieval=true\&useSSL=false
```

### Step 6 – View the report

```bash
# Shows the most recent run automatically
java -cp target/nasa-etl-1.0.0-shaded.jar com.nasa.etl.report.Reporter \
  jdbc:postgresql://localhost:5432/nasa_etl postgres your_password

# Show a specific run by UUID
java -cp target/nasa-etl-1.0.0-shaded.jar com.nasa.etl.report.Reporter \
  jdbc:postgresql://localhost:5432/nasa_etl postgres your_password \
  <run-uuid>
```

---

## Query Definitions

### Query 1 – Daily Traffic Summary
```
Table: q1_daily_traffic
Columns: log_date | status_code | request_count | total_bytes
Key: (log_date, status_code)
```

### Query 2 – Top 20 Requested Resources
```
Table: q2_top_resources
Columns: resource_path | request_count | total_bytes | distinct_host_count
Sorted by request_count DESC, limited to 20 rows
```

### Query 3 – Hourly Error Analysis
```
Table: q3_hourly_error
Columns: log_date | log_hour | error_request_count | total_request_count
       | error_rate | distinct_error_hosts
Error defined as: status_code BETWEEN 400 AND 599
```

---

## Batching Semantics

- **Batch size** = number of input log lines processed before a counter is
  incremented in the Mapper (configured via `--batch`).
- **Batch IDs** start at 1 and increase sequentially per Mapper task.
- **Average batch size** = `total_valid_records / total_batches_processed`
  (reported per query in `run_metadata`).
- The final partial batch is counted as a valid batch.
- Runtime is measured from the first HDFS read to the last DB write (download
  and report rendering time are excluded).

---

## Database Tables

| Table | Purpose |
|-------|---------|
| `run_metadata` | One row per query per run (pipeline, runtime, batch stats) |
| `q1_daily_traffic` | Q1 aggregated results |
| `q2_top_resources` | Q2 top-20 resource results |
| `q3_hourly_error` | Q3 per-hour error analysis |

All result tables include `run_id`, `pipeline_name`, and `batch_id` for
traceability and fair cross-pipeline comparison.

---

## Design Notes

- **No preprocessing**: `LogRecord.parse()` handles raw log lines directly.
  Missing/dash bytes are treated as 0. Malformed records are counted via
  Hadoop counters and never silently dropped.
- **Single pass per query**: Each query is one MapReduce job reading the same
  raw HDFS input.
- **Q2 uses a single reducer** to compute the global top-20 across all
  resource paths. All paths fit in the reducer heap (≈50 K distinct paths).
- **JDBC loading** happens from the Driver JVM after each job completes,
  reading the HDFS `part-r-*` output files via the Hadoop FileSystem API.
- **Reporter is standalone**: runs anywhere with the fat JAR and JDBC access
  to the database — no Hadoop needed at report time.
