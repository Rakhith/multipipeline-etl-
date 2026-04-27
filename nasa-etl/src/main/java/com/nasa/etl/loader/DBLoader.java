package com.nasa.etl.loader;

import com.nasa.etl.common.RunMetadata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * DBLoader – reads MapReduce output from HDFS and loads it into the
 * target relational database (MySQL or PostgreSQL).
 *
 * The loader handles all three query result schemas and the
 * run_metadata table.
 *
 * Configuration keys read from Hadoop conf (set by driver):
 *   nasa.etl.db.url      – JDBC URL
 *   nasa.etl.db.user     – DB username
 *   nasa.etl.db.password – DB password
 *   nasa.etl.run.id      – UUID for this run
 */
public class DBLoader {

    // ----------------------------------------------------------------- JDBC keys

    public static final String DB_URL_KEY  = "nasa.etl.db.url";
    public static final String DB_USER_KEY = "nasa.etl.db.user";
    public static final String DB_PASS_KEY = "nasa.etl.db.password";
    public static final String RUN_ID_KEY  = "nasa.etl.run.id";

    // ----------------------------------------------------------------- public API

    /**
     * Creates all required tables (if they do not exist) in the target DB.
     */
    public static void createTables(Connection conn) throws SQLException {
        String[] ddl = {
            // --- run metadata ---
            "CREATE TABLE IF NOT EXISTS run_metadata (" +
            "  id               SERIAL PRIMARY KEY," +
            "  pipeline_name    VARCHAR(64)," +
            "  run_id           VARCHAR(64)," +
            "  query_name       VARCHAR(64)," +
            "  batch_size       INT," +
            "  batch_count      BIGINT," +
            "  avg_batch_size   DOUBLE PRECISION," +
            "  total_records    BIGINT," +
            "  malformed_count  BIGINT," +
            "  runtime_ms       BIGINT," +
            "  execution_time   VARCHAR(32)" +
            ")",

            // --- Q1 ---
            "CREATE TABLE IF NOT EXISTS q1_daily_traffic (" +
            "  id             SERIAL PRIMARY KEY," +
            "  run_id         VARCHAR(64)," +
            "  pipeline_name  VARCHAR(64)," +
            "  batch_id       VARCHAR(64)," +
            "  log_date       DATE," +
            "  status_code    INT," +
            "  request_count  BIGINT," +
            "  total_bytes    BIGINT," +
            "  loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
            ")",

            // --- Q2 ---
            "CREATE TABLE IF NOT EXISTS q2_top_resources (" +
            "  id                 SERIAL PRIMARY KEY," +
            "  run_id             VARCHAR(64)," +
            "  pipeline_name      VARCHAR(64)," +
            "  batch_id           VARCHAR(64)," +
            "  resource_path      TEXT," +
            "  request_count      BIGINT," +
            "  total_bytes        BIGINT," +
            "  distinct_host_count BIGINT," +
            "  loaded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
            ")",

            // --- Q3 ---
            "CREATE TABLE IF NOT EXISTS q3_hourly_error (" +
            "  id                    SERIAL PRIMARY KEY," +
            "  run_id                VARCHAR(64)," +
            "  pipeline_name         VARCHAR(64)," +
            "  batch_id              VARCHAR(64)," +
            "  log_date              DATE," +
            "  log_hour              INT," +
            "  error_request_count   BIGINT," +
            "  total_request_count   BIGINT," +
            "  error_rate            DOUBLE PRECISION," +
            "  distinct_error_hosts  BIGINT," +
            "  loaded_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
            ")"
        };

        try (Statement st = conn.createStatement()) {
            for (String sql : ddl) {
                st.execute(sql);
            }
            conn.commit();
        }
    }

    // ----------------------------------------------------------------- Q1

    public static void loadQuery1(Connection conn, Configuration conf,
                                  String hdfsOutputDir,
                                  String runId, String batchId)
            throws Exception {

        String sql =
            "INSERT INTO q1_daily_traffic " +
            "(run_id, pipeline_name, batch_id, log_date, status_code, " +
            " request_count, total_bytes) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?)";

        List<String> lines = readHdfsOutput(conf, hdfsOutputDir);
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (String line : lines) {
                // key = "log_date\tstatus_code"   value = "req_count\ttotal_bytes"
                String[] kv  = line.split("\t", 3);
                if (kv.length < 3) continue;
                String date  = kv[0];
                int    code  = Integer.parseInt(kv[1].trim());
                String[] val = kv[2].split("\t");
                long   reqs  = Long.parseLong(val[0].trim());
                long   bytes = Long.parseLong(val[1].trim());

                ps.setString(1, runId);
                ps.setString(2, "Hadoop-MapReduce");
                ps.setString(3, batchId);
                ps.setDate  (4, java.sql.Date.valueOf(date));
                ps.setInt   (5, code);
                ps.setLong  (6, reqs);
                ps.setLong  (7, bytes);
                ps.addBatch();
            }
            ps.executeBatch();
            conn.commit();
        }
        System.out.println("[DBLoader] Q1 rows loaded: " + lines.size());
    }

    // ----------------------------------------------------------------- Q2

    public static void loadQuery2(Connection conn, Configuration conf,
                                  String hdfsOutputDir,
                                  String runId, String batchId)
            throws Exception {

        String sql =
            "INSERT INTO q2_top_resources " +
            "(run_id, pipeline_name, batch_id, resource_path, " +
            " request_count, total_bytes, distinct_host_count) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?)";

        List<String> lines = readHdfsOutput(conf, hdfsOutputDir);
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (String line : lines) {
                // key = "resource_path"  value = "req\tbytes\thosts"
                int    tab1  = line.indexOf('\t');
                if (tab1 < 0) continue;
                String path  = line.substring(0, tab1);
                String[] val = line.substring(tab1 + 1).split("\t");
                if (val.length < 3) continue;
                long reqs    = Long.parseLong(val[0].trim());
                long bytes   = Long.parseLong(val[1].trim());
                long dHosts  = Long.parseLong(val[2].trim());

                ps.setString(1, runId);
                ps.setString(2, "Hadoop-MapReduce");
                ps.setString(3, batchId);
                ps.setString(4, path);
                ps.setLong  (5, reqs);
                ps.setLong  (6, bytes);
                ps.setLong  (7, dHosts);
                ps.addBatch();
            }
            ps.executeBatch();
            conn.commit();
        }
        System.out.println("[DBLoader] Q2 rows loaded: " + lines.size());
    }

    // ----------------------------------------------------------------- Q3

    public static void loadQuery3(Connection conn, Configuration conf,
                                  String hdfsOutputDir,
                                  String runId, String batchId)
            throws Exception {

        String sql =
            "INSERT INTO q3_hourly_error " +
            "(run_id, pipeline_name, batch_id, log_date, log_hour, " +
            " error_request_count, total_request_count, error_rate, " +
            " distinct_error_hosts) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

        List<String> lines = readHdfsOutput(conf, hdfsOutputDir);
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (String line : lines) {
                // key = "log_date\tlog_hour"  value = "err\ttot\trate\thosts"
                String[] kv  = line.split("\t", 3);
                if (kv.length < 3) continue;
                String date  = kv[0];
                int    hour  = Integer.parseInt(kv[1].trim());
                String[] val = kv[2].split("\t");
                if (val.length < 4) continue;
                long   errReq  = Long.parseLong(val[0].trim());
                long   totReq  = Long.parseLong(val[1].trim());
                double rate    = Double.parseDouble(val[2].trim());
                long   dHosts  = Long.parseLong(val[3].trim());

                ps.setString(1, runId);
                ps.setString(2, "Hadoop-MapReduce");
                ps.setString(3, batchId);
                ps.setDate  (4, java.sql.Date.valueOf(date));
                ps.setInt   (5, hour);
                ps.setLong  (6, errReq);
                ps.setLong  (7, totReq);
                ps.setDouble(8, rate);
                ps.setLong  (9, dHosts);
                ps.addBatch();
            }
            ps.executeBatch();
            conn.commit();
        }
        System.out.println("[DBLoader] Q3 rows loaded: " + lines.size());
    }

    // ----------------------------------------------------------------- metadata

    public static void saveRunMetadata(Connection conn, RunMetadata meta)
            throws SQLException {

        String sql =
            "INSERT INTO run_metadata " +
            "(pipeline_name, run_id, query_name, batch_size, batch_count, " +
            " avg_batch_size, total_records, malformed_count, runtime_ms, " +
            " execution_time) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1,  meta.getPipelineName());
            ps.setString(2,  meta.getRunId());
            ps.setString(3,  meta.getQueryName());
            ps.setInt   (4,  meta.getBatchSize());
            ps.setLong  (5,  meta.getBatchCount());
            ps.setDouble(6,  meta.getAvgBatchSize());
            ps.setLong  (7,  meta.getTotalRecords());
            ps.setLong  (8,  meta.getMalformedCount());
            ps.setLong  (9,  meta.getRuntimeMs());
            ps.setString(10, meta.getExecutionTime());
            ps.executeUpdate();
            conn.commit();
        }
    }

    // ----------------------------------------------------------------- HDFS read

    /**
     * Reads all part-r-* and part-m-* files from an HDFS output directory
     * and returns every non-empty line.
     */
    static List<String> readHdfsOutput(Configuration conf, String dir)
            throws Exception {

        FileSystem fs = FileSystem.get(conf);
        Path outPath  = new Path(dir);
        List<String> lines = new ArrayList<>();

        org.apache.hadoop.fs.FileStatus[] statuses =
            fs.listStatus(outPath, path -> {
                String name = path.getName();
                return name.startsWith("part-r-") || name.startsWith("part-m-");
            });

        for (org.apache.hadoop.fs.FileStatus status : statuses) {
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(
                        fs.open(status.getPath()), StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (!line.trim().isEmpty()) lines.add(line);
                }
            }
        }
        return lines;
    }

    // ----------------------------------------------------------------- connection

    public static Connection openConnection(Configuration conf) throws SQLException {
        String url  = conf.get(DB_URL_KEY);
        String user = conf.get(DB_USER_KEY);
        String pass = conf.get(DB_PASS_KEY);
        Connection conn = DriverManager.getConnection(url, user, pass);
        conn.setAutoCommit(false);
        return conn;
    }
}
