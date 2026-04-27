package com.nasa.etl.loader;

import com.nasa.etl.common.RunMetadata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Loads MapReduce outputs into PostgreSQL using scripts/schema.sql as the
 * database contract.
 */
public class DBLoader {

    public static final String DB_URL_KEY  = "nasa.etl.db.url";
    public static final String DB_USER_KEY = "nasa.etl.db.user";
    public static final String DB_PASS_KEY = "nasa.etl.db.password";
    public static final String RUN_ID_KEY  = "nasa.etl.run.id";

    public static void createTables(Connection conn) throws SQLException {
        String[] ddl = {
            "CREATE TABLE IF NOT EXISTS run_metadata (" +
            "  run_id           SERIAL PRIMARY KEY," +
            "  pipeline_name    VARCHAR(64)," +
            "  batch_size       INT," +
            "  total_batches    INT," +
            "  avg_batch_size   DOUBLE PRECISION," +
            "  total_records    BIGINT," +
            "  malformed_count  BIGINT," +
            "  q1_runtime_ms    BIGINT," +
            "  q2_runtime_ms    BIGINT," +
            "  q3_runtime_ms    BIGINT," +
            "  runtime_ms       BIGINT," +
            "  executed_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
            ")",

            "CREATE TABLE IF NOT EXISTS batch_metadata (" +
            "  id               SERIAL PRIMARY KEY," +
            "  run_id           INT REFERENCES run_metadata(run_id)," +
            "  batch_id         INT NOT NULL," +
            "  query_name       VARCHAR(8) NOT NULL," +
            "  record_count     BIGINT," +
            "  batch_runtime_ms BIGINT," +
            "  loaded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
            ")",

            "CREATE TABLE IF NOT EXISTS q1_daily_traffic (" +
            "  id             SERIAL PRIMARY KEY," +
            "  run_id         INT REFERENCES run_metadata(run_id)," +
            "  batch_id       INT NOT NULL," +
            "  log_date       DATE," +
            "  status_code    INT," +
            "  request_count  BIGINT," +
            "  total_bytes    BIGINT," +
            "  loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
            ")",

            "CREATE TABLE IF NOT EXISTS q2_top_resources (" +
            "  id                  SERIAL PRIMARY KEY," +
            "  run_id              INT REFERENCES run_metadata(run_id)," +
            "  batch_id            INT NOT NULL," +
            "  resource_path       TEXT," +
            "  request_count       BIGINT," +
            "  total_bytes         BIGINT," +
            "  distinct_host_count BIGINT," +
            "  loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
            ")",

            "CREATE TABLE IF NOT EXISTS q3_hourly_error (" +
            "  id                   SERIAL PRIMARY KEY," +
            "  run_id               INT REFERENCES run_metadata(run_id)," +
            "  batch_id             INT NOT NULL," +
            "  log_date             DATE," +
            "  log_hour             INT," +
            "  error_request_count  BIGINT," +
            "  total_request_count  BIGINT," +
            "  error_rate           DOUBLE PRECISION," +
            "  distinct_error_hosts BIGINT," +
            "  loaded_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
            ")",

            "CREATE INDEX IF NOT EXISTS idx_q1_run ON q1_daily_traffic(run_id)",
            "CREATE INDEX IF NOT EXISTS idx_q1_batch ON q1_daily_traffic(batch_id)",
            "CREATE INDEX IF NOT EXISTS idx_q2_run ON q2_top_resources(run_id)",
            "CREATE INDEX IF NOT EXISTS idx_q2_batch ON q2_top_resources(batch_id)",
            "CREATE INDEX IF NOT EXISTS idx_q3_run ON q3_hourly_error(run_id)",
            "CREATE INDEX IF NOT EXISTS idx_q3_batch ON q3_hourly_error(batch_id)",
            "CREATE INDEX IF NOT EXISTS idx_batch_run ON batch_metadata(run_id)",
            "CREATE INDEX IF NOT EXISTS idx_meta_pipeline ON run_metadata(pipeline_name)",
            "CREATE INDEX IF NOT EXISTS idx_meta_batch ON run_metadata(batch_size)"
        };

        try (Statement st = conn.createStatement()) {
            for (String sql : ddl) {
                st.execute(sql);
            }
            conn.commit();
        }
    }

    public static int createRunMetadata(Connection conn, String pipelineName,
                                        int batchSize)
            throws SQLException {
        String sql =
            "INSERT INTO run_metadata " +
            "(pipeline_name, batch_size, total_batches, avg_batch_size, " +
            " total_records, malformed_count, q1_runtime_ms, q2_runtime_ms, " +
            " q3_runtime_ms, runtime_ms) " +
            "VALUES (?, ?, 0, 0.0, 0, 0, 0, 0, 0, 0)";

        try (PreparedStatement ps = conn.prepareStatement(
                sql, new String[] { "run_id" })) {
            ps.setString(1, pipelineName);
            ps.setInt(2, batchSize);
            ps.executeUpdate();

            try (ResultSet rs = ps.getGeneratedKeys()) {
                if (rs.next()) {
                    int runId = rs.getInt(1);
                    conn.commit();
                    return runId;
                }
            }
        }

        conn.rollback();
        throw new SQLException("Could not retrieve generated run_id");
    }

    public static void updateRunMetadata(Connection conn, RunMetadata meta)
            throws SQLException {
        String sql =
            "UPDATE run_metadata SET " +
            "  pipeline_name = ?, batch_size = ?, total_batches = ?, " +
            "  avg_batch_size = ?, total_records = ?, malformed_count = ?, " +
            "  q1_runtime_ms = ?, q2_runtime_ms = ?, q3_runtime_ms = ?, " +
            "  runtime_ms = ? " +
            "WHERE run_id = ?";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, meta.getPipelineName());
            ps.setInt   (2, meta.getBatchSize());
            ps.setLong  (3, meta.getTotalBatches());
            ps.setDouble(4, meta.getAvgBatchSize());
            ps.setLong  (5, meta.getTotalRecords());
            ps.setLong  (6, meta.getMalformedCount());
            ps.setLong  (7, meta.getQ1RuntimeMs());
            ps.setLong  (8, meta.getQ2RuntimeMs());
            ps.setLong  (9, meta.getQ3RuntimeMs());
            ps.setLong  (10, meta.getRuntimeMs());
            ps.setInt   (11, meta.getRunId());
            ps.executeUpdate();
            conn.commit();
        }
    }

    public static void saveBatchMetadata(Connection conn, int runId,
                                         int batchId, String queryName,
                                         long recordCount,
                                         long batchRuntimeMs)
            throws SQLException {
        String sql =
            "INSERT INTO batch_metadata " +
            "(run_id, batch_id, query_name, record_count, batch_runtime_ms) " +
            "VALUES (?, ?, ?, ?, ?)";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt   (1, runId);
            ps.setInt   (2, batchId);
            ps.setString(3, queryName);
            ps.setLong  (4, recordCount);
            ps.setLong  (5, batchRuntimeMs);
            ps.executeUpdate();
            conn.commit();
        }
    }

    public static void loadQuery1(Connection conn, Configuration conf,
                                  String hdfsOutputDir, int runId,
                                  int batchId)
            throws Exception {
        String sql =
            "INSERT INTO q1_daily_traffic " +
            "(run_id, batch_id, log_date, status_code, request_count, total_bytes) " +
            "VALUES (?, ?, ?, ?, ?, ?)";

        List<String> lines = readHdfsOutput(conf, hdfsOutputDir);
        int loaded = 0;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (String line : lines) {
                String[] kv = line.split("\t", 3);
                if (kv.length < 3) continue;

                String date = kv[0];
                int code = Integer.parseInt(kv[1].trim());
                String[] val = kv[2].split("\t");
                if (val.length < 2) continue;

                ps.setInt(1, runId);
                ps.setInt(2, batchId);
                ps.setDate(3, java.sql.Date.valueOf(date));
                ps.setInt(4, code);
                ps.setLong(5, Long.parseLong(val[0].trim()));
                ps.setLong(6, Long.parseLong(val[1].trim()));
                ps.addBatch();
                loaded++;
            }
            ps.executeBatch();
            conn.commit();
        }
        System.out.println("[DBLoader] Q1 rows loaded: " + loaded);
    }

    public static void loadQuery2(Connection conn, Configuration conf,
                                  String hdfsOutputDir, int runId,
                                  int batchId)
            throws Exception {
        String sql =
            "INSERT INTO q2_top_resources " +
            "(run_id, batch_id, resource_path, request_count, " +
            " total_bytes, distinct_host_count) " +
            "VALUES (?, ?, ?, ?, ?, ?)";

        List<String> lines = readHdfsOutput(conf, hdfsOutputDir);
        int loaded = 0;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (String line : lines) {
                int tab1 = line.indexOf('\t');
                if (tab1 < 0) continue;

                String path = line.substring(0, tab1);
                String[] val = line.substring(tab1 + 1).split("\t");
                if (val.length < 3) continue;

                ps.setInt(1, runId);
                ps.setInt(2, batchId);
                ps.setString(3, path);
                ps.setLong(4, Long.parseLong(val[0].trim()));
                ps.setLong(5, Long.parseLong(val[1].trim()));
                ps.setLong(6, Long.parseLong(val[2].trim()));
                ps.addBatch();
                loaded++;
            }
            ps.executeBatch();
            conn.commit();
        }
        System.out.println("[DBLoader] Q2 rows loaded: " + loaded);
    }

    public static void loadQuery3(Connection conn, Configuration conf,
                                  String hdfsOutputDir, int runId,
                                  int batchId)
            throws Exception {
        String sql =
            "INSERT INTO q3_hourly_error " +
            "(run_id, batch_id, log_date, log_hour, error_request_count, " +
            " total_request_count, error_rate, distinct_error_hosts) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        List<String> lines = readHdfsOutput(conf, hdfsOutputDir);
        int loaded = 0;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (String line : lines) {
                String[] kv = line.split("\t", 3);
                if (kv.length < 3) continue;

                String date = kv[0];
                int hour = Integer.parseInt(kv[1].trim());
                String[] val = kv[2].split("\t");
                if (val.length < 4) continue;

                ps.setInt(1, runId);
                ps.setInt(2, batchId);
                ps.setDate(3, java.sql.Date.valueOf(date));
                ps.setInt(4, hour);
                ps.setLong(5, Long.parseLong(val[0].trim()));
                ps.setLong(6, Long.parseLong(val[1].trim()));
                ps.setDouble(7, Double.parseDouble(val[2].trim()));
                ps.setLong(8, Long.parseLong(val[3].trim()));
                ps.addBatch();
                loaded++;
            }
            ps.executeBatch();
            conn.commit();
        }
        System.out.println("[DBLoader] Q3 rows loaded: " + loaded);
    }

    static List<String> readHdfsOutput(Configuration conf, String dir)
            throws Exception {
        FileSystem fs = FileSystem.get(conf);
        Path outPath = new Path(dir);
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

    public static Connection openConnection(Configuration conf)
            throws SQLException {
        String url = conf.get(DB_URL_KEY);
        String user = conf.get(DB_USER_KEY);
        String pass = conf.get(DB_PASS_KEY);
        Connection conn = DriverManager.getConnection(url, user, pass);
        conn.setAutoCommit(false);
        return conn;
    }
}
