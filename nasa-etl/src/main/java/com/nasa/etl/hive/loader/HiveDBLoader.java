package com.nasa.etl.hive.loader;

import com.nasa.etl.common.RunMetadata;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * HiveDBLoader
 *
 * Loads the TSV output files produced by the three Hive queries into the
 * same relational tables used by MapReduce and Pig pipelines (DBLoader,
 * PigDBLoader).  All DDL uses IF NOT EXISTS so the schema is shared.
 *
 * Hive writes output as tab-separated values into part-000* files under
 * the INSERT OVERWRITE DIRECTORY target.  Column order per query:
 *
 *   Q1 (q1_daily_traffic):
 *     batch_id \t log_date \t status_code \t request_count \t total_bytes
 *
 *   Q2 (q2_top_resources):
 *     batch_id \t resource_path \t request_count \t total_bytes \t distinct_host_count
 *
 *   Q3 (q3_hourly_error):
 *     batch_id \t log_date \t log_hour \t error_request_count \t
 *     total_request_count \t error_rate \t distinct_error_hosts
 *
 * This is byte-for-byte identical to the PigDBLoader column contract so the
 * Reporter module works without modification.
 */
public class HiveDBLoader {

    // ----------------------------------------------------------------- DDL
    // Identical to DBLoader / PigDBLoader so all three pipelines share tables.

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

            "CREATE INDEX IF NOT EXISTS idx_q1_run     ON q1_daily_traffic(run_id)",
            "CREATE INDEX IF NOT EXISTS idx_q1_batch   ON q1_daily_traffic(batch_id)",
            "CREATE INDEX IF NOT EXISTS idx_q2_run     ON q2_top_resources(run_id)",
            "CREATE INDEX IF NOT EXISTS idx_q2_batch   ON q2_top_resources(batch_id)",
            "CREATE INDEX IF NOT EXISTS idx_q3_run     ON q3_hourly_error(run_id)",
            "CREATE INDEX IF NOT EXISTS idx_q3_batch   ON q3_hourly_error(batch_id)",
            "CREATE INDEX IF NOT EXISTS idx_batch_run  ON batch_metadata(run_id)",
            "CREATE INDEX IF NOT EXISTS idx_meta_pipe  ON run_metadata(pipeline_name)",
            "CREATE INDEX IF NOT EXISTS idx_meta_batch ON run_metadata(batch_size)"
        };

        try (Statement st = conn.createStatement()) {
            for (String sql : ddl) st.execute(sql);
            conn.commit();
        }
    }

    // ----------------------------------------------------------------- run_metadata

    public static int createRunMetadata(Connection conn, String pipelineName, int batchSize)
            throws SQLException {
        String sql =
            "INSERT INTO run_metadata " +
            "(pipeline_name, batch_size, total_batches, avg_batch_size, " +
            " total_records, malformed_count, q1_runtime_ms, q2_runtime_ms, " +
            " q3_runtime_ms, runtime_ms) " +
            "VALUES (?, ?, 0, 0.0, 0, 0, 0, 0, 0, 0)";

        try (PreparedStatement ps = conn.prepareStatement(sql, new String[]{"run_id"})) {
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

    public static void saveBatchMetadata(Connection conn, int runId, int batchId,
                                          String queryName, long recordCount,
                                          long batchRuntimeMs) throws SQLException {
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

    // ----------------------------------------------------------------- Q1 loader

    /**
     * Reads part-* files from localQ1Dir and inserts into q1_daily_traffic.
     *
     * Expected TSV format (produced by query1_daily_traffic.hql):
     *   batch_id \t log_date \t status_code \t request_count \t total_bytes
     *
     * Returns a map of batch_id -> sum(request_count) for proportional
     * runtime slicing in the driver (same contract as PigDBLoader.loadQuery1).
     */
    public static Map<Integer, Long> loadQuery1(Connection conn,
                                                 String localQ1Dir,
                                                 int runId) throws Exception {
        String sql =
            "INSERT INTO q1_daily_traffic " +
            "(run_id, batch_id, log_date, status_code, request_count, total_bytes) " +
            "VALUES (?, ?, ?, ?, ?, ?)";

        Map<Integer, Long> batchRecordCounts = new LinkedHashMap<>();
        int loaded = 0;

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (String line : readPartFiles(downloadIfHdfs(localQ1Dir))) {
                String[] cols = line.split("\t");
                if (cols.length < 5) continue;

                int    batchId  = Integer.parseInt(cols[0].trim());
                String date     = cols[1].trim();
                int    status   = Integer.parseInt(cols[2].trim());
                long   reqCount = Long.parseLong(cols[3].trim());
                long   totBytes = Long.parseLong(cols[4].trim());

                ps.setInt   (1, runId);
                ps.setInt   (2, batchId);
                ps.setDate  (3, java.sql.Date.valueOf(date));
                ps.setInt   (4, status);
                ps.setLong  (5, reqCount);
                ps.setLong  (6, totBytes);
                ps.addBatch();
                loaded++;

                batchRecordCounts.merge(batchId, reqCount, Long::sum);
            }
            ps.executeBatch();
            conn.commit();
        }
        System.out.println("[HiveDBLoader] Q1 rows loaded: " + loaded);
        return batchRecordCounts;
    }

    // ----------------------------------------------------------------- Q2 loader

    /**
     * Reads part-* files from localQ2Dir and inserts into q2_top_resources.
     *
     * Expected TSV format (produced by query2_top_resources.hql):
     *   batch_id \t resource_path \t request_count \t total_bytes \t distinct_host_count
     */
    public static void loadQuery2(Connection conn, String localQ2Dir, int runId)
            throws Exception {
        String sql =
            "INSERT INTO q2_top_resources " +
            "(run_id, batch_id, resource_path, request_count, total_bytes, distinct_host_count) " +
            "VALUES (?, ?, ?, ?, ?, ?)";

        int loaded = 0;

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (String line : readPartFiles(downloadIfHdfs(localQ2Dir))) {
                String[] cols = line.split("\t");
                if (cols.length < 5) continue;

                int    batchId  = Integer.parseInt(cols[0].trim());
                String path     = cols[1];
                long   reqCount = Long.parseLong(cols[2].trim());
                long   totBytes = Long.parseLong(cols[3].trim());
                long   dHosts   = Long.parseLong(cols[4].trim());

                ps.setInt   (1, runId);
                ps.setInt   (2, batchId);
                ps.setString(3, path);
                ps.setLong  (4, reqCount);
                ps.setLong  (5, totBytes);
                ps.setLong  (6, dHosts);
                ps.addBatch();
                loaded++;
            }
            ps.executeBatch();
            conn.commit();
        }
        System.out.println("[HiveDBLoader] Q2 rows loaded: " + loaded);
    }

    // ----------------------------------------------------------------- Q3 loader

    /**
     * Reads part-* files from localQ3Dir and inserts into q3_hourly_error.
     *
     * Expected TSV format (produced by query3_hourly_error.hql):
     *   batch_id \t log_date \t log_hour \t error_request_count \t
     *   total_request_count \t error_rate \t distinct_error_hosts
     */
    public static void loadQuery3(Connection conn, String localQ3Dir, int runId)
            throws Exception {
        String sql =
            "INSERT INTO q3_hourly_error " +
            "(run_id, batch_id, log_date, log_hour, error_request_count, " +
            " total_request_count, error_rate, distinct_error_hosts) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        int loaded = 0;

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (String line : readPartFiles(downloadIfHdfs(localQ3Dir))) {
                String[] cols = line.split("\t");
                if (cols.length < 7) continue;

                int    batchId  = Integer.parseInt(cols[0].trim());
                String date     = cols[1].trim();
                int    hour     = Integer.parseInt(cols[2].trim());
                long   errCount = Long.parseLong(cols[3].trim());
                long   totCount = Long.parseLong(cols[4].trim());
                double errRate  = Double.parseDouble(cols[5].trim());
                long   dHosts   = Long.parseLong(cols[6].trim());

                ps.setInt   (1, runId);
                ps.setInt   (2, batchId);
                ps.setDate  (3, java.sql.Date.valueOf(date));
                ps.setInt   (4, hour);
                ps.setLong  (5, errCount);
                ps.setLong  (6, totCount);
                ps.setDouble(7, errRate);
                ps.setLong  (8, dHosts);
                ps.addBatch();
                loaded++;
            }
            ps.executeBatch();
            conn.commit();
        }
        System.out.println("[HiveDBLoader] Q3 rows loaded: " + loaded);
    }

    // ----------------------------------------------------------------- helpers

    /** Query batch record counts from q1_daily_traffic for a given run. */
    public static Map<Integer, Long> getQ1BatchRecordCounts(Connection conn, int runId)
            throws SQLException {
        String sql =
            "SELECT batch_id, SUM(request_count) AS record_count " +
            "FROM q1_daily_traffic WHERE run_id = ? " +
            "GROUP BY batch_id ORDER BY batch_id";

        Map<Integer, Long> result = new LinkedHashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.put(rs.getInt("batch_id"), rs.getLong("record_count"));
                }
            }
        }
        return result;
    }

    /** Open a JDBC connection (auto-commit off). */
    public static Connection openConnection(String url, String user, String pass)
            throws SQLException {
        try { Class.forName("org.postgresql.Driver"); }
        catch (ClassNotFoundException ignored) {}
        try { Class.forName("com.mysql.cj.jdbc.Driver"); }
        catch (ClassNotFoundException ignored) {}

        Connection conn = DriverManager.getConnection(url, user, pass);
        conn.setAutoCommit(false);
        return conn;
    }

    /**
     * If dirPath is an HDFS path, downloads it to a local temp directory
     * using `hdfs dfs -get` and returns the local path.
     * If it is already a local path, returns it unchanged.
     *
     * Mirrors PigDBLoader.downloadIfHdfs() exactly so both loaders behave
     * identically when the pipeline writes output to HDFS.
     */
    static String downloadIfHdfs(String dirPath) throws IOException {
        boolean isHdfs = dirPath.startsWith("hdfs://")
                      || dirPath.startsWith("hdfs:/")
                      || dirPath.startsWith("viewfs://")
                      || dirPath.startsWith("/user/");   // bare HDFS convention

        if (!isHdfs) return dirPath;   // already local, nothing to do

        // Create a unique local temp dir for this download
        Path localTmp = Files.createTempDirectory("hive-hdfs-dl-");
        String localDir = localTmp.toAbsolutePath().toString();

        List<String> cmd = Arrays.asList("hdfs", "dfs", "-get", dirPath, localDir);
        System.out.println("[HiveDBLoader] Downloading HDFS path to local: "
                           + dirPath + " -> " + localDir);

        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        Process proc = pb.start();

        // Drain stdout/stderr so the process never blocks on a full pipe buffer
        StringBuilder output = new StringBuilder();
        try (java.io.BufferedReader br = new java.io.BufferedReader(
                new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) output.append(line).append('\n');
        }

        int exitCode;
        try {
            exitCode = proc.waitFor();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("hdfs dfs -get interrupted", e);
        }

        if (exitCode != 0) {
            throw new IOException("hdfs dfs -get failed (exit " + exitCode + "):\n" + output);
        }

        // -get downloads the src directory itself into localDir, so the actual
        // content sits at localDir/<basename-of-dirPath>
        String baseName = dirPath.replaceAll(".*[/]", "");   // last path component
        String resultPath = localDir + File.separator + baseName;
        System.out.println("[HiveDBLoader] Download complete: " + resultPath);
        return resultPath;
    }

    /**
     * Reads all Hive output part files from a local directory.
     * Hive names them 000000_0, 000001_0, … or part-r-00000, part-m-00000.
     * Both naming conventions are handled.
     */
    static List<String> readPartFiles(String dirPath) throws IOException {
        List<String> lines = new ArrayList<>();
        Path dir = Paths.get(dirPath);

        if (!Files.isDirectory(dir)) {
            if (Files.isRegularFile(dir)) {
                for (String l : Files.readAllLines(dir, StandardCharsets.UTF_8)) {
                    if (!l.trim().isEmpty()) lines.add(l);
                }
            }
            return lines;
        }

        try (java.util.stream.Stream<Path> stream = Files.list(dir)) {
            List<Path> parts = stream
                .filter(Files::isRegularFile)
                .filter(p -> {
                    String n = p.getFileName().toString();
                    // Hive local-mode output: 000000_0, 000001_0, ...
                    // Hive MR-mode output   : part-r-00000, part-m-00000
                    return n.matches("\\d{6}_\\d+")
                        || n.startsWith("part-r-")
                        || n.startsWith("part-m-")
                        || n.startsWith("part-00");
                })
                .sorted()
                .collect(Collectors.toList());

            for (Path p : parts) {
                for (String l : Files.readAllLines(p, StandardCharsets.UTF_8)) {
                    if (!l.trim().isEmpty()) lines.add(l);
                }
            }
        }
        return lines;
    }
}