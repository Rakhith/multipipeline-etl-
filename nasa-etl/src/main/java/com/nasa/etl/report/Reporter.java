package com.nasa.etl.report;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Reporter – reads aggregated query results and run metadata from the
 * relational database and displays them in a formatted report.
 *
 * Can be run independently of Hadoop (only needs the JDBC driver + nasa-etl.jar).
 *
 * Usage:
 *   java -cp nasa-etl.jar:postgresql.jar com.nasa.etl.report.Reporter \
 *        <jdbc-url> <db-user> <db-pass> [run-id]
 *
 * If run-id is omitted, the most recent run is shown.
 */
public class Reporter {

    // ------------------------------------------------------------------ entry

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println(
                "Usage: Reporter <jdbc-url> <db-user> <db-pass> [run-id]");
            System.exit(1);
        }

        String url   = args[0];
        String user  = args[1];
        String pass  = args[2];
        Integer runId = args.length >= 4 ? Integer.valueOf(args[3]) : null;

        // Load driver
        try { Class.forName("org.postgresql.Driver"); }
        catch (ClassNotFoundException ignored) {}
        try { Class.forName("com.mysql.cj.jdbc.Driver"); }
        catch (ClassNotFoundException ignored) {}

        try (Connection conn = DriverManager.getConnection(url, user, pass)) {
            conn.setAutoCommit(true);

            // Resolve run ID
            if (runId == null) {
                runId = resolveLatestRunId(conn);
                if (runId == null) {
                    System.err.println("No runs found in database.");
                    System.exit(1);
                }
            }

            printBanner(runId);
            printRunMetadata(conn, runId);
            printBatchMetadata(conn, runId);
            printQuery1(conn, runId);
            printQuery2(conn, runId);
            printQuery3(conn, runId);
        }
    }
    // ----------------------------------------------------------------- banner

    private static void printBanner(int runId) {
        String line = "=".repeat(90);
        System.out.println(line);
        System.out.println("  NASA HTTP Log Analytics – Hadoop MapReduce Pipeline – Execution Report");
        System.out.println(line);
        System.out.printf("  Run ID : %s%n", runId);
        System.out.println(line);
    }

    // ----------------------------------------------------------------- metadata

    private static void printRunMetadata(Connection conn, int runId)
            throws SQLException {
        String sql =
            "SELECT pipeline_name, batch_size, total_batches, avg_batch_size, " +
            "       total_records, malformed_count, q1_runtime_ms, " +
            "       q2_runtime_ms, q3_runtime_ms, runtime_ms, executed_at " +
            "FROM run_metadata " +
            "WHERE run_id = ?";

        System.out.println("\n  EXECUTION METADATA");
        System.out.println("  " + "-".repeat(88));
        System.out.printf("  %-20s %8s %8s %12s %10s %10s %10s%n",
            "Pipeline", "Batch", "Batches", "AvgBatch",
            "Valid", "Malformed", "Runtime(ms)");
        System.out.println("  " + "-".repeat(88));

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    System.out.printf(
                        "  %-20s %8d %8d %12.1f %10d %10d %10d%n",
                        rs.getString("pipeline_name"),
                        rs.getInt   ("batch_size"),
                        rs.getLong  ("total_batches"),
                        rs.getDouble("avg_batch_size"),
                        rs.getLong  ("total_records"),
                        rs.getLong  ("malformed_count"),
                        rs.getLong  ("runtime_ms"));
                    System.out.printf(
                        "  Q runtimes: Q1=%,d ms | Q2=%,d ms | Q3=%,d ms%n",
                        rs.getLong("q1_runtime_ms"),
                        rs.getLong("q2_runtime_ms"),
                        rs.getLong("q3_runtime_ms"));
                }
            }
        }
        System.out.println("  " + "-".repeat(88));
    }

    private static void printBatchMetadata(Connection conn, int runId)
            throws SQLException {
        System.out.println("\n  BATCH METADATA");
        System.out.printf("  %-8s %-8s %-10s %14s %18s%n",
            "batch_id", "query", "records", "batch_runtime_ms", "loaded_at");
        System.out.println("  " + "-".repeat(70));
    
        String sql =
            "SELECT batch_id, query_name, record_count, batch_runtime_ms, loaded_at " +
            "FROM batch_metadata WHERE run_id = ? " +
            "ORDER BY query_name, batch_id";
    
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    System.out.printf("  %-8d %-8s %14d %18d %-25s%n",
                        rs.getInt("batch_id"),
                        rs.getString("query_name"),
                        rs.getLong("record_count"),
                        rs.getLong("batch_runtime_ms"),
                        rs.getTimestamp("loaded_at"));
                }
            }
        }
        System.out.println("  " + "-".repeat(70));
    }

    // ----------------------------------------------------------------- Q1

    private static void printQuery1(Connection conn, int runId)
            throws SQLException {
        System.out.println("\n\n  QUERY 1 – Daily Traffic Summary");
        // System.out.println("  " + "-".repeat(55));
        // System.out.printf("  %-12s %8s %16s %20s%n", "log_date", "status", "request_count", "total_bytes");
        // System.out.println("  " + "-".repeat(55));
        // System.out.printf("  %-12s %11s %16s %20s%n", "log_date", "status_code", "request_count", "total_bytes");
        // System.out.println("  " + "-".repeat(63));
        System.out.printf("  %-6s %6s %8s %-12s %11s %16s %20s %-25s%n",
            "id", "run_id", "batch_id", "log_date", "status_code", "request_count", "total_bytes", "loaded_at");
        System.out.println("  " + "-".repeat(110));

        String sql =
            "SELECT id, run_id, batch_id, log_date, status_code, request_count, total_bytes, loaded_at " +
            "FROM q1_daily_traffic " +
            "WHERE run_id = ? " +
            "ORDER BY batch_id, log_date, status_code";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                int rowCount = 0;
                while (rs.next()) {
                    // System.out.printf("  %-12s %11d %16d %20d%n", rs.getDate ("log_date"), rs.getInt ("status_code"), rs.getLong ("request_count"), rs.getLong ("total_bytes"));
                    System.out.printf("  %-6d %6d %8d %-12s %11d %16d %20d %-25s%n",
                        rs.getInt   ("id"),
                        rs.getInt   ("run_id"),
                        rs.getInt   ("batch_id"),
                        rs.getDate  ("log_date"),
                        rs.getInt   ("status_code"),
                        rs.getLong  ("request_count"),
                        rs.getLong  ("total_bytes"),
                        rs.getTimestamp("loaded_at"));
                    rowCount++;
                }
                System.out.println("  " + "-".repeat(110));
                System.out.printf("  Total rows: %d%n", rowCount);
            }
        }
    }

    // ----------------------------------------------------------------- Q2

    private static void printQuery2(Connection conn, int runId)
            throws SQLException {
        System.out.println("\n\n  QUERY 2 – Top 20 Requested Resources");
        // System.out.println("  " + "-".repeat(85));
        // System.out.printf("  %-4s %-45s %14s %16s %12s%n", "Rank", "resource_path", "request_count", "total_bytes", "distinct_hosts");
        // System.out.println("  " + "-".repeat(85));
        // System.out.printf("  %-4s %-45s %14s %16s %20s%n", "Rank", "resource_path", "request_count", "total_bytes", "distinct_host_count");
        // System.out.println("  " + "-".repeat(105));
        System.out.printf("  %-6s %6s %8s %-45s %14s %16s %20s %-25s%n",
            "id", "run_id", "batch_id", "resource_path", "request_count", "total_bytes", "distinct_host_count", "loaded_at");
        System.out.println("  " + "-".repeat(150));

        String sql =
            "SELECT id, run_id, batch_id, resource_path, request_count, total_bytes, distinct_host_count, loaded_at " +
            "FROM q2_top_resources " +
            "WHERE run_id = ? " +
            "ORDER BY batch_id, request_count DESC";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                int rank = 1;
                while (rs.next()) {
                    String path = rs.getString("resource_path");
                    if (path.length() > 44) path = path.substring(0, 41) + "...";
                    // System.out.printf("  %-4d %-45s %14d %16d %20d%n", rank++, path, rs.getLong("request_count"), rs.getLong("total_bytes"), rs.getLong("distinct_host_count"));
                    System.out.printf("  %-6d %6d %8d %-45s %14d %16d %20d %-25s%n",
                        rs.getInt   ("id"),
                        rs.getInt   ("run_id"),
                        rs.getInt   ("batch_id"),
                        path,
                        rs.getLong  ("request_count"),
                        rs.getLong  ("total_bytes"),
                        rs.getLong  ("distinct_host_count"),
                        rs.getTimestamp("loaded_at"));
                }
            }
        }
        System.out.println("  " + "-".repeat(150));
    }

    // ----------------------------------------------------------------- Q3

    private static void printQuery3(Connection conn, int runId)
            throws SQLException {
        System.out.println("\n\n  QUERY 3 – Hourly Error Analysis");
        // System.out.println("  " + "-".repeat(88));
        // System.out.printf("  %-12s %6s %14s %14s %12s %16s%n", "log_date", "hour", "error_reqs", "total_reqs", "error_rate", "distinct_err_hosts");
        // System.out.println("  " + "-".repeat(88));
        // System.out.printf("  %-12s %8s %20s %20s %12s %20s%n", "log_date", "log_hour", "error_request_count", "total_request_count", "error_rate", "distinct_error_hosts");
        // System.out.println("  " + "-".repeat(105));
        System.out.printf("  %-6s %6s %8s %-12s %8s %20s %20s %12s %20s %-25s%n",
            "id", "run_id", "batch_id", "log_date", "log_hour", "error_request_count", "total_request_count", "error_rate", "distinct_error_hosts", "loaded_at");
        System.out.println("  " + "-".repeat(155));

        String sql =
            "SELECT id, run_id, batch_id, log_date, log_hour, error_request_count, total_request_count, " +
            "       error_rate, distinct_error_hosts, loaded_at " +
            "FROM q3_hourly_error " +
            "WHERE run_id = ? " +
            "ORDER BY batch_id, log_date, log_hour";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                int rowCount = 0;
                while (rs.next()) {
                    // System.out.printf("  %-12s %8d %20d %20d %12.4f %20d%n", rs.getDate ("log_date"), rs.getInt ("log_hour"), rs.getLong ("error_request_count"), rs.getLong ("total_request_count"), rs.getDouble("error_rate"), rs.getLong ("distinct_error_hosts"));
                    System.out.printf("  %-6d %6d %8d %-12s %8d %20d %20d %12.4f %20d %-25s%n",
                        rs.getInt   ("id"),
                        rs.getInt   ("run_id"),
                        rs.getInt   ("batch_id"),
                        rs.getDate  ("log_date"),
                        rs.getInt   ("log_hour"),
                        rs.getLong  ("error_request_count"),
                        rs.getLong  ("total_request_count"),
                        rs.getDouble("error_rate"),
                        rs.getLong  ("distinct_error_hosts"),
                        rs.getTimestamp("loaded_at"));
                    rowCount++;
                }
                System.out.println("  " + "-".repeat(155));
                System.out.printf("  Total rows: %d%n", rowCount);
            }
        }

        System.out.println("\n" + "=".repeat(90));
        System.out.println("  End of Report");
        System.out.println("=".repeat(90) + "\n");
    }

    // ----------------------------------------------------------------- helper

    private static Integer resolveLatestRunId(Connection conn) throws SQLException {
        String sql = "SELECT run_id FROM run_metadata ORDER BY run_id DESC LIMIT 1";
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            return rs.next() ? rs.getInt("run_id") : null;
        }
    }
}
