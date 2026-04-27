package com.nasa.etl;

import com.nasa.etl.common.RunMetadata;
import com.nasa.etl.loader.DBLoader;
import com.nasa.etl.mapreduce.BatchedLineInputFormat;
import com.nasa.etl.mapreduce.ETLCounters;
import com.nasa.etl.mapreduce.Query1DailyTraffic;
import com.nasa.etl.mapreduce.Query2TopResources;
import com.nasa.etl.mapreduce.Query3HourlyError;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.sql.Connection;
import java.util.UUID;

/**
 * ETLDriver – Main entry point for the NASA HTTP Log ETL Framework.
 *
 * Usage:
 *   hadoop jar nasa-etl.jar com.nasa.etl.ETLDriver \
 *     --input   <hdfs-input-dir>      \
 *     --output  <hdfs-output-base>    \
 *     --db-url  <jdbc-url>            \
 *     --db-user <username>            \
 *     --db-pass <password>            \
 *     --batch   <batch-size>
 *
 * Example:
 *   hadoop jar nasa-etl.jar com.nasa.etl.ETLDriver \
 *     --input   /user/hadoop/nasa-logs      \
 *     --output  /user/hadoop/nasa-output    \
 *     --db-url  jdbc:postgresql://localhost:5432/nasa_etl \
 *     --db-user postgres \
 *     --db-pass secret   \
 *     --batch   50000
 *
 * All three queries run sequentially. The total runtime (used for
 * comparison) covers from reading the first input line to the last
 * DB write, matching the project spec.
 */
public class ETLDriver extends Configured implements Tool {

    // ------------------------------------------------------------------ run

    @Override
    public int run(String[] args) throws Exception {

        // ---- parse arguments ----
        String inputDir   = null;
        String outputBase = null;
        String dbUrl      = null;
        String dbUser     = null;
        String dbPass     = null;
        int    batchSize  = BatchedLineInputFormat.DEFAULT_BATCH_SIZE;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--input"  : inputDir   = args[++i]; break;
                case "--output" : outputBase = args[++i]; break;
                case "--db-url" : dbUrl      = args[++i]; break;
                case "--db-user": dbUser     = args[++i]; break;
                case "--db-pass": dbPass     = args[++i]; break;
                case "--batch"  : batchSize  = Integer.parseInt(args[++i]); break;
                default: System.err.println("Unknown arg: " + args[i]);
            }
        }

        if (inputDir == null || outputBase == null || dbUrl == null) {
            printUsage();
            return 1;
        }

        // ---- configure ----
        Configuration conf = getConf();
        BatchedLineInputFormat.setBatchSize(conf, batchSize);
        conf.set(DBLoader.DB_URL_KEY,  dbUrl);
        conf.set(DBLoader.DB_USER_KEY, dbUser != null ? dbUser : "");
        conf.set(DBLoader.DB_PASS_KEY, dbPass != null ? dbPass : "");

        // Ensure JDBC driver is on the classpath (driver jars in the fat jar)
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException ignored) {}
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException ignored) {}

        String runId = UUID.randomUUID().toString();
        conf.set(DBLoader.RUN_ID_KEY, runId);

        System.out.println("========================================");
        System.out.println(" NASA HTTP Log ETL – Hadoop MapReduce  ");
        System.out.println("========================================");
        System.out.printf(" Run ID     : %s%n", runId);
        System.out.printf(" Input      : %s%n", inputDir);
        System.out.printf(" Output base: %s%n", outputBase);
        System.out.printf(" Batch size : %,d%n", batchSize);
        System.out.printf(" DB URL     : %s%n", dbUrl);
        System.out.println("========================================\n");

        // ---- open DB, create tables ----
        Connection conn = DBLoader.openConnection(conf);
        DBLoader.createTables(conn);

        // ======== START RUNTIME CLOCK ========
        long globalStart = System.currentTimeMillis();

        // ---- Q1 ----
        String q1Out = outputBase + "/q1";
        deleteIfExists(conf, q1Out);
        long q1Start = System.currentTimeMillis();
        Job q1Job = Query1DailyTraffic.buildJob(conf, inputDir, q1Out);
        boolean q1ok = q1Job.waitForCompletion(true);
        long q1End = System.currentTimeMillis();
        if (!q1ok) { System.err.println("Q1 failed"); return 2; }

        Counters q1c = q1Job.getCounters();
        DBLoader.loadQuery1(conn, conf, q1Out, runId, "1");
        saveMetadata(conn, runId, "Q1-DailyTrafficSummary", batchSize,
                     q1Start, q1End, q1c);

        // ---- Q2 ----
        String q2Out = outputBase + "/q2";
        deleteIfExists(conf, q2Out);
        long q2Start = System.currentTimeMillis();
        Job q2Job = Query2TopResources.buildJob(conf, inputDir, q2Out);
        boolean q2ok = q2Job.waitForCompletion(true);
        long q2End = System.currentTimeMillis();
        if (!q2ok) { System.err.println("Q2 failed"); return 3; }

        Counters q2c = q2Job.getCounters();
        DBLoader.loadQuery2(conn, conf, q2Out, runId, "2");
        saveMetadata(conn, runId, "Q2-TopRequestedResources", batchSize,
                     q2Start, q2End, q2c);

        // ---- Q3 ----
        String q3Out = outputBase + "/q3";
        deleteIfExists(conf, q3Out);
        long q3Start = System.currentTimeMillis();
        Job q3Job = Query3HourlyError.buildJob(conf, inputDir, q3Out);
        boolean q3ok = q3Job.waitForCompletion(true);
        long q3End = System.currentTimeMillis();
        if (!q3ok) { System.err.println("Q3 failed"); return 4; }

        Counters q3c = q3Job.getCounters();
        DBLoader.loadQuery3(conn, conf, q3Out, runId, "3");
        saveMetadata(conn, runId, "Q3-HourlyErrorAnalysis", batchSize,
                     q3Start, q3End, q3c);

        // ======== STOP RUNTIME CLOCK ========
        long globalEnd   = System.currentTimeMillis();
        long totalMs     = globalEnd - globalStart;

        conn.close();

        // ---- summary ----
        System.out.println("\n========================================");
        System.out.printf(" All queries complete.%n");
        System.out.printf(" Total runtime : %,d ms (%.2f s)%n",
                          totalMs, totalMs / 1000.0);
        System.out.printf(" Run ID        : %s%n", runId);
        System.out.println("========================================");
        System.out.println(" Run the report script to view results:");
        System.out.println("   java -cp nasa-etl.jar com.nasa.etl.report.Reporter "
                           + "<db-url> <db-user> <db-pass> " + runId);
        System.out.println("========================================\n");

        return 0;
    }

    // ----------------------------------------------------------------- helpers

    private static void saveMetadata(Connection conn, String runId,
                                     String queryName, int batchSize,
                                     long startMs, long endMs,
                                     Counters counters) throws Exception {
        long valid     = counters.findCounter(ETLCounters.VALID_RECORDS)    .getValue();
        long malformed = counters.findCounter(ETLCounters.MALFORMED_RECORDS).getValue();
        long batches   = counters.findCounter(ETLCounters.BATCHES_PROCESSED).getValue();

        RunMetadata meta = new RunMetadata(runId, queryName, batchSize);
        meta.finish(startMs, endMs, valid, malformed, batches);
        DBLoader.saveRunMetadata(conn, meta);

        System.out.printf("[%s] runtime=%,d ms | valid=%,d | malformed=%,d "
                          + "| batches=%,d | avg_batch=%.1f%n",
                          queryName, meta.getRuntimeMs(), valid, malformed,
                          batches, meta.getAvgBatchSize());
    }

    private static void deleteIfExists(Configuration conf, String path)
            throws Exception {
        FileSystem fs = FileSystem.get(conf);
        Path p = new Path(path);
        if (fs.exists(p)) fs.delete(p, true);
    }

    private static void printUsage() {
        System.err.println(
            "Usage: hadoop jar nasa-etl.jar com.nasa.etl.ETLDriver \\\n" +
            "  --input   <hdfs-input-dir>   \\\n" +
            "  --output  <hdfs-output-base> \\\n" +
            "  --db-url  <jdbc-url>         \\\n" +
            "  --db-user <username>         \\\n" +
            "  --db-pass <password>         \\\n" +
            "  [--batch  <batch-size>]      \n\n" +
            "Examples:\n" +
            "  JDBC URL for PostgreSQL: jdbc:postgresql://localhost:5432/nasa_etl\n" +
            "  JDBC URL for MySQL:      jdbc:mysql://localhost:3306/nasa_etl"
        );
    }

    // ----------------------------------------------------------------- main

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new ETLDriver(), args);
        System.exit(exitCode);
    }
}
