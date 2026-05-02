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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

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

    private static final String DEFAULT_PIPELINE_NAME = "Hadoop-MapReduce";


    @Override
    public int run(String[] args) throws Exception {

        // ---- parse arguments ----
        String inputDir   = null;
        String outputBase = null;
        String dbUrl      = null;
        String dbUser     = null;
        String dbPass     = null;
        String pipelineName = DEFAULT_PIPELINE_NAME;
        int    batchSize  = BatchedLineInputFormat.DEFAULT_BATCH_SIZE;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--input"  : inputDir   = args[++i]; break;
                case "--output" : outputBase = args[++i]; break;
                case "--db-url" : dbUrl      = args[++i]; break;
                case "--db-user": dbUser     = args[++i]; break;
                case "--db-pass": dbPass     = args[++i]; break;
                case "--pipeline-name": pipelineName = args[++i]; break;
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

        // ---- open DB, create tables, reserve run id ----
        Connection conn = DBLoader.openConnection(conf);
        DBLoader.createTables(conn);
        int runId = DBLoader.createRunMetadata(conn, pipelineName, batchSize);
        conf.setInt(DBLoader.RUN_ID_KEY, runId);

        System.out.println("========================================");
        System.out.println(" NASA HTTP Log ETL – Hadoop MapReduce  ");
        System.out.println("========================================");
        System.out.printf(" Run ID     : %s%n", runId);
        System.out.printf(" Input      : %s%n", inputDir);
        System.out.printf(" Output base: %s%n", outputBase);
        System.out.printf(" Pipeline   : %s%n", pipelineName);
        System.out.printf(" Batch size : %,d%n", batchSize);
        System.out.printf(" DB URL     : %s%n", dbUrl);
        System.out.println("========================================\n");

        // ======== START RUNTIME CLOCK ========
        long globalStart = System.currentTimeMillis();
        RunMetadata metadata = new RunMetadata(runId, pipelineName, batchSize);
        Map<Integer, Long> batchRecordCounts = null;

        // ---- Q1 ----
        String q1Out = outputBase + "/q1";
        deleteIfExists(conf, q1Out);
        long q1Start = System.currentTimeMillis();
        Job q1Job = Query1DailyTraffic.buildJob(conf, inputDir, q1Out);
        boolean q1ok = q1Job.waitForCompletion(true);
        long q1End = System.currentTimeMillis();
        if (!q1ok) { System.err.println("Q1 failed"); return 2; }

        Counters q1c = q1Job.getCounters();
        DBLoader.loadQuery1(conn, conf, q1Out, runId);
        batchRecordCounts = DBLoader.getQ1BatchRecordCounts(conn, runId);
        saveQ1Metadata(conn, metadata, runId, "Q1", q1Start, q1End, q1c, batchRecordCounts);

        // ---- Q2 ----
        String q2Out = outputBase + "/q2";
        deleteIfExists(conf, q2Out);
        long q2Start = System.currentTimeMillis();
        Job q2Job = Query2TopResources.buildJob(conf, inputDir, q2Out);
        boolean q2ok = q2Job.waitForCompletion(true);
        long q2End = System.currentTimeMillis();
        if (!q2ok) { System.err.println("Q2 failed"); return 3; }

        Counters q2c = q2Job.getCounters();
        DBLoader.loadQuery2(conn, conf, q2Out, runId);
        saveQueryBatchMetadata(conn, metadata, runId, "Q2", q2Start, q2End, q2c, batchRecordCounts);

        // ---- Q3 ----
        String q3Out = outputBase + "/q3";
        deleteIfExists(conf, q3Out);
        long q3Start = System.currentTimeMillis();
        Job q3Job = Query3HourlyError.buildJob(conf, inputDir, q3Out);
        boolean q3ok = q3Job.waitForCompletion(true);
        long q3End = System.currentTimeMillis();
        if (!q3ok) { System.err.println("Q3 failed"); return 4; }

        Counters q3c = q3Job.getCounters();
        DBLoader.loadQuery3(conn, conf, q3Out, runId);
        saveQueryBatchMetadata(conn, metadata, runId, "Q3", q3Start, q3End, q3c, batchRecordCounts);

        // ======== STOP RUNTIME CLOCK ========
        long globalEnd   = System.currentTimeMillis();
        long totalMs     = globalEnd - globalStart;
        metadata.setRuntimeMs(totalMs);
        DBLoader.updateRunMetadata(conn, metadata);

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

    private static void saveQ1Metadata(Connection conn, RunMetadata metadata,
                                       int runId, String queryName,
                                       long startMs, long endMs,
                                       Counters counters,
                                       Map<Integer, Long> batchRecordCounts) throws Exception {
        long valid     = counters.findCounter(ETLCounters.VALID_RECORDS)    .getValue();
        long malformed = counters.findCounter(ETLCounters.MALFORMED_RECORDS).getValue();
        long runtimeMs = endMs - startMs;

        metadata.setQ1RuntimeMs(runtimeMs);
        metadata.setTotalRecords(valid);
        metadata.setMalformedCount(malformed);
        metadata.setTotalBatches(batchRecordCounts.size());
        metadata.setAvgBatchSize(batchRecordCounts.isEmpty()
                                 ? 0.0 : (double) valid / batchRecordCounts.size());

        saveBatchRowsFromCounts(conn, runId, queryName, runtimeMs, batchRecordCounts);

        System.out.printf("[%s] runtime=%,d ms | valid=%,d | malformed=%,d "
                          + "| batches=%,d | avg_batch=%.1f%n",
                          queryName, runtimeMs, valid, malformed,
                          metadata.getTotalBatches(), metadata.getAvgBatchSize());
    }

    private static void saveQueryBatchMetadata(Connection conn, RunMetadata metadata,
                                               int runId, String queryName,
                                               long startMs, long endMs,
                                               Counters counters,
                                               Map<Integer, Long> batchRecordCounts)
            throws Exception {
        long valid = counters.findCounter(ETLCounters.VALID_RECORDS).getValue();
        long malformed = counters.findCounter(ETLCounters.MALFORMED_RECORDS).getValue();
        long runtimeMs = endMs - startMs;

        if ("Q2".equals(queryName)) metadata.setQ2RuntimeMs(runtimeMs);
        if ("Q3".equals(queryName)) metadata.setQ3RuntimeMs(runtimeMs);

        saveBatchRowsFromCounts(conn, runId, queryName, runtimeMs, batchRecordCounts);

        System.out.printf("[%s] runtime=%,d ms | valid=%,d | malformed=%,d "
                          + "| batches=%,d | avg_batch=%.1f%n",
                          queryName, runtimeMs, valid, malformed,
                          metadata.getTotalBatches(), metadata.getAvgBatchSize());
    }

    private static void saveBatchRowsFromCounts(Connection conn, int runId,
                                                String queryName, long runtimeMs,
                                                Map<Integer, Long> batchRecordCounts)
            throws Exception {
        if (batchRecordCounts == null || batchRecordCounts.isEmpty()) {
            DBLoader.saveBatchMetadata(conn, runId, 1, queryName, 0L, runtimeMs);
            return;
        }

        long totalRecords = 0;
        for (long count : batchRecordCounts.values()) totalRecords += count;

        List<BatchRuntimeSlice> slices = new ArrayList<>();
        long assigned = 0;

        for (Map.Entry<Integer, Long> entry : batchRecordCounts.entrySet()) {
            int batchId = entry.getKey();
            long records = entry.getValue();
            double exact = totalRecords > 0
                           ? ((double) runtimeMs * records) / totalRecords
                           : 0.0;
            long base = (long) Math.floor(exact);
            assigned += base;
            slices.add(new BatchRuntimeSlice(batchId, records, base, exact - base));
        }

        long remaining = runtimeMs - assigned;
        slices.sort(Comparator.comparingDouble(BatchRuntimeSlice::fractionalRemainder).reversed());
        for (int i = 0; i < slices.size() && remaining > 0; i++, remaining--) {
            slices.get(i).runtimeMs++;
        }
        slices.sort(Comparator.comparingInt(BatchRuntimeSlice::batchId));

        for (BatchRuntimeSlice slice : slices) {
            DBLoader.saveBatchMetadata(conn, runId, slice.batchId, queryName,
                                       slice.records, slice.runtimeMs);
        }
    }

    private static class BatchRuntimeSlice {
        final int batchId;
        final long records;
        long runtimeMs;
        final double fractional;

        BatchRuntimeSlice(int batchId, long records, long runtimeMs, double fractional) {
            this.batchId = batchId;
            this.records = records;
            this.runtimeMs = runtimeMs;
            this.fractional = fractional;
        }

        int batchId() { return batchId; }
        double fractionalRemainder() { return fractional; }
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
            "  [--pipeline-name <name>]     \\\n" +
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
