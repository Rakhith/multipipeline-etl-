package com.nasa.etl;

import com.nasa.etl.common.RunMetadata;
import com.nasa.etl.pig.loader.PigDBLoader;

import java.io.File;
import java.sql.Connection;
import java.util.*;

/**
 * PigETLDriver – Main entry point for the NASA HTTP Log ETL Pig Pipeline.
 *
 * Mirrors {@link com.nasa.etl.ETLDriver} exactly: same CLI flags, same DB tables,
 * same runtime measurement semantics, same metadata reporting.
 *
 * Usage:
 *   java -cp nasa-etl.jar com.nasa.etl.PigETLDriver \
 *     --input      <input-file-or-dir>   \
 *     --output     <output-base-dir>     \
 *     --db-url     <jdbc-url>            \
 *     --db-user    <username>            \
 *     --db-pass    <password>            \
 *     --pig-jar    <path/to/nasa-etl.jar> \
 *     --batch      <batch-size>          \
 *     [--exec-type local|mapreduce]      \
 *     [--pipeline-name <name>]
 *
 * Execution flow:
 *   1. Parse args, open DB connection, create tables, reserve run_id.
 *   2. Run Q1 Pig script  →  load results  →  save metadata.
 *   3. Run Q2 Pig script  →  load results  →  save metadata.
 *   4. Run Q3 Pig script  →  load results  →  save metadata.
 *   5. Update run_metadata with total runtime.
 *
 * The three Pig scripts live in the same JAR's resources or alongside it:
 *   scripts/query1_daily_traffic.pig
 *   scripts/query2_top_resources.pig
 *   scripts/query3_hourly_error.pig
 *
 * Pig is invoked via the `pig` CLI so that the full Pig runtime is used.
 * All parameters are passed as -p (param) flags matching the placeholders
 * defined in the .pig scripts:
 *   INPUT, OUTPUT, UDF_JAR, BATCH_SIZE
 */
public class PigETLDriver {
    private static class Slice {
        private final int batchId;
        private final long records;
        private final long ms;
        private final double frac;

        public Slice(int batchId, long records, long ms, double frac) {
            this.batchId = batchId;
            this.records = records;
            this.ms = ms;
            this.frac = frac;
        }

        public int batchId() { return batchId; }
        public long records() { return records; }
        public long ms() { return ms; }
        public double frac() { return frac; }
    }
    private static final String DEFAULT_PIPELINE_NAME = "Apache-Pig";
    private static final String DEFAULT_EXEC_TYPE     = "local";

    public static void main(String[] args) throws Exception {

        // ---- parse arguments ----
        String inputPath     = null;
        String outputBase    = null;
        String dbUrl         = null;
        String dbUser        = null;
        String dbPass        = null;
        String pigJar        = null;
        String execType      = DEFAULT_EXEC_TYPE;
        String pipelineName  = DEFAULT_PIPELINE_NAME;
        int    batchSize     = 10_000;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--input"        : inputPath    = args[++i]; break;
                case "--output"       : outputBase   = args[++i]; break;
                case "--db-url"       : dbUrl        = args[++i]; break;
                case "--db-user"      : dbUser       = args[++i]; break;
                case "--db-pass"      : dbPass       = args[++i]; break;
                case "--pig-jar"      : pigJar       = args[++i]; break;
                case "--exec-type"    : execType     = args[++i]; break;
                case "--pipeline-name": pipelineName = args[++i]; break;
                case "--batch"        : batchSize    = Integer.parseInt(args[++i]); break;
                default: System.err.println("Unknown arg: " + args[i]);
            }
        }

        if (inputPath == null || outputBase == null || dbUrl == null || pigJar == null) {
            printUsage();
            System.exit(1);
        }

        // ---- open DB, create schema, reserve run_id ----
        Connection conn = PigDBLoader.openConnection(
                dbUrl,
                dbUser != null ? dbUser : "",
                dbPass != null ? dbPass : "");
        PigDBLoader.createTables(conn);
        int runId = PigDBLoader.createRunMetadata(conn, pipelineName, batchSize);

        System.out.println("========================================");
        System.out.println(" NASA HTTP Log ETL – Apache Pig         ");
        System.out.println("========================================");
        System.out.printf(" Run ID      : %d%n",   runId);
        System.out.printf(" Input       : %s%n",   inputPath);
        System.out.printf(" Output base : %s%n",   outputBase);
        System.out.printf(" Pipeline    : %s%n",   pipelineName);
        System.out.printf(" Batch size  : %,d%n",  batchSize);
        System.out.printf(" Exec type   : %s%n",   execType);
        System.out.printf(" DB URL      : %s%n",   dbUrl);
        System.out.println("========================================\n");

        RunMetadata metadata = new RunMetadata(runId, pipelineName, batchSize);

        // ======== START RUNTIME CLOCK ========
        long globalStart = System.currentTimeMillis();
        Map<Integer, Long> batchRecordCounts = null;

        // ---- Q1: Daily Traffic Summary ----
        String q1Out = outputBase + "/q1";
        deleteDir(q1Out);
        long q1Start = System.currentTimeMillis();
        runPigScript("query1_daily_traffic.pig", inputPath, q1Out,
                     pigJar, batchSize, execType, runId);
        long q1End = System.currentTimeMillis();

        batchRecordCounts = PigDBLoader.loadQuery1(conn, q1Out, runId);
        long q1Runtime = q1End - q1Start;

        metadata.setQ1RuntimeMs(q1Runtime);
        metadata.setTotalBatches(batchRecordCounts.size());
        long q1TotalRecords = batchRecordCounts.values().stream()
                                               .mapToLong(Long::longValue).sum();
        metadata.setTotalRecords(q1TotalRecords);
        metadata.setAvgBatchSize(batchRecordCounts.isEmpty()
                ? 0.0 : (double) q1TotalRecords / batchRecordCounts.size());
        // malformed count is embedded in counters file if available
        metadata.setMalformedCount(readCounterFile(q1Out, "MALFORMED_RECORDS"));

        saveBatchMetadata(conn, runId, "Q1", q1Runtime, batchRecordCounts);
        System.out.printf("[Q1] runtime=%,d ms | batches=%,d | records=%,d%n",
                          q1Runtime, batchRecordCounts.size(), q1TotalRecords);

        // ---- Q2: Top Requested Resources ----
        String q2Out = outputBase + "/q2";
        deleteDir(q2Out);
        long q2Start = System.currentTimeMillis();
        runPigScript("query2_top_resources.pig", inputPath, q2Out,
                     pigJar, batchSize, execType, runId);
        long q2End = System.currentTimeMillis();

        PigDBLoader.loadQuery2(conn, q2Out, runId);
        long q2Runtime = q2End - q2Start;
        metadata.setQ2RuntimeMs(q2Runtime);
        saveBatchMetadata(conn, runId, "Q2", q2Runtime, batchRecordCounts);
        System.out.printf("[Q2] runtime=%,d ms | batches=%,d%n",
                          q2Runtime, batchRecordCounts == null ? 0 : batchRecordCounts.size());

        // ---- Q3: Hourly Error Analysis ----
        String q3Out = outputBase + "/q3";
        deleteDir(q3Out);
        long q3Start = System.currentTimeMillis();
        runPigScript("query3_hourly_error.pig", inputPath, q3Out,
                     pigJar, batchSize, execType, runId);
        long q3End = System.currentTimeMillis();

        PigDBLoader.loadQuery3(conn, q3Out, runId);
        long q3Runtime = q3End - q3Start;
        metadata.setQ3RuntimeMs(q3Runtime);
        saveBatchMetadata(conn, runId, "Q3", q3Runtime, batchRecordCounts);
        System.out.printf("[Q3] runtime=%,d ms | batches=%,d%n",
                          q3Runtime, batchRecordCounts == null ? 0 : batchRecordCounts.size());

        // ======== STOP RUNTIME CLOCK ========
        long globalEnd = System.currentTimeMillis();
        long totalMs   = globalEnd - globalStart;
        metadata.setRuntimeMs(totalMs);
        PigDBLoader.updateRunMetadata(conn, metadata);

        conn.close();

        System.out.println("\n========================================");
        System.out.printf(" All queries complete.%n");
        System.out.printf(" Total runtime : %,d ms (%.2f s)%n",
                          totalMs, totalMs / 1000.0);
        System.out.printf(" Run ID        : %d%n", runId);
        System.out.println("========================================");
        System.out.println(" Run the report script to view results:");
        System.out.printf("   java -cp nasa-etl.jar com.nasa.etl.report.Reporter%n" +
                          "        <db-url> <db-user> <db-pass> %d%n", runId);
        System.out.println("========================================\n");
    }

    // ----------------------------------------------------------------- Pig execution

    /**
     * Invokes the Pig CLI as a subprocess.
     *
     * The script is resolved in this order:
     *   1. scripts/<name>  relative to the current working directory
     *   2. ../scripts/<name>  (when running from inside the pig/ folder)
     *
     * Parameters passed to every Pig script:
     *   INPUT      – input path (local or HDFS)
     *   OUTPUT     – output directory for this query
     *   UDF_JAR    – path to the fat JAR containing all UDFs
     *   BATCH_SIZE – records per logical batch (informational; batching is week-based)
     *   RUN_ID     – current run identifier (written to counter files)
     */
    private static void runPigScript(String scriptName,
                                     String input,
                                     String output,
                                     String udfJar,
                                     int batchSize,
                                     String execType,
                                     int runId) throws Exception {
        String scriptPath = resolveScript(scriptName);

        List<String> cmd = new ArrayList<>();
        cmd.add("pig");
        cmd.add("-x"); cmd.add(execType);
        cmd.add("-p"); cmd.add("INPUT="     + input);
        cmd.add("-p"); cmd.add("OUTPUT="    + output);
        cmd.add("-p"); cmd.add("UDF_JAR="   + udfJar);
        cmd.add("-p"); cmd.add("BATCH_SIZE=" + batchSize);
        cmd.add("-p"); cmd.add("RUN_ID="    + runId);
        cmd.add(scriptPath);

        System.out.println("[PigDriver] Running: " + String.join(" ", cmd));

        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.inheritIO();
        int exitCode = pb.start().waitFor();

        if (exitCode != 0) {
            throw new RuntimeException("Pig script [" + scriptName +
                                       "] failed with exit code " + exitCode);
        }
    }

    private static String resolveScript(String scriptName) {
        String[] candidates = {
            "scripts/" + scriptName,
            "../scripts/" + scriptName,
            "pig/scripts/" + scriptName,
            "scripts/pig/" + scriptName,
        };
        for (String c : candidates) {
            if (new File(c).exists()) return c;
        }
        // Fallback: let Pig fail with a clear error
        return "scripts/" + scriptName;
    }

    // ----------------------------------------------------------------- batch metadata helpers
    // (proportional runtime slice – identical algorithm to ETLDriver)

    private static void saveBatchMetadata(Connection conn, int runId,
                                          String queryName, long runtimeMs,
                                          Map<Integer, Long> batchRecordCounts)
            throws Exception {
        if (batchRecordCounts == null || batchRecordCounts.isEmpty()) {
            PigDBLoader.saveBatchMetadata(conn, runId, 1, queryName, 0L, runtimeMs);
            return;
        }

        long totalRecords = 0;
        for (long c : batchRecordCounts.values()) totalRecords += c;

        // Proportional runtime slicing (same as ETLDriver.saveBatchRowsFromCounts)
        // record Slice(int batchId, long records, long ms, double frac) {};
        List<Slice> slices = new ArrayList<>();
        long assigned = 0;

        for (Map.Entry<Integer, Long> e : batchRecordCounts.entrySet()) {
            int  batchId = e.getKey();
            long records = e.getValue();
            double exact = totalRecords > 0
                    ? ((double) runtimeMs * records) / totalRecords : 0.0;
            long base = (long) Math.floor(exact);
            assigned += base;
            slices.add(new Slice(batchId, records, base, exact - base));
        }

        long remaining = runtimeMs - assigned;
        slices.sort(Comparator.comparingDouble(Slice::frac).reversed());
        List<Slice> adjusted = new ArrayList<>();
        for (int i = 0; i < slices.size(); i++) {
            Slice s = slices.get(i);
            long ms = s.ms() + (remaining-- > 0 ? 1 : 0);
            adjusted.add(new Slice(s.batchId(), s.records(), ms, s.frac()));
        }
        adjusted.sort(Comparator.comparingInt(Slice::batchId));

        for (Slice s : adjusted) {
            PigDBLoader.saveBatchMetadata(conn, runId, s.batchId(),
                                          queryName, s.records(), s.ms());
        }
    }

    // ----------------------------------------------------------------- counter file

    /**
     * Reads a simple counter from a side-car file written by the Pig STORE
     * companion script.  File format: one line per counter, KEY=VALUE.
     * Returns 0 if the file doesn't exist (counter tracking is best-effort).
     */
    private static long readCounterFile(String outputDir, String key) {
        try {
            File f = new File(outputDir, "_counters.txt");
            if (!f.exists()) return 0L;
            for (String line : java.nio.file.Files.readAllLines(f.toPath())) {
                if (line.startsWith(key + "=")) {
                    return Long.parseLong(line.substring(key.length() + 1).trim());
                }
            }
        } catch (Exception ignored) {}
        return 0L;
    }

    // ----------------------------------------------------------------- utils

    private static void deleteDir(String path) {
        File f = new File(path);
        if (f.exists()) deleteRecursive(f);
    }

    private static void deleteRecursive(File f) {
        if (f.isDirectory()) {
            File[] children = f.listFiles();
            if (children != null) for (File c : children) deleteRecursive(c);
        }
        f.delete();
    }

    private static void printUsage() {
        System.err.println(
            "Usage: java -cp nasa-etl.jar com.nasa.etl.pig.driver.PigDriver \\\n" +
            "  --input       <input-file-or-dir>          \\\n" +
            "  --output      <output-base-dir>            \\\n" +
            "  --db-url      <jdbc-url>                   \\\n" +
            "  --db-user     <username>                   \\\n" +
            "  --db-pass     <password>                   \\\n" +
            "  --pig-jar     <path/to/nasa-etl.jar>       \\\n" +
            "  [--exec-type  local|mapreduce]             \\\n" +
            "  [--pipeline-name <name>]                   \\\n" +
            "  [--batch      <batch-size>]                \n\n" +
            "Examples:\n" +
            "  --db-url jdbc:postgresql://localhost:5432/nasa_etl\n" +
            "  --db-url jdbc:mysql://localhost:3306/nasa_etl\n" +
            "  --exec-type local          # single-node / pseudo-distributed\n" +
            "  --exec-type mapreduce      # full YARN cluster"
        );
    }
}