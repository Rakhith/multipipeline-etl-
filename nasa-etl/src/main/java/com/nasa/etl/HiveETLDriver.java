package com.nasa.etl;

import com.nasa.etl.common.RunMetadata;
import com.nasa.etl.hive.loader.HiveDBLoader;

import java.io.File;
import java.sql.Connection;
import java.util.*;

/**
 * HiveETLDriver – Main entry point for the NASA HTTP Log ETL Hive Pipeline.
 *
 * Mirrors {@link com.nasa.etl.ETLDriver} and {@link com.nasa.etl.PigETLDriver}
 * exactly: same CLI flags, same DB tables, same runtime measurement semantics,
 * same proportional batch-runtime-slicing algorithm, same console output.
 *
 * Usage:
 *   java -cp nasa-etl.jar com.nasa.etl.hive.driver.HiveETLDriver \
 *     --input      <hdfs-or-local-input-dir>   \
 *     --output     <hdfs-or-local-output-base> \
 *     --db-url     <jdbc-url>                  \
 *     --db-user    <username>                  \
 *     --db-pass    <password>                  \
 *     --hive-jar   <path/to/nasa-etl.jar>      \
 *     --batch      <batch-size>                \
 *     [--exec-type local|mr|tez|spark]         \
 *     [--hive-db   <hive-database>]            \
 *     [--pipeline-name <name>]
 *
 * Execution flow:
 *   1. Parse args, open DB connection, create tables, reserve run_id.
 *   2. Run setup_table.hql  – creates EXTERNAL table over raw log files.
 *   3. Run Q1 HQL script    → load TSV output → save batch metadata.
 *   4. Run Q2 HQL script    → load TSV output → save batch metadata.
 *   5. Run Q3 HQL script    → load TSV output → save batch metadata.
 *   6. Update run_metadata  with total runtime.
 *
 * Hive is invoked via the `hive` CLI subprocess so that the full Hive runtime
 * is used.  All parameters are passed as --hivevar KEY=VALUE flags, matching
 * the ${hivevar:KEY} placeholders in the .hql scripts.
 *
 * Output directories produced by Hive's INSERT OVERWRITE DIRECTORY are read
 * back as local TSV files by HiveDBLoader (Hive writes part-files that
 * HiveDBLoader::readPartFiles enumerates).
 */
public class HiveETLDriver {

    private static final String DEFAULT_PIPELINE_NAME = "Apache-Hive";
    private static final String DEFAULT_EXEC_TYPE     = "mr";   // mr | tez | spark | local
    private static final String DEFAULT_HIVE_DB       = "default";
    private static final String DEFAULT_INPUT_TABLE   = "nasa_raw_logs";

    // Inner class for proportional runtime slicing — identical to ETLDriver
    private static final class Slice {
        final int    batchId;
        final long   records;
        long         ms;
        final double frac;

        Slice(int batchId, long records, long ms, double frac) {
            this.batchId = batchId;
            this.records = records;
            this.ms      = ms;
            this.frac    = frac;
        }
    }

    // ----------------------------------------------------------------- main

    public static void main(String[] args) throws Exception {

        // ---- parse arguments ----
        String inputDir      = null;
        String outputBase    = null;
        String dbUrl         = null;
        String dbUser        = null;
        String dbPass        = null;
        String hiveJar       = null;
        String execType      = DEFAULT_EXEC_TYPE;
        String pipelineName  = DEFAULT_PIPELINE_NAME;
        String hiveDb        = DEFAULT_HIVE_DB;
        String inputTable    = DEFAULT_INPUT_TABLE;
        int    batchSize     = 10_000;
        String hiveUrl = "jdbc:hive2://localhost:10000";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--input"        : inputDir     = args[++i]; break;
                case "--output"       : outputBase   = args[++i]; break;
                case "--db-url"       : dbUrl        = args[++i]; break;
                case "--db-user"      : dbUser       = args[++i]; break;
                case "--db-pass"      : dbPass       = args[++i]; break;
                case "--hive-jar"     : hiveJar      = args[++i]; break;
                case "--exec-type"    : execType     = args[++i]; break;
                case "--pipeline-name": pipelineName = args[++i]; break;
                case "--hive-db"      : hiveDb       = args[++i]; break;
                case "--input-table"  : inputTable   = args[++i]; break;
                case "--batch"        : batchSize    = Integer.parseInt(args[++i]); break;
                // In the args parsing block, add:
                case "--hive-url": hiveUrl = args[++i]; break;
                default: System.err.println("Unknown arg: " + args[i]);
            }
        }

        if (inputDir == null || outputBase == null || dbUrl == null || hiveJar == null) {
            printUsage();
            System.exit(1);
        }

        // ---- open DB, create schema, reserve run_id ----
        Connection conn = HiveDBLoader.openConnection(
                dbUrl,
                dbUser != null ? dbUser : "",
                dbPass != null ? dbPass : "");
        HiveDBLoader.createTables(conn);
        int runId = HiveDBLoader.createRunMetadata(conn, pipelineName, batchSize);

        System.out.println("========================================");
        System.out.println(" NASA HTTP Log ETL – Apache Hive        ");
        System.out.println("========================================");
        System.out.printf(" Run ID      : %d%n",   runId);
        System.out.printf(" Input       : %s%n",   inputDir);
        System.out.printf(" Output base : %s%n",   outputBase);
        System.out.printf(" Pipeline    : %s%n",   pipelineName);
        System.out.printf(" Batch size  : %,d%n",  batchSize);
        System.out.printf(" Exec type   : %s%n",   execType);
        System.out.printf(" Hive DB     : %s%n",   hiveDb);
        System.out.printf(" Input table : %s%n",   inputTable);
        System.out.printf(" DB URL      : %s%n",   dbUrl);
        System.out.println("========================================\n");

        RunMetadata metadata = new RunMetadata(runId, pipelineName, batchSize);

        // ======== START RUNTIME CLOCK ========
        long globalStart = System.currentTimeMillis();
        Map<Integer, Long> batchRecordCounts = null;

        try {
            // ---- Setup: create external table over raw log files ----
            runHiveScript("setup_table.hql",
                    Map.of(
                        "INPUT_DIR",   inputDir,
                        "INPUT_TABLE", inputTable,
                        "DB_NAME",     hiveDb,
                        "UDF_JAR",     hiveJar,
                        "BATCH_SIZE",  String.valueOf(batchSize)
                    ),
                    execType, hiveUrl);

            // ---- Q1: Daily Traffic Summary ----
            String q1Out = outputBase + "/q1";
            deleteIfExists(q1Out);
            long q1Start = System.currentTimeMillis();
            runHiveScript("query1_daily_traffic.hql",
                    Map.of(
                        "INPUT_TABLE", hiveDb + "." + inputTable,
                        "OUTPUT_DIR",  q1Out,
                        "UDF_JAR",     hiveJar,
                        "BATCH_SIZE",  String.valueOf(batchSize)
                    ),
                    execType, hiveUrl);

            batchRecordCounts = HiveDBLoader.loadQuery1(conn, q1Out, runId);
            long q1End     = System.currentTimeMillis();
            long q1Runtime = q1End - q1Start;

            metadata.setQ1RuntimeMs(q1Runtime);
            metadata.setTotalBatches(batchRecordCounts.size());
            long q1TotalRecords = batchRecordCounts.values().stream()
                                                   .mapToLong(Long::longValue).sum();
            metadata.setTotalRecords(q1TotalRecords);
            metadata.setAvgBatchSize(batchRecordCounts.isEmpty()
                    ? 0.0 : (double) q1TotalRecords / batchRecordCounts.size());
            // Malformed count: best-effort from counter side-car if present
            metadata.setMalformedCount(readCounterFile(q1Out, "MALFORMED_RECORDS"));

            saveBatchMetadata(conn, runId, "Q1", q1Runtime, batchRecordCounts);
            System.out.printf("[Q1] runtime=%,d ms | batches=%,d | records=%,d%n",
                              q1Runtime, batchRecordCounts.size(), q1TotalRecords);

            // ---- Q2: Top Requested Resources ----
            String q2Out = outputBase + "/q2";
            deleteIfExists(q2Out);
            long q2Start = System.currentTimeMillis();
            runHiveScript("query2_top_resources.hql",
                    Map.of(
                        "INPUT_TABLE", hiveDb + "." + inputTable,
                        "OUTPUT_DIR",  q2Out,
                        "UDF_JAR",     hiveJar,
                        "BATCH_SIZE",  String.valueOf(batchSize)
                    ),
                    execType, hiveUrl);

            HiveDBLoader.loadQuery2(conn, q2Out, runId);
            long q2End     = System.currentTimeMillis();
            long q2Runtime = q2End - q2Start;
            metadata.setQ2RuntimeMs(q2Runtime);

            saveBatchMetadata(conn, runId, "Q2", q2Runtime, batchRecordCounts);
            System.out.printf("[Q2] runtime=%,d ms | batches=%,d%n",
                              q2Runtime, batchRecordCounts.size());

            // ---- Q3: Hourly Error Analysis ----
            String q3Out = outputBase + "/q3";
            deleteIfExists(q3Out);
            long q3Start = System.currentTimeMillis();
            runHiveScript("query3_hourly_error.hql",
                    Map.of(
                        "INPUT_TABLE", hiveDb + "." + inputTable,
                        "OUTPUT_DIR",  q3Out,
                        "UDF_JAR",     hiveJar,
                        "BATCH_SIZE",  String.valueOf(batchSize)
                    ),
                    execType,hiveUrl);

            HiveDBLoader.loadQuery3(conn, q3Out, runId);
            long q3End     = System.currentTimeMillis();
            long q3Runtime = q3End - q3Start;
            metadata.setQ3RuntimeMs(q3Runtime);

            saveBatchMetadata(conn, runId, "Q3", q3Runtime, batchRecordCounts);
            System.out.printf("[Q3] runtime=%,d ms | batches=%,d%n",
                              q3Runtime, batchRecordCounts.size());

        } finally {
            // ======== STOP RUNTIME CLOCK ========
            long globalEnd = System.currentTimeMillis();
            long totalMs   = globalEnd - globalStart;
            metadata.setRuntimeMs(totalMs);
            HiveDBLoader.updateRunMetadata(conn, metadata);
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
    }

    // ----------------------------------------------------------------- Hive execution

    /**
     * Invokes the Hive CLI as a subprocess.
     *
     * Script resolution order:
     *   1. scripts/<name>          (relative to CWD)
     *   2. ../scripts/<name>       (when running from inside hive/ folder)
     *   3. hive/scripts/<name>
     *   4. scripts/hive/<name>
     *
     * All parameters are passed as --hivevar KEY=VALUE.
     * The exec-type maps to Hive's --hiveconf hive.execution.engine setting.
     *
     * @param scriptName  filename of the .hql script (e.g. "query1_daily_traffic.hql")
     * @param params      map of hivevar KEY → VALUE substitutions
     * @param execType    "mr" | "tez" | "spark" | "local"
     */
    private static void runHiveScript(String scriptName,
                                  Map<String, String> params,
                                  String execType,
                                  String hiveUrl) throws Exception {

    // Read the original script
    String scriptPath = resolveScript(scriptName);
    String scriptContent = new String(
        java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(scriptPath)),
        java.nio.charset.StandardCharsets.UTF_8);

    // Substitute all ${hiveconf:KEY} and ${hivevar:KEY} placeholders
    for (Map.Entry<String, String> e : params.entrySet()) {
        String key = e.getValue() == null ? "" : e.getValue();
        scriptContent = scriptContent
            .replace("${hiveconf:" + e.getKey() + "}", key)
            .replace("${hivevar:"  + e.getKey() + "}", key);
    }

    // Write substituted script to a temp file
    java.io.File tempScript = java.io.File.createTempFile("hive_etl_", ".hql");
    tempScript.deleteOnExit();
    java.nio.file.Files.write(tempScript.toPath(),
        scriptContent.getBytes(java.nio.charset.StandardCharsets.UTF_8));

    System.out.println("[HiveDriver] Substituted script written to: "
                       + tempScript.getAbsolutePath());

    // Build beeline command — no --hivevar or --hiveconf for params needed
    List<String> cmd = new ArrayList<>();
    cmd.add("beeline");
    cmd.add("-u");
    cmd.add(hiveUrl);
    cmd.add("--hiveconf");
    cmd.add("hive.execution.engine=" + mapExecType(execType));
    cmd.add("--hiveconf");
    cmd.add("hive.variable.substitute=false"); // already substituted, avoid double-processing
    cmd.add("-f");
    cmd.add(tempScript.getAbsolutePath());

    System.out.println("[HiveDriver] Running: " + String.join(" ", cmd));

    ProcessBuilder pb = new ProcessBuilder(cmd);
    pb.inheritIO();
    int exitCode = pb.start().waitFor();

    // Clean up temp file
    tempScript.delete();

    if (exitCode != 0) {
        throw new RuntimeException("Hive script [" + scriptName +
                                   "] failed with exit code " + exitCode);
    }
}

    /**
     * Maps the user-facing --exec-type flag to Hive's engine names.
     * "local" stays as-is (Hive local mode).
     */
    private static String mapExecType(String execType) {
        switch (execType.toLowerCase(Locale.ROOT)) {
            case "mr"    :
            case "mapreduce": return "mr";
            case "tez"   : return "tez";
            case "spark" : return "spark";
            case "local" : return "mr";   // Hive uses mr in local mode too
            default      : return execType;
        }
    }

    private static String resolveScript(String scriptName) {
        String[] candidates = {
            "scripts/"      + scriptName,
            "../scripts/"   + scriptName,
            "hive/scripts/" + scriptName,
            "scripts/hive/" + scriptName,
        };
        for (String c : candidates) {
            if (new File(c).exists()) return c;
        }
        return "scripts/" + scriptName;
    }

    // ----------------------------------------------------------------- batch metadata
    // Proportional runtime slicing — identical algorithm to ETLDriver and PigETLDriver.

    private static void saveBatchMetadata(Connection conn, int runId,
                                          String queryName, long runtimeMs,
                                          Map<Integer, Long> batchRecordCounts)
            throws Exception {
        if (batchRecordCounts == null || batchRecordCounts.isEmpty()) {
            HiveDBLoader.saveBatchMetadata(conn, runId, 1, queryName, 0L, runtimeMs);
            return;
        }

        long totalRecords = 0;
        for (long c : batchRecordCounts.values()) totalRecords += c;

        List<Slice> slices  = new ArrayList<>();
        long        assigned = 0;

        for (Map.Entry<Integer, Long> e : batchRecordCounts.entrySet()) {
            int    batchId = e.getKey();
            long   records = e.getValue();
            double exact   = totalRecords > 0
                             ? ((double) runtimeMs * records) / totalRecords : 0.0;
            long   base    = (long) Math.floor(exact);
            assigned += base;
            slices.add(new Slice(batchId, records, base, exact - base));
        }

        // Largest-remainder rounding to ensure slices sum to runtimeMs exactly
        long remaining = runtimeMs - assigned;
        slices.sort(Comparator.comparingDouble((Slice s) -> s.frac).reversed());
        for (Slice s : slices) {
            if (remaining <= 0) break;
            s.ms++;
            remaining--;
        }
        slices.sort(Comparator.comparingInt(s -> s.batchId));

        for (Slice s : slices) {
            HiveDBLoader.saveBatchMetadata(conn, runId, s.batchId,
                                           queryName, s.records, s.ms);
        }
    }

    // ----------------------------------------------------------------- counter file

    /**
     * Reads a counter value from a KEY=VALUE side-car file (_counters.txt)
     * that a companion script may write alongside Hive output.
     * Returns 0 if the file is absent (best-effort).
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

    /**
     * Deletes a local directory tree or a single file.
     * For HDFS paths the Hive INSERT OVERWRITE DIRECTORY already handles
     * overwriting, so this is only needed for local-mode runs.
     */
    private static void deleteIfExists(String path) {
        File f = new File(path);
        if (f.exists()) deleteRecursive(f);
    }

    private static void deleteRecursive(File f) {
        if (f.isDirectory()) {
            File[] children = f.listFiles();
            if (children != null) {
                for (File c : children) deleteRecursive(c);
            }
        }
        f.delete();
    }

    private static void printUsage() {
        System.err.println(
            "Usage: java -cp nasa-etl.jar com.nasa.etl.hive.driver.HiveETLDriver \\\n" +
            "  --input        <hdfs-or-local-input-dir>   \\\n" +
            "  --output       <hdfs-or-local-output-base> \\\n" +
            "  --db-url       <jdbc-url>                  \\\n" +
            "  --db-user      <username>                  \\\n" +
            "  --db-pass      <password>                  \\\n" +
            "  --hive-jar     <path/to/nasa-etl.jar>      \\\n" +
            "  [--exec-type   local|mr|tez|spark]         \\\n" +
            "  [--hive-db     <hive-database>]            \\\n" +
            "  [--input-table <table-name>]               \\\n" +
            "  [--pipeline-name <name>]                   \\\n" +
            "  [--batch       <batch-size>]               \n\n" +
            "Examples:\n" +
            "  --db-url jdbc:postgresql://localhost:5432/nasa_etl\n" +
            "  --db-url jdbc:mysql://localhost:3306/nasa_etl\n" +
            "  --exec-type mr       # MapReduce execution engine (default)\n" +
            "  --exec-type tez      # Tez execution engine (faster, needs Tez installed)\n" +
            "  --exec-type spark    # Spark execution engine\n" +
            "  --exec-type local    # local mode (single-node testing)"
        );
    }
}