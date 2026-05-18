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
 * Execution flow:
 *   1. Parse args, open DB connection, create tables, reserve run_id.
 *   2. Run setup_table.hql  – creates EXTERNAL table over raw log files.
 *   3. Run Q1 HQL script    → load TSV output → save batch metadata.
 *   4. Run Q2 HQL script    → load TSV output → save batch metadata.
 *   5. Run Q3 HQL script    → load TSV output → save batch metadata.
 *   6. Update run_metadata  with total runtime.
 *
 * Hive is invoked via the `beeline` CLI subprocess so that the full Hive
 * runtime is used. All parameters are substituted directly into the script
 * before execution (${hiveconf:KEY} / ${hivevar:KEY} replaced in-process).
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
        String hiveUrl       = "jdbc:hive2://localhost:10000";
        String hiveUser      = System.getProperty("user.name", "");
        String hivePass      = "";
        int    queryNum      = 0;

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
                case "--hive-url"     : hiveUrl      = args[++i]; break;
                case "--hive-user"    : hiveUser     = args[++i]; break;
                case "--hive-pass"    : hivePass     = args[++i]; break;
                case "--query"        : queryNum     = Integer.parseInt(args[++i]); break;
                default: System.err.println("Unknown arg: " + args[i]);
            }
        }

        if (inputDir == null || outputBase == null || dbUrl == null || hiveJar == null) {
            printUsage();
            System.exit(1);
        }

        if (queryNum < 0 || queryNum > 3) {
            System.err.println("ERROR: Invalid --query value. Must be 1, 2, or 3.");
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
        System.out.printf(" Hive URL    : %s%n",   hiveUrl);
        System.out.printf(" DB URL      : %s%n",   dbUrl);
        System.out.println("========================================\n");

        RunMetadata metadata = new RunMetadata(runId, pipelineName,batchSize);       // ======== START RUNTIME CLOCK ========
        long globalStart = System.currentTimeMillis();
        Map<Integer, Long> batchRecordCounts = null;

        try {
            // ---- Prepare master script for all phases ----
            String q1Out = outputBase + "/q1";
            String q2Out = outputBase + "/q2";
            String q3Out = outputBase + "/q3";
            
            if (queryNum == 0 || queryNum == 1) deleteIfExists(q1Out);
            if (queryNum == 0 || queryNum == 2) deleteIfExists(q2Out);
            if (queryNum == 0 || queryNum == 3) deleteIfExists(q3Out);

            Map<String, String> masterParams = new HashMap<>();
            masterParams.put("INPUT_DIR",   inputDir);
            masterParams.put("INPUT_TABLE", hiveDb + "." + inputTable);
            masterParams.put("DB_NAME",     hiveDb);
            masterParams.put("UDF_JAR",     hiveJar);
            masterParams.put("BATCH_SIZE",  String.valueOf(batchSize));
            masterParams.put("Q1_OUT",      q1Out);
            masterParams.put("Q2_OUT",      q2Out);
            masterParams.put("Q3_OUT",      q3Out);

            // Execute setup + selected queries in ONE beeline session
            runMasterHivePipeline(masterParams, execType, hiveUrl, hiveUser, hivePass, queryNum);

            // ---- Load results sequentially into PostgreSQL ----
            if (queryNum == 0 || queryNum == 1) {
                long q1Start = System.currentTimeMillis();
                batchRecordCounts = HiveDBLoader.loadQuery1(conn, q1Out, runId);
                long q1Runtime = System.currentTimeMillis() - q1Start;
                metadata.setQ1RuntimeMs(q1Runtime);
                
                long q1TotalRecords = batchRecordCounts.values().stream().mapToLong(Long::longValue).sum();
                metadata.setTotalRecords(q1TotalRecords);
                metadata.setTotalBatches(batchRecordCounts.size());
                metadata.setAvgBatchSize(batchRecordCounts.isEmpty() ? 0.0 : (double) q1TotalRecords / batchRecordCounts.size());
                saveBatchMetadata(conn, runId, "Q1", q1Runtime, batchRecordCounts);
            }

            if (queryNum == 0 || queryNum == 2) {
                long q2Start = System.currentTimeMillis();
                HiveDBLoader.loadQuery2(conn, q2Out, runId);
                long q2Runtime = System.currentTimeMillis() - q2Start;
                metadata.setQ2RuntimeMs(q2Runtime);
                saveBatchMetadata(conn, runId, "Q2", q2Runtime, batchRecordCounts);
            }

            if (queryNum == 0 || queryNum == 3) {
                long q3Start = System.currentTimeMillis();
                HiveDBLoader.loadQuery3(conn, q3Out, runId);
                long q3Runtime = System.currentTimeMillis() - q3Start;
                metadata.setQ3RuntimeMs(q3Runtime);
                saveBatchMetadata(conn, runId, "Q3", q3Runtime, batchRecordCounts);
            }

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

    /**
     * Executes the entire Hive pipeline in a single beeline session.
     */
    private static void runMasterHivePipeline(Map<String, String> params,
                                             String execType,
                                             String hiveUrl,
                                             String hiveUser,
                                             String hivePass,
                                             int queryNum) throws Exception {

        List<String> scripts = new ArrayList<>();
        scripts.add("setup_table.hql");
        if (queryNum == 0 || queryNum == 1) {
            scripts.add("query1_daily_traffic.hql");
        }
        if (queryNum == 0 || queryNum == 2) {
            scripts.add("query2_top_resources.hql");
        }
        if (queryNum == 0 || queryNum == 3) {
            scripts.add("query3_hourly_error.hql");
        }

        StringBuilder masterContent = new StringBuilder();
        masterContent.append("-- Master Hive Pipeline Script\n");
        masterContent.append("SET hive.execution.engine=").append(mapExecType(execType)).append(";\n");
        masterContent.append("SET hive.variable.substitute=true;\n\n");

        for (String sName : scripts) {
            String sPath = resolveScript(sName);
            String content = new String(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(sPath)),
                                       java.nio.charset.StandardCharsets.UTF_8);
            
            // Map specific output dirs to the unified script variables
            if (sName.contains("query1")) content = content.replace("${OUTPUT_DIR}", "${Q1_OUT}");
            if (sName.contains("query2")) content = content.replace("${OUTPUT_DIR}", "${Q2_OUT}");
            if (sName.contains("query3")) content = content.replace("${OUTPUT_DIR}", "${Q3_OUT}");

            // Correct INPUT_TABLE for setup vs queries
            if (sName.equals("setup_table.hql")) {
                // In setup, we use raw table name (passed as INPUT_TABLE)
                // But our masterParams has "DB.TABLE". Let's fix that.
                String rawTable = params.get("INPUT_TABLE");
                if (rawTable.contains(".")) rawTable = rawTable.substring(rawTable.lastIndexOf(".") + 1);
                content = content.replace("${INPUT_TABLE}", rawTable);
            }

            masterContent.append("-- BEGIN ").append(sName).append("\n");
            masterContent.append(content).append("\n");
            masterContent.append("-- END ").append(sName).append("\n\n");
        }

        // Final brute force substitution for any remaining ${var}
        String finalContent = masterContent.toString();
        for (Map.Entry<String, String> e : params.entrySet()) {
            String val = e.getValue() == null ? "" : e.getValue();
            finalContent = finalContent
                .replace("${" + e.getKey() + "}", val)
                .replace("${hivevar:" + e.getKey() + "}", val)
                .replace("${hiveconf:" + e.getKey() + "}", val);
        }

        java.io.File masterScript = java.io.File.createTempFile("hive_master_", ".hql");
        masterScript.deleteOnExit();
        java.nio.file.Files.write(masterScript.toPath(), 
                                finalContent.getBytes(java.nio.charset.StandardCharsets.UTF_8));

        System.out.println("[HiveDriver] Running master script: " + masterScript.getAbsolutePath());

        List<String> cmd = new ArrayList<>();
        cmd.add("beeline");
        cmd.add("-u");
        cmd.add(hiveUrl);
        if (hiveUser != null && !hiveUser.trim().isEmpty()) {
            cmd.add("-n"); cmd.add(hiveUser);
        }
        if (hivePass != null && !hivePass.isEmpty()) {
            cmd.add("-p"); cmd.add(hivePass);
        }
        cmd.add("-f");
        cmd.add(masterScript.getAbsolutePath());

        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.inheritIO();
        int exitCode = pb.start().waitFor();
        masterScript.delete();

        if (exitCode != 0) {
            throw new RuntimeException("Master Hive pipeline failed with exit code " + exitCode);
        }
    }

    /**
     * Maps the user-facing --exec-type flag to Hive's engine names.
     * "local" in this driver means local MR mode (not embedded).
     */
    private static String mapExecType(String execType) {
        switch (execType.toLowerCase(Locale.ROOT)) {
            case "mr"       :
            case "mapreduce": return "mr";
            case "tez"      : return "tez";
            case "spark"    : return "spark";
            case "local"    : return "mr";   // Hive uses mr in local mode too
            default         : return execType;
        }
    }

    /**
     * Resolves the path to a Hive script.
     * Checks scripts/hive/ first since that is the canonical location.
     */
    private static String resolveScript(String scriptName) {
        String[] candidates = {
            "scripts/hive/" + scriptName,       // canonical – run from nasa-etl/
            "../scripts/hive/" + scriptName,    // run from nasa-etl/scripts/
            "scripts/"      + scriptName,       // flat layout fallback
            "../scripts/"   + scriptName,
            "hive/scripts/" + scriptName,
        };
        for (String c : candidates) {
            if (new File(c).exists()) return c;
        }
        throw new RuntimeException(
            "Cannot find Hive script '" + scriptName + "'. " +
            "Expected at: scripts/hive/" + scriptName +
            " (run from the nasa-etl directory)");
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
     * Reads a counter value from a KEY=VALUE side-car file (_counters.txt).
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
            "Usage: java -cp nasa-etl.jar com.nasa.etl.HiveETLDriver \\\n" +
            "  --input        <hdfs-or-local-input-dir>   \\\n" +
            "  --output       <hdfs-or-local-output-base> \\\n" +
            "  --db-url       <jdbc-url>                  \\\n" +
            "  --db-user      <username>                  \\\n" +
            "  --db-pass      <password>                  \\\n" +
            "  --hive-jar     <path/to/nasa-etl.jar>      \\\n" +
            "  [--query       <1|2|3>]                    \\\n" +
            "  [--hive-url    <beeline-jdbc-url>]         \\\n" +
            "  [--hive-user   <hive-username>]            \\\n" +
            "  [--hive-pass   <hive-password>]            \\\n" +
            "  [--exec-type   local|mr|tez|spark]         \\\n" +
            "  [--hive-db     <hive-database>]            \\\n" +
            "  [--input-table <table-name>]               \\\n" +
            "  [--pipeline-name <name>]                   \\\n" +
            "  [--batch       <batch-size>]               \n\n" +
            "Examples:\n" +
            "  --db-url jdbc:postgresql://localhost:5432/nasa_etl\n" +
            "  --hive-url jdbc:hive2://localhost:10000\n" +
            "  --hive-url jdbc:hive2://    # embedded/local HiveServer2\n" +
            "  --exec-type mr       # MapReduce execution engine (default)\n" +
            "  --exec-type tez      # Tez execution engine (faster, needs Tez)\n" +
            "  --exec-type spark    # Spark execution engine\n" +
            "  --exec-type local    # local mode (single-node testing)"
        );
    }
}
