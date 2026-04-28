package com.nasa.etl;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.nasa.etl.common.RunMetadata;
import com.nasa.etl.loader.DBLoader;
import com.nasa.etl.loader.MongoLoader;
import com.nasa.etl.mongodb.Query1DailyTraffic;
import com.nasa.etl.mongodb.Query2TopResources;
import com.nasa.etl.mongodb.Query3HourlyError;
import org.bson.Document;
import com.mongodb.client.AggregateIterable;

import java.sql.Connection;
import java.util.List;

/**
 * MongoETLDriver – Main entry point for the MongoDB pipeline of the
 * NASA HTTP Log ETL Framework.
 *
 * This driver mirrors {@code ETLDriver} (the Hadoop MapReduce driver) in
 * structure and output exactly so that both can be compared fairly:
 *
 *   Phase 1 – Ingestion
 *     Reads raw NASA log files from the local filesystem, parses each line with
 *     the shared {@link com.nasa.etl.common.LogRecord} parser, and inserts
 *     documents into MongoDB in configurable batches using
 *     {@link MongoLoader}.  Both valid and malformed records are stored;
 *     the {@code malformed} flag filters them during the query phase.
 *
 *   Phase 2 – MapReduce queries
 *     Runs the three analytical queries using MongoDB's server-side
 *     {@code mapReduce} command.  Each query class exposes MAP_FUNCTION,
 *     REDUCE_FUNCTION, (and for Q2/Q3) FINALIZE_FUNCTION strings that are
 *     sent verbatim to the MongoDB engine, then iterates the inline results
 *     back in Java — analogous to the Hadoop driver reading HDFS part files.
 *
 *   Phase 3 – Relational load
 *     Passes the result rows from each query to {@link DBLoader}, which
 *     bulk-inserts them into PostgreSQL / MySQL using the exact same schema
 *     as the Hadoop pipeline.
 *
 *   Phase 4 – Metadata
 *     Records run metadata (pipeline name, batch size, runtimes, record
 *     counts) in the {@code run_metadata} and {@code batch_metadata} tables.
 *
 * Usage:
 *   java -cp nasa-etl.jar com.nasa.etl.MongoETLDriver \
 *     --input      <local-log-file-or-dir>  \
 *     --mongo-uri  <mongodb-connection-uri>  \
 *     --database   <mongo-db-name>           \
 *     --collection <mongo-collection-name>   \
 *     --db-url     <jdbc-url>                \
 *     --db-user    <db-username>             \
 *     --db-pass    <db-password>             \
 *     --batch      <batch-size>              \
 *     [--drop]                               \
 *     [--pipeline-name <label>]
 *
 * Example:
 *   java -cp nasa-etl.jar com.nasa.etl.MongoETLDriver \
 *     --input      /data/nasa-logs             \
 *     --mongo-uri  mongodb://localhost:27017    \
 *     --database   nasa_etl                    \
 *     --collection logs                        \
 *     --db-url     jdbc:postgresql://localhost:5432/nasa_etl \
 *     --db-user    postgres                    \
 *     --db-pass    secret                      \
 *     --batch      50000                       \
 *     --drop
 */
public class MongoETLDriver {

    private static final String DEFAULT_PIPELINE_NAME = "MongoDB-MapReduce";
    private static final String DEFAULT_MONGO_URI     = "mongodb://localhost:27017";
    private static final String DEFAULT_DATABASE      = "nasa_etl";
    private static final String DEFAULT_COLLECTION    = "logs";

    // ------------------------------------------------------------------ entry

    public static void main(String[] args) throws Exception {

        // ---- parse arguments ----
        String inputPath     = null;
        String mongoUri      = DEFAULT_MONGO_URI;
        String databaseName  = DEFAULT_DATABASE;
        String collectionName = DEFAULT_COLLECTION;
        String dbUrl         = null;
        String dbUser        = "";
        String dbPass        = "";
        String pipelineName  = DEFAULT_PIPELINE_NAME;
        int    batchSize     = MongoLoader.DEFAULT_BATCH_SIZE;
        boolean dropFirst    = false;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--input":         inputPath      = args[++i]; break;
                case "--mongo-uri":     mongoUri       = args[++i]; break;
                case "--database":      databaseName   = args[++i]; break;
                case "--collection":    collectionName = args[++i]; break;
                case "--db-url":        dbUrl          = args[++i]; break;
                case "--db-user":       dbUser         = args[++i]; break;
                case "--db-pass":       dbPass         = args[++i]; break;
                case "--pipeline-name": pipelineName   = args[++i]; break;
                case "--batch":         batchSize      = Integer.parseInt(args[++i]); break;
                case "--drop":          dropFirst      = true; break;
                default: System.err.println("Unknown arg: " + args[i]);
            }
        }

        if (inputPath == null || dbUrl == null) {
            printUsage();
            System.exit(1);
        }

        // Load JDBC drivers
        try { Class.forName("org.postgresql.Driver"); }
        catch (ClassNotFoundException ignored) {}
        try { Class.forName("com.mysql.cj.jdbc.Driver"); }
        catch (ClassNotFoundException ignored) {}

        // ---- open DB connection, create tables, reserve run ID ----
        Connection conn = DBLoader.openConnection(dbUrl, dbUser, dbPass);
        DBLoader.createTables(conn);
        int runId = DBLoader.createRunMetadata(conn, pipelineName, batchSize);

        System.out.println("========================================");
        System.out.println(" NASA HTTP Log ETL – MongoDB MapReduce ");
        System.out.println("========================================");
        System.out.printf(" Run ID     : %d%n",  runId);
        System.out.printf(" Input      : %s%n",  inputPath);
        System.out.printf(" Mongo URI  : %s%n",  mongoUri);
        System.out.printf(" Database   : %s%n",  databaseName);
        System.out.printf(" Collection : %s%n",  collectionName);
        System.out.printf(" Pipeline   : %s%n",  pipelineName);
        System.out.printf(" Batch size : %,d%n", batchSize);
        System.out.printf(" DB URL     : %s%n",  dbUrl);
        System.out.println("========================================\n");

        // ======== START RUNTIME CLOCK ========
        long globalStart = System.currentTimeMillis();

        // ---- open MongoDB ----
        try (MongoClient mongoClient = MongoClients.create(mongoUri)) {

            MongoDatabase   db         = mongoClient.getDatabase(databaseName);
            MongoCollection<Document> collection = db.getCollection(collectionName);

            if (dropFirst) {
                System.out.println("[INFO] Dropping existing collection...");
                collection.drop();
                collection = db.getCollection(collectionName);
            }

            // ================================================================
            // Phase 1 – Ingestion
            // ================================================================
            System.out.println("[Phase 1] Ingesting log records into MongoDB...");
            long ingestStart = System.currentTimeMillis();

            MongoLoader.LoadStats ingestStats =
                MongoLoader.load(java.nio.file.Paths.get(inputPath), collection, batchSize);

            long ingestEnd = System.currentTimeMillis();
            System.out.printf("[Phase 1] Done. lines=%,d | valid=%,d | malformed=%,d "
                              + "| batches=%,d | time=%,d ms%n",
                ingestStats.totalLines,
                ingestStats.validRecords,
                ingestStats.malformedRecords,
                ingestStats.batchesInserted,
                ingestEnd - ingestStart);

            // ================================================================
            // Phase 2+3 – MapReduce queries + DB load
            // ================================================================

            RunMetadata metadata = new RunMetadata(runId, pipelineName, batchSize);

            // ---- Q1 ----
            System.out.println("\n[Q1] Running Daily Traffic MapReduce...");
            long q1Start = System.currentTimeMillis();
            // List<String[]> q1Rows = Query1DailyTraffic.run(collection);
            AggregateIterable<Document> q1Docs = Query1DailyTraffic.run(collection);
            long q1End   = System.currentTimeMillis();

            int q1Loaded = DBLoader.loadQuery1(conn, q1Docs, runId);
            saveQueryMetadata(conn, metadata, runId, 1, "Q1",
                              q1Start, q1End,
                              ingestStats.validRecords,
                              ingestStats.malformedRecords,
                              ingestStats.batchesInserted);

            System.out.printf("[Q1] runtime=%,d ms | rows=%d%n",
                q1End - q1Start, q1Loaded);

            // ---- Q2 ----
            System.out.println("\n[Q2] Running Top Resources MapReduce...");
            long q2Start = System.currentTimeMillis();
            AggregateIterable<Document> q2Docs = Query2TopResources.run(collection);
            // List<String[]> q2Rows = Query2TopResources.run(collection);
            long q2End   = System.currentTimeMillis();

            int q2Loaded = DBLoader.loadQuery2(conn, q2Docs, runId);
            metadata.setQ2RuntimeMs(q2End - q2Start);
            saveQueryBatchMetadataOnly(conn, runId, "Q2",
                                       ingestStats.validRecords,
                                       q2End - q2Start,
                                       ingestStats.batchesInserted);

            System.out.printf("[Q2] runtime=%,d ms | rows=%d%n",
                q2End - q2Start, q2Loaded);

            // ---- Q3 ----
            System.out.println("\n[Q3] Running Hourly Error MapReduce...");
            long q3Start = System.currentTimeMillis();
            AggregateIterable<Document> q3Docs = Query3HourlyError.run(collection);
            // List<String[]> q3Rows = Query3HourlyError.run(collection);
            long q3End   = System.currentTimeMillis();

            int q3Loaded = DBLoader.loadQuery3(conn, q3Docs, runId);
            metadata.setQ3RuntimeMs(q3End - q3Start);
            saveQueryBatchMetadataOnly(conn, runId, "Q3",
                                       ingestStats.validRecords,
                                       q3End - q3Start,
                                       ingestStats.batchesInserted);

            System.out.printf("[Q3] runtime=%,d ms | rows=%d%n",
                q3End - q3Start, q3Loaded);

            // ======== STOP RUNTIME CLOCK ========
            long globalEnd = System.currentTimeMillis();
            long totalMs   = globalEnd - globalStart;
            metadata.setRuntimeMs(totalMs);
            DBLoader.updateRunMetadata(conn, metadata);
            conn.close();

            // ---- summary ----
            System.out.println("\n========================================");
            System.out.printf(" All queries complete.%n");
            System.out.printf(" Total runtime : %,d ms (%.2f s)%n",
                totalMs, totalMs / 1000.0);
            System.out.printf(" Run ID        : %d%n", runId);
            System.out.println("========================================");
            System.out.println(" Run the report script to view results:");
            System.out.println("   java -cp nasa-etl.jar com.nasa.etl.report.Reporter "
                               + "<db-url> <db-user> <db-pass> " + runId);
            System.out.println("========================================\n");
        }
    }

    // ----------------------------------------------------------------- helpers

    /**
     * Record Q1 stats in RunMetadata and persist per-logical-batch rows to
     * batch_metadata.  Called only for Q1 because Q1 is the "canonical" query
     * that sets totalRecords, malformedCount, and totalBatches in run_metadata.
     */
    private static void saveQueryMetadata(Connection conn,
                                          RunMetadata metadata,
                                          int runId,
                                          int queryNumber,
                                          String queryName,
                                          long startMs, long endMs,
                                          long validRecords,
                                          long malformedRecords,
                                          long batchCount) throws Exception {
        long runtimeMs = endMs - startMs;
        metadata.addQueryStats(queryNumber, runtimeMs,
                               validRecords, malformedRecords, batchCount);
        saveLogicalBatchRows(conn, runId, queryName,
                             validRecords, runtimeMs, batchCount);
        System.out.printf("[%s] runtime=%,d ms | valid=%,d | malformed=%,d "
                          + "| batches=%,d | avg_batch=%.1f%n",
            queryName, runtimeMs, validRecords, malformedRecords,
            batchCount, metadata.getAvgBatchSize());
    }

    /**
     * Persist per-logical-batch rows to batch_metadata for Q2 and Q3.
     * (They share the same ingestion stats as Q1.)
     */
    private static void saveQueryBatchMetadataOnly(Connection conn,
                                                   int runId,
                                                   String queryName,
                                                   long records,
                                                   long runtimeMs,
                                                   long batches) throws Exception {
        saveLogicalBatchRows(conn, runId, queryName, records, runtimeMs, batches);
    }

    /**
     * Distribute records and runtime evenly across logical batch rows.
     * Mirrors the identical method in {@code ETLDriver} so batch_metadata has
     * the same structure regardless of pipeline.
     */
    private static void saveLogicalBatchRows(Connection conn, int runId,
                                             String queryName,
                                             long records, long runtimeMs,
                                             long batches) throws Exception {
        long safeBatches  = Math.max(1, batches);
        long baseRecords  = records / safeBatches;
        long extraRecords = records % safeBatches;
        long baseRuntime  = runtimeMs / safeBatches;
        long extraRuntime = runtimeMs % safeBatches;

        for (int i = 1; i <= safeBatches; i++) {
            long batchRecords = baseRecords + (i <= extraRecords ? 1 : 0);
            long batchRuntime = baseRuntime + (i <= extraRuntime ? 1 : 0);
            DBLoader.saveBatchMetadata(conn, runId, i, queryName,
                                       batchRecords, batchRuntime);
        }
    }

    // ----------------------------------------------------------------- usage

    private static void printUsage() {
        System.err.println(
            "Usage: java -cp nasa-etl.jar com.nasa.etl.MongoETLDriver \\\n" +
            "  --input      <local-log-file-or-dir>     \\\n" +
            "  --db-url     <jdbc-url>                  \\\n" +
            "  --db-user    <db-username>               \\\n" +
            "  --db-pass    <db-password>               \\\n" +
            "  [--mongo-uri  mongodb://localhost:27017]  \\\n" +
            "  [--database   nasa_etl]                  \\\n" +
            "  [--collection logs]                      \\\n" +
            "  [--batch      <batch-size>]              \\\n" +
            "  [--drop]                                 \\\n" +
            "  [--pipeline-name <label>]                \n\n" +
            "JDBC URL examples:\n" +
            "  PostgreSQL : jdbc:postgresql://localhost:5432/nasa_etl\n" +
            "  MySQL      : jdbc:mysql://localhost:3306/nasa_etl"
        );
    }
}