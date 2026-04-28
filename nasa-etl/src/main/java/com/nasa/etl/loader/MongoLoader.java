package com.nasa.etl.loader;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.nasa.etl.common.LogRecord;
import com.nasa.etl.mongodb.MongoLogBatch;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * MongoLoader – reads raw NASA HTTP log files and inserts parsed records into
 * a MongoDB collection in configurable batches.
 *
 * Both valid AND malformed records are stored; the malformed flag lets the query pipeline skip them while 
 * still allowing the driver to count and report malformed records accurately.
 *
 * This class is also used as a standalone ingestion tool:
 *
 * <pre>
 *   java -cp nasa-etl.jar com.nasa.etl.loader.MongoLoader \
 *     --input /path/to/logs          \
 *     --mongo-uri mongodb://host:27017 \
 *     --database  nasa_etl           \
 *     --collection logs              \
 *     --batch 10000                  \
 *     [--drop]
 * </pre>
 *
 * Bug fixes vs. the original version:
 *   1. {@code batch.clear()} was called on the snapshot list returned by
 *      {@code MongoLogBatch.add()}, which was a no-op (the internal buffer had
 *      already been cleared inside {@code drainBuffer()}). The flush helper now
 *      simply calls {@code insertMany} on the non-empty snapshot.
 *   2. {@code stats.batchesInserted} was incremented inside {@code flush()} even
 *      when the list was empty, causing an off-by-one. The guard
 *      {@code if (batch.isEmpty()) return} prevents that.
 *   3. ISO-8859-1 charset is preserved (the NASA logs use it).
 */
public class MongoLoader {

    public static final int DEFAULT_BATCH_SIZE = 10_000;

    public static class LoadStats {
        public long totalLines; //useful fields for metadata analysis
        public long validRecords;
        public long malformedRecords;
        public long batchesInserted;

        public long getTotalLines()       { return totalLines; }
        public long getValidRecords()     { return validRecords; }
        public long getMalformedRecords() { return malformedRecords; }
        public long getBatchesInserted()  { return batchesInserted; }
    }


    public static void main(String[] args) throws Exception {

        String inputPath     = null;
        String mongoUri      = "mongodb://localhost:27017";
        String databaseName  = "nasa_etl";
        String collectionName = "logs";
        int    batchSize     = DEFAULT_BATCH_SIZE;
        boolean dropCollection = false;

        //parsing through input arguments
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--input":      inputPath      = args[++i]; break;
                case "--mongo-uri":  mongoUri       = args[++i]; break;
                case "--database":   databaseName   = args[++i]; break;
                case "--collection": collectionName = args[++i]; break;
                case "--batch":      batchSize      = Integer.parseInt(args[++i]); break;
                case "--drop":       dropCollection = true; break;
                default: System.err.println("Unknown arg: " + args[i]);
            }
        }

        if (inputPath == null || batchSize <= 0) {
            printUsage();
            System.exit(1);
        }

        try (MongoClient client = MongoClients.create(mongoUri)) {
            MongoDatabase db = client.getDatabase(databaseName);
            MongoCollection<Document> collection = db.getCollection(collectionName);

            // if --drop was passed, deletes the existing collection and gets a fresh handle
            if (dropCollection) {
                collection.drop();
                collection = db.getCollection(collectionName);
            }

            //executes the ingestion process and keeps time of execution time
            long      startMs = System.currentTimeMillis();
            LoadStats stats   = load(Paths.get(inputPath), collection, batchSize);
            long      runtimeMs = System.currentTimeMillis() - startMs;

            System.out.println("========================================");
            System.out.println(" NASA HTTP Log ETL – MongoDB Loader");
            System.out.println("========================================");
            System.out.printf(" Input       : %s%n", inputPath);
            System.out.printf(" Mongo URI   : %s%n", mongoUri);
            System.out.printf(" Database    : %s%n", databaseName);
            System.out.printf(" Collection  : %s%n", collectionName);
            System.out.printf(" Batch size  : %,d%n", batchSize);
            System.out.printf(" Lines read  : %,d%n", stats.totalLines);
            System.out.printf(" Valid       : %,d%n", stats.validRecords);
            System.out.printf(" Malformed   : %,d%n", stats.malformedRecords);
            System.out.printf(" Batches     : %,d%n", stats.batchesInserted);
            System.out.printf(" Runtime     : %,d ms%n", runtimeMs);
            System.out.println("========================================");
        }
    }


    /**
     * Load all log files from inputPath(file or directory) into collection using the given batchSize
     * @return accumulated load statistics
     */
    public static LoadStats load(Path inputPath,
                                 MongoCollection<Document> collection,
                                 int batchSize) throws IOException {
        LoadStats     stats   = new LoadStats();
        MongoLogBatch batcher = new MongoLogBatch(batchSize);

        // checking if input is a directory or a file and then iterating over all of them
        if (Files.isDirectory(inputPath)) {
            try (java.util.stream.Stream<Path> paths = Files.list(inputPath)) {
                for (Path file : (Iterable<Path>) paths
                        .filter(Files::isRegularFile)
                        .sorted()::iterator) {
                            // batcher is shared so need of a flushing partial batches everytime
                    readFile(file, collection, batcher, stats);
                }
            }
        } else {
            readFile(inputPath, collection, batcher, stats);
        }

        // Flush the final partial batch (if any)
        insertBatch(collection, batcher.flush(), stats);
        return stats;
    }

    private static void readFile(Path file,
                                 MongoCollection<Document> collection,
                                 MongoLogBatch batcher,
                                 LoadStats stats) throws IOException {

        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.ISO_8859_1)) {
            String line;
            while ((line = reader.readLine()) != null) {
                stats.totalLines++;
                LogRecord record = LogRecord.parse(line);

                if (record.isMalformed()) {
                    stats.malformedRecords++;
                } else {
                    stats.validRecords++;
                }

                // add() returns a non-empty snapshot when the batch is full.
                List<Document> ready = batcher.add(record);
                // ready is non-empty only when batch size is filled
                insertBatch(collection, ready, stats);
            }
        }
    }

    //Insert a batch into MongoDB if it is non-empty
    private static void insertBatch(MongoCollection<Document> collection,
                                    List<Document> batch,
                                    LoadStats stats) {
        if (batch.isEmpty()) return;
        collection.insertMany(batch);
        stats.batchesInserted++;
    }

    // prints the correct command syntax, so the user knows how to run the program correctly
    private static void printUsage() {
        System.err.println(
            "Usage: java -cp nasa-etl.jar com.nasa.etl.loader.MongoLoader \\\n" +
            "  --input <local-file-or-directory>     \\\n" +
            "  [--mongo-uri mongodb://localhost:27017] \\\n" +
            "  [--database  nasa_etl]                \\\n" +
            "  [--collection logs]                   \\\n" +
            "  [--batch <batch-size>]                \\\n" +
            "  [--drop]"
        );
    }
}