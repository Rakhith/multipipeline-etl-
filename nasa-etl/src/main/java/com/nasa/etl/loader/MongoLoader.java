package com.nasa.etl.loader;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.nasa.etl.common.LogRecord;
import com.nasa.etl.mongodb.MongoLogRecord;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class MongoLoader {

    public static final int DEFAULT_BATCH_SIZE = 10_000;

    public static class LoadStats {
        private long totalLines;
        private long validRecords;
        private long malformedRecords;
        private long batchesInserted;

        public long getTotalLines() { return totalLines; }
        public long getValidRecords() { return validRecords; }
        public long getMalformedRecords() { return malformedRecords; }
        public long getBatchesInserted() { return batchesInserted; }
    }

    public static void main(String[] args) throws Exception {
        String inputPath = null;
        String mongoUri = "mongodb://localhost:27017";
        String databaseName = "nasa_etl";
        String collectionName = "logs";
        int batchSize = DEFAULT_BATCH_SIZE;
        boolean dropCollection = false;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--input":
                    inputPath = args[++i];
                    break;
                case "--mongo-uri":
                    mongoUri = args[++i];
                    break;
                case "--database":
                    databaseName = args[++i];
                    break;
                case "--collection":
                    collectionName = args[++i];
                    break;
                case "--batch":
                    batchSize = Integer.parseInt(args[++i]);
                    break;
                case "--drop":
                    dropCollection = true;
                    break;
                default:
                    System.err.println("Unknown arg: " + args[i]);
            }
        }

        if (inputPath == null || batchSize <= 0) {
            printUsage();
            System.exit(1);
        }

        try (MongoClient client = MongoClients.create(mongoUri)) {
            MongoDatabase db = client.getDatabase(databaseName);
            MongoCollection<Document> collection = db.getCollection(collectionName);

            if (dropCollection) {
                collection.drop();
                collection = db.getCollection(collectionName);
            }

            long startMs = System.currentTimeMillis();
            LoadStats stats = load(Paths.get(inputPath), collection, batchSize);
            long runtimeMs = System.currentTimeMillis() - startMs;

            System.out.println("========================================");
            System.out.println(" NASA HTTP Log ETL - MongoDB Loader");
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

    public static LoadStats load(Path inputPath,
                                 MongoCollection<Document> collection,
                                 int batchSize) throws IOException {
        LoadStats stats = new LoadStats();
        List<Document> batch = new ArrayList<>(batchSize);

        if (Files.isDirectory(inputPath)) {
            try (java.util.stream.Stream<Path> paths = Files.list(inputPath)) {
                for (Path file : (Iterable<Path>) paths
                        .filter(Files::isRegularFile)
                        .sorted()::iterator) {
                    readFile(file, collection, batch, batchSize, stats);
                }
            }
        } else {
            readFile(inputPath, collection, batch, batchSize, stats);
        }

        flush(collection, batch, stats);
        return stats;
    }

    private static void readFile(Path inputFile,
                                 MongoCollection<Document> collection,
                                 List<Document> batch,
                                 int batchSize,
                                 LoadStats stats) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(inputFile, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                stats.totalLines++;
                LogRecord record = LogRecord.parse(line);

                int batchId = (int) ((stats.totalLines - 1) / batchSize) + 1;

                if (record.isMalformed()) {
                    stats.malformedRecords++;
                } else {
                    stats.validRecords++;
                }

                batch.add(MongoLogRecord.toDocument(record, batchId));

                if (batch.size() == batchSize) {
                    flush(collection, batch, stats);
                }
            }
        }
    }

    private static void flush(MongoCollection<Document> collection,
                              List<Document> batch,
                              LoadStats stats) {
        if (batch.isEmpty()) {
            return;
        }

        collection.insertMany(batch);
        batch.clear();
        stats.batchesInserted++;
    }

    private static void printUsage() {
        System.err.println(
            "Usage: java -cp nasa-etl.jar com.nasa.etl.loader.MongoLoader \\\n" +
            "  --input <local-file-or-directory> \\\n" +
            "  [--mongo-uri mongodb://localhost:27017] \\\n" +
            "  [--database nasa_etl] \\\n" +
            "  [--collection logs] \\\n" +
            "  [--batch <batch-size>] \\\n" +
            "  [--drop]"
        );
    }
}
