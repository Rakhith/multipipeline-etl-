package com.nasa.etl.mongodb;

import com.nasa.etl.common.LogRecord;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * MongoDB-specific batch builder for parsed NASA log records.
 */
public class MongoLogBatch {

    private final int batchSize;
    private final List<Document> currentBatch;
    private int batchId = 1;

    public MongoLogBatch(int batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than zero");
        }
        this.batchSize = batchSize;
        this.currentBatch = new ArrayList<>(batchSize);
    }

    public List<Document> add(LogRecord record) {
        currentBatch.add(MongoLogRecord.toDocument(record, batchId));

        if (currentBatch.size() == batchSize) {
            return drainBatch();
        }
        return Collections.emptyList();
    }

    public List<Document> flush() {
        if (currentBatch.isEmpty()) {
            return Collections.emptyList();
        }
        return drainBatch();
    }

    private List<Document> drainBatch() {
        List<Document> completedBatch = new ArrayList<>(currentBatch);
        currentBatch.clear();
        batchId++;
        return completedBatch;
    }
}
