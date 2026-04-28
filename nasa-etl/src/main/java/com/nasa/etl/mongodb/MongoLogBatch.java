package com.nasa.etl.mongodb;

import com.nasa.etl.common.LogRecord;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Batching helper for the MongoDB ingestion pipeline.
 *
 * Accumulates {@link LogRecord} instances (including malformed ones, which are
 * still stored in Mongo so the malformed flag can be reported) and emits a
 * completed batch — a ready-to-insert {@code List<Document>} — whenever the
 * batch size threshold is crossed.
 *
 * Usage pattern:
 * <pre>
 *   MongoLogBatch batcher = new MongoLogBatch(batchSize);
 *   for each line:
 *       List<Document> ready = batcher.add(record);
 *       if (!ready.isEmpty()) collection.insertMany(ready);
 *
 *   // after all lines:
 *   List<Document> tail = batcher.flush();
 *   if (!tail.isEmpty()) collection.insertMany(tail);
 * </pre>
 *
 */
public class MongoLogBatch {

    private final int           batchSize;
    private final List<Document> buffer;
    private int batchId = 1; //first batchID

    /** Tracks how many complete batches have been emitted (excludes the tail flush). */
    private int completedBatches = 0;

    public MongoLogBatch(int batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be > 0, got: " + batchSize);
        }
        this.batchSize = batchSize;
        this.buffer    = new ArrayList<>(batchSize);
    }

    /**
     * Add a record to the current batch.
     *
     * @return a non-empty list of documents ready for insertMany operation if the
     *         batch is now full; {@link Collections#emptyList()} otherwise.
     */
    public List<Document> add(LogRecord record) {
        buffer.add(MongoLogRecord.toDocument(record, batchId));
        if (buffer.size() >= batchSize) {
            completedBatches++;
            return drainBuffer();
        }
        return Collections.emptyList(); // signals there is nothing to return - batching incomplete 
    }

    /**
     * Flush any remaining records as a (possibly partial) final batch.
     *
     * @return the remaining documents, or an empty list if the buffer is empty.
     */
    public List<Document> flush() {
        if (buffer.isEmpty()) {
            return Collections.emptyList();
        }
        return drainBuffer();
    }


    public int getCurrentBatchId() { return batchId; }

    public int getCompletedBatches() { return completedBatches; }


    private List<Document> drainBuffer() {
        List<Document> snapshot = new ArrayList<>(buffer);
        buffer.clear();
        batchId++;           // next records belong to the next batch
        return snapshot;
    }
}