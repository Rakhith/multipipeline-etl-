package com.nasa.etl.mongodb;

import com.nasa.etl.common.LogRecord;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Insert-buffering helper for the MongoDB ingestion pipeline.
 *
 * Accumulates {@link LogRecord} instances (including malformed ones, which are
 * still stored in Mongo so the malformed flag can be reported) and emits a
 * completed batch — a ready-to-insert {@code List<Document>} — whenever the
 * batch size threshold is crossed. Document batch ids are ISO week ids
 * (YYYYWW), so one week is one logical ETL batch.
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
        int batchId = MongoWeekBatching.batchIdForIsoDate(record.getLogDate());
        buffer.add(MongoLogRecord.toDocument(record, batchId));
        if (buffer.size() >= batchSize) {
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

    private List<Document> drainBuffer() {
        List<Document> snapshot = new ArrayList<>(buffer);
        buffer.clear();
        return snapshot;
    }
}
