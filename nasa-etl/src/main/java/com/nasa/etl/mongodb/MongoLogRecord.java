package com.nasa.etl.mongodb;

import com.nasa.etl.common.LogRecord;
import org.bson.Document;

/**
 * Utility class that converts a LogRecord into a MongoDB BSON Document.
 */
public class MongoLogRecord {

    private MongoLogRecord() {}

    /**
     * Converts a parsed LogRecord into a MongoDB BSON Document.
     * Called once per record during the MongoDB ingestion phase.
     */
    public static Document toDocument(LogRecord r) {
        return toDocument(r, 0);
    }

    /**
     * Converts a parsed LogRecord into a MongoDB BSON Document and tags it with
     * the ingestion batch that flushed it.
     */
    public static Document toDocument(LogRecord r, int batchId) {
        return new Document()
            .append("batchId",          batchId)
            .append("host",             r.getHost())
            .append("timestamp",        r.getTimestamp())
            .append("logDate",          r.getLogDate())
            .append("logHour",          r.getLogHour())
            .append("httpMethod",       r.getHttpMethod())
            .append("resourcePath",     r.getResourcePath())
            .append("protocolVersion",  r.getProtocolVersion())
            .append("statusCode",       r.getStatusCode())
            .append("bytesTransferred", r.getBytesTransferred())
            .append("malformed",        r.isMalformed());
    }
}
