package com.nasa.etl.mongodb;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.Arrays;

/**
 * Query 2 - Top 20 Requested Resources.
 */
public class Query2TopResources {

    private Query2TopResources() {}

    public static AggregateIterable<Document> run(MongoCollection<Document> logs) {
        return logs.aggregate(Arrays.asList(
            new Document("$match", new Document("malformed", false)),
            new Document("$group", new Document("_id", new Document()
                    .append("batchId", "$batchId")
                    .append("resourcePath", "$resourcePath"))
                .append("requestCount", new Document("$sum", 1))
                .append("totalBytes", new Document("$sum", "$bytesTransferred"))
                .append("hosts", new Document("$addToSet", "$host"))),
            new Document("$project", new Document("_id", 0)
                .append("batchId", "$_id.batchId")
                .append("resourcePath", new Document("$ifNull", Arrays.asList("$_id.resourcePath", "(empty)")))
                .append("requestCount", 1)
                .append("totalBytes", 1)
                .append("distinctHostCount", new Document("$size", "$hosts"))),
            new Document("$sort", new Document("batchId", 1).append("requestCount", -1)),
            new Document("$group", new Document("_id", "$batchId")
                .append("rows", new Document("$push", "$$ROOT"))),
            new Document("$project", new Document("_id", 0)
                .append("rows", new Document("$slice", Arrays.asList("$rows", 20)))),
            new Document("$unwind", "$rows"),
            new Document("$replaceRoot", new Document("newRoot", "$rows"))
        ));
    }
}
