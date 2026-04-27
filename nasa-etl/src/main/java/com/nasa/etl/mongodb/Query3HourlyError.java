package com.nasa.etl.mongodb;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.Arrays;

/**
 * Query 3 - Hourly Error Analysis.
 */
public class Query3HourlyError {

    private Query3HourlyError() {}

    public static AggregateIterable<Document> run(MongoCollection<Document> logs) {
        return logs.aggregate(Arrays.asList(
            new Document("$match", new Document("malformed", false)),
            new Document("$project", new Document()
                .append("batchId", 1)
                .append("logDate", 1)
                .append("logHour", 1)
                .append("host", 1)
                .append("isError", new Document("$cond", Arrays.asList(
                    new Document("$and", Arrays.asList(
                        new Document("$gte", Arrays.asList("$statusCode", 400)),
                        new Document("$lte", Arrays.asList("$statusCode", 599))
                    )),
                    1,
                    0
                )))),
            new Document("$group", new Document("_id", new Document()
                    .append("batchId", "$batchId")
                    .append("logDate", "$logDate")
                    .append("logHour", "$logHour"))
                .append("errorRequestCount", new Document("$sum", "$isError"))
                .append("totalRequestCount", new Document("$sum", 1))
                .append("errorHosts", new Document("$addToSet", new Document("$cond", Arrays.asList(
                    new Document("$eq", Arrays.asList("$isError", 1)),
                    "$host",
                    "$$REMOVE"
                ))))),
            new Document("$project", new Document("_id", 0)
                .append("batchId", "$_id.batchId")
                .append("logDate", "$_id.logDate")
                .append("logHour", "$_id.logHour")
                .append("errorRequestCount", 1)
                .append("totalRequestCount", 1)
                .append("errorRate", new Document("$cond", Arrays.asList(
                    new Document("$gt", Arrays.asList("$totalRequestCount", 0)),
                    new Document("$divide", Arrays.asList("$errorRequestCount", "$totalRequestCount")),
                    0
                )))
                .append("distinctErrorHosts", new Document("$size", "$errorHosts"))),
            new Document("$sort", new Document("batchId", 1)
                .append("logDate", 1)
                .append("logHour", 1))
        ));
    }
}
