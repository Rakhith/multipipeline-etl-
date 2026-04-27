package com.nasa.etl.mongodb;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.Arrays;

/**
 * Query 1 - Daily Traffic Summary.
 */
public class Query1DailyTraffic {

    private Query1DailyTraffic() {}

    public static AggregateIterable<Document> run(MongoCollection<Document> logs) {
        return logs.aggregate(Arrays.asList(
            new Document("$match", new Document("malformed", false)),
            new Document("$group", new Document("_id", new Document()
                    .append("batchId", "$batchId")
                    .append("logDate", "$logDate")
                    .append("statusCode", "$statusCode"))
                .append("requestCount", new Document("$sum", 1))
                .append("totalBytes", new Document("$sum", "$bytesTransferred"))),
            new Document("$project", new Document("_id", 0)
                .append("batchId", "$_id.batchId")
                .append("logDate", "$_id.logDate")
                .append("statusCode", "$_id.statusCode")
                .append("requestCount", 1)
                .append("totalBytes", 1)),
            new Document("$sort", new Document("batchId", 1)
                .append("logDate", 1)
                .append("statusCode", 1))
        ));
    }
}
