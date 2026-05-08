package com.nasa.etl.pig.udf;

import com.nasa.etl.common.LogRecord;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;

/**
 * Pig UDF: LogParser
 *
 * Parses one raw NASA HTTP log line into a structured Tuple.
 * Reuses the shared {@link LogRecord} parser so parsing rules are
 * identical to the MapReduce pipeline.
 *
 * Input  : chararray  (one raw log line)
 * Output : tuple (host, log_date, log_hour, http_method, resource_path,
 *                 protocol_version, status_code, bytes_transferred,
 *                 batch_id, malformed)
 *
 * Fields:
 *   0  host             : chararray
 *   1  log_date         : chararray  (YYYY-MM-DD)
 *   2  log_hour         : int
 *   3  http_method      : chararray
 *   4  resource_path    : chararray
 *   5  protocol_version : chararray
 *   6  status_code      : int
 *   7  bytes_transferred: long
 *   8  batch_id         : int        (ISO week-based: YYYYWW)
 *   9  malformed        : int        (0 = valid, 1 = malformed)
 */
public class LogParser extends EvalFunc<Tuple> {

    private static final TupleFactory TF = TupleFactory.getInstance();

    @Override
    public Tuple exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) return malformedTuple("");
        Object o = input.get(0);
        if (o == null) return malformedTuple("");

        String line = o.toString();
        LogRecord rec = LogRecord.parse(line);

        Tuple out = TF.newTuple(10);
        out.set(0, rec.getHost()            != null ? rec.getHost()            : "");
        out.set(1, rec.getLogDate()         != null ? rec.getLogDate()         : "");
        out.set(2, rec.getLogHour());
        out.set(3, rec.getHttpMethod()      != null ? rec.getHttpMethod()      : "");
        out.set(4, rec.getResourcePath()    != null ? rec.getResourcePath()    : "");
        out.set(5, rec.getProtocolVersion() != null ? rec.getProtocolVersion() : "");
        out.set(6, rec.getStatusCode());
        out.set(7, rec.getBytesTransferred());
        out.set(8, rec.isMalformed() ? 0 : WeekBatchUDF.computeBatchId(rec.getLogDate()));
        out.set(9, rec.isMalformed() ? 1 : 0);
        return out;
    }

    private static Tuple malformedTuple(String line) throws IOException {
        Tuple out = TF.newTuple(10);
        out.set(0, "");
        out.set(1, "");
        out.set(2, 0);
        out.set(3, "");
        out.set(4, "");
        out.set(5, "");
        out.set(6, 0);
        out.set(7, 0L);
        out.set(8, 0);
        out.set(9, 1);
        return out;
    }

    @Override
    public Schema outputSchema(Schema input) {
        try {
            Schema tupleSchema = new Schema();
            tupleSchema.add(new Schema.FieldSchema("host",             DataType.CHARARRAY));
            tupleSchema.add(new Schema.FieldSchema("log_date",         DataType.CHARARRAY));
            tupleSchema.add(new Schema.FieldSchema("log_hour",         DataType.INTEGER));
            tupleSchema.add(new Schema.FieldSchema("http_method",      DataType.CHARARRAY));
            tupleSchema.add(new Schema.FieldSchema("resource_path",    DataType.CHARARRAY));
            tupleSchema.add(new Schema.FieldSchema("protocol_version", DataType.CHARARRAY));
            tupleSchema.add(new Schema.FieldSchema("status_code",      DataType.INTEGER));
            tupleSchema.add(new Schema.FieldSchema("bytes_transferred",DataType.LONG));
            tupleSchema.add(new Schema.FieldSchema("batch_id",         DataType.INTEGER));
            tupleSchema.add(new Schema.FieldSchema("malformed",        DataType.INTEGER));
            return new Schema(new Schema.FieldSchema("parsed_record",  tupleSchema, DataType.TUPLE));
        } catch (Exception e) {
            return null;
        }
    }
}