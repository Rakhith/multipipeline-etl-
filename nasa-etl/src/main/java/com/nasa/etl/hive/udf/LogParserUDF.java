package com.nasa.etl.hive.udf;

import com.nasa.etl.common.LogRecord;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Hive GenericUDF: parse_log_line
 *
 * Parses one raw NASA HTTP log line into a named struct.
 * Delegates to the shared {@link LogRecord#parse(String)} so parsing rules
 * are byte-for-byte identical to the MapReduce and Pig pipelines.
 *
 * Hive usage (after ADD JAR and CREATE TEMPORARY FUNCTION):
 *   SELECT parse_log_line(line).log_date, parse_log_line(line).status_code
 *   FROM raw_logs;
 *
 * Input  : STRING  (one raw log line)
 * Output : STRUCT<
 *            host             STRING,
 *            log_date         STRING,   -- YYYY-MM-DD
 *            log_hour         INT,
 *            http_method      STRING,
 *            resource_path    STRING,
 *            protocol_version STRING,
 *            status_code      INT,
 *            bytes_transferred BIGINT,
 *            batch_id         INT,      -- ISO week-based YYYYWW
 *            malformed        INT       -- 0=valid, 1=malformed
 *          >
 *
 * NOTE: In practice the HQL scripts prefer the lighter-weight approach of
 * using regexp_extract directly in SQL for performance; this UDF is provided
 * for exact parity with the Pig LogParser UDF and for any Hive job that
 * needs strict parsing equivalence.
 */
@Description(
    name    = "parse_log_line",
    value   = "_FUNC_(line) - Parses a NASA HTTP log line into a named struct",
    extended = "Returns STRUCT<host,log_date,log_hour,http_method,resource_path," +
               "protocol_version,status_code,bytes_transferred,batch_id,malformed>"
)
public class LogParserUDF extends GenericUDF {

    private StringObjectInspector inputOI;

    // Field names for the output struct — must match column names in HQL scripts
    private static final List<String> FIELD_NAMES = Arrays.asList(
        "host", "log_date", "log_hour", "http_method",
        "resource_path", "protocol_version", "status_code",
        "bytes_transferred", "batch_id", "malformed"
    );

    // Reusable output array — Hive reuses these per-row for efficiency
    private final Object[] result = new Object[10];

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
            throws UDFArgumentException {

        if (arguments.length != 1) {
            throw new UDFArgumentLengthException(
                "parse_log_line requires exactly 1 argument, got " + arguments.length);
        }
        if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE ||
            ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory()
                != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException(
                "parse_log_line requires a STRING argument");
        }

        inputOI = (StringObjectInspector) arguments[0];

        // Build output struct OI
        List<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);  // host
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);  // log_date
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);     // log_hour
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);  // http_method
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);  // resource_path
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);  // protocol_version
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);     // status_code
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);    // bytes_transferred
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);     // batch_id
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);     // malformed

        return ObjectInspectorFactory.getStandardStructObjectInspector(FIELD_NAMES, fieldOIs);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments[0] == null || arguments[0].get() == null) {
            return malformedResult();
        }

        String line = inputOI.getPrimitiveJavaObject(arguments[0].get());
        LogRecord rec = LogRecord.parse(line);

        if (rec.isMalformed()) {
            return malformedResult();
        }

        int batchId = WeekBatchUDF.computeBatchId(rec.getLogDate());

        result[0] = new Text(rec.getHost()            != null ? rec.getHost()            : "");
        result[1] = new Text(rec.getLogDate()         != null ? rec.getLogDate()         : "");
        result[2] = new IntWritable(rec.getLogHour());
        result[3] = new Text(rec.getHttpMethod()      != null ? rec.getHttpMethod()      : "");
        result[4] = new Text(rec.getResourcePath()    != null ? rec.getResourcePath()    : "");
        result[5] = new Text(rec.getProtocolVersion() != null ? rec.getProtocolVersion() : "");
        result[6] = new IntWritable(rec.getStatusCode());
        result[7] = new LongWritable(rec.getBytesTransferred());
        result[8] = new IntWritable(batchId);
        result[9] = new IntWritable(0);   // valid

        return result;
    }

    private Object[] malformedResult() {
        result[0] = new Text("");
        result[1] = new Text("");
        result[2] = new IntWritable(0);
        result[3] = new Text("");
        result[4] = new Text("");
        result[5] = new Text("");
        result[6] = new IntWritable(0);
        result[7] = new LongWritable(0L);
        result[8] = new IntWritable(0);
        result[9] = new IntWritable(1);  // malformed
        return result;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "parse_log_line(" + (children.length > 0 ? children[0] : "") + ")";
    }
}