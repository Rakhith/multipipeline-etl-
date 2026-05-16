package com.nasa.etl.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.IntWritable;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.time.temporal.WeekFields;

/**
 * Hive GenericUDF: week_batch_id
 *
 * Computes the ISO calendar-week batch ID for a given date string.
 * Batch ID format : YYYYWW  (ISO week-based year × 100 + ISO week number)
 * Example         : '1995-07-01'  →  199526
 *
 * This mirrors {@link com.nasa.etl.mapreduce.WeekBatching#batchIdForIsoDate}
 * and {@link com.nasa.etl.pig.udf.WeekBatchUDF} exactly, so batch IDs are
 * identical across MapReduce, Pig, and Hive pipelines.
 *
 * Hive usage (after ADD JAR and CREATE TEMPORARY FUNCTION):
 *   SELECT week_batch_id(log_date) FROM nasa_logs;
 *
 * Input  : STRING  (ISO date string, e.g. '1995-07-01')
 * Output : INT     (batch ID, 0 if the date cannot be parsed)
 */
@Description(
    name      = "week_batch_id",
    value     = "_FUNC_(date_str) - Returns YYYYWW ISO week-based batch ID for the given date",
    extended  = "Example: week_batch_id('1995-07-01') returns 199526"
)
public class WeekBatchUDF extends GenericUDF {

    private StringObjectInspector inputOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
            throws UDFArgumentException {

        if (arguments.length != 1) {
            throw new UDFArgumentLengthException(
                "week_batch_id requires exactly 1 argument, got " + arguments.length);
        }

        if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE ||
            ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory()
                != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException(
                "week_batch_id requires a STRING argument");
        }

        inputOI = (StringObjectInspector) arguments[0];
        return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments[0] == null || arguments[0].get() == null) {
            return new IntWritable(0);
        }
        String dateStr = inputOI.getPrimitiveJavaObject(arguments[0].get());
        return new IntWritable(computeBatchId(dateStr));
    }

    /**
     * Static helper reusable from LogParserUDF without instantiating this UDF.
     * Identical algorithm to WeekBatching.batchIdForIsoDate() and
     * WeekBatchUDF.computeBatchId() in the Pig pipeline.
     */
    public static int computeBatchId(String isoDate) {
        if (isoDate == null || isoDate.isEmpty()) return 0;
        try {
            LocalDate  date     = LocalDate.parse(isoDate);
            WeekFields iso      = WeekFields.ISO;
            int        weekYear = date.get(iso.weekBasedYear());
            int        week     = date.get(iso.weekOfWeekBasedYear());
            return weekYear * 100 + week;
        } catch (DateTimeParseException e) {
            return 0;
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return "week_batch_id(" + (children.length > 0 ? children[0] : "") + ")";
    }
}