package com.nasa.etl.pig.udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.time.temporal.WeekFields;

/**
 * Pig UDF: WeekBatchUDF
 *
 * Computes the ISO calendar-week batch ID for a given date string.
 * Batch ID format : YYYYWW  (ISO week-based year × 100 + ISO week number)
 * Example         : 1995-07-01  →  199526
 *
 * This mirrors {@link com.nasa.etl.mapreduce.WeekBatching#batchIdForIsoDate}
 * exactly so that batch IDs are identical across the MapReduce and Pig pipelines.
 *
 * Input  : chararray  (ISO date string, e.g. "1995-07-01")
 * Output : int        (batch ID, 0 if the date cannot be parsed)
 */
public class WeekBatchUDF extends EvalFunc<Integer> {

    @Override
    public Integer exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0 || input.get(0) == null) return 0;
        return computeBatchId(input.get(0).toString());
    }

    /**
     * Static helper so {@link LogParser} can reuse the logic without
     * instantiating the UDF.
     */
    public static int computeBatchId(String isoDate) {
        if (isoDate == null || isoDate.isEmpty()) return 0;
        try {
            LocalDate date    = LocalDate.parse(isoDate);
            WeekFields iso    = WeekFields.ISO;
            int weekYear      = date.get(iso.weekBasedYear());
            int week          = date.get(iso.weekOfWeekBasedYear());
            return weekYear * 100 + week;
        } catch (DateTimeParseException e) {
            return 0;
        }
    }
}