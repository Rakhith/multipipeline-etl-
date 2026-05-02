package com.nasa.etl.mapreduce;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.time.temporal.WeekFields;

/**
 * Utilities for grouping records into calendar-week batches.
 *
 * Batch ID format: YYYYWW (ISO week-based year + ISO week number).
 * Example: 1995-07-01 -> 199526
 */
public final class WeekBatching {

    private WeekBatching() {}

    public static int batchIdForIsoDate(String isoDate) {
        try {
            LocalDate date = LocalDate.parse(isoDate);
            WeekFields iso = WeekFields.ISO;
            int weekYear = date.get(iso.weekBasedYear());
            int week = date.get(iso.weekOfWeekBasedYear());
            return weekYear * 100 + week;
        } catch (DateTimeParseException e) {
            return 0;
        }
    }
}
