package com.nasa.etl.mongodb;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.time.temporal.WeekFields;

/**
 * MongoDB-local weekly batch helper.
 *
 * Valid records use ISO week ids (YYYYWW). Malformed records may not have a
 * parsed date, so they are assigned batch 0 and filtered out by query matches.
 */
public final class MongoWeekBatching {

    private MongoWeekBatching() {}

    public static int batchIdForIsoDate(String isoDate) {
        if (isoDate == null || isoDate.isEmpty()) return 0;
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
