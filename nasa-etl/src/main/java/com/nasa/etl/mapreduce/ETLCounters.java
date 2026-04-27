package com.nasa.etl.mapreduce;

/**
 * Shared Hadoop counter group for the NASA ETL pipeline.
 * All three MapReduce jobs use these same counters so the driver
 * can collect unified run statistics.
 */
public enum ETLCounters {
    /** Total raw input lines read (including blank and malformed). */
    TOTAL_LINES_READ,
    /** Lines that could not be parsed into a valid LogRecord. */
    MALFORMED_RECORDS,
    /** Lines successfully parsed. */
    VALID_RECORDS,
    /** Number of mapper batches dispatched (one per batch flush). */
    BATCHES_PROCESSED
}
