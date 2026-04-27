package com.nasa.etl.common;

import java.time.Instant;

/**
 * Captures execution metadata for one query's MapReduce run.
 * This is serialised to JSON and stored in the run_metadata table.
 */
public class RunMetadata {

    private String  pipelineName  = "Hadoop-MapReduce";
    private String  runId;           // UUID assigned by driver
    private String  queryName;
    private long    startEpochMs;
    private long    endEpochMs;
    private long    runtimeMs;
    private int     batchSize;       // configured batch size
    private long    totalRecords;    // valid records processed
    private long    malformedCount;
    private long    batchCount;
    private double  avgBatchSize;
    private String  executionTime;   // ISO-8601

    public RunMetadata() {}

    public RunMetadata(String runId, String queryName, int batchSize) {
        this.runId     = runId;
        this.queryName = queryName;
        this.batchSize = batchSize;
    }

    // -------------------------------------------------------- computed helpers

    public void finish(long startMs, long endMs, long totalRecords,
                       long malformedCount, long batchCount) {
        this.startEpochMs   = startMs;
        this.endEpochMs     = endMs;
        this.runtimeMs      = endMs - startMs;
        this.totalRecords   = totalRecords;
        this.malformedCount = malformedCount;
        this.batchCount     = batchCount;
        this.avgBatchSize   = batchCount > 0
                              ? (double) totalRecords / batchCount : 0.0;
        this.executionTime  = Instant.ofEpochMilli(startMs).toString();
    }

    // ------------------------------------------------- simple JSON serialiser

    public String toJson() {
        return String.format(
            "{\"pipeline\":\"%s\",\"runId\":\"%s\",\"query\":\"%s\"," +
            "\"runtimeMs\":%d,\"batchSize\":%d,\"totalRecords\":%d," +
            "\"malformedCount\":%d,\"batchCount\":%d,\"avgBatchSize\":%.2f," +
            "\"executionTime\":\"%s\"}",
            pipelineName, runId, queryName, runtimeMs, batchSize,
            totalRecords, malformedCount, batchCount, avgBatchSize, executionTime);
    }

    // ------------------------------------------------- getters / setters

    public String getPipelineName()  { return pipelineName; }
    public String getRunId()         { return runId; }
    public String getQueryName()     { return queryName; }
    public long   getRuntimeMs()     { return runtimeMs; }
    public int    getBatchSize()     { return batchSize; }
    public long   getTotalRecords()  { return totalRecords; }
    public long   getMalformedCount(){ return malformedCount; }
    public long   getBatchCount()    { return batchCount; }
    public double getAvgBatchSize()  { return avgBatchSize; }
    public String getExecutionTime() { return executionTime; }

    public void setRunId(String runId)                { this.runId = runId; }
    public void setQueryName(String queryName)        { this.queryName = queryName; }
    public void setBatchSize(int batchSize)           { this.batchSize = batchSize; }
    public long getStartEpochMs()                     { return startEpochMs; }
    public long getEndEpochMs()                       { return endEpochMs; }
}
