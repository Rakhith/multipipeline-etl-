package com.nasa.etl.common;

/**
 * Run-level execution metadata shared by all pipelines.
 * Mirrors the run_metadata table in scripts/schema.sql.
 */
public class RunMetadata {

    private int    runId;
    private String pipelineName;
    private int    batchSize;
    private long   totalBatches;
    private double avgBatchSize;
    private long   totalRecords;
    private long   malformedCount;
    private long   q1RuntimeMs;
    private long   q2RuntimeMs;
    private long   q3RuntimeMs;
    private long   runtimeMs;

    public RunMetadata() {}

    public RunMetadata(String pipelineName, int batchSize) {
        this.pipelineName = pipelineName;
        this.batchSize = batchSize;
    }

    public RunMetadata(int runId, String pipelineName, int batchSize) {
        this(pipelineName, batchSize);
        this.runId = runId;
    }

    public void addQueryStats(int queryNumber, long queryRuntimeMs,
                              long records, long malformed, long batches) {
        if (queryNumber == 1) {
            totalRecords = records;
            malformedCount = malformed;
            totalBatches = batches;
            avgBatchSize = totalBatches > 0
                ? (double) totalRecords / totalBatches : 0.0;
            q1RuntimeMs = queryRuntimeMs;
        } else if (queryNumber == 2) {
            q2RuntimeMs = queryRuntimeMs;
        } else if (queryNumber == 3) {
            q3RuntimeMs = queryRuntimeMs;
        }
    }

    public int getRunId()              { return runId; }
    public String getPipelineName()    { return pipelineName; }
    public int getBatchSize()          { return batchSize; }
    public long getTotalBatches()      { return totalBatches; }
    public double getAvgBatchSize()    { return avgBatchSize; }
    public long getTotalRecords()      { return totalRecords; }
    public long getMalformedCount()    { return malformedCount; }
    public long getQ1RuntimeMs()       { return q1RuntimeMs; }
    public long getQ2RuntimeMs()       { return q2RuntimeMs; }
    public long getQ3RuntimeMs()       { return q3RuntimeMs; }
    public long getRuntimeMs()         { return runtimeMs; }

    public void setRunId(int runId)                       { this.runId = runId; }
    public void setPipelineName(String pipelineName)      { this.pipelineName = pipelineName; }
    public void setBatchSize(int batchSize)               { this.batchSize = batchSize; }
    public void setTotalBatches(long totalBatches)        { this.totalBatches = totalBatches; }
    public void setAvgBatchSize(double avgBatchSize)      { this.avgBatchSize = avgBatchSize; }
    public void setTotalRecords(long totalRecords)        { this.totalRecords = totalRecords; }
    public void setMalformedCount(long malformedCount)    { this.malformedCount = malformedCount; }
    public void setQ1RuntimeMs(long q1RuntimeMs)          { this.q1RuntimeMs = q1RuntimeMs; }
    public void setQ2RuntimeMs(long q2RuntimeMs)          { this.q2RuntimeMs = q2RuntimeMs; }
    public void setQ3RuntimeMs(long q3RuntimeMs)          { this.q3RuntimeMs = q3RuntimeMs; }
    public void setRuntimeMs(long runtimeMs)              { this.runtimeMs = runtimeMs; }
}
