package com.nasa.etl.common;

/**
 * Represents one parsed NASA HTTP log record.
 * Pipeline-agnostic: works for MapReduce, MongoDB, Pig, and Hive.
 * We have kept this parser has common, the different pipelines have seperate loggers
 * within thier folders to do further processing 
 */
public class LogRecord {
    private String  host;
    private String  timestamp;
    private String  logDate;
    private int     logHour;
    private String  httpMethod;
    private String  resourcePath;
    private String  protocolVersion;
    private int     statusCode;
    private long    bytesTransferred;
    private boolean malformed;

    public LogRecord() {}
    /**
     * Parses one raw log line and returns a populated LogRecord.
     * sets malformed=true on any parse failure.
     * Used by ALL pipelines.
     */
    public static LogRecord parse(String line) {
        LogRecord r = new LogRecord();
        if (line == null || line.trim().isEmpty()) {
            r.malformed = true;
            return r;
        }

        try {
            //This is host field
            int p = line.indexOf(' ');
            if (p < 0) { r.malformed = true; return r; }
            r.host = line.substring(0, p);

            // We skip the 2 -- present in every record and then we look for timestamp field
            int lb = line.indexOf('[', p);
            if (lb < 0) { r.malformed = true; return r; }
            int rb = line.indexOf(']', lb);
            if (rb < 0) { r.malformed = true; return r; }

            r.timestamp = line.substring(lb + 1, rb);
            parseTimestamp(r, r.timestamp);

            // We get the request string
            int q1 = line.indexOf('"', rb);
            if (q1 < 0) { r.malformed = true; return r; }
            int q2 = line.indexOf('"', q1 + 1);
            if (q2 < 0) { r.malformed = true; return r; }

            // The content inside quotes may sometimes include stray spaces, so trimming ensures clean parsing
            String request = line.substring(q1 + 1, q2).trim();
            parseRequest(r, request);

            // Status code
            String rest = line.substring(q2 + 1).trim();
            String[] parts = rest.split("\\s+");
            if (parts.length < 1) { r.malformed = true; return r; }
            try {
                r.statusCode = Integer.parseInt(parts[0]);
            } catch (NumberFormatException e) {
                r.malformed = true; return r;
            }

            // If the bytes field is missing or -, we treated it as 0
            if (parts.length >= 2) {
                String bytesStr = parts[1];
                if (bytesStr.equals("-")) {
                    r.bytesTransferred = 0;
                } else {
                    try {
                        r.bytesTransferred = Long.parseLong(bytesStr);
                    } catch (NumberFormatException e) {
                        r.bytesTransferred = 0;
                    }
                }
            }
        } catch (Exception e) {
            r.malformed = true;
        }
        return r;
    }

    // helpers

    private static final String[] MONTHS = {
        "Jan","Feb","Mar","Apr","May","Jun",
        "Jul","Aug","Sep","Oct","Nov","Dec"
    };

    private static void parseTimestamp(LogRecord r, String ts) {
        try {
            String[] dateTime  = ts.split(":");
            String[] dateParts = dateTime[0].split("/");
            int day   = Integer.parseInt(dateParts[0]);
            int month = monthIndex(dateParts[1]);
            if (month == -1) { r.malformed = true; return; }
            int year  = Integer.parseInt(dateParts[2]);
            r.logDate = String.format("%04d-%02d-%02d", year, month, day);
            r.logHour = Integer.parseInt(dateTime[1]);
        } catch (Exception e) {
            r.malformed = true; // mark as malformed
        }
    }

    private static int monthIndex(String mon) {
        for (int i = 0; i < MONTHS.length; i++) {
            if (MONTHS[i].equalsIgnoreCase(mon)) return i + 1;
        }
        return -1;
    }

    private static void parseRequest(LogRecord r, String req) {
        String[] parts = req.split("\\s+");
        if (parts.length == 3) {
            r.httpMethod      = parts[0];
            r.resourcePath    = parts[1];
            r.protocolVersion = parts[2];
        } else if (parts.length == 2) {
            if (parts[1].startsWith("HTTP/")) {
                // method + protocol, no path
                r.httpMethod      = parts[0];
                r.resourcePath    = null;
                r.protocolVersion = parts[1];
            } else if (parts[0].startsWith("HTTP/")) {
                // protocol + path, no method
                r.httpMethod      = null;
                r.resourcePath    = parts[1];
                r.protocolVersion = parts[0];
            } else {
                // method + path, no protocol
                r.httpMethod      = parts[0];
                r.resourcePath    = parts[1];
                r.protocolVersion = null;
            }
        } else if (parts.length == 1) {
            if (parts[0].startsWith("HTTP/")) {
                // only protocol present
                r.httpMethod      = null;
                r.resourcePath    = null;
                r.protocolVersion = parts[0];
            } else if (parts[0].startsWith("/")) {
                // only path present
                r.httpMethod      = null;
                r.resourcePath    = parts[0];
                r.protocolVersion = null;
            } else {
                // only method present
                r.httpMethod      = parts[0];
                r.resourcePath    = null;
                r.protocolVersion = null;
            }
        } else {
            // zero parts — unparseable
            r.malformed = true;
        }
    }

    // getters

    public String  getHost()             { return host; }
    public String  getTimestamp()        { return timestamp; }
    public String  getLogDate()          { return logDate; }
    public int     getLogHour()          { return logHour; }
    public String  getHttpMethod()       { return httpMethod; }
    public String  getResourcePath()     { return resourcePath; }
    public String  getProtocolVersion()  { return protocolVersion; }
    public int     getStatusCode()       { return statusCode; }
    public long    getBytesTransferred() { return bytesTransferred; }
    public boolean isMalformed()         { return malformed; }

    // setters
    // Required by WritableLogRecord.readFields() in the MapReduce pipeline.

    public void setHost(String host)                        { this.host = host; }
    public void setTimestamp(String timestamp)              { this.timestamp = timestamp; }
    public void setLogDate(String logDate)                  { this.logDate = logDate; }
    public void setLogHour(int logHour)                     { this.logHour = logHour; }
    public void setHttpMethod(String httpMethod)            { this.httpMethod = httpMethod; }
    public void setResourcePath(String resourcePath)        { this.resourcePath = resourcePath; }
    public void setProtocolVersion(String protocolVersion)  { this.protocolVersion = protocolVersion; }
    public void setStatusCode(int statusCode)               { this.statusCode = statusCode; }
    public void setBytesTransferred(long bytesTransferred)  { this.bytesTransferred = bytesTransferred; }
    public void setMalformed(boolean malformed)             { this.malformed = malformed; }

    @Override
    public String toString() {
        return String.format(
            "LogRecord{host='%s', logDate='%s', logHour=%d, method='%s', " +
            "path='%s', status=%d, bytes=%d, malformed=%b}",
            host, logDate, logHour, httpMethod,
            resourcePath, statusCode, bytesTransferred, malformed);
    }
}