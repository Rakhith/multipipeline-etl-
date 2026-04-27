package com.nasa.etl.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 * Represents one parsed NASA HTTP log record.
 * Implements Writable so it can be used as a MapReduce value type.
 *
 * Raw log format (Combined Log Format):
 *   host - - [DD/Mon/YYYY:HH:MM:SS -ZONE] "METHOD /path HTTP/x.x" status bytes
 */
public class LogRecord implements Writable {

    private String host;
    private String timestamp;       // raw timestamp string
    private String logDate;         // YYYY-MM-DD
    private int    logHour;         // 0-23
    private String httpMethod;
    private String resourcePath;
    private String protocolVersion;
    private int    statusCode;
    private long   bytesTransferred;
    private boolean malformed;

    public LogRecord() {}

    // ------------------------------------------------------------------ parse

    /**
     * Parses one raw log line and returns a populated LogRecord.
     * Never returns null – sets malformed=true on any parse failure.
     */
    public static LogRecord parse(String line) {
        LogRecord r = new LogRecord();
        if (line == null || line.trim().isEmpty()) {
            r.malformed = true;
            return r;
        }

        try {
            // --- host (first token) ---
            int p = line.indexOf(' ');
            if (p < 0) { r.malformed = true; return r; }
            r.host = line.substring(0, p);

            // --- skip " - - " ---
            // find opening bracket
            int lb = line.indexOf('[', p);
            if (lb < 0) { r.malformed = true; return r; }
            int rb = line.indexOf(']', lb);
            if (rb < 0) { r.malformed = true; return r; }

            // timestamp: e.g.  01/Jul/1995:00:00:01 -0400
            r.timestamp = line.substring(lb + 1, rb);
            parseTimestamp(r, r.timestamp);

            // --- request string: "METHOD /path HTTP/x.x" ---
            int q1 = line.indexOf('"', rb);
            if (q1 < 0) { r.malformed = true; return r; }
            int q2 = line.indexOf('"', q1 + 1);
            if (q2 < 0) { r.malformed = true; return r; }
            String request = line.substring(q1 + 1, q2).trim();
            parseRequest(r, request);

            // --- status code ---
            String rest = line.substring(q2 + 1).trim();
            String[] parts = rest.split("\\s+");
            if (parts.length < 1) { r.malformed = true; return r; }
            try {
                r.statusCode = Integer.parseInt(parts[0]);
            } catch (NumberFormatException e) {
                r.malformed = true; return r;
            }

            // --- bytes ---
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

            r.malformed = false;
        } catch (Exception e) {
            r.malformed = true;
        }
        return r;
    }

    // ---------------------------------------------------------------- helpers

    private static final String[] MONTHS = {
        "Jan","Feb","Mar","Apr","May","Jun",
        "Jul","Aug","Sep","Oct","Nov","Dec"
    };

    private static void parseTimestamp(LogRecord r, String ts) {
        // 01/Jul/1995:00:00:01 -0400
        try {
            String[] dateTime = ts.split(":");
            // dateTime[0] = "01/Jul/1995"
            String[] dateParts = dateTime[0].split("/");
            int day   = Integer.parseInt(dateParts[0]);
            int month = monthIndex(dateParts[1]);
            int year  = Integer.parseInt(dateParts[2]);
            r.logDate = String.format("%04d-%02d-%02d", year, month, day);
            r.logHour = Integer.parseInt(dateTime[1]);
        } catch (Exception e) {
            r.logDate = "1970-01-01";
            r.logHour = 0;
        }
    }

    private static int monthIndex(String mon) {
        for (int i = 0; i < MONTHS.length; i++) {
            if (MONTHS[i].equalsIgnoreCase(mon)) return i + 1;
        }
        return 1;
    }

    private static void parseRequest(LogRecord r, String req) {
        // "GET /path HTTP/1.0"  or just "/path"  or empty
        String[] parts = req.split("\\s+");
        if (parts.length == 3) {
            r.httpMethod      = parts[0];
            r.resourcePath    = parts[1];
            r.protocolVersion = parts[2];
        } else if (parts.length == 2) {
            r.httpMethod   = parts[0];
            r.resourcePath = parts[1];
            r.protocolVersion = "";
        } else if (parts.length == 1) {
            r.httpMethod      = "";
            r.resourcePath    = parts[0];
            r.protocolVersion = "";
        } else {
            r.httpMethod      = "";
            r.resourcePath    = "";
            r.protocolVersion = "";
        }
    }

    // --------------------------------------------------- Writable serialisation

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(host           == null ? "" : host);
        out.writeUTF(timestamp      == null ? "" : timestamp);
        out.writeUTF(logDate        == null ? "" : logDate);
        out.writeInt(logHour);
        out.writeUTF(httpMethod     == null ? "" : httpMethod);
        out.writeUTF(resourcePath   == null ? "" : resourcePath);
        out.writeUTF(protocolVersion== null ? "" : protocolVersion);
        out.writeInt(statusCode);
        out.writeLong(bytesTransferred);
        out.writeBoolean(malformed);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        host            = in.readUTF();
        timestamp       = in.readUTF();
        logDate         = in.readUTF();
        logHour         = in.readInt();
        httpMethod      = in.readUTF();
        resourcePath    = in.readUTF();
        protocolVersion = in.readUTF();
        statusCode      = in.readInt();
        bytesTransferred= in.readLong();
        malformed       = in.readBoolean();
    }

    // ---------------------------------------------------------- getters/setters

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

    @Override
    public String toString() {
        return String.format(
            "LogRecord{host='%s', logDate='%s', logHour=%d, method='%s', " +
            "path='%s', status=%d, bytes=%d, malformed=%b}",
            host, logDate, logHour, httpMethod,
            resourcePath, statusCode, bytesTransferred, malformed);
    }
}
