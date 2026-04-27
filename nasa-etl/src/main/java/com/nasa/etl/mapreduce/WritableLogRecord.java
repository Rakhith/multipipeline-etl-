package com.nasa.etl.mapreduce;

import com.nasa.etl.common.LogRecord;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * MapReduce-specific extension of LogRecord.
 * Adds Hadoop Writable serialisation for intermediate HDFS shuffle.
 * Never used by MongoDB, Pig, or Hive pipelines.
 */
public class WritableLogRecord extends LogRecord implements Writable {

    public WritableLogRecord() { super(); }

    // --------------------------------------------------- Writable serialisation

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(getHost()            == null ? "" : getHost());
        out.writeUTF(getTimestamp()       == null ? "" : getTimestamp());
        out.writeUTF(getLogDate()         == null ? "" : getLogDate());
        out.writeInt(getLogHour());
        out.writeUTF(getHttpMethod()      == null ? "" : getHttpMethod());
        out.writeUTF(getResourcePath()    == null ? "" : getResourcePath());
        out.writeUTF(getProtocolVersion() == null ? "" : getProtocolVersion());
        out.writeInt(getStatusCode());
        out.writeLong(getBytesTransferred());
        out.writeBoolean(isMalformed());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        setHost(in.readUTF());
        setTimestamp(in.readUTF());
        setLogDate(in.readUTF());
        setLogHour(in.readInt());
        setHttpMethod(in.readUTF());
        setResourcePath(in.readUTF());
        setProtocolVersion(in.readUTF());
        setStatusCode(in.readInt());
        setBytesTransferred(in.readLong());
        setMalformed(in.readBoolean());
    }
}