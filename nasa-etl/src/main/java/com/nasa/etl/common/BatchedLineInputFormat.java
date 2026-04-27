package com.nasa.etl.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * Custom InputFormat that honours the configured batch size.
 *
 * Configuration key: nasa.etl.batch.size   (default 10 000)
 *
 * Each RecordReader key = global line offset (LongWritable).
 * Each RecordReader value = exactly one raw log line (Text).
 *
 * The "batching" visible to Hadoop is enforced via a counter increment
 * every <batchSize> records inside the Mapper; splitting is done
 * using standard NLineInputFormat-style byte-offset splits so that
 * Hadoop can parallelise across nodes naturally.
 *
 * For single-node / pseudo-distributed testing the default split
 * behaviour (one split per file) is fine.
 */
public class BatchedLineInputFormat extends FileInputFormat<LongWritable, Text> {

    public static final String BATCH_SIZE_KEY     = "nasa.etl.batch.size";
    public static final int    DEFAULT_BATCH_SIZE = 10_000;

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(
            InputSplit split, TaskAttemptContext context) {
        return new LineRecordReader();
    }

    // ---------------------------------------------------------------- inner reader

    public static class LineRecordReader extends RecordReader<LongWritable, Text> {

        private long         start;
        private long         end;
        private long         pos;
        private BufferedReader reader;
        private LongWritable  key   = new LongWritable();
        private Text          value = new Text();
        private FSDataInputStream fis;

        @Override
        public void initialize(InputSplit genericSplit, TaskAttemptContext ctx)
                throws IOException {
            FileSplit split = (FileSplit) genericSplit;
            Configuration conf = ctx.getConfiguration();
            Path path = split.getPath();
            FileSystem fs = path.getFileSystem(conf);

            fis   = fs.open(path);
            start = split.getStart();
            end   = start + split.getLength();
            pos   = start;

            fis.seek(start);
            // If not at the beginning of the file, skip partial line
            if (start != 0) {
                // read and discard until newline
                int b;
                while ((b = fis.read()) != -1 && b != '\n') {
                    pos++;
                }
                pos++;
            }
            reader = new BufferedReader(
                new InputStreamReader(fis, StandardCharsets.UTF_8));
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            if (pos >= end) return false;
            String line = reader.readLine();
            if (line == null) return false;
            key.set(pos);
            value.set(line);
            pos += line.length() + 1; // +1 for newline
            return true;
        }

        @Override public LongWritable getCurrentKey()   { return key; }
        @Override public Text         getCurrentValue() { return value; }
        @Override public float getProgress()            { return 0; }

        @Override
        public void close() throws IOException {
            if (fis != null) fis.close();
        }
    }

    // ---------------------------------------------------------------- statics

    /** Helper: set batch size in job configuration. */
    public static void setBatchSize(Configuration conf, int batchSize) {
        conf.setInt(BATCH_SIZE_KEY, batchSize);
    }

    /** Helper: read batch size from job configuration. */
    public static int getBatchSize(Configuration conf) {
        return conf.getInt(BATCH_SIZE_KEY, DEFAULT_BATCH_SIZE);
    }
}
