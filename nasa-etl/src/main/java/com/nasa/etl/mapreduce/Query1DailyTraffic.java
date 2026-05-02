package com.nasa.etl.mapreduce;

import com.nasa.etl.common.LogRecord;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Query 1 – Daily Traffic Summary
 *
 * For each (log_date, status_code) pair, compute:
 *   request_count  – total HTTP requests
 *   total_bytes    – sum of bytes transferred
 *
 * Output key  : "batch_id\tlog_date\tstatus_code"
 * Output value: "request_count\ttotal_bytes"
 *
 * batch_id is week-based (YYYYWW), derived from each record's log_date.
 */
public class Query1DailyTraffic {

    // ======================================================================
    // Mapper
    // ======================================================================
    public static class DailyTrafficMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable offset, Text line, Context ctx)
                throws IOException, InterruptedException {

            ctx.getCounter(ETLCounters.TOTAL_LINES_READ).increment(1);

            LogRecord rec = LogRecord.parse(line.toString());

            if (rec.isMalformed()) {
                ctx.getCounter(ETLCounters.MALFORMED_RECORDS).increment(1);
                return;
            }

            ctx.getCounter(ETLCounters.VALID_RECORDS).increment(1);
            int batchId = WeekBatching.batchIdForIsoDate(rec.getLogDate());

            // Key: batchId TAB date TAB status_code
            String outKey   = batchId + "\t" + rec.getLogDate() + "\t" + rec.getStatusCode();
            // Value: 1 TAB bytes
            String outValue = "1\t" + rec.getBytesTransferred();

            ctx.write(new Text(outKey), new Text(outValue));
        }
    }

    // ======================================================================
    // Reducer
    // ======================================================================
    public static class DailyTrafficReducer
            extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context ctx)
                throws IOException, InterruptedException {

            long requestCount = 0;
            long totalBytes   = 0;

            for (Text val : values) {
                String[] parts = val.toString().split("\t");
                requestCount += Long.parseLong(parts[0]);
                totalBytes   += Long.parseLong(parts[1]);
            }

            // Output: request_count TAB total_bytes
            ctx.write(key, new Text(requestCount + "\t" + totalBytes));
        }
    }

    // ======================================================================
    // Job factory
    // ======================================================================

    /**
     * Creates and configures (but does not submit) the Query 1 Job.
     *
     * @param conf      shared Hadoop configuration
     * @param inputDir  HDFS path(s) to raw log files
     * @param outputDir HDFS output directory (must not exist)
     */
    public static Job buildJob(Configuration conf, String inputDir, String outputDir)
            throws IOException {

        Job job = Job.getInstance(conf, "NASA-ETL-Q1-DailyTrafficSummary");
        job.setJarByClass(Query1DailyTraffic.class);

        job.setInputFormatClass(BatchedLineInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(inputDir));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        job.setMapperClass(DailyTrafficMapper.class);
        job.setReducerClass(DailyTrafficReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }
}
