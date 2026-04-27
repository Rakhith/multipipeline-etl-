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
import java.util.HashSet;
import java.util.Set;

/**
 * Query 3 – Hourly Error Analysis
 *
 * For each (log_date, log_hour) pair, compute:
 *   error_request_count  – requests with status 400–599
 *   total_request_count  – all requests in that hour
 *   error_rate           – error_request_count / total_request_count
 *   distinct_error_hosts – unique hosts that produced ≥1 error in that hour
 *
 * Map output key  : "batch_id\tlog_date\tlog_hour"
 * Map output value: "is_error(0|1)\thost"
 *
 * Reduce output key  : "batch_id\tlog_date\tlog_hour"
 * Reduce output value: "error_req\ttotal_req\terror_rate\tdistinct_error_hosts"
 */
public class Query3HourlyError {

    // ======================================================================
    // Mapper
    // ======================================================================
    public static class HourlyErrorMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        private int batchSize;
        private int lineCount = 0;
        private int batchOffset = 0;

        @Override
        protected void setup(Context ctx) {
            batchSize = BatchedLineInputFormat.getBatchSize(ctx.getConfiguration());
            batchOffset = ctx.getTaskAttemptID().getTaskID().getId() * 1_000_000;
        }

        @Override
        protected void map(LongWritable offset, Text line, Context ctx)
                throws IOException, InterruptedException {

            ctx.getCounter(ETLCounters.TOTAL_LINES_READ).increment(1);
            lineCount++;
            int batchId = batchOffset + ((lineCount - 1) / batchSize) + 1;
            if (lineCount % batchSize == 0) {
                ctx.getCounter(ETLCounters.BATCHES_PROCESSED).increment(1);
            }

            LogRecord rec = LogRecord.parse(line.toString());
            if (rec.isMalformed()) {
                ctx.getCounter(ETLCounters.MALFORMED_RECORDS).increment(1);
                return;
            }
            ctx.getCounter(ETLCounters.VALID_RECORDS).increment(1);

            int currentBatch = ((lineCount - 1) / batchSize) + 1;
            String key = currentBatch + "\t" + rec.getLogDate() + "\t" + rec.getLogHour();

            // is_error flag  (1 = error, 0 = not error)
            int status  = rec.getStatusCode();
            int isError = (status >= 400 && status <= 599) ? 1 : 0;

            // Value: isError TAB host
            String val = isError + "\t" + rec.getHost();
            ctx.write(new Text(key), new Text(val));
        }

        @Override
        protected void cleanup(Context ctx) throws IOException, InterruptedException {
            if (lineCount % batchSize != 0 && lineCount > 0) {
                ctx.getCounter(ETLCounters.BATCHES_PROCESSED).increment(1);
            }
        }
    }

    // ======================================================================
    // Reducer
    // ======================================================================
    public static class HourlyErrorReducer
            extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context ctx)
                throws IOException, InterruptedException {

            long        totalRequests = 0;
            long        errorRequests = 0;
            Set<String> errorHosts    = new HashSet<>();

            for (Text val : values) {
                String[] parts = val.toString().split("\t", 2);
                int isError = Integer.parseInt(parts[0]);
                String host = parts.length > 1 ? parts[1] : "";

                totalRequests++;
                if (isError == 1) {
                    errorRequests++;
                    errorHosts.add(host);
                }
            }

            double errorRate = totalRequests > 0
                               ? (double) errorRequests / totalRequests : 0.0;

            // Output: error_req TAB total_req TAB error_rate TAB distinct_error_hosts
            String out = errorRequests + "\t" + totalRequests
                       + "\t" + String.format("%.6f", errorRate)
                       + "\t" + errorHosts.size();

            ctx.write(key, new Text(out));
        }
    }

    // ======================================================================
    // Job factory
    // ======================================================================
    public static Job buildJob(Configuration conf, String inputDir, String outputDir)
            throws IOException {

        Job job = Job.getInstance(conf, "NASA-ETL-Q3-HourlyErrorAnalysis");
        job.setJarByClass(Query3HourlyError.class);

        job.setInputFormatClass(BatchedLineInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(inputDir));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        job.setMapperClass(HourlyErrorMapper.class);
        job.setReducerClass(HourlyErrorReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }
}
