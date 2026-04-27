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
import java.util.*;

/**
 * Query 2 – Top-20 Requested Resources
 *
 * For the 20 most-requested resource paths, compute:
 *   request_count      – total hits
 *   total_bytes        – sum of bytes transferred
 *   distinct_host_count– number of unique host values
 *
 * Implementation uses a single MapReduce pass with an in-reducer
 * top-20 selection (all data lands on one reducer keyed by resource path;
 * a cleanup() phase sorts and emits only the top 20).
 *
 * Map  output key  : "batch_id\tresource_path"
 * Map  output value: "host\tbytes"
 * Reduce output key: "batch_id\tresource_path"
 * Reduce output value: "request_count\ttotal_bytes\tdistinct_host_count"
 */
public class Query2TopResources {

    // ======================================================================
    // Mapper
    // ======================================================================
    public static class TopResourceMapper
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

            String path  = rec.getResourcePath();
            if (path == null || path.isEmpty()) path = "(empty)";

            int currentBatch = ((lineCount - 1) / batchSize) + 1;
            String outKey = String.valueOf(currentBatch);
            // Value: path TAB host TAB bytes
            String val = path + "\t" + rec.getHost() + "\t" + rec.getBytesTransferred();
            ctx.write(new Text(outKey), new Text(val));
        }

        @Override
        protected void cleanup(Context ctx) throws IOException, InterruptedException {
            if (lineCount % batchSize != 0 && lineCount > 0) {
                ctx.getCounter(ETLCounters.BATCHES_PROCESSED).increment(1);
            }
        }
    }

    // ======================================================================
    // Reducer  –  aggregates per-path, then emits only top 20
    // ======================================================================
    public static class TopResourceReducer
            extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context ctx) 
                throws IOException, InterruptedException {
            int currentBatch = Integer.parseInt(key.toString());
            Map<String, long[]> accumulator = new HashMap<>();
            Map<String, Set<String>> hostSets = new HashMap<>();

            for (Text val : values) {
                String[] parts = val.toString().split("\t", 3);
                if (parts.length < 1) continue;
                String path = parts[0];
                
                long[] stats = accumulator.computeIfAbsent(path, k -> new long[2]);
                Set<String> hosts = hostSets.computeIfAbsent(path, k -> new HashSet<>());

                stats[0]++;                               // request count
                if (parts.length == 3) {
                    try { stats[1] += Long.parseLong(parts[2]); }
                    catch (NumberFormatException ignored) {}
                    hosts.add(parts[1]);                  // host
                }
            }

            // Sort by request count descending, take top 20 for this batch
            List<Map.Entry<String, long[]>> entries = new ArrayList<>(accumulator.entrySet());
            entries.sort((a, b) -> Long.compare(b.getValue()[0], a.getValue()[0]));

            int limit = Math.min(20, entries.size());
            for (int i = 0; i < limit; i++) {
                String path   = entries.get(i).getKey();
                long[] stats  = entries.get(i).getValue();
                int    dHosts = hostSets.getOrDefault(path, Collections.emptySet()).size();
                
                String outKey = currentBatch + "\t" + path;
                String outVal = stats[0] + "\t" + stats[1] + "\t" + dHosts;
                ctx.write(new Text(outKey), new Text(outVal));
            }
        }
    }

    // ======================================================================
    // Job factory
    // ======================================================================
    public static Job buildJob(Configuration conf, String inputDir, String outputDir)
            throws IOException {

        Job job = Job.getInstance(conf, "NASA-ETL-Q2-TopRequestedResources");
        job.setJarByClass(Query2TopResources.class);

        job.setInputFormatClass(BatchedLineInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(inputDir));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        job.setMapperClass(TopResourceMapper.class);
        job.setReducerClass(TopResourceReducer.class);

        // Single reducer so all paths land in one place for global top-20
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }
}
