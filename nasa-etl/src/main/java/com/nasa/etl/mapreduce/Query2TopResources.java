package com.nasa.etl.mapreduce;

import com.nasa.etl.common.BatchedLineInputFormat;
import com.nasa.etl.common.ETLCounters;
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
 * Map  output key  : resource_path
 * Map  output value: "host\tbytes"
 * Reduce output key: resource_path
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

        @Override
        protected void setup(Context ctx) {
            batchSize = BatchedLineInputFormat.getBatchSize(ctx.getConfiguration());
        }

        @Override
        protected void map(LongWritable offset, Text line, Context ctx)
                throws IOException, InterruptedException {

            ctx.getCounter(ETLCounters.TOTAL_LINES_READ).increment(1);
            lineCount++;
            if (lineCount % batchSize == 1) { /* batch start */ }
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

            // Value: host TAB bytes
            String val = rec.getHost() + "\t" + rec.getBytesTransferred();
            ctx.write(new Text(path), new Text(val));
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

        // Accumulate ALL paths in memory, then select top-20 in cleanup()
        // (dataset is ~3 M records with ~50 K distinct paths – fits in heap)
        private final Map<String, long[]> accumulator = new HashMap<>();
        // accumulator value: [requestCount, totalBytes]
        private final Map<String, Set<String>> hostSets = new HashMap<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context ctx) {
            String path = key.toString();
            long[] stats = accumulator.computeIfAbsent(path, k -> new long[2]);
            Set<String> hosts = hostSets.computeIfAbsent(
                path, k -> new HashSet<>());

            for (Text val : values) {
                String[] parts = val.toString().split("\t", 2);
                stats[0]++;                               // request count
                if (parts.length == 2) {
                    try { stats[1] += Long.parseLong(parts[1]); }
                    catch (NumberFormatException ignored) {}
                    hosts.add(parts[0]);                  // host
                }
            }
        }

        @Override
        protected void cleanup(Context ctx)
                throws IOException, InterruptedException {

            // Sort by request count descending, take top 20
            List<Map.Entry<String, long[]>> entries =
                new ArrayList<>(accumulator.entrySet());
            entries.sort((a, b) -> Long.compare(b.getValue()[0], a.getValue()[0]));

            int limit = Math.min(20, entries.size());
            for (int i = 0; i < limit; i++) {
                String path   = entries.get(i).getKey();
                long[] stats  = entries.get(i).getValue();
                int    dHosts = hostSets.getOrDefault(path, Collections.emptySet()).size();
                // Output: request_count TAB total_bytes TAB distinct_host_count
                String out = stats[0] + "\t" + stats[1] + "\t" + dHosts;
                ctx.write(new Text(path), new Text(out));
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
