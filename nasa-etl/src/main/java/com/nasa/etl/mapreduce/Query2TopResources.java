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
 * Query 2 – Top-20 Requested Resources (per batch)
 *
 * For the 20 most-requested resource paths within each batch, compute:
 *   request_count       – total hits
 *   total_bytes         – sum of bytes transferred
 *   distinct_host_count – number of unique hosts
 *
 * Map  key  : batch_id (as string)
 * Map  value: "resource_path\thost\tbytes"
 *
 * Reduce key  : "batch_id\tresource_path"
 * Reduce value: "request_count\ttotal_bytes\tdistinct_host_count"
 *
 * batch_id is week-based (YYYYWW), derived from each record's log_date.
 */
public class Query2TopResources {

    // ======================================================================
    // Mapper
    // ======================================================================
    public static class TopResourceMapper
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

            String path = rec.getResourcePath();
            if (path == null || path.isEmpty()) path = "(empty)";

            // Key  : batch_id
            // Value: path TAB host TAB bytes
            String val = path + "\t" + rec.getHost() + "\t" + rec.getBytesTransferred();
            ctx.write(new Text(String.valueOf(batchId)), new Text(val));
        }
    }

    // ======================================================================
    // Reducer – aggregates per-path within each batch, emits top 20
    // ======================================================================
    public static class TopResourceReducer
            extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context ctx)
                throws IOException, InterruptedException {

            int currentBatch = Integer.parseInt(key.toString());

            Map<String, long[]>    accumulator = new HashMap<>();
            Map<String, Set<String>> hostSets  = new HashMap<>();

            for (Text val : values) {
                String[] parts = val.toString().split("\t", 3);
                if (parts.length < 1) continue;

                String path  = parts[0];
                long[] stats = accumulator.computeIfAbsent(path, k -> new long[2]);
                Set<String> hosts = hostSets.computeIfAbsent(path, k -> new HashSet<>());

                stats[0]++; // request count
                if (parts.length == 3) {
                    try { stats[1] += Long.parseLong(parts[2]); }
                    catch (NumberFormatException ignored) {}
                    hosts.add(parts[1]); // host
                }
            }

            // Sort by request count descending, emit top 20 for this batch
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

        // Single reducer so all paths for each batch land in one place
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }
}
