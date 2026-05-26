package com.region.partitioning;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * ============================================================
 * RegionDriver — Hadoop MapReduce Job Driver
 * ============================================================
 *
 * DRIVER RESPONSIBILITIES:
 * -------------------------
 * The Driver is the entry point of a Hadoop MapReduce application.
 * It is responsible for:
 *
 *   1. Creating and configuring the Job object.
 *   2. Registering the Mapper, Reducer, and Partitioner classes.
 *   3. Declaring input/output key-value types.
 *   4. Setting the number of Reducer tasks.
 *   5. Configuring input/output HDFS paths.
 *   6. Submitting the job to the YARN ResourceManager.
 *   7. Waiting for job completion and reporting success/failure.
 *
 * HADOOP JOB LIFECYCLE:
 * ----------------------
 *   Phase 1 — MAP
 *     YARN launches one mapper task per input split (default 128 MB).
 *     Each mapper reads its split line-by-line, calling map() for
 *     each record, and writes output to an in-memory circular buffer.
 *     When the buffer is 80% full, it spills to local disk and sorts
 *     the spill by (partition, key).
 *
 *   Phase 2 — SHUFFLE & SORT
 *     After all mappers finish, the Hadoop framework:
 *       a. Merges all spill files on the mapper node.
 *       b. Transfers each partition's data to the assigned reducer node
 *          over the network (the "shuffle").
 *       c. Merge-sorts all received files by key ("sort").
 *
 *   Phase 3 — REDUCE
 *     Each reducer iterates over sorted (key, Iterable<value>) groups,
 *     calls reduce() once per unique key, and writes final output to
 *     HDFS.
 *
 * NUMBER OF REDUCERS:
 * --------------------
 * We set exactly 4 reducers — one per geographic region:
 *   Reducer 0 → North
 *   Reducer 1 → South
 *   Reducer 2 → East
 *   Reducer 3 → West
 *
 * This is controlled by:  job.setNumReduceTasks(NUM_REDUCERS)
 * The RegionPartitioner enforces the mapping.
 *
 * EXTENDS Configured / IMPLEMENTS Tool:
 * ---------------------------------------
 * The Configured + Tool pattern is the recommended Hadoop idiom.
 * It allows the job to accept Hadoop generic options on the command
 * line (e.g., -D mapreduce.job.reduces=8) and integrates cleanly
 * with the Hadoop Configuration framework.
 */
public class RegionDriver extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(RegionDriver.class);

    /** One reducer per region for deterministic partitioning. */
    private static final int NUM_REDUCERS = 4;

    /**
     * Configure and run the MapReduce job.
     *
     * @param args command-line arguments: [0] = input path, [1] = output path
     * @return 0 on success, 1 on failure (standard Unix exit code convention)
     * @throws Exception if job configuration or submission fails
     */
    @Override
    public int run(String[] args) throws Exception {

        // -------------------------------------------------------
        // STEP 1 — Validate command-line arguments
        // -------------------------------------------------------
        if (args.length < 2) {
            System.err.println("Usage: RegionDriver <input_path> <output_path>");
            System.err.println("Example: hadoop jar region-partitioning.jar "
                    + "com.region.partitioning.RegionDriver /input/stores /output/regions");
            return 1;
        }

        Path inputPath  = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        LOG.info("[RegionDriver] Input path  : " + inputPath);
        LOG.info("[RegionDriver] Output path : " + outputPath);

        // -------------------------------------------------------
        // STEP 2 — Build the Hadoop Configuration
        // -------------------------------------------------------
        // getConf() retrieves the Configuration injected by ToolRunner.
        // It includes all site configs (core-site.xml, hdfs-site.xml,
        // mapred-site.xml, yarn-site.xml) loaded from the classpath.
        Configuration conf = getConf();

        // Performance tuning for large datasets (1 GB+)
        // Increase mapper output buffer to reduce spill frequency
        conf.set("mapreduce.task.io.sort.mb",       "256");
        // Spill when buffer is 80% full (default)
        conf.set("mapreduce.map.sort.spill.percent", "0.80");
        // Use LZO or Snappy compression on mapper output to reduce
        // shuffle network traffic. Snappy is available on Cloudera CDH.
        conf.setBoolean("mapreduce.map.output.compress", true);
        conf.set("mapreduce.map.output.compress.codec",
                 "org.apache.hadoop.io.compress.SnappyCodec");

        // -------------------------------------------------------
        // STEP 3 — Create the Job
        // -------------------------------------------------------
        Job job = Job.getInstance(conf, "Region-Based Partitioning — Store Performance");

        // Set the JAR containing this Driver class.
        // Hadoop distributes this JAR to all task nodes via the
        // Distributed Cache so mappers and reducers can be loaded.
        job.setJarByClass(RegionDriver.class);

        // -------------------------------------------------------
        // STEP 4 — Register Mapper, Reducer, Partitioner
        // -------------------------------------------------------
        job.setMapperClass(RegionMapper.class);
        job.setReducerClass(RegionReducer.class);
        job.setPartitionerClass(RegionPartitioner.class);

        // -------------------------------------------------------
        // STEP 5 — Declare Mapper output types
        // (required when they differ from the final output types)
        // -------------------------------------------------------
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(RegionMetricsWritable.class);

        // -------------------------------------------------------
        // STEP 6 — Declare final (Reducer) output types
        // -------------------------------------------------------
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // -------------------------------------------------------
        // STEP 7 — Configure input/output formats
        // -------------------------------------------------------
        // TextInputFormat reads the file line-by-line.
        // Each line becomes the value; the byte offset is the key.
        job.setInputFormatClass(TextInputFormat.class);

        // TextOutputFormat writes "key\tvalue\n" lines to HDFS.
        job.setOutputFormatClass(TextOutputFormat.class);

        // -------------------------------------------------------
        // STEP 8 — Set the number of Reducers
        // -------------------------------------------------------
        // CRITICAL: Must match the number of distinct partitions
        // defined in RegionPartitioner. If NUM_REDUCERS < 4, some
        // regions will be mixed in the same output file.
        job.setNumReduceTasks(NUM_REDUCERS);

        // -------------------------------------------------------
        // STEP 9 — Set input/output paths
        // -------------------------------------------------------
        FileInputFormat.addInputPath(job, inputPath);

        // Auto-delete output directory if it already exists.
        // Hadoop refuses to overwrite existing output directories,
        // so we clean up automatically to simplify re-runs.
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            LOG.warn("[RegionDriver] Output path already exists — deleting: " + outputPath);
            System.err.println("[RegionDriver] Deleting existing output: " + outputPath);
            fs.delete(outputPath, true);  // true = recursive delete
        }

        FileOutputFormat.setOutputPath(job, outputPath);

        // -------------------------------------------------------
        // STEP 10 — Submit the job and wait for completion
        // -------------------------------------------------------
        LOG.info("[RegionDriver] Submitting job to YARN cluster...");
        boolean success = job.waitForCompletion(true); // true = print progress to stdout

        // Print final counter report
        if (success) {
            LOG.info("[RegionDriver] Job completed successfully.");
            System.out.println("\n=== JOB COMPLETED SUCCESSFULLY ===");
            System.out.println("Output written to: " + outputPath);
            System.out.println("Check individual part files:");
            System.out.println("  part-r-00000  →  North");
            System.out.println("  part-r-00001  →  South");
            System.out.println("  part-r-00002  →  East");
            System.out.println("  part-r-00003  →  West");
        } else {
            LOG.error("[RegionDriver] Job FAILED. Check YARN logs for details.");
            System.err.println("[RegionDriver] Job FAILED.");
        }

        return success ? 0 : 1;
    }

    /**
     * Main entry point — called by the JVM when running:
     *   hadoop jar region-partitioning.jar com.region.partitioning.RegionDriver
     *            /input/stores /output/regions
     *
     * ToolRunner.run():
     *   1. Parses Hadoop generic options (e.g., -conf, -D, -fs, -jt).
     *   2. Initialises the Configuration object.
     *   3. Delegates to RegionDriver.run(args).
     *
     * @param args [0] = HDFS input path, [1] = HDFS output path
     * @throws Exception propagated from ToolRunner
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new RegionDriver(), args);
        System.exit(exitCode);
    }
}
