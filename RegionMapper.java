package com.region.partitioning;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * ============================================================
 * RegionMapper — Hadoop Mapper
 * ============================================================
 *
 * INPUT:
 *   Key:   LongWritable  — byte offset of the line in the input split
 *   Value: Text          — raw CSV line from the input file
 *
 * OUTPUT:
 *   Key:   Text                    — region name  (e.g. "North")
 *   Value: RegionMetricsWritable   — {visitors=500, storeCount=1}
 *
 * RESPONSIBILITIES:
 * -----------------
 * 1. Parse the CSV record into its five fields.
 * 2. Validate all relevant fields before processing.
 * 3. Skip and log any malformed or invalid record safely.
 * 4. Track processed and skipped record counts via Hadoop Counters.
 * 5. Emit one (region, metrics) pair per valid store record.
 *
 * SCALABILITY NOTE:
 * ------------------
 * Hadoop splits large input files into 128 MB blocks (default HDFS
 * block size). One Mapper task is spawned per split. For a 1 GB file
 * that means ~8 mapper tasks run in parallel. Each mapper processes
 * its own split independently, so this class must be stateless
 * between map() calls (all state is local to the method body).
 */
public class RegionMapper extends Mapper<LongWritable, Text, Text, RegionMetricsWritable> {

    private static final Log LOG = LogFactory.getLog(RegionMapper.class);

    // Expected number of CSV fields per valid record
    private static final int EXPECTED_FIELDS = 5;

    // CSV column indices
    private static final int IDX_STORE_ID      = 0;
    private static final int IDX_REGION        = 1;
    private static final int IDX_VISITORS      = 2;
    // IDX_DAILY_REVENUE = 3  (not used in this task)
    // IDX_OPENING_DATE  = 4  (not used in this task)

    /**
     * Hadoop Counter group and counter names.
     * Counters are visible in the job log and via the YARN UI after
     * the job completes, making them invaluable for data quality audits.
     */
    enum RecordCounters {
        RECORDS_PROCESSED,   // valid records that were emitted
        RECORDS_SKIPPED      // invalid/malformed records that were skipped
    }

    // Reusable output key — avoids allocating a new Text object per record.
    // This is a Hadoop best practice to reduce GC pressure at scale.
    private final Text outKey    = new Text();

    // Reusable output value — same rationale as outKey
    private final RegionMetricsWritable outValue = new RegionMetricsWritable();

    /**
     * Process one CSV line.
     *
     * @param key     byte offset (not used)
     * @param value   raw CSV text line
     * @param context Hadoop job context — used to write output and
     *                increment counters
     * @throws IOException          if context.write() fails
     * @throws InterruptedException if the task is interrupted
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        // -------------------------------------------------------
        // GUARD 1 — Skip empty lines and header lines
        // -------------------------------------------------------
        if (line.isEmpty() || line.startsWith("store_id")) {
            // Not an error; silently skip
            return;
        }

        // -------------------------------------------------------
        // GUARD 2 — Split and check field count
        // -------------------------------------------------------
        String[] fields = line.split(",", -1);  // -1 keeps trailing empty tokens

        if (fields.length < EXPECTED_FIELDS) {
            LOG.warn("[RegionMapper] Malformed record — expected " + EXPECTED_FIELDS
                    + " fields but got " + fields.length + ". Line: [" + line + "]");
            System.err.println("[RegionMapper] SKIPPING malformed line: " + line);
            context.getCounter(RecordCounters.RECORDS_SKIPPED).increment(1);
            return;
        }

        // -------------------------------------------------------
        // GUARD 3 — Validate individual fields
        // -------------------------------------------------------

        // Extract relevant fields
        String storeId = fields[IDX_STORE_ID].trim();
        String region  = fields[IDX_REGION].trim();
        String visitorsStr = fields[IDX_VISITORS].trim();

        // store_id must not be blank
        if (storeId.isEmpty()) {
            LOG.warn("[RegionMapper] Missing store_id in line: [" + line + "]");
            System.err.println("[RegionMapper] SKIPPING record with empty store_id: " + line);
            context.getCounter(RecordCounters.RECORDS_SKIPPED).increment(1);
            return;
        }

        // region must not be blank
        if (region.isEmpty()) {
            LOG.warn("[RegionMapper] Missing region in line: [" + line + "]");
            System.err.println("[RegionMapper] SKIPPING record with empty region: " + line);
            context.getCounter(RecordCounters.RECORDS_SKIPPED).increment(1);
            return;
        }

        // visitors must be a valid non-negative long
        long visitors;
        try {
            visitors = Long.parseLong(visitorsStr);
            if (visitors < 0) {
                throw new NumberFormatException("Negative visitor count: " + visitors);
            }
        } catch (NumberFormatException e) {
            LOG.warn("[RegionMapper] Invalid visitors value '" + visitorsStr
                    + "' in line: [" + line + "]. Reason: " + e.getMessage());
            System.err.println("[RegionMapper] SKIPPING record with invalid visitors '"
                    + visitorsStr + "': " + line);
            context.getCounter(RecordCounters.RECORDS_SKIPPED).increment(1);
            return;
        }

        // -------------------------------------------------------
        // EMIT — Record is valid; send to partitioner and reducer
        // -------------------------------------------------------

        // Set key: region name (used by RegionPartitioner to route
        //          this record to the correct reducer)
        outKey.set(region);

        // Set value: {totalVisitors=visitors, storeCount=1}
        // storeCount is always 1 here; the reducer sums them up.
        outValue.setTotalVisitors(visitors);
        outValue.setStoreCount(1L);

        // Write the (key, value) pair to the output collector.
        // Hadoop serializes both using their write() methods and
        // stores them in an in-memory buffer that spills to disk
        // once it fills up (default 100 MB per mapper).
        context.write(outKey, outValue);

        // Track successfully processed records
        context.getCounter(RecordCounters.RECORDS_PROCESSED).increment(1);
    }
}
