package com.region.partitioning;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * ============================================================
 * RegionReducer — Hadoop Reducer
 * ============================================================
 *
 * INPUT:
 *   Key:    Text                              — region name
 *   Values: Iterable<RegionMetricsWritable>   — one per store
 *
 * OUTPUT:
 *   Key:    Text   — region name
 *   Value:  Text   — formatted metrics string
 *
 * REDUCER WORKFLOW — STEP BY STEP:
 * ----------------------------------
 * STEP 1 — PARTITION ASSIGNMENT
 *   The RegionPartitioner ensures this reducer receives records
 *   for exactly ONE region (e.g., reducer 0 → "North" only).
 *
 * STEP 2 — SHUFFLE & SORT (handled by Hadoop, before reduce())
 *   All (region, metrics) pairs emitted by mappers are:
 *     a. Sorted by key (region name, lexicographically)
 *     b. Grouped so all values for the same key are collected
 *        into one Iterable
 *   Hadoop calls reduce() once per unique key.
 *
 * STEP 3 — AGGREGATION (inside reduce())
 *   Iterate over all RegionMetricsWritable values for this region:
 *     - Accumulate totalVisitors
 *     - Accumulate storeCount
 *
 * STEP 4 — COMPUTE AVERAGE
 *   averageVisitors = totalVisitors / storeCount
 *   Guard against division-by-zero (storeCount should never be 0
 *   here because we only enter reduce() if there is at least one
 *   value, but we check defensively).
 *
 * STEP 5 — EMIT FORMATTED RESULT
 *   Emit a human-readable line matching the expected output format.
 *
 * LARGE-SCALE AGGREGATION:
 * -------------------------
 * For 1 GB+ datasets each reducer may receive millions of
 * RegionMetricsWritable values in its Iterable. Hadoop streams
 * these from disk — they are NOT all loaded into heap memory at
 * once. The iterator reads one value at a time, so this reducer
 * operates in O(1) memory regardless of input size.
 */
public class RegionReducer extends Reducer<Text, RegionMetricsWritable, Text, Text> {

    private static final Log LOG = LogFactory.getLog(RegionReducer.class);

    // Reusable output value to avoid per-record Text allocation
    private final Text outValue = new Text();

    /**
     * Aggregate all metrics for a single region.
     *
     * @param key     region name (e.g., "North")
     * @param values  stream of RegionMetricsWritable objects, one per store
     * @param context Hadoop context — used to write the result
     * @throws IOException          if context.write() fails
     * @throws InterruptedException if the task is interrupted
     */
    @Override
    protected void reduce(Text key, Iterable<RegionMetricsWritable> values, Context context)
            throws IOException, InterruptedException {

        String region = key.toString();
        long totalVisitors = 0L;
        long storeCount    = 0L;

        // -------------------------------------------------------
        // STEP 3 — Iterate and accumulate
        // -------------------------------------------------------
        // Hadoop reuses the same RegionMetricsWritable object across
        // iterations (object reuse optimization). We must read field
        // values IMMEDIATELY inside the loop — storing the reference
        // and reading it later would return the last-written state.
        for (RegionMetricsWritable metrics : values) {
            totalVisitors += metrics.getTotalVisitors();
            storeCount    += metrics.getStoreCount();
        }

        // -------------------------------------------------------
        // STEP 4 — Compute average
        // -------------------------------------------------------
        double averageVisitors = 0.0;
        if (storeCount > 0) {
            averageVisitors = (double) totalVisitors / storeCount;
        } else {
            // Defensive — should never happen unless all records were malformed
            LOG.warn("[RegionReducer] Region '" + region
                    + "' has storeCount=0. Cannot compute average.");
            System.err.println("[RegionReducer] WARNING: storeCount=0 for region: " + region);
        }

        // -------------------------------------------------------
        // STEP 5 — Emit formatted output
        // -------------------------------------------------------
        // Output format matches the expected result specification:
        // North    Total Visitors: 1680    Stores: 3    Average Daily Visitors: 560.00
        String result = String.format(
                "Total Visitors: %d\tStores: %d\tAverage Daily Visitors: %.2f",
                totalVisitors, storeCount, averageVisitors
        );

        outValue.set(result);
        context.write(key, outValue);

        LOG.info("[RegionReducer] Emitted result for region '" + region
                + "': " + result);
    }
}
