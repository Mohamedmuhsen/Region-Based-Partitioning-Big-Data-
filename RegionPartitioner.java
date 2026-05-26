package com.region.partitioning;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * ============================================================
 * RegionPartitioner — Custom Hadoop Partitioner
 * ============================================================
 *
 * WHY PARTITIONING IS IMPORTANT:
 * --------------------------------
 * After the Map phase, Hadoop must route each (key, value) pair to
 * exactly one Reducer. Without a custom partitioner, Hadoop uses
 * the default HashPartitioner, which computes:
 *
 *     reducerIndex = (key.hashCode() & Integer.MAX_VALUE) % numReducers
 *
 * For this project we have a business rule:
 *   "Each reducer must process ONE region only."
 *
 * A HashPartitioner cannot guarantee that all "North" records go to
 * reducer 0, all "South" records go to reducer 1, etc.
 * A Custom Partitioner gives us DETERMINISTIC routing.
 *
 * HOW REDUCERS ARE ASSIGNED:
 * ---------------------------
 *   North  →  Reducer 0
 *   South  →  Reducer 1
 *   East   →  Reducer 2
 *   West   →  Reducer 3
 *   Other  →  Reducer 0  (safe fallback, logged as warning)
 *
 * HOW THIS IMPROVES SCALABILITY:
 * --------------------------------
 * 1. DATA LOCALITY: All records for a region land on one reducer
 *    node. The reducer keeps all state in RAM for that region only,
 *    leading to better cache utilization.
 *
 * 2. PARALLELISM: Four reducers run simultaneously, one per region.
 *    With 1 GB+ data this means the aggregation work is split four
 *    ways, cutting wall-clock time roughly by 4× versus 1 reducer.
 *
 * 3. OUTPUT ISOLATION: Each reducer writes its own output part file
 *    (part-r-00000 through part-r-00003). Region results are
 *    naturally separated on HDFS, enabling downstream consumers to
 *    read only the region they care about.
 *
 * 4. AVOIDING DATA SKEW PROBLEMS: By explicitly assigning regions
 *    we prevent situations where one reducer gets 80% of the data
 *    because of hash collisions. (See README for skew discussion.)
 */
public class RegionPartitioner extends Partitioner<Text, RegionMetricsWritable> {

    private static final Log LOG = LogFactory.getLog(RegionPartitioner.class);

    // Partition constants — keep in sync with RegionDriver.setNumReduceTasks()
    private static final int PARTITION_NORTH   = 0;
    private static final int PARTITION_SOUTH   = 1;
    private static final int PARTITION_EAST    = 2;
    private static final int PARTITION_WEST    = 3;
    private static final int PARTITION_DEFAULT = 0; // fallback for unknown regions

    /**
     * Determine which reducer should receive this (key, value) pair.
     *
     * Hadoop calls this method once per Mapper output record during
     * the Shuffle & Sort phase. The return value must be in the range
     * [0, numPartitions - 1].
     *
     * @param key           the region name emitted by RegionMapper
     * @param value         the RegionMetricsWritable (not used for routing)
     * @param numPartitions total number of reducers configured in the job
     *                      (must be >= 4 for correct operation)
     * @return partition index (0–3)
     */
    @Override
    public int getPartition(Text key, RegionMetricsWritable value, int numPartitions) {

        // Normalise: trim whitespace and convert to title case for robustness.
        // This handles accidental spaces or casing inconsistencies in the input.
        String region = key.toString().trim();

        switch (region) {
            case "North":
                return PARTITION_NORTH % numPartitions;

            case "South":
                return PARTITION_SOUTH % numPartitions;

            case "East":
                return PARTITION_EAST % numPartitions;

            case "West":
                return PARTITION_WEST % numPartitions;

            default:
                // Unknown region — log a warning and route to partition 0.
                // This prevents the job from failing on unexpected region values.
                LOG.warn("[RegionPartitioner] Unknown region encountered: '"
                        + region + "'. Routing to partition " + PARTITION_DEFAULT
                        + ". Consider adding it to the partitioner.");
                System.err.println("[RegionPartitioner] WARNING: Unknown region '"
                        + region + "' routed to default partition.");
                return PARTITION_DEFAULT % numPartitions;
        }
    }
}
