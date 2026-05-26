package com.region.partitioning;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * ============================================================
 * RegionMetricsWritable — Custom Hadoop Writable
 * ============================================================
 *
 * WHY A CUSTOM WRITABLE IS NEEDED:
 * ---------------------------------
 * Hadoop's default serialization (Java's ObjectOutputStream) is NOT
 * used in MapReduce because it is too slow and produces large byte
 * payloads over the network during the Shuffle & Sort phase.
 *
 * Hadoop defines its own lightweight serialization framework via the
 * "Writable" interface. Every key and value that travels between
 * Mapper → Shuffle → Reducer must implement Writable.
 *
 * In this project each Mapper emits TWO metrics per store record:
 *   1. totalVisitors  — the visitor count for that store
 *   2. storeCount     — always 1 per record (incremented in reducer)
 *
 * We cannot emit two separate values for one key in standard
 * MapReduce; we therefore bundle them inside a single Custom Writable.
 *
 * WHY HADOOP SERIALIZATION MATTERS AT SCALE:
 * -------------------------------------------
 * For datasets larger than 1 GB:
 *   - Hundreds of mappers run in parallel across the cluster.
 *   - Every intermediate (key, value) pair is serialized to local
 *     disk and then sent over the network to the reducer nodes.
 *   - Compact, fast serialization directly reduces:
 *       * Disk I/O during the spill phase
 *       * Network bandwidth during the shuffle phase
 *       * GC pressure (no Java object graph overhead)
 *
 * This custom Writable uses only two primitive longs (16 bytes per
 * record), making it extremely compact and fast.
 */
public class RegionMetricsWritable implements Writable {

    // -------------------------------------------------------
    // Fields
    // -------------------------------------------------------

    /** Sum of visitor counts accumulated for a region. */
    private long totalVisitors;

    /** Number of stores seen for a region. */
    private long storeCount;

    // -------------------------------------------------------
    // Constructors
    // -------------------------------------------------------

    /**
     * No-arg constructor required by Hadoop's reflective
     * deserialization. Hadoop instantiates objects via
     * Class.newInstance() before calling readFields().
     */
    public RegionMetricsWritable() {
        this.totalVisitors = 0L;
        this.storeCount    = 0L;
    }

    /**
     * Convenience constructor used by the Mapper when emitting
     * a single store record (storeCount always starts at 1).
     *
     * @param totalVisitors visitor count for this single record
     * @param storeCount    always 1 when emitted from the Mapper
     */
    public RegionMetricsWritable(long totalVisitors, long storeCount) {
        this.totalVisitors = totalVisitors;
        this.storeCount    = storeCount;
    }

    // -------------------------------------------------------
    // Writable Interface — Serialization
    // -------------------------------------------------------

    /**
     * Serialize this object to a DataOutput stream.
     *
     * Called by Hadoop when writing:
     *   - Mapper output spills to local disk
     *   - Shuffle network transfer to reducers
     *
     * We write primitives in a fixed, compact binary format.
     * WriteLong writes exactly 8 bytes per field (big-endian).
     *
     * @param out the output stream provided by Hadoop
     * @throws IOException if writing fails
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(totalVisitors);
        out.writeLong(storeCount);
    }

    /**
     * Deserialize this object from a DataInput stream.
     *
     * Called by Hadoop when reading:
     *   - Reducer input after the shuffle phase
     *   - During merge/sort of mapper output spills
     *
     * Fields MUST be read in the EXACT same order as write().
     *
     * @param in the input stream provided by Hadoop
     * @throws IOException if reading fails
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        totalVisitors = in.readLong();
        storeCount    = in.readLong();
    }

    // -------------------------------------------------------
    // Getters and Setters
    // -------------------------------------------------------

    /** @return the cumulative visitor total */
    public long getTotalVisitors() {
        return totalVisitors;
    }

    /** @param totalVisitors set the cumulative visitor total */
    public void setTotalVisitors(long totalVisitors) {
        this.totalVisitors = totalVisitors;
    }

    /** @return the number of stores counted */
    public long getStoreCount() {
        return storeCount;
    }

    /** @param storeCount set the store count */
    public void setStoreCount(long storeCount) {
        this.storeCount = storeCount;
    }

    // -------------------------------------------------------
    // Object overrides
    // -------------------------------------------------------

    /**
     * Human-readable representation — useful for debugging
     * via hadoop fs -cat or when inspecting text output.
     */
    @Override
    public String toString() {
        return "RegionMetricsWritable{totalVisitors=" + totalVisitors
                + ", storeCount=" + storeCount + "}";
    }
}
