# Region-Based Partitioning — Hadoop MapReduce Project

**Task 12**
**Hadoop MapReduce Project**

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture Explanation](#2-architecture-explanation)
3. [Hadoop Workflow](#3-hadoop-workflow)
4. [Data Flow Explanation](#4-data-flow-explanation)
5. [Mapper Explanation](#5-mapper-explanation)
6. [Reducer Explanation](#6-reducer-explanation)
7. [Partitioner Explanation](#7-partitioner-explanation)
8. [Custom Writable Explanation](#8-custom-writable-explanation)
9. [Compilation Steps](#9-compilation-steps)
10. [Execution Steps](#10-execution-steps)
11. [Eclipse Setup](#11-eclipse-setup)
12. [Cloudera Setup](#12-cloudera-setup)
13. [HDFS Commands](#13-hdfs-commands)
14. [Input / Output Examples](#14-input--output-examples)
15. [Scalability Discussion](#15-scalability-discussion)
16. [Assumptions](#16-assumptions)
17. [Testing Strategy](#17-testing-strategy)
18. [Performance Discussion](#18-performance-discussion)
19. [Best Practices](#19-best-practices)
20. [Troubleshooting](#20-troubleshooting)

---

## 1. Project Overview

This project implements a **Region-Based Partitioning** Hadoop MapReduce pipeline. It reads store performance data from a CSV file stored in HDFS and calculates per-region statistics:

| Metric | Description |
|---|---|
| Total Visitors | Sum of visitors across all stores in the region |
| Number of Stores | Count of store records for the region |
| Average Daily Visitors | Total Visitors ÷ Number of Stores |

A **Custom Partitioner** ensures each Reducer handles exactly **one** geographic region, achieving clean data isolation and horizontal scalability.

### Java Classes

| Class | Role |
|---|---|
| `RegionMapper.java` | Parse CSV lines, validate fields, emit (region, metrics) |
| `RegionReducer.java` | Aggregate metrics and compute averages per region |
| `RegionPartitioner.java` | Route each key deterministically to one reducer |
| `RegionMetricsWritable.java` | Custom Hadoop Writable to carry two fields as one value |
| `RegionDriver.java` | Configure and submit the MapReduce job |

---

## 2. Architecture Explanation

```
HDFS Input File(s)
       │
       ▼
┌──────────────────────────────────────────────┐
│               MAP PHASE                       │
│  Split 1 → Mapper 0                           │
│  Split 2 → Mapper 1    (parallel)             │
│  Split N → Mapper N                           │
│  Each mapper emits: (region, metrics)         │
└────────────────────┬─────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────┐
│          SHUFFLE & SORT PHASE                 │
│  RegionPartitioner routes:                    │
│    North → Reducer 0                          │
│    South → Reducer 1                          │
│    East  → Reducer 2                          │
│    West  → Reducer 3                          │
└────────────────────┬─────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────┐
│              REDUCE PHASE                     │
│  Reducer 0 → North aggregation → part-r-00000│
│  Reducer 1 → South aggregation → part-r-00001│
│  Reducer 2 → East  aggregation → part-r-00002│
│  Reducer 3 → West  aggregation → part-r-00003│
└──────────────────────────────────────────────┘
       │
       ▼
HDFS Output Directory
```

---

## 3. Hadoop Workflow

### Phase 1 — Map

1. HDFS splits the input file into 128 MB chunks (blocks).
2. YARN assigns one Mapper task per block.
3. Mappers run in parallel across the cluster.
4. `RegionMapper.map()` is called once per CSV line.
5. Valid records are serialized as `(Text region, RegionMetricsWritable)` and written to the mapper's memory buffer (default 100 MB).
6. When the buffer is 80% full, Hadoop sorts it by partition + key and spills to local disk.
7. After all input is processed, spill files are merged into one sorted file per mapper.

### Phase 2 — Shuffle & Sort

1. `RegionPartitioner.getPartition()` assigns every mapper output record to partition 0–3.
2. Hadoop copies partition N data from every mapper to Reducer N's node (this is "the shuffle").
3. Reducer N merge-sorts its received data by key before calling `reduce()`.

### Phase 3 — Reduce

1. `RegionReducer.reduce()` is called once per unique region.
2. Aggregated results are written to HDFS as `part-r-0000X` files.

---

## 4. Data Flow Explanation

```
INPUT LINE:
  "S001,North,500,2500,2020-01-15"
          │
          ▼
  RegionMapper.map()
          │
          │  Parse CSV
          │  Validate: region="North", visitors=500
          │
          ▼
  EMIT: Key="North"  Value={totalVisitors=500, storeCount=1}
          │
          ▼
  RegionPartitioner.getPartition("North", ...)
          │
          │  Returns 0
          │
          ▼
  Reducer 0 receives:
    "North" → [{500,1}, {600,1}, {580,1}]
          │
          ▼
  RegionReducer.reduce():
    totalVisitors = 500+600+580 = 1680
    storeCount    = 1+1+1 = 3
    average       = 1680/3 = 560.00
          │
          ▼
  OUTPUT: "North  Total Visitors: 1680  Stores: 3  Average Daily Visitors: 560.00"
```

---

## 5. Mapper Explanation

**File:** `RegionMapper.java`

The Mapper performs these steps for each input line:

1. **Skip** empty lines and the CSV header (`store_id,...`).
2. **Split** the line by comma. If fewer than 5 fields are found, skip and log a warning.
3. **Validate** each required field:
   - `store_id` must not be blank.
   - `region` must not be blank.
   - `visitors` must parse as a non-negative `long`.
4. **Increment Hadoop Counter** — `RECORDS_SKIPPED` for bad lines, `RECORDS_PROCESSED` for good ones.
5. **Emit** `(region, {visitors, 1})` for valid records.

**Object reuse:** `outKey` and `outValue` are declared as instance fields and re-used across calls. This avoids creating millions of short-lived objects for large datasets, reducing GC pressure.

---

## 6. Reducer Explanation

**File:** `RegionReducer.java`

The Reducer aggregates one region at a time:

1. Receives all `RegionMetricsWritable` values for one key (region).
2. Iterates through the `Iterable`, accumulating `totalVisitors` and `storeCount`.
3. Computes `averageVisitors = totalVisitors / storeCount`.
4. Emits a formatted string:
   ```
   North   Total Visitors: 1680    Stores: 3    Average Daily Visitors: 560.00
   ```

**Memory note:** Hadoop streams values from disk through the Iterable. At no point are all values held in heap memory simultaneously. This makes the reducer handle billions of records in O(1) space.

---

## 7. Partitioner Explanation

**File:** `RegionPartitioner.java`

```
"North" → partition 0
"South" → partition 1
"East"  → partition 2
"West"  → partition 3
Unknown → partition 0  (with warning log)
```

Without this custom partitioner, the default `HashPartitioner` would use:
```java
(key.hashCode() & Integer.MAX_VALUE) % numReducers
```

This cannot guarantee `"North"` always maps to the same reducer. Our custom partitioner makes routing **deterministic**, which is essential for the "one region per reducer" requirement.

---

## 8. Custom Writable Explanation

**File:** `RegionMetricsWritable.java`

Hadoop requires all keys and values to implement the `Writable` interface. Standard Java serialization (`ObjectInputStream/ObjectOutputStream`) is not used because it:
- Is slow (reflection-heavy)
- Produces large byte arrays (full class metadata included)
- Is not optimized for streaming across a network

Our custom Writable stores two `long` fields (16 bytes total per record). Serialization is done with `DataOutput.writeLong()` — a simple, fast binary write.

```java
@Override
public void write(DataOutput out) throws IOException {
    out.writeLong(totalVisitors);   // 8 bytes
    out.writeLong(storeCount);      // 8 bytes
}                                   // Total: 16 bytes per record
```

For a 1 GB input file with ~10 million records, this produces ~160 MB of intermediate data — far smaller than Java object serialization would generate.

---

## 9. Compilation Steps

### Prerequisites

- JDK 8 or 11
- Hadoop 2.x or 3.x (CDH 5/6 on Cloudera)
- Eclipse IDE for Java Developers

### Option A — Eclipse Export (Recommended for Cloudera)

See [Section 11 — Eclipse Setup](#11-eclipse-setup).

### Option B — Manual `javac` Compilation

```bash
# Set paths
export HADOOP_HOME=/usr/lib/hadoop
export HADOOP_MAPRED_HOME=/usr/lib/hadoop-mapreduce

# Compile all Java files
mkdir -p build/classes
javac -classpath \
  ${HADOOP_HOME}/hadoop-common-*.jar:\
  ${HADOOP_MAPRED_HOME}/hadoop-mapreduce-client-core-*.jar \
  -d build/classes \
  src/main/java/com/region/partitioning/*.java

# Package into a runnable JAR
jar -cvf region-partitioning.jar -C build/classes .
```

### Option C — Maven (if Maven is available)

```bash
mvn clean package -DskipTests
# Output: target/region-partitioning-1.0.jar
```

---

## 10. Execution Steps

### 1. Upload input data to HDFS

```bash
hdfs dfs -mkdir -p /user/cloudera/input/stores
hdfs dfs -put data/stores.csv /user/cloudera/input/stores/
hdfs dfs -ls /user/cloudera/input/stores/
```

### 2. Run the MapReduce job

```bash
hadoop jar region-partitioning.jar \
  com.region.partitioning.RegionDriver \
  /user/cloudera/input/stores \
  /user/cloudera/output/regions
```

### 3. View output

```bash
# View all output files at once
hdfs dfs -cat /user/cloudera/output/regions/part-r-*

# View individual region files
hdfs dfs -cat /user/cloudera/output/regions/part-r-00000   # North
hdfs dfs -cat /user/cloudera/output/regions/part-r-00001   # South
hdfs dfs -cat /user/cloudera/output/regions/part-r-00002   # East
hdfs dfs -cat /user/cloudera/output/regions/part-r-00003   # West
```

---

## 11. Eclipse Setup

### Step 1 — Create a new Java Project

1. Open Eclipse → **File → New → Java Project**
2. Project name: `RegionPartitioning`
3. JRE: Java 8 (must match the Cloudera cluster JVM version)
4. Click **Finish**

### Step 2 — Create the package structure

1. Right-click `src` → **New → Package**
2. Package name: `com.region.partitioning`
3. Create the five Java classes inside this package.

### Step 3 — Add Hadoop JARs to the build path

Required JARs (find them in `/usr/lib/hadoop` on Cloudera):

| JAR | Purpose |
|---|---|
| `hadoop-common-*.jar` | Core Hadoop API (Configuration, FileSystem, etc.) |
| `hadoop-mapreduce-client-core-*.jar` | MapReduce API (Mapper, Reducer, Job, etc.) |
| `hadoop-mapreduce-client-common-*.jar` | Counter, context classes |
| `commons-logging-*.jar` | `Log` and `LogFactory` |

Steps:
1. Right-click project → **Properties → Java Build Path → Libraries**
2. Click **Add External JARs...**
3. Navigate to `/usr/lib/hadoop/` and select `hadoop-common-*.jar`
4. Navigate to `/usr/lib/hadoop-mapreduce/` and select `hadoop-mapreduce-client-core-*.jar` and `hadoop-mapreduce-client-common-*.jar`
5. Click **Apply and Close**

### Step 4 — Export as Runnable JAR

1. Right-click project → **Export → Java → Runnable JAR file**
2. Launch configuration: Select `RegionDriver`
3. Export destination: `/home/cloudera/region-partitioning.jar`
4. Library handling: **Package required libraries into generated JAR**
5. Click **Finish**

### Step 5 — Transfer to Cloudera VM

If developing on a separate machine, copy the JAR with SCP:

```bash
scp region-partitioning.jar cloudera@<VM_IP>:/home/cloudera/
```

---

## 12. Cloudera Setup

### Environment Details

| Item | Value |
|---|---|
| Platform | Cloudera CDH 5 or CDH 6 |
| HDFS NameNode | hdfs://localhost:8020 (or as configured) |
| YARN ResourceManager | http://localhost:8088 |
| Hadoop home | `/usr/lib/hadoop` |

### Verify Hadoop is running

```bash
# Check HDFS
hdfs dfs -ls /

# Check YARN
yarn node -list

# Check Hadoop version
hadoop version
```

### Check available reducers

```bash
# View YARN node capacity
yarn node -list -all
```

---

## 13. HDFS Commands

```bash
# Create input directory
hdfs dfs -mkdir -p /user/cloudera/input/stores

# Upload input file
hdfs dfs -put /home/cloudera/stores.csv /user/cloudera/input/stores/

# List files in input
hdfs dfs -ls /user/cloudera/input/stores/

# Preview the input file
hdfs dfs -cat /user/cloudera/input/stores/stores.csv

# List output after job completes
hdfs dfs -ls /user/cloudera/output/regions/

# View all reducer output
hdfs dfs -cat /user/cloudera/output/regions/part-r-*

# Delete output directory (for re-running)
hdfs dfs -rm -r /user/cloudera/output/regions

# Copy output from HDFS to local filesystem
hdfs dfs -get /user/cloudera/output/regions /home/cloudera/output
```

---

## 14. Input / Output Examples

### Input (`stores.csv`)

```
store_id,region,visitors,daily_revenue,opening_date
S001,North,500,2500,2020-01-15
S002,North,600,3000,2020-02-20
S003,South,400,2000,2020-03-10
S004,East,700,3500,2020-04-05
S005,West,550,2750,2020-05-12
S006,North,580,2900,2020-06-18
S007,South,450,2250,2020-07-22
S008,East,720,3600,2020-08-30
```

### Expected Output

```
# part-r-00000 (North)
North   Total Visitors: 1680    Stores: 3    Average Daily Visitors: 560.00

# part-r-00001 (South)
South   Total Visitors: 850     Stores: 2    Average Daily Visitors: 425.00

# part-r-00002 (East)
East    Total Visitors: 1420    Stores: 2    Average Daily Visitors: 710.00

# part-r-00003 (West)
West    Total Visitors: 550     Stores: 1    Average Daily Visitors: 550.00
```

### Manual Verification

```
North:
  500 + 600 + 580 = 1680 visitors, 3 stores, 1680/3 = 560.00 ✓

South:
  400 + 450 = 850 visitors, 2 stores, 850/2 = 425.00 ✓

East:
  700 + 720 = 1420 visitors, 2 stores, 1420/2 = 710.00 ✓

West:
  550 visitors, 1 store, 550/1 = 550.00 ✓
```

---

## 15. Scalability Discussion

### How Hadoop handles 1 GB+ datasets

Hadoop's architecture is designed from the ground up for datasets that do not fit in memory on a single machine. The key mechanisms are:

**1. Block-Based Storage (HDFS)**
HDFS splits files into 128 MB blocks (configurable). Each block is stored on a DataNode and replicated 3 times for fault tolerance. A 1 GB file = ~8 blocks = ~8 parallel mapper tasks.

**2. Mapper Parallelism**
One mapper task runs per input split. All mappers run simultaneously across the cluster nodes. With 32 cores in the cluster and 8 splits, all 8 mappers can run in parallel, taking roughly the same time as processing one 128 MB block.

**3. Shuffle and Sort Optimization**
- Mapper output is sorted in-memory (using a circular buffer) before spilling to local disk. This reduces the number of random disk seeks during the merge phase.
- Intermediate data is compressed (Snappy codec enabled in `RegionDriver.java`) to reduce network bandwidth by 50–70%.

**4. Reducer Parallelism**
Four reducers run simultaneously. Each reducer processes one region independently. For very large datasets with millions of stores per region, all four reducers work in parallel, cutting total reduce time by 4×.

**5. Combiner (Optional Optimization)**
For even larger datasets, a Combiner can be added that partially aggregates mapper output locally before the shuffle. This reduces the volume of data transferred over the network:
```java
job.setCombinerClass(RegionReducer.class); // same logic as reducer
```
This works because our aggregation (sum) is associative and commutative.

**6. Horizontal Scaling**
To handle a 10 GB dataset, simply add more DataNodes to the HDFS cluster. Hadoop automatically distributes the extra blocks across new nodes, and YARN schedules mapper tasks on those nodes. No code changes required.

**7. Data Skew Considerations**
If one region (e.g., "North") has 10× more stores than others, its reducer will take 10× longer. Mitigations:
- Add a secondary sort key (store_id) to spread intra-region load.
- Pre-aggregate at map phase with a Combiner.
- Monitor with Hadoop Counter: `RECORDS_PROCESSED` per region.

**8. Memory Tuning**
Set in `mapred-site.xml` or job configuration:
```xml
<property>
  <name>mapreduce.map.memory.mb</name>
  <value>1024</value>
</property>
<property>
  <name>mapreduce.reduce.memory.mb</name>
  <value>2048</value>
</property>
```

---

## 16. Assumptions

1. The input CSV uses comma as the delimiter with no quoted fields.
2. The `visitors` column contains integer values ≥ 0.
3. The four known regions are: North, South, East, West.
4. Unknown regions are routed to partition 0 (North's reducer) and logged.
5. The header line (`store_id,region,...`) is always the first line and is skipped silently.
6. HDFS and YARN are running on the Cloudera cluster before job submission.
7. The output directory does not need to be created manually — the Driver deletes and recreates it.

---

## 17. Testing Strategy

### Unit Test — CSV Parsing

Create a small test file with known records and verify mapper output using `MRUnit`:

```java
// MRUnit mapper test (add mrunit JAR to classpath)
MapDriver<LongWritable, Text, Text, RegionMetricsWritable> mapDriver;
mapDriver = MapDriver.newMapDriver(new RegionMapper());
mapDriver.withInput(new LongWritable(0), new Text("S001,North,500,2500,2020-01-15"));
mapDriver.withOutput(new Text("North"), new RegionMetricsWritable(500, 1));
mapDriver.runTest();
```

### Edge Case Tests

| Test Case | Expected Behaviour |
|---|---|
| Empty line | Silently skipped |
| Header line | Silently skipped |
| Fewer than 5 fields | `RECORDS_SKIPPED` counter incremented, WARN logged |
| Negative visitors | `RECORDS_SKIPPED` counter incremented |
| Non-numeric visitors | `RECORDS_SKIPPED` counter incremented |
| Unknown region | Routed to partition 0, WARN logged |

### Large Dataset Test

See Section 18 for 1 GB+ dataset generation commands.

---

## 18. Performance Discussion

### Generating a 1 GB+ Test Dataset

```bash
# Step 1 — Create a base dataset with 1000 records
cat > /tmp/base.csv << 'EOF'
S001,North,500,2500,2020-01-15
S002,South,400,2000,2020-02-20
S003,East,700,3500,2020-03-10
S004,West,550,2750,2020-04-05
S005,North,600,3000,2020-05-12
S006,South,450,2250,2020-06-18
S007,East,720,3600,2020-07-22
S008,West,480,2400,2020-08-30
EOF

# Step 2 — Duplicate it to generate ~1 GB
# Each line is ~45 bytes. We need ~22,000,000 lines for 1 GB.
yes "$(cat /tmp/base.csv)" | head -n 22000000 > /tmp/stores_1gb.csv

# Alternative using seq for unique store IDs (more realistic):
python3 -c "
import random, sys
regions = ['North','South','East','West']
for i in range(1, 10000001):
    r = random.choice(regions)
    v = random.randint(100, 2000)
    rev = v * 5
    print(f'S{i:07d},{r},{v},{rev},2020-01-01')
" > /tmp/stores_1gb.csv

# Step 3 — Check file size
ls -lh /tmp/stores_1gb.csv
wc -l /tmp/stores_1gb.csv

# Step 4 — Split into multiple smaller files (optional, for testing splits)
split -l 1000000 /tmp/stores_1gb.csv /tmp/stores_part_

# Step 5 — Upload to HDFS
hdfs dfs -mkdir -p /user/cloudera/input/stores_large
hdfs dfs -put /tmp/stores_1gb.csv /user/cloudera/input/stores_large/
hdfs dfs -ls -h /user/cloudera/input/stores_large/
```

### Performance Monitoring

```bash
# During job execution, check YARN Web UI:
# http://localhost:8088/cluster/apps

# View mapper/reducer progress in the terminal (printed by job.waitForCompletion(true))

# After job completion, view counters:
hadoop job -counters <job_id>

# View job history:
mapred job -history /user/cloudera/output/regions/_logs
```

---

## 19. Best Practices

| Practice | Implementation |
|---|---|
| Object reuse | `outKey` and `outValue` declared as instance fields, not created per `map()` call |
| Defensive parsing | All `Long.parseLong()` calls wrapped in try-catch |
| Hadoop Counters | `RECORDS_PROCESSED` and `RECORDS_SKIPPED` tracked for data quality auditing |
| Dual logging | Both `LOG.warn` (Hadoop log4j) and `System.err` for visibility |
| Compression | Snappy compression on mapper output enabled in Driver |
| Auto-cleanup | Driver deletes existing output directory automatically |
| Configured + Tool | Job uses `ToolRunner` to support generic Hadoop options |
| Null safety | All CSV fields trimmed before validation |
| Header skip | CSV header detected by checking `startsWith("store_id")` |
| Comment coverage | All classes and methods documented with purpose and rationale |

---

## 20. Troubleshooting

### Problem: `Output directory already exists`
**Solution:** The Driver auto-deletes it. If you see this error, ensure `FileSystem.delete()` is not being blocked by HDFS permissions.
```bash
hdfs dfs -rm -r /user/cloudera/output/regions
```

### Problem: `ClassNotFoundException: com.region.partitioning.RegionDriver`
**Solution:** The JAR was not exported with the classes bundled.
- In Eclipse: re-export as **Runnable JAR** with **"Package required libraries"** selected.
- Check: `jar tf region-partitioning.jar | grep RegionDriver`

### Problem: `java.lang.NumberFormatException` in logs
**Solution:** The `visitors` column contains non-numeric data. Check your input CSV for corrupted rows. The Mapper's try-catch block will skip them — check the `RECORDS_SKIPPED` counter in the job output.

### Problem: All output goes to `part-r-00000` only
**Solution:** `job.setNumReduceTasks()` was not called, or was set to 1. Verify `RegionDriver.java` calls `job.setNumReduceTasks(4)`.

### Problem: `Connection refused` when running `hadoop jar`
**Solution:** YARN is not running.
```bash
# On Cloudera QuickStart VM
sudo service hadoop-yarn-resourcemanager start
sudo service hadoop-yarn-nodemanager start
# Or use the Cloudera Manager UI
```

### Problem: Mapper output not reaching correct reducer
**Solution:** `RegionPartitioner` is not registered.
```java
// Verify this line exists in RegionDriver.java
job.setPartitionerClass(RegionPartitioner.class);
```

### Problem: OutOfMemoryError on Reducer
**Solution:** Increase reducer heap in `mapred-site.xml`:
```xml
<property>
  <name>mapreduce.reduce.java.opts</name>
  <value>-Xmx1536m</value>
</property>
```

### Checking Hadoop Logs

```bash
# YARN application logs (replace <app_id> from job output)
yarn logs -applicationId application_<timestamp>_<id>

# MapReduce job history
mapred job -history all /user/cloudera/output/regions

# Individual task logs (via YARN Web UI)
# http://localhost:8088 → Running/Completed Apps → Click app → Logs
```

---

*Generated for Section 2 — Task 12: Region-Based Partitioning*
*Hadoop MapReduce | Java | Cloudera CDH | Eclipse IDE*
