# MapReduce Implementation in Python

A distributed MapReduce system implemented in Python with Docker, gRPC, and HDFS. This system supports distributed computation across multiple worker nodes with built-in worker failure handling and task reassignment.

## Overview

This MapReduce implementation provides:
- **Distributed Processing**: 4 worker containers with 1 CPU each for parallel execution
- **Fault Tolerance**: Automatic worker failure detection and task reassignment
- **Flexible Tasking**: Support for arbitrary number of map/reduce tasks distributed across workers
- **HDFS Storage**: Multi-container HDFS cluster with namenode and datanodes
- **gRPC Communication**: Efficient RPC-based communication between components
- **Parquet Format**: Efficient columnar storage for all data

### Architecture

```
Client (host) ──gRPC──> Master ──gRPC──> Workers (4x)
                           │
                           └──────> HDFS (namenode + 3 datanodes)
```

**Components:**
- **Client**: Runs on host machine, submits jobs and monitors status
- **Master**: Centralized coordinator that manages job lifecycle and task scheduling
- **Workers**: Execute map and reduce tasks (4 replicas, 1 CPU each)
- **HDFS Cluster**: Distributed file system with 1 namenode and 3 datanodes

## Prerequisites

- Docker and Docker Compose
- Python 3.10+ (for client running on host)
- At least 8GB RAM and 4 CPU cores recommended

## Quick Start

Here's a complete workflow to test the system:

```bash
# 1. Set project name and navigate to directory
export PROJECT=mapreduce
cd MapReduceImpl

# 2. Build the base HDFS image
docker build -t mapreduce-hdfs -f Dockerfile.hdfs .

# 3. Build all services
docker compose build

# 4. Start all services
docker compose up -d

# 5. Wait a few seconds for HDFS to initialize, then verify services
docker compose ps

# 6. Copy data generation script to master container
docker cp create_test_data.py mapreduce-master-1:/create_test_data.py

# 7. Generate test data (must run inside container)
docker compose exec master bash -c 'export CLASSPATH=`$HADOOP_HOME/bin/hdfs classpath --glob` && python3 /create_test_data.py --size 10MB'

# 8. Verify data in HDFS
docker compose exec master bash -c 'export CLASSPATH=`$HADOOP_HOME/bin/hdfs classpath --glob` && hdfs dfs -ls -h hdfs://namenode:9000/data/'

# 9. Set up Python virtual environment on host (for client)
python3 -m venv venv
source venv/bin/activate
pip install grpcio grpcio-tools

# 10. Generate gRPC protocol files (if not already present)
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. map_reduce.proto

# 11. Submit a MapReduce job (from host)
python3 client.py

# 12. Check results (note: must use full HDFS URI)
docker compose exec master bash -c 'export CLASSPATH=`$HADOOP_HOME/bin/hdfs classpath --glob` && hdfs dfs -ls -h hdfs://namenode:9000/output/'

# 13. View MapReduce job logs
# Use appropriate job ID from step 11 output
docker logs mapreduce-master-1 2>&1 | grep "job-<job-id>"

# 14. Verify output correctness
# See "Verifying Results" section below for complete verification script
```

For detailed explanations of each step, see the sections below.

## Building the System

### 1. Set Project Name

```bash
export PROJECT=mapreduce
```

### 2. Build Base HDFS Image First

The system uses a layered Docker build. Build the base HDFS image first:

```bash
cd /home/shukla35/MapReduceImpl
docker build -t mapreduce-hdfs -f Dockerfile.hdfs .
```

### 3. Build All Services

```bash
docker compose build
```

This builds:
- `mapreduce-namenode`: HDFS namenode
- `mapreduce-datanode`: HDFS datanodes (3 replicas)
- `mapreduce-master`: Master coordinator
- `mapreduce-worker`: Worker nodes (4 replicas)

## Setting up the Python Environment

### For Client (Running on Host)

The client script (`client.py`) runs on your host machine and communicates with the Master service via gRPC. You need to install the gRPC Python packages on your host.

It is recommended to use a virtual environment to manage Python dependencies:

1.  **Create a virtual environment:**

    ```bash
    python3 -m venv venv
    ```

2.  **Activate the virtual environment:**

    -   On macOS and Linux:
        ```bash
        source venv/bin/activate
        ```
    -   On Windows:
        ```bash
        .\\venv\\Scripts\\activate
        ```

3.  **Install required packages:**

    With the virtual environment activated, install the gRPC packages:
    ```bash
    pip install grpcio grpcio-tools
    ```

**Note:** You do NOT need to install Hadoop, Java, PyArrow, or Pandas on your host machine. These are already installed in the Docker containers. Any scripts that interact with HDFS (like `create_test_data.py`) must be run inside a container.

## Starting the System

### 1. Start All Services

```bash
export PROJECT=mapreduce
docker compose up -d
```

This starts:
- 1 HDFS namenode
- 3 HDFS datanodes
- 1 Master service
- 4 Worker services (each with 1 CPU limit)

### 2. Verify Services Are Running

```bash
docker compose ps
```

All services should show as "running". Wait a few seconds for HDFS to initialize.

### 3. Check HDFS

```bash
docker compose exec namenode hadoop fs -ls /
```

### 4. View Logs

```bash
# View all logs
docker compose logs -f

# View specific service
docker compose logs -f master
docker compose logs -f namenode

# View worker logs (note: workers are scaled, so check specific container)
docker compose logs -f
```

## Uploading Example Data

### Generate Test Datasets

The system includes a test data generator that **must be run inside a container** (it requires access to HDFS libraries):

```bash
# Generate sample dataset (run inside master container)
docker compose exec master bash -c 'export CLASSPATH=`$HADOOP_HOME/bin/hdfs classpath --glob` && python3 /create_test_data.py --size 10MB'
```

This creates a Parquet file in HDFS at `/data/test_10mb.parquet`.

Supported sizes: `1MB`, `10MB`, `100MB`, `500MB`, or `1GB`

**Note:** The script is copied to the container as `/create_test_data.py`. If you modify the script, copy it to the container:
```bash
docker cp create_test_data.py mapreduce-master-1:/create_test_data.py
```

### Verify Data Upload

```bash
# Using HDFS commands inside a container with proper CLASSPATH
docker compose exec master bash -c 'export CLASSPATH=`$HADOOP_HOME/bin/hdfs classpath --glob` && hdfs dfs -ls -h hdfs://namenode:9000/data/'

# Or use hadoop fs command (shorter, but less explicit)
docker compose exec master bash -c 'export CLASSPATH=`$HADOOP_HOME/bin/hdfs classpath --glob` && hadoop fs -ls -h /data/'
```

Expected output:
```
Found 1 items
-rw-r--r--   3 root supergroup      2.7 M <timestamp> hdfs://namenode:9000/data/test_10mb.parquet
```

### View HDFS Web UI

Access the HDFS namenode web interface at:
```
http://localhost:9870
```

## Writing MapReduce Jobs

### Map Function

Create a file `user_funcs/map_func.py`:

```python
def map_function(key, record):
    """
    Process each record and emit (key, value) pairs.
    
    Args:
        key: Record index
        record: Dictionary with record data
    
    Yields:
        Tuples of (key, value)
    """
    # Example: Word count from text field
    if 'text' in record:
        words = record['text'].split()
        for word in words:
            yield (word.lower(), 1)
    
    # Example: Group by value buckets
    if 'value' in record:
        bucket = (record['value'] // 10) * 10
        yield (f"bucket_{bucket}", 1)
```

### Reduce Function

Create a file `user_funcs/reduce_func.py`:

```python
def reduce_function(key, values):
    """
    Aggregate all values for a key.
    
    Args:
        key: The key to aggregate
        values: List of all values for this key
    
    Yields:
        Tuples of (key, aggregated_value)
    """
    # Example: Sum all values
    total = sum(values)
    yield (key, total)
```

### Complete Example: Word Count

**user_funcs/map_func.py:**
```python
def map_function(key, record):
    if 'text' in record:
        words = record['text'].split()
        for word in words:
            word = word.lower().strip('.,!?;:')
            if word:
                yield (word, 1)
```

**user_funcs/reduce_func.py:**
```python
def reduce_function(key, values):
    yield (key, sum(values))
```

## Submitting Jobs

### Prerequisites

Before submitting jobs, ensure:
1. You have generated test data (see "Uploading Example Data" section)
2. You have gRPC packages installed: `pip install grpcio grpcio-tools`
3. You have generated the protocol files: `python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. map_reduce.proto`

### Method 1: Using client.py

The `client.py` script is configured to submit a job with the test data. Run from your host machine:

```bash
# From the host machine (with venv activated)
python3 client.py
```

The client will:
1. Connect to the master service via gRPC
2. Submit a MapReduce job using the pre-generated dataset
3. Poll and display job status
4. Show the output path when complete

**Note:** The client connects to the master via gRPC (port 50051) and does NOT need HDFS access. Data must be pre-generated using `create_test_data.py` (see above)

### Method 2: Custom Job Submission

Modify `client.py` or create your own client script:

```python
import grpc
import map_reduce_pb2
import map_reduce_pb2_grpc

# Connect to master
channel = grpc.insecure_channel("localhost:50051")
stub = map_reduce_pb2_grpc.MasterStub(channel)

# Submit job
request = map_reduce_pb2.SubmitJobRequest(
    dataset_path="hdfs://namenode:9000/data/sample.parquet",
    num_map_tasks=8,      # Number of map tasks
    num_reduce_tasks=4,   # Number of reduce tasks
    map_function_path="/app/user_funcs/map_func.py",
    reduce_function_path="/app/user_funcs/reduce_func.py"
)

response = stub.SubmitJob(request)
print(f"Job ID: {response.job_id}")
print(f"Status: {response.status}")
```

### Job Parameters

- `dataset_path`: HDFS path to input Parquet file (must start with `hdfs://namenode:9000`)
- `num_map_tasks`: Number of map tasks (can exceed worker count - they'll be queued)
- `num_reduce_tasks`: Number of reduce tasks and output partitions
- `map_function_path`: Path to map function inside worker containers
- `reduce_function_path`: Path to reduce function inside worker containers

## Monitoring Jobs

The client automatically polls for job status. You can also monitor through logs:

```bash
# Watch master logs for job progress
docker compose logs -f master

# Watch all services
docker compose logs -f
```

Master logs show detailed execution with enhanced logging:
- ✓ Worker registrations and heartbeats
- → Task assignments (MAP/REDUCE) with worker addresses
- ✓ Task completions with worker identification
- ====== Phase boundaries (MAP phase, REDUCE phase)
- Job completion with duration and statistics

**Example log output:**
```
[MASTER] ✓ New worker registered: 172.18.0.5:50053
[MASTER]   Total workers: 1
...
[MASTER] Received job submission: job-9e7b2fe1
[MASTER]   Dataset: hdfs://namenode:9000/data/test_10mb.parquet
[MASTER]   Map tasks: 8
[MASTER]   Reduce tasks: 4
[MASTER] 4 workers available

[MASTER] ====== Starting MAP phase for job job-9e7b2fe1 ======
[MASTER]   Number of map tasks: 8
[MASTER] → Assigning MAP task job-9e7b2fe1-map-0 to worker 172.18.0.5:50053
[MASTER] → Assigning MAP task job-9e7b2fe1-map-1 to worker 172.18.0.9:50053
[MASTER] ✓ MAP task job-9e7b2fe1-map-0 completed by 172.18.0.5:50053
[MASTER] ✓ MAP task job-9e7b2fe1-map-1 completed by 172.18.0.9:50053
...
[MASTER] ====== MAP phase completed for job job-9e7b2fe1 ======

[MASTER] ====== Starting REDUCE phase for job job-9e7b2fe1 ======
[MASTER]   Number of reduce tasks: 4
[MASTER] → Assigning REDUCE task job-9e7b2fe1-reduce-0 to worker 172.18.0.5:50053
[MASTER] ✓ REDUCE task job-9e7b2fe1-reduce-0 completed by 172.18.0.5:50053
...
[MASTER] ====== REDUCE phase completed for job job-9e7b2fe1 ======

[MASTER] Job job-9e7b2fe1 COMPLETED in 8.85s
[MASTER]   Failures: 0
[MASTER]   Reassignments: 0
```

Worker logs show task execution:
```
[WORKER 2183fcacf3c4] ✓ Successfully registered with master
[WORKER 2183fcacf3c4] Heartbeat thread started
[WORKER 2183fcacf3c4] Received task: job-1e6b18cf-map-3 (type: 0)
[WORKER 2183fcacf3c4] Task job-1e6b18cf-map-3 completed successfully (total: 1)
[WORKER 2183fcacf3c4] Received task: job-1e6b18cf-reduce-3 (type: 1)
[WORKER 2183fcacf3c4] Task job-1e6b18cf-reduce-3 completed successfully (total: 2)
[WORKER 2183fcacf3c4] ♥ Heartbeat sent to master
```

## Accessing Results

### List Output Files

**Important:** Always use the full HDFS URI `hdfs://namenode:9000/` prefix for reliability:

```bash
# List all job outputs
docker compose exec master bash -c 'export CLASSPATH=`$HADOOP_HOME/bin/hdfs classpath --glob` && hdfs dfs -ls -h hdfs://namenode:9000/output/'

# List specific job output (replace <job-id> with actual job ID)
docker compose exec master bash -c 'export CLASSPATH=`$HADOOP_HOME/bin/hdfs classpath --glob` && hdfs dfs -ls -h hdfs://namenode:9000/output/job-<job-id>/'
```

Example output:
```
Found 4 items
-rw-r--r--   3 root supergroup      2.3 K 2025-11-11 04:54 hdfs://namenode:9000/output/job-1e6b18cf/part-0.parquet
-rw-r--r--   3 root supergroup      2.3 K 2025-11-11 04:54 hdfs://namenode:9000/output/job-1e6b18cf/part-1.parquet
-rw-r--r--   3 root supergroup      2.2 K 2025-11-11 04:54 hdfs://namenode:9000/output/job-1e6b18cf/part-2.parquet
-rw-r--r--   3 root supergroup      2.3 K 2025-11-11 04:54 hdfs://namenode:9000/output/job-1e6b18cf/part-3.parquet
```

Each job creates output files (one per reduce task):
```
/output/job-abc123/part-0.parquet  # Results for keys hashed to partition 0
/output/job-abc123/part-1.parquet  # Results for keys hashed to partition 1
/output/job-abc123/part-2.parquet  # Results for keys hashed to partition 2
/output/job-abc123/part-3.parquet  # Results for keys hashed to partition 3
```

**Important:** Keys are distributed across partitions by hash. To get complete aggregated results for a specific key (like `x_bucket_0_sum`), you must read ALL partitions and sum the partial results.

### Option 1: Read Results Inside Container (Recommended)

Create a Python script inside a container to read and analyze results:

```bash
# Create a script to read results
docker compose exec master bash -c 'cat > /read_results.py << "EOF"
import pyarrow.parquet as pq
import pyarrow as pa
import sys

job_id = sys.argv[1] if len(sys.argv) > 1 else "job-abc123"

# Connect to HDFS
fs = pa.fs.HadoopFileSystem("namenode", 9000)

# Read first partition
table = pq.read_table(f"/output/{job_id}/part-0.parquet", filesystem=fs)
df = table.to_pandas()
print(df.head(10))
print(f"\nTotal rows in part-0: {len(df)}")
EOF'

# Run the script
docker compose exec master bash -c 'export CLASSPATH=`$HADOOP_HOME/bin/hdfs classpath --glob` && python3 /read_results.py job-abc123'
```

### Option 2: Download Results to Host

Download Parquet files from HDFS to your local machine:

```bash
# Create local output directory
mkdir -p ./results

# Download all partitions
docker compose exec master bash -c 'export CLASSPATH=`$HADOOP_HOME/bin/hdfs classpath --glob` && hdfs dfs -get /output/job-abc123/*.parquet /tmp/'
docker cp mapreduce-master-1:/tmp/part-0.parquet ./results/
docker cp mapreduce-master-1:/tmp/part-1.parquet ./results/
# ... repeat for all partitions
```

Then read locally with Python (on your host):

```python
import pandas as pd
import pyarrow.parquet as pq

# Read a partition
df = pd.read_parquet('./results/part-0.parquet')
print(df.head())

# Or read all partitions
import glob
files = glob.glob('./results/part-*.parquet')
dfs = [pd.read_parquet(f) for f in files]
final_df = pd.concat(dfs, ignore_index=True)
print(f"Total rows: {len(final_df)}")
```

### Option 3: Read Directly from Host (Requires Java Setup)

If you have Java and Hadoop libraries installed on your host:

```python
import pyarrow.parquet as pq
import pyarrow as pa

# Connect to HDFS (requires Java/libhdfs on host)
fs = pa.fs.HadoopFileSystem("localhost", 9000)

# Read partitions
table = pq.read_table("/output/job-abc123/part-0.parquet", filesystem=fs)
df = table.to_pandas()
print(df)
```

**Note:** This requires Java 11+ and proper environment variables (`JAVA_HOME`, `CLASSPATH`) set on your host. The container-based approach (Option 1) is recommended as it avoids host configuration issues.

## Verifying Results

### Quick View: See Output Data

To quickly see what your MapReduce job calculated:

```bash
# Read output from a single partition (replace <job-id>)
docker compose exec master bash -c 'export CLASSPATH=`$HADOOP_HOME/bin/hdfs classpath --glob` && python3 << "EOF"
import pyarrow.parquet as pq
import pyarrow as pa

fs = pa.fs.HadoopFileSystem("namenode", 9000)
table = pq.read_table("/output/job-<job-id>/part-0.parquet", filesystem=fs)
df = table.to_pandas()
print("Sample output from partition 0:")
print(df.head(20))
print(f"\nTotal rows in partition 0: {len(df)}")
EOF'
```

**Example output:**
```
Sample output from partition 0:
              key          value
0  x_bucket_0_sum  128794.252537
1  x_bucket_0_count    2597.000000
2  x_bucket_0_avg      49.593474
3  x_bucket_10_sum  321684.425506
4  x_bucket_10_count    6553.000000
5  x_bucket_10_avg      49.089642
...

Total rows in partition 0: 42
```

Each row represents an aggregated statistic:
- `x_bucket_0_sum`: Total sum of values where x ∈ [0,9]
- `x_bucket_0_count`: Number of records where x ∈ [0,9]
- `x_bucket_0_avg`: Average value where x ∈ [0,9]

### Verify Calculations

The example map/reduce functions calculate statistics (sum, count, average) for bucketed data. Here's how to verify correctness:

```bash
# Replace <job-id> with your actual job ID
docker compose exec master bash -c 'export CLASSPATH=`$HADOOP_HOME/bin/hdfs classpath --glob` && python3 << "EOF"
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd

fs = pa.fs.HadoopFileSystem("namenode", 9000)

# Read input data
input_table = pq.read_table("/data/test_10mb.parquet", filesystem=fs)
input_df = input_table.to_pandas()

print("INPUT DATA")
print(f"Total rows: {len(input_df):,}")
print(f"Columns: {list(input_df.columns)}")

# Calculate expected values for x_bucket_0 (x values 0-9)
x_bucket_0_data = input_df[input_df["x"] < 10]
expected_sum = x_bucket_0_data["value"].sum()
expected_count = len(x_bucket_0_data)
expected_avg = expected_sum / expected_count

print(f"\nEXPECTED (x_bucket_0):")
print(f"  Sum:     {expected_sum:.6f}")
print(f"  Count:   {expected_count}")
print(f"  Average: {expected_avg:.6f}")

# Read MapReduce output from all partitions
dfs = []
for i in range(4):
    table = pq.read_table(f"/output/<job-id>/part-{i}.parquet", filesystem=fs)
    dfs.append(table.to_pandas())
output_df = pd.concat(dfs, ignore_index=True)

# Aggregate partial results across partitions
mr_sum = output_df[output_df["key"] == "x_bucket_0_sum"]["value"].sum()
mr_count = output_df[output_df["key"] == "x_bucket_0_count"]["value"].sum()
mr_avg = mr_sum / mr_count

print(f"\nMAPREDUCE OUTPUT (x_bucket_0):")
print(f"  Sum:     {mr_sum:.6f}")
print(f"  Count:   {mr_count:.0f}")
print(f"  Average: {mr_avg:.6f}")

# Verify
sum_ok = abs(mr_sum - expected_sum) < 0.01
count_ok = mr_count == expected_count
avg_ok = abs(mr_avg - expected_avg) < 0.01

print(f"\nVERIFICATION:")
print(f"  Sum:     {'✓ CORRECT' if sum_ok else '✗ INCORRECT'}")
print(f"  Count:   {'✓ CORRECT' if count_ok else '✗ INCORRECT'}")
print(f"  Average: {'✓ CORRECT' if avg_ok else '✗ INCORRECT'}")

if sum_ok and count_ok and avg_ok:
    print("\n✅ MapReduce calculations are CORRECT!")
else:
    print("\n❌ MapReduce calculations have errors")
EOF'
```

**What the job calculates:**
- The map function groups records into buckets based on x and y values (0-9, 10-19, 20-29, etc.)
- The reduce function computes sum, count, and average for each bucket
- Each output key has a suffix: `_sum`, `_count`, or `_avg`
- Results are distributed across 4 output partitions (by key hash)

**Expected behavior:**
- For 104,857 input rows with x values 0-99, each bucket should have ~10,000 records
- The MapReduce output should exactly match manual calculations from the input
- All calculations should show "✓ CORRECT" when verified

**Example verification output:**
```
INPUT DATA
Total rows: 104,857
Columns: ['id', 'x', 'y', 'value', 'category', 'text']

EXPECTED (x_bucket_0):
  Sum:     522928.788518
  Count:   10493
  Average: 49.835966

MAPREDUCE OUTPUT (x_bucket_0):
  Sum:     522928.788518
  Count:   10493
  Average: 49.835966

VERIFICATION:
  Sum:     ✓ CORRECT
  Count:   ✓ CORRECT
  Average: ✓ CORRECT

✅ MapReduce calculations are CORRECT!
```

**Note:** The partial sums/counts across the 4 output partitions must be aggregated to get final results. Each partition contains a subset of the keys based on their hash.

## Worker Failure Testing

The system includes built-in fault tolerance. To test it:

### Simulate Worker Failure

```bash
# Kill a worker container
docker compose stop mapreduce-worker-1

# Or force kill
docker kill mapreduce-worker-1
```

The master will:
1. Detect the worker timeout
2. Mark in-progress tasks as failed
3. Reassign tasks to healthy workers
4. Complete the job successfully

### View Failure Statistics

Check master logs after job completion:

```bash
docker compose logs master | grep -A 5 "COMPLETED"
```

You'll see statistics like:
- Total failures
- Number of task reassignments
- Job completion time

## Performance Evaluation

### Run Benchmark Tests

```bash
python3 benchmark.py
```

This runs multiple tests with varying worker counts and data sizes.

### Plot Results

```bash
chmod +x req_install.sh
./req_install.sh
python3 plot_results.py benchmark_results.csv
```

Generates plots showing:
- Speedup with multiple workers
- Impact of worker failures
- Task distribution efficiency

### Recovery Metrics Analysis

For detailed recovery latency and throughput analysis with failure simulation, see **[RECOVERY_METRICS.md](RECOVERY_METRICS.md)**.

The system automatically exports detailed metrics for each job to `./jobs_executed/<job_id>_metrics.json`. These metrics include:
- Recovery latency after node failures
- Throughput before, during, and after failures
- Worker count timeline
- CPU usage over time

**Quick start:**
```bash
# After running a job with failure (see RECOVERY_METRICS.md for details)
# Build the plotter image
docker build -t mapreduce-plotter -f Dockerfile.plotter .

# Generate recovery plots using the plotter container
docker run --rm \
  -v $(pwd)/jobs_executed:/app/jobs_executed \
  -v $(pwd)/plots:/app/plots \
  mapreduce-plotter \
  python3 plot_recovery.py --input /app/jobs_executed/<job_id>_metrics.json --output-dir /app/plots

# Or run directly on host (requires matplotlib, pandas, numpy)
python3 plot_recovery.py --input ./jobs_executed/<job_id>_metrics.json --output-dir ./plots

# View the generated plots
open plots/recovery_timeline.png
```

See [RECOVERY_METRICS.md](RECOVERY_METRICS.md) for complete documentation on recovery metrics collection and visualization.

## Troubleshooting

### Services Won't Start

```bash
# Check if ports are in use
netstat -tuln | grep -E '9000|9870|50051'

# Check Docker logs
docker compose logs
```

### HDFS Not Ready

Wait 10-20 seconds for HDFS to fully initialize:

```bash
# Check namenode status
docker compose logs namenode

# Verify HDFS is accessible (use hdfs dfs with full URI)
docker compose exec master bash -c 'export CLASSPATH=`$HADOOP_HOME/bin/hdfs classpath --glob` && hdfs dfs -ls hdfs://namenode:9000/'
```

### "Unable to load libhdfs" or "Unable to load libjvm" Errors

These errors occur when trying to run HDFS-dependent scripts from the host machine. **Solution:**

- Always run scripts that interact with HDFS **inside a container** (master or worker)
- Use `docker compose exec` to run commands inside containers
- Example: `docker compose exec master bash -c 'export CLASSPATH=... && python3 /script.py'`

The containers have all required libraries (Java, Hadoop, libhdfs) pre-installed. Running on the host requires complex Java/Hadoop setup.

### Workers Can't Connect to Master

```bash
# Verify master is running
docker compose ps master

# Check master logs
docker compose logs master

# Restart services
docker compose restart
```

### Job Fails Immediately

```bash
# Check if input file exists in HDFS (use full URI)
docker compose exec master bash -c 'export CLASSPATH=`$HADOOP_HOME/bin/hdfs classpath --glob` && hdfs dfs -ls hdfs://namenode:9000/data/'

# Verify user functions are present (check any worker)
docker compose exec master bash -c 'docker exec mapreduce-worker-1 ls -la /user_funcs/ 2>/dev/null || echo "Use: docker exec mapreduce-worker-1 ls -la /user_funcs/"'
```

### HDFS Commands Return Local Filesystem

If `hadoop fs -ls /` returns your container's filesystem instead of HDFS, use the full HDFS URI:

```bash
# Wrong (might return local filesystem)
hadoop fs -ls /

# Correct (explicitly uses HDFS)
hdfs dfs -ls hdfs://namenode:9000/
```

## Stopping the System

### Stop All Services

```bash
docker compose down
```

### Stop and Remove Volumes (Clean Slate)

```bash
docker compose down -v
```

This removes all HDFS data. Use with caution!

## Project Structure

```
MapReduceImpl/
├── master.py                 # Master coordinator service
├── worker.py                 # Worker service
├── client.py                 # Client for job submission
├── map_reduce.proto          # gRPC protocol definitions
├── map_reduce_pb2.py         # Generated protobuf code
├── map_reduce_pb2_grpc.py    # Generated gRPC code
├── docker-compose.yml        # Service orchestration
├── Dockerfile.hdfs           # Base HDFS image
├── Dockerfile.namenode       # HDFS namenode
├── Dockerfile.datanode       # HDFS datanode
├── Dockerfile.master         # Master service
├── Dockerfile.worker         # Worker service
├── user_funcs/               # User-defined functions
│   ├── map_func.py
│   └── reduce_func.py
├── create_test_data.py       # Test data generator
├── benchmark.py              # Performance benchmarking
├── plot_results.py           # Visualization
└── README.md                 # This file
```

## Design Documentation

For detailed architecture and design decisions, see [Design.md](Design.md).

## Special Features

1. **Worker Failure Handling**: Automatic detection and task reassignment
2. **Task Timeout Detection**: Tasks that take too long are reassigned
3. **Flexible Task Distribution**: More tasks than workers are automatically queued
4. **HDFS Integration**: Reliable distributed storage with replication

## Important: Host vs Container Execution

Understanding where to run different components is crucial:

### Run on Host Machine:
- ✅ `client.py` - Submits jobs via gRPC (requires: `grpcio`, `grpcio-tools`)
- ✅ Viewing logs: `docker compose logs`
- ✅ Managing containers: `docker compose up/down`

### Run Inside Containers (via `docker compose exec`):
- ✅ `create_test_data.py` - Uploads data to HDFS
- ✅ HDFS commands (`hdfs dfs`, `hadoop fs`)
- ✅ Any Python scripts that read/write HDFS data
- ✅ Reading MapReduce results from HDFS

### Why?
Scripts that interact with HDFS require:
- Java 11+ (JVM)
- Hadoop libraries (`libhdfs.so`, `libjvm.so`)
- Proper `CLASSPATH` environment variable

These are **pre-installed in all containers** but would require complex setup on the host. The container-based approach is simpler, more reliable, and platform-independent.

### Quick Reference

```bash
# Run client from host (gRPC only, no HDFS access needed)
python3 client.py

# Run HDFS operations inside container (requires Hadoop libraries)
docker compose exec master bash -c 'export CLASSPATH=`$HADOOP_HOME/bin/hdfs classpath --glob` && <command>'
```

## Contributing

This is an educational project for CS 544 at UW-Madison. Contributions should maintain:
- Clear separation of concerns
- Comprehensive error handling
- Detailed logging for debugging
- Test coverage for new features

## License

Educational use only - CS 544 project.
