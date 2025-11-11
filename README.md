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

## Installation

### For Client (Running on Host)

Install required Python packages on your host machine:

```bash
pip install grpcio grpcio-tools pandas pyarrow
```

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

The system includes a test data generator:

```bash
# Generate sample dataset (run from host or inside a worker container)
python3 create_test_data.py --size 10MB
```

This creates a Parquet file at `/data/sample.parquet` in HDFS.

Supported sizes: `1MB`, `10MB`, `100MB`, `500MB`, or `1GB`

### Verify Data Upload

```bash
docker compose exec namenode hadoop fs -ls /data/
docker compose exec namenode hadoop fs -du -h /data/
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

### Method 1: Using client.py

The `client.py` script is already configured to submit a sample job. Simply run:

```bash
# From the host machine
python3 client.py
```

The client will:
1. Generate a sample dataset if it doesn't exist
2. Submit a MapReduce job to the master
3. Poll and display job status
4. Show the output path when complete

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

Master logs show:
- Job submission and ID assignment
- Map phase progress
- Reduce phase progress
- Task failures and reassignments
- Job completion with statistics

## Accessing Results

### List Output Files

```bash
docker compose exec namenode hadoop fs -ls /output/
docker compose exec namenode hadoop fs -ls /output/job-<job-id>/
```

Each job creates output files:
```
/output/job-abc123/part-0.parquet
/output/job-abc123/part-1.parquet
/output/job-abc123/part-2.parquet
/output/job-abc123/part-3.parquet
```

### Download and Read Results

From the host machine:

```python
import pyarrow.parquet as pq
import pyarrow as pa

# Connect to HDFS
fs = pa.fs.HadoopFileSystem("localhost", 9000)

# Read a partition
table = pq.read_table("/output/job-abc123/part-0.parquet", filesystem=fs)
df = table.to_pandas()
print(df)
```

### Merge All Partitions

```python
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

fs = pa.fs.HadoopFileSystem("localhost", 9000)

# Read all partitions
job_id = "job-abc123"
num_partitions = 4
dfs = []

for i in range(num_partitions):
    table = pq.read_table(f"/output/{job_id}/part-{i}.parquet", filesystem=fs)
    dfs.append(table.to_pandas())

# Combine
final_df = pd.concat(dfs, ignore_index=True)
print(final_df)
```

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
python3 plot_results.py
```

Generates plots showing:
- Speedup with multiple workers
- Impact of worker failures
- Task distribution efficiency

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

# Verify HDFS is accessible
docker compose exec namenode hadoop fs -ls /
```

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
# Check if input file exists
docker compose exec namenode hadoop fs -ls /data/sample.parquet

# Verify user functions are present
docker compose exec mapreduce-worker-1 ls -la /app/user_funcs/
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

## Contributing

This is an educational project for CS 544 at UW-Madison. Contributions should maintain:
- Clear separation of concerns
- Comprehensive error handling
- Detailed logging for debugging
- Test coverage for new features

## License

Educational use only - CS 544 project.
