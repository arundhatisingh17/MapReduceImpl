# MapReduce Implementation in Python

A distributed MapReduce system implemented in Python with Docker, gRPC, and HDFS. This system supports distributed computation across multiple worker nodes with built-in worker failure handling and task reassignment.

## Overview

This MapReduce implementation provides:
- **Distributed Processing**: 4 worker containers with 1 CPU each for parallel execution
- **Fault Tolerance**: Automatic worker failure detection and task reassignment
- **Flexible Tasking**: Support for arbitrary number of map/reduce tasks distributed across workers
- **HDFS Storage**: Shared storage for input, intermediate, and output data
- **gRPC Communication**: Efficient RPC-based communication between components
- **Parquet Format**: Efficient columnar storage for all data

### Architecture

```
Client ──gRPC──> Scheduler ──gRPC──> Boss ──gRPC──> Workers (4x)
                                             │
                                             └──────> HDFS
```

**Components:**
- **Client**: Submits jobs and monitors status
- **Scheduler**: Receives jobs and coordinates with Boss
- **Boss**: Orchestrates task distribution and monitors worker health
- **Workers**: Execute map and reduce tasks
- **HDFS**: Distributed file system for data storage

## Prerequisites

- Docker and Docker Compose
- Python 3.9+ (for client if running outside container)
- At least 8GB RAM and 4 CPU cores recommended

## Building the System

### 1. Build Proto Files (Optional - done in Docker)

The proto files are automatically compiled inside Docker containers, but if you want to compile them locally:

```bash
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. map_reduce.proto
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. boss.proto
```

### 2. Build Docker Images

Set the project name and build all images:

```bash
export PROJECT=mapreduce
docker compose build
docker compose up -d
```

This builds:
- `mapreduce-scheduler`: Scheduler service
- `mapreduce-boss`: Boss orchestrator
- `mapreduce-worker`: Worker nodes (4 instances)
- `mapreduce-client`: Client container
- `p4-hdfs`: HDFS container (if not already built)

## Installation

### For Running Client Outside Container

Install required Python packages:

```bash
pip install grpcio grpcio-tools pandas pyarrow numpy matplotlib
```

## Starting the System

### 1. Start All Services

```bash
export PROJECT=mapreduce
docker compose build
docker compose up -d
```

This starts:
- 1 HDFS namenode
- 1 Scheduler
- 1 Boss
- 4 Workers (worker1, worker2, worker3, worker4)

### 2. Verify Services Are Running

```bash
docker compose ps
```

All services should show as "running".

### 3. Check HDFS

```bash
docker compose exec hdfs hadoop fs -ls /
```

### 4. View Logs

```bash
# View all logs
docker compose logs -f

# View specific service
docker compose logs -f boss
docker compose logs -f worker1
```

## Uploading Example Data

### Generate Test Datasets

The system includes a test data generator:

```bash
# From within the client container
docker compose run client python create_test_data.py --size 10MB

# Or specify custom size and path
docker compose run client python create_test_data.py --size 50MB --output hdfs://nn:9000/data/my_test.parquet
```

Supported sizes: `1MB`, `10MB`, `100MB`, `500MB`, or `1GB`

### Verify Data Upload

```bash
docker compose exec hdfs hadoop fs -ls /data/
docker compose exec hdfs hadoop fs -du -h /data/
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
    # Example: Group by x value buckets
    if 'x' in record:
        x_bucket = (record['x'] // 10) * 10
        yield (f"bucket_{x_bucket}", record['value'])
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

### Example: Word Count

**map_func.py:**
```python
def map_function(key, record):
    if 'text' in record:
        words = record['text'].split()
        for word in words:
            yield (word.lower(), 1)
```

**reduce_func.py:**
```python
def reduce_function(key, values):
    yield (key, sum(values))
```

## Submitting Jobs

### Method 1: Modify client.py

Edit `client.py` to specify your job parameters:

```python
request = map_reduce_pb2.ScheduleJobRequest(
    dataset_path="hdfs://nn:9000/data/test_10mb.parquet",
    num_partitions=8,  # Number of map tasks
    map_function_path="/app/user_funcs/map_func.py",
    reduce_function_path="/app/user_funcs/reduce_func.py",
    repartition_threshold=0.1,
    custom_hash_func=""
)
```

Then run:

```bash
docker-compose run client python client.py
```

### Method 2: Use Benchmark Script

```bash
docker-compose run client python benchmark.py \
    --datasets hdfs://nn:9000/data/test_10mb.parquet \
    --partitions 8 \
    --runs 3
```

### Job Parameters

- `dataset_path`: HDFS path to input Parquet file
- `num_partitions`: Number of map tasks (can exceed worker count - they'll be distributed)
- `map_function_path`: Path to map function inside container
- `reduce_function_path`: Path to reduce function inside container

## Monitoring Jobs

The client automatically polls for job status. You can also check logs:

```bash
# Watch job progress
docker-compose logs -f boss

# Check specific worker
docker-compose logs -f worker1
```

## Accessing Results

### List Output Files

```bash
docker compose exec hdfs hadoop fs -ls /output/
```

Each job creates a directory `/output/job-<uuid>/` with partition files:
```
/output/job-xyz/part-0.parquet
/output/job-xyz/part-1.parquet
...
```

### Read Results

```python
import pyarrow.parquet as pq
import pyarrow as pa

fs = pa.fs.HadoopFileSystem("nn", 9000)
table = pq.read_table("/output/job-xyz/part-0.parquet", filesystem=fs)
df = table.to_pandas()
print(df)
```

### Merge Partition Files

```python
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

fs = pa.fs.HadoopFileSystem("nn", 9000)

# Read all partitions
dfs = []
for i in range(num_partitions):
    table = pq.read_table(f"/output/job-xyz/part-{i}.parquet", filesystem=fs)
    dfs.append(table.to_pandas())

# Combine
final_df = pd.concat(dfs, ignore_index=True)
```

## Worker Failure Testing

### Simulate Worker Failures

Set the `FAIL_AFTER` environment variable to make a worker fail after N tasks:

```yaml
# In docker-compose.yml, modify a worker:
worker1:
  environment:
    - WORKER_ID=worker-1
    - FAIL_AFTER=2  # Fail after 2 tasks
```

Then restart:
```bash
docker compose up -d worker1
```

### Observe Failure Recovery

```bash
# Watch the boss detect and reassign tasks
docker compose logs -f boss

# Watch worker failure
docker compose logs -f worker1
```

The Boss will:
1. Detect worker failure (timeout or crash)
2. Mark tasks as failed
3. Reassign tasks to healthy workers
4. Continue job execution

### Run Benchmark Comparisons

```bash
# Normal execution
docker compose run client python benchmark.py \
    --datasets hdfs://nn:9000/data/test_10mb.parquet \
    --output results_normal.csv

# With one worker failing (manually set FAIL_AFTER first)
docker compose run client python benchmark.py \
    --datasets hdfs://nn:9000/data/test_10mb.parquet \
    --output results_with_failures.csv
```

## Performance Analysis

### Generate Plots

```bash
docker compose run client python plot_results.py benchmark_results.csv --output-dir /data/plots
```

This creates:
- `execution_time_by_dataset.png`: Bar chart of execution times
- `failure_comparison.png`: Normal vs. failure recovery performance
- `time_series.png`: Performance across multiple runs

### Key Metrics

The system tracks:
- **Execution time**: Total job duration
- **Failures**: Number of task failures
- **Reassignments**: Number of task reassignments
- **Throughput**: Data processed per second

## Cleanup

### Stop Services

```bash
docker compose down
```

### Remove Data

```bash
# Clear HDFS data
docker compose exec hdfs hadoop fs -rm -r /data/*
docker compose exec hdfs hadoop fs -rm -r /output/*
docker compose exec hdfs hadoop fs -rm -r /intermediate/*
```

### Remove All Containers and Images

```bash
docker compose down -v
docker rmi mapreduce-scheduler mapreduce-boss mapreduce-worker mapreduce-client
```

## Troubleshooting

### Workers Not Registering

Check if Boss is running:
```bash
docker compose logs boss
```

Restart workers:
```bash
docker compose restart worker1 worker2 worker3 worker4
```

### HDFS Connection Issues

Verify HDFS is accessible:
```bash
docker compose exec client python -c "import pyarrow as pa; fs = pa.fs.HadoopFileSystem('nn', 9000); print(fs.get_file_info('/'))"
```

### Job Stuck in SCHEDULED

Check Boss logs for errors:
```bash
docker compose logs boss | grep ERROR
```

Verify Boss can reach workers:
```bash
docker compose exec boss ping worker1
```

### Out of Memory

Reduce dataset size or increase memory limits in `docker-compose.yml`:
```yaml
deploy:
  resources:
    limits:
      memory: 2g  # Increase as needed
```

## Architecture Details

### Component Communication

1. **Client → Scheduler**: Job submission via gRPC
2. **Scheduler → Boss**: Job assignment via gRPC
3. **Boss → Workers**: Task assignment via gRPC
4. **Workers → HDFS**: Data read/write via PyArrow
5. **Boss → Scheduler**: Status updates (future enhancement)

### Data Flow

1. **Input**: Client uploads data to HDFS
2. **Partitioning**: Boss partitions input into N chunks
3. **Map Phase**: Workers read partitions, execute map function, write intermediate results
4. **Shuffle**: Data automatically partitioned by key hash
5. **Reduce Phase**: Workers read intermediate data, execute reduce function, write output
6. **Output**: Final results in HDFS

### Failure Handling

- **Task timeout**: 120 seconds
- **Worker timeout**: 30 seconds  
- **Max retries**: 3 attempts per task
- **Detection**: Heartbeat monitoring + task completion tracking
- **Recovery**: Automatic task reassignment to healthy workers

## Configuration

Key configuration in `boss.py`:
```python
MAX_RETRIES = 3          # Max task retry attempts
TASK_TIMEOUT = 120       # Task timeout in seconds
WORKER_TIMEOUT = 30      # Worker heartbeat timeout
```

Resource limits in `docker-compose.yml`:
```yaml
deploy:
  resources:
    limits:
      cpus: "1.0"        # CPU limit per worker
      memory: 1g         # Memory limit
```

## License

This is an educational project for demonstrating MapReduce concepts.

## Authors

MapReduce Implementation Project

