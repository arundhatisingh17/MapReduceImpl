# MapReduceImpl - Design Document

## Architecture Overview

Our MapReduce implementation follows a simplified master-worker architecture inspired by the classic MapReduce paper. The system consists of three main components:

1. **Master Service** (`master.py`): A centralized coordinator that manages job submission, task scheduling, and worker coordination
2. **Worker Services** (`worker.py`): Multiple worker containers that execute map and reduce tasks
3. **HDFS Cluster**: A distributed file system with one namenode and multiple datanodes for reliable data storage

This design simplifies the original multi-service architecture (boss + scheduler) into a single master service, reducing complexity and potential failure points.

## System Components

### 1. Master Service (`master.py`)

The master service is the central coordinator of the MapReduce system. It:

- **Accepts Job Submissions**: Receives MapReduce jobs from clients via gRPC
- **Manages Task Lifecycle**: Creates map and reduce tasks, assigns them to workers, and tracks their completion
- **Handles Worker Registration**: Maintains a registry of available workers and their current status
- **Coordinates Phases**: Orchestrates the map phase, shuffle phase, and reduce phase
- **Provides Fault Tolerance**: Monitors worker health, detects failures, and reassigns tasks as needed
- **Connects to HDFS**: Reads/writes metadata and partitions data using PyArrow's HDFS filesystem

*Reference*: Inspired by the gRPC server pattern in `/home/shukla35/p4_avasisht2_shukla35/server.py`

### 2. Worker Services (`worker.py`)

Worker services execute the actual map and reduce operations. Each worker:

- **Registers with Master**: Announces itself to the master on startup
- **Executes Tasks**: Runs user-defined map and reduce functions on assigned data partitions
- **Reads from HDFS**: Fetches input data and intermediate results from the distributed file system
- **Writes to HDFS**: Stores map outputs and final reduce results
- **Reports Status**: Sends task completion status back to the master

### 3. HDFS Cluster

A multi-container HDFS deployment provides reliable, distributed storage:

- **Namenode**: Single namenode container managing the filesystem namespace and metadata (hostname: `namenode`)
- **Datanodes**: Multiple datanode containers (replicas: 3-4) storing actual data blocks
- **Data Locality**: Workers can potentially be co-located with datanodes to leverage data locality (special feature)

*Reference*: Architecture based on `/home/shukla35/main/p4/docker-compose.yml`

## Client Interface

The client (`client.py`) runs **outside** of any container (as per ProjectSpec.md) and submits jobs to the master via gRPC. A job submission includes:

- **Dataset Path**: HDFS path to the input data (Parquet format)
- **Number of Map Tasks**: How many map partitions to create
- **Number of Reduce Tasks**: How many reduce partitions to create
- **Map Function Path**: Path to a Python file containing the user's map function
- **Reduce Function Path**: Path to a Python file containing the user's reduce function

The client can also query job status using the job ID returned upon submission.

*Reference*: Client pattern from `/home/shukla35/main/p4/client.py`

## Communication Protocol

All inter-service communication uses **gRPC** with protocol buffers:

- **Client ↔ Master**: Job submission and status queries
- **Master ↔ Workers**: Task assignment and completion reporting
- **All Services ↔ HDFS**: File I/O via PyArrow HadoopFileSystem

The consolidated `map_reduce.proto` defines all RPC services and messages.

## Job Lifecycle

### Phase 1: Job Submission
1. Client submits job to master with input path, number of tasks, and user functions
2. Master assigns a unique job ID and acknowledges receipt
3. Master partitions input data from HDFS into N chunks for N map tasks

### Phase 2: Map Phase
1. Master creates map task assignments (one per input partition)
2. Master assigns map tasks to available workers
3. Workers execute the user's map function on their assigned partition
4. Workers write intermediate key-value pairs to HDFS, partitioned by reduce task ID
5. Master tracks task completion and handles failures/stragglers

### Phase 3: Shuffle Phase
Implicit - map tasks write their outputs pre-partitioned for reduce tasks. Each reduce task reads its corresponding partition from all map outputs.

### Phase 4: Reduce Phase
1. Master creates reduce task assignments (one per reduce partition)
2. Master assigns reduce tasks to available workers
3. Workers read their assigned intermediate partitions from HDFS
4. Workers execute the user's reduce function on grouped key-value pairs
5. Workers write final results to HDFS output path
6. Master tracks task completion

### Phase 5: Completion
1. Master marks job as COMPLETED and updates status
2. Client can retrieve final results from HDFS output path

## Data Storage

### HDFS Directory Structure
```
/data/
  input/              # Original input datasets
  partitions/         # Input partitions created by master
  intermediate/       # Map task outputs, organized by job_id and reduce partition
    job-001/
      reduce-0/       # All map outputs for reduce task 0
      reduce-1/       # All map outputs for reduce task 1
  output/             # Final reduce outputs
    job-001/
      part-0.parquet
      part-1.parquet
```

### File Format
All data (input, intermediate, output) uses **Parquet** format for:
- Efficient columnar storage
- Schema preservation without inference overhead
- Compatibility with PyArrow and pandas

*Reference*: HDFS interaction pattern from `/home/shukla35/p4_avasisht2_shukla35/server.py` lines 54-65

## Language and Framework

- **Language**: Python 3.10
- **RPC Framework**: gRPC with Protocol Buffers
- **Storage**: HDFS (via PyArrow HadoopFileSystem)
- **Data Processing**: pandas + PyArrow
- **Container Orchestration**: Docker Compose

## Deployment

The system deploys via Docker Compose on a single VM with:
- 1 master container
- 4 worker containers (each limited to 1 CPU core, per ProjectSpec.md)
- 1 namenode container
- 3-4 datanode containers

This ensures that distributing tasks across all 4 workers provides better performance than single-worker execution (as required by ProjectSpec.md).

## Special Feature: Data Locality & Fault Tolerance

Our implementation includes two key features:

1. **Worker Failure Handling**: The master monitors worker health via heartbeats and task timeouts. Failed tasks are automatically reassigned to healthy workers with retry limits.

2. **Task Straggler Detection**: The master tracks task execution times and can detect stragglers (slow tasks). This enables launching speculative duplicate tasks for improved performance.

## Testing and Evaluation

### Functional Testing
- End-to-end job execution with sample word count data
- Verification of map and reduce outputs
- Testing with various partition counts and data sizes

### Performance Evaluation
- Measure speedup when using 4 workers vs. 1 worker
- Compare execution time with and without fault tolerance features
- Create plots showing:
  - Job completion time vs. number of workers
  - Task reassignment counts during worker failures
  - Impact of straggler detection on overall job time

