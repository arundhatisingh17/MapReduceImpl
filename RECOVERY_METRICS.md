# MapReduce Recovery Metrics Analysis Guide

This guide shows you how to measure and visualize **recovery latency** and system behavior during node failures in your MapReduce system.

## What You'll Get

A comprehensive visualization showing:
- **Node failure markers** (when workers die)
- **Recovery completion** (when system recovers)
- **Recovery latency** (per-task recovery times)
- **Worker count** over time
- **CPU usage** during job execution
- **Summary statistics** panel with all key metrics

Example output:
```
Recovery Metrics Summary:
━━━━━━━━━━━━━━━━━━━━━━━━━
Avg Recovery Latency: 12.3s
Min Recovery Latency: 8.1s
Max Recovery Latency: 18.5s

Total Failures: 3
Total Reassignments: 3

Avg Worker Count: 1.8
Avg CPU Usage: 42.3%
```

## Complete Setup and Execution

### Step 1: Start the MapReduce Cluster

```bash
# Navigate to project directory
cd /path/to/MapReduceImpl

# Set project name
export PROJECT=mapreduce

# Build base HDFS image
docker build -t mapreduce-hdfs -f Dockerfile.hdfs .

# Build all services
docker compose build

# Start all services (HDFS + Master + 4 Workers)
docker compose up -d

# Wait for services to initialize (~10 seconds)
sleep 10

# Verify all services are running
docker compose ps
```

Expected output - all services should be "Up":
```
NAME                     STATUS
mapreduce-datanode-1     Up
mapreduce-datanode-2     Up
mapreduce-datanode-3     Up
mapreduce-master-1       Up
mapreduce-namenode-1     Up
mapreduce-worker-1       Up
mapreduce-worker-2       Up
mapreduce-worker-3       Up
mapreduce-worker-4       Up
```

### Step 2: Prepare Test Data

```bash
# Copy data generation script to master container
docker cp create_test_data.py mapreduce-master-1:/create_test_data.py

# Generate test data in HDFS
docker compose exec master bash -c 'export CLASSPATH=`$HADOOP_HOME/bin/hdfs classpath --glob` && python3 /create_test_data.py --size 10MB'

# Verify data was created
docker compose exec master bash -c 'export CLASSPATH=`$HADOOP_HOME/bin/hdfs classpath --glob` && hdfs dfs -ls -h hdfs://namenode:9000/data/'
```

You should see:
```
-rw-r--r--   3 root supergroup      2.7 M <timestamp> hdfs://namenode:9000/data/test_10mb.parquet
```

### Step 3: Set Up Client Environment (First Time Only)

```bash
# Create Python virtual environment
python3 -m venv venv
source venv/bin/activate

# Install required packages
pip install grpcio grpcio-tools matplotlib pandas

# Generate gRPC protocol files
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. map_reduce.proto
```

### Step 4: Run Job with Simulated Failure

**Terminal 1 - Submit Job:**
```bash
# Make sure venv is activated
source venv/bin/activate

# Submit a MapReduce job
python3 benchmark.py --datasets hdfs://namenode:9000/data/test_10mb.parquet --runs 1
```

**Terminal 2 - Monitor & Kill Worker (within 5 seconds of job start):**
```bash
# Watch the master logs to see when job starts
docker compose logs -f master

# Wait until you see "Starting MAP phase for job-xxxxx"
# Then kill a worker to simulate failure
docker stop mapreduce-worker-1

# Or for immediate failure:
docker kill mapreduce-worker-1
```

**What happens:**
1. Job starts executing with 4 workers
2. Worker 1 dies mid-execution
3. Master detects worker timeout (~60 seconds)
4. Master reassigns failed tasks to remaining workers
5. Job completes successfully
6. Master automatically exports metrics to `./jobs_executed/<job_id>_metrics.json`

### Step 5: Generate Recovery Plots

#### Option A: Using Docker Plotter (Recommended - No Local Dependencies)

```bash
# Build the plotter image (first time only)
docker build -t mapreduce-plotter -f Dockerfile.plotter .

# Find the metrics file
ls -lh ./jobs_executed/

# Generate all recovery plots using Docker
docker run --rm \
  -v $(pwd)/jobs_executed:/app/jobs_executed \
  -v $(pwd)/plots:/app/plots \
  mapreduce-plotter \
  python3 plot_recovery.py --input /app/jobs_executed/<JOB_ID>_metrics.json --output-dir /app/plots

# View the results (plots are in ./plots/ directory)
open plots/recovery_timeline.png        # Main comprehensive plot
open plots/recovery_latency.png         # Per-task recovery times
```

#### Option B: Run Directly on Host (Requires Python Packages)

```bash
# Install dependencies (first time only)
pip install matplotlib pandas numpy

# Find the metrics file
ls -lh ./jobs_executed/

# Generate all recovery plots
python3 plot_recovery.py --input ./jobs_executed/<JOB_ID>_metrics.json --output-dir ./plots

# View the results
open plots/recovery_timeline.png
open plots/recovery_latency.png
```

## Understanding the Plots

### 1. Recovery Timeline (Main Plot)

**File:** `recovery_timeline.png`

This comprehensive plot has 3 subplots stacked vertically:

**Top - Failure and Recovery Events:**
- Red dashed lines: Node failure events (MAP phase)
- Orange dashed lines: Node failure events (REDUCE phase)
- Blue dotted lines: Individual task recovery events
- Green dotted line: Recovery complete
- Yellow shaded area: Recovery period
- Purple line: Map→Reduce phase transition

**Middle - Worker Count:**
- Green line: Number of active workers
- Shows when workers drop (failure) and return (recovery)

**Bottom - CPU Usage:**
- Orange line: System CPU percentage
- Shows resource consumption during failure/recovery

**Right Panel - Summary Statistics:**
- Recovery latency (avg/min/max)
- Failure counts
- Resource usage

### 2. Recovery Latency Breakdown

**File:** `recovery_latency.png`

Bar chart showing:
- Recovery time for each individual failed task
- Color-coded: Red = slow recovery, Green = fast recovery
- Red horizontal line = average recovery latency

## Metrics Tracked

### Recovery Metrics
- **Average Recovery Latency**: Mean time from failure to successful task completion
- **Min/Max Recovery Latency**: Range of recovery times
- **Number of Recoveries**: How many tasks had to be recovered

### System Metrics
- **Worker Count Timeline**: Active workers every 2 seconds
- **CPU Usage**: System CPU percentage every 2 seconds
- **Task Completion Timeline**: Timestamp of every task completion
- **Failure Events**: Timestamp and details of each worker/task failure
- **Recovery Events**: Timestamp and duration of each task recovery

## Exported Metrics File Structure

Location: `./jobs_executed/<job_id>_metrics.json`

```json
{
  "job_id": "job-abc123",
  "status": "COMPLETED",
  "start_time": 1234567890.123,

  "map_phase_start": 1234567891.0,
  "map_phase_end": 1234567920.5,
  "reduce_phase_start": 1234567921.0,
  "reduce_phase_end": 1234567945.2,

  "failures": 3,
  "reassignments": 3,

  "failure_events": [
    {
      "time": 1234567900.0,
      "task_id": "job-abc123-map-2",
      "worker": "worker1:50052",
      "phase": "MAP",
      "reason": "exception: connection refused"
    }
  ],

  "recovery_events": [
    {
      "time": 1234567915.0,
      "task_id": "job-abc123-map-2",
      "recovery_duration": 15.0
    }
  ],

  "task_completions": [
    {
      "time": 1234567895.0,
      "task_id": "job-abc123-map-0",
      "task_type": "MAP",
      "duration": 4.2
    }
  ],

  "worker_count_timeline": [
    {
      "time": 1234567891.0,
      "worker_count": 2,
      "cpu_percent": 35.2
    }
  ]
}
```

## Alternative Failure Simulation Methods

### Method 1: Kill Worker Mid-Job (Recommended)
```bash
# Already covered in Step 4 above
docker stop mapreduce-worker-1
```

### Method 2: Use FAIL_AFTER Environment Variable
```yaml
# Edit docker-compose.yml
services:
  worker:
    environment:
      - FAIL_AFTER=2  # Worker will fail after completing 2 tasks

# Then restart services
docker compose up -d
```

### Method 3: Multiple Worker Failures
```bash
# Kill first worker
docker stop mapreduce-worker-1
sleep 30

# Kill second worker during recovery
docker stop mapreduce-worker-2
```

## Comparing Different Scenarios

To compare recovery under different conditions:

```bash
# Scenario 1: No failures (baseline)
python3 benchmark.py --datasets hdfs://namenode:9000/data/test_10mb.parquet --runs 1
cp ./jobs_executed/*.json ./baseline_metrics.json

# Scenario 2: Single worker failure
# (Start job, then: docker stop mapreduce-worker-1)
python3 benchmark.py --datasets hdfs://namenode:9000/data/test_10mb.parquet --runs 1
cp ./jobs_executed/*.json ./single_failure_metrics.json

# Scenario 3: Multiple failures
# (Start job, kill worker-1, then worker-2)
python3 benchmark.py --datasets hdfs://namenode:9000/data/test_10mb.parquet --runs 1
cp ./jobs_executed/*.json ./multi_failure_metrics.json

# Generate comparison plots
mkdir -p comparison_plots
python3 plot_recovery.py --input baseline_metrics.json --output-dir comparison_plots/baseline
python3 plot_recovery.py --input single_failure_metrics.json --output-dir comparison_plots/single
python3 plot_recovery.py --input multi_failure_metrics.json --output-dir comparison_plots/multi

# View all three side-by-side
open comparison_plots/*/recovery_timeline.png
```

## Interpreting Results

### ✅ Good Recovery Performance

- **Recovery Latency** < 30 seconds
- **Worker Count** quickly returns to original level
- **Clear failure and recovery markers** visible in timeline
- **CPU usage** stabilizes after recovery

### ⚠️ Potential Issues

**Long Recovery Latency (> 60s):**
- Task timeout setting too high → Lower `TASK_TIMEOUT` in master.py (line 30)
- Worker restart/re-registration slow → Check worker startup time
- Resource contention → Check CPU/memory availability

**Worker Count Not Recovering:**
- Workers not restarting properly → Check Docker container status
- Network issues preventing re-registration → Check master logs
- Resource limits preventing new workers → Check system resources

**High CPU During Recovery:**
- System under pressure redistributing tasks → Expected behavior
- Too many concurrent reassignments → May need to throttle task distribution
- Resource contention → Check if system is overloaded

## Advanced Configuration

### Adjust Recovery Detection Speed

Edit `master.py`:
```python
# Line 30-31
self.TASK_TIMEOUT = 300  # Lower for faster failure detection (e.g., 60)
self.WORKER_TIMEOUT = 60  # Worker heartbeat timeout (e.g., 30)
```

### Change Metrics Collection Frequency

Edit `master.py`:
```python
# Line 440
time.sleep(2)  # Change to 1 for more granular data, 5 for less overhead
```

## Troubleshooting

### No metrics file generated
**Check:**
```bash
# Master logs should show "Metrics exported to..."
docker compose logs master | grep "Metrics exported"

# Verify directory exists
ls -la ./jobs_executed/
```
**Fix:**
```bash
# Ensure directory is writable
mkdir -p ./jobs_executed
chmod 755 ./jobs_executed
```

### Empty timeline data in plots
**Check:**
```bash
# Verify job completed successfully
cat ./jobs_executed/<job_id>_metrics.json | grep status

# Check if task_completions array has data
cat ./jobs_executed/<job_id>_metrics.json | grep -A 5 task_completions
```
**Fix:** Job needs to run for > 10 seconds to collect meaningful metrics

### Plot shows no failures
**Check:**
```bash
# Verify failure_events array
cat ./jobs_executed/<job_id>_metrics.json | grep -A 10 failure_events
```
**Fix:**
- Actually kill a worker during job execution
- Check that worker died: `docker compose ps`
- Verify worker timeout is working: `docker compose logs master | grep "down"`

### Import errors when running plot_recovery.py
**Fix:**
```bash
# Install missing packages
pip install matplotlib pandas numpy pyarrow
```

## Files Reference

**Modified:**
- `master.py` - Enhanced with metrics collection and export

**New Scripts:**
- `plot_recovery.py` - Main plotting script (3 plots)
- `benchmark_recovery.py` - Enhanced benchmark with recovery metrics (optional)
- `extract_metrics.py` - Helper for manual metrics extraction (optional)

**Documentation:**
- `RECOVERY_METRICS.md` - This file (complete guide)
- `RECOVERY_METRICS_README.md` - Extended technical documentation
- `QUICK_START_RECOVERY.md` - Original quick reference (deprecated)

## Additional Metrics Suggestions

### Currently Implemented ✓
1. Recovery latency (avg/min/max)
2. Worker count timeline
3. CPU usage
4. Failure and recovery event tracking

### Good Future Additions
1. **Memory Usage** - Track memory alongside CPU
2. **Network I/O** - Data transfer rates during recovery
3. **Task Queue Depth** - Backlog of pending tasks
4. **Per-Worker Metrics** - Individual worker performance
5. **Task Throughput** - Tasks completed per unit time

### Not Recommended for Single Plot
- Detailed error logs (too verbose)
- Network latency (separate analysis)
- Disk I/O rates (separate plot)

## Quick Reference Commands

```bash
# Complete workflow in one script
export PROJECT=mapreduce
docker compose up -d && sleep 10
docker compose exec master bash -c 'export CLASSPATH=`$HADOOP_HOME/bin/hdfs classpath --glob` && python3 /create_test_data.py --size 10MB'
python3 benchmark.py --datasets hdfs://namenode:9000/data/test_10mb.parquet --runs 1

# Mid-job: docker stop mapreduce-worker-1

# After job completes - using Docker plotter:
docker build -t mapreduce-plotter -f Dockerfile.plotter .
JOB_ID=$(ls ./jobs_executed/ | head -1 | cut -d'_' -f1)
docker run --rm \
  -v $(pwd)/jobs_executed:/app/jobs_executed \
  -v $(pwd)/plots:/app/plots \
  mapreduce-plotter \
  python3 plot_recovery.py --input /app/jobs_executed/${JOB_ID}_metrics.json --output-dir /app/plots
```

## Getting Help

- **Full technical details**: See `RECOVERY_METRICS_README.md`
- **Main project setup**: See `README.md`
- **Design documentation**: See `Design.md`

For issues or questions about the recovery metrics system, check:
1. Master logs: `docker compose logs master`
2. Metrics files: `./jobs_executed/<job_id>_metrics.json`
3. Generated plots: `./plots/recovery_timeline.png`
4. Code implementation: `master.py` (lines 60-70, 437-523)
