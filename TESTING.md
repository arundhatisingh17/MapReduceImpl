# MapReduce Testing and Benchmarking Guide

This guide provides detailed instructions for testing the MapReduce system and evaluating the worker failure handling feature.

## Table of Contents

1. [Basic Functionality Testing](#basic-functionality-testing)
2. [Failure Recovery Testing](#failure-recovery-testing)
3. [Performance Benchmarking](#performance-benchmarking)
4. [Interpreting Results](#interpreting-results)

## Basic Functionality Testing

### Test 1: Simple Job Execution

**Purpose**: Verify basic MapReduce functionality works end-to-end.

**Steps**:

1. Start the system:
```bash
export PROJECT=mapreduce
docker-compose up -d
```

2. Wait for all services to be ready (30 seconds):
```bash
sleep 30
docker-compose ps
```

3. Generate a small test dataset:
```bash
docker-compose run client python create_test_data.py --size 1MB
```

4. Submit a job:
```bash
docker-compose run client python client.py
```

5. Observe the output - you should see:
   - Job submitted with a job ID
   - Status changing: SCHEDULED → MAP_IN_PROGRESS → COMPLETED
   - No errors in logs

**Expected Results**:
- Job completes successfully
- Output files created in `/output/job-<id>/`
- All 4 workers participate in execution

**Verification**:
```bash
# Check output exists
docker-compose exec hdfs hadoop fs -ls /output/

# View boss logs for task distribution
docker-compose logs boss | grep "Task.*completed"

# Count successful tasks
docker-compose logs boss | grep "completed successfully" | wc -l
```

### Test 2: Multiple Workers Processing

**Purpose**: Verify tasks are distributed across all 4 workers.

**Steps**:

1. Submit a job with 8 map tasks (more than 4 workers):
```bash
# Edit client.py to set num_partitions=8
docker-compose run client python client.py
```

2. Monitor worker logs:
```bash
# Terminal 1
docker-compose logs -f worker1 | grep "Task.*completed"

# Terminal 2  
docker-compose logs -f worker2 | grep "Task.*completed"

# Terminal 3
docker-compose logs -f worker3 | grep "Task.*completed"

# Terminal 4
docker-compose logs -f worker4 | grep "Task.*completed"
```

**Expected Results**:
- Each worker processes at least 1 task
- Total tasks completed = num_partitions (8)
- Tasks distributed roughly evenly

### Test 3: Data Correctness

**Purpose**: Verify map and reduce functions produce correct results.

**Steps**:

1. Create a dataset with known properties:
```bash
docker-compose run client python create_test_data.py --size 10MB
```

2. Use the default map/reduce functions (bucket aggregation)

3. Submit job and wait for completion

4. Read and verify output:
```python
import pyarrow.parquet as pq
import pyarrow as pa

fs = pa.fs.HadoopFileSystem("nn", 9000)

# Read all output partitions
job_id = "job-xxx"  # Replace with actual job ID
total_records = 0

for i in range(4):  # num_reduce_tasks
    try:
        table = pq.read_table(f"/output/{job_id}/part-{i}.parquet", filesystem=fs)
        df = table.to_pandas()
        print(f"Partition {i}: {len(df)} records")
        print(df.head())
        total_records += len(df)
    except:
        pass

print(f"Total output records: {total_records}")
```

**Expected Results**:
- All partitions contain valid data
- Keys are properly aggregated
- No duplicate keys across partitions

## Failure Recovery Testing

### Test 4: Single Worker Failure

**Purpose**: Test system recovery from one worker failing.

**Steps**:

1. Configure worker1 to fail after 2 tasks:
```bash
# Stop services
docker-compose down

# Edit docker-compose.yml, add to worker1:
# environment:
#   - FAIL_AFTER=2

# Restart
docker-compose up -d
```

2. Submit a job with 8 map tasks:
```bash
docker-compose run client python client.py
```

3. Watch the failure and recovery:
```bash
# Watch boss detect failure
docker-compose logs -f boss | grep -E "FAILED|timed out|Re-queuing"

# Watch worker1 fail
docker-compose logs worker1
```

**Expected Results**:
- Worker1 fails after completing 2 tasks
- Boss detects the failure within 30 seconds
- Failed tasks are reassigned to other workers
- Job completes successfully
- Total execution time is longer than without failure

**Verification**:
```bash
# Check Boss logs for reassignments
docker-compose logs boss | grep -i "reassign"

# Check completion message shows failures
docker-compose logs boss | grep "completed.*failures"
```

### Test 5: Multiple Concurrent Failures

**Purpose**: Test recovery from multiple workers failing.

**Steps**:

1. Configure 2 workers to fail:
```yaml
worker1:
  environment:
    - FAIL_AFTER=1
worker2:
  environment:
    - FAIL_AFTER=2
```

2. Submit a job with 16 map tasks

3. Monitor the recovery process

**Expected Results**:
- Both workers fail during execution
- Tasks are redistributed to worker3 and worker4
- Job completes successfully (may take longer)
- Final logs show multiple reassignments

### Test 6: Failure During Reduce Phase

**Purpose**: Verify failure handling works in reduce phase too.

**Steps**:

1. Let workers complete map phase normally

2. Manually kill a worker during reduce phase:
```bash
# Start job, wait for map phase to complete
docker-compose run client python client.py &

# Wait for reduce phase to start (watch logs)
sleep 45

# Kill a worker
docker-compose kill worker2
```

3. Observe recovery

**Expected Results**:
- Boss detects worker2 failure
- Reduce tasks from worker2 are reassigned
- Job completes successfully

## Performance Benchmarking

### Benchmark 1: Baseline Performance

**Purpose**: Establish baseline performance without failures.

**Steps**:

1. Ensure all workers are healthy (no FAIL_AFTER set)

2. Run benchmark with multiple dataset sizes:
```bash
docker-compose run client python benchmark.py \
    --datasets \
        hdfs://nn:9000/data/test_1mb.parquet \
        hdfs://nn:9000/data/test_10mb.parquet \
        hdfs://nn:9000/data/test_50mb.parquet \
    --partitions 8 \
    --runs 5 \
    --output baseline_results.csv
```

3. Generate plots:
```bash
docker-compose run client python plot_results.py baseline_results.csv
```

**Expected Results**:
- Consistent execution times across runs
- Larger datasets take proportionally longer
- No failures or reassignments

### Benchmark 2: Performance With Failures

**Purpose**: Measure overhead of failure recovery.

**Steps**:

1. Configure one worker to fail mid-job:
```yaml
worker1:
  environment:
    - FAIL_AFTER=3
```

2. Restart and run benchmark:
```bash
docker-compose up -d
docker-compose run client python benchmark.py \
    --datasets hdfs://nn:9000/data/test_10mb.parquet \
    --partitions 8 \
    --runs 5 \
    --output failure_results.csv
```

**Expected Results**:
- Execution times are 10-30% longer than baseline
- All jobs still complete successfully
- Failure and reassignment counts > 0

### Benchmark 3: Scalability Test

**Purpose**: Test how task distribution affects performance.

**Steps**:

1. Run jobs with different partition counts:
```bash
# 4 partitions (matches worker count)
docker-compose run client python benchmark.py \
    --datasets hdfs://nn:9000/data/test_10mb.parquet \
    --partitions 4 \
    --runs 3 \
    --output scale_4.csv

# 8 partitions (2x worker count)
docker-compose run client python benchmark.py \
    --datasets hdfs://nn:9000/data/test_10mb.parquet \
    --partitions 8 \
    --runs 3 \
    --output scale_8.csv

# 16 partitions (4x worker count)
docker-compose run client python benchmark.py \
    --datasets hdfs://nn:9000/data/test_10mb.parquet \
    --partitions 16 \
    --runs 3 \
    --output scale_16.csv
```

**Expected Results**:
- Sweet spot around 8-12 partitions (2-3x worker count)
- Too few partitions = underutilized workers
- Too many partitions = overhead from task management

### Benchmark 4: Comparative Analysis

**Purpose**: Compare baseline vs. failure recovery performance.

**Steps**:

1. Run benchmark suite without failures:
```bash
# Ensure no FAIL_AFTER set
docker-compose run client python benchmark.py \
    --datasets hdfs://nn:9000/data/test_10mb.parquet \
    --partitions 8 \
    --runs 10 \
    --output normal.csv
```

2. Run benchmark suite with failures:
```bash
# Set FAIL_AFTER=3 on worker1
docker-compose run client python benchmark.py \
    --datasets hdfs://nn:9000/data/test_10mb.parquet \
    --partitions 8 \
    --runs 10 \
    --output with_failure.csv
```

3. Combine results and plot:
```bash
# Merge CSVs (manually or with script)
# Ensure one has failure_mode='normal', other has failure_mode='with_failure'

docker-compose run client python plot_results.py combined_results.csv
```

**Expected Results**:
- Clear visualization showing recovery overhead
- Consistent overhead percentage across runs
- All jobs complete successfully in both modes

## Interpreting Results

### Success Metrics

✅ **System is working correctly if:**
- All jobs complete with status "COMPLETED"
- Output files are created and contain valid data
- Workers distribute tasks evenly
- No tasks exceed MAX_RETRIES

### Failure Recovery Metrics

✅ **Failure handling is working if:**
- Boss detects failures within 30 seconds
- Failed tasks are successfully reassigned
- Jobs complete despite worker failures
- Reassignment count > 0 when failures occur

### Performance Metrics

**Key metrics to analyze:**

1. **Execution Time**
   - Baseline: Time without failures
   - With failures: Time with simulated failures
   - Overhead: % increase due to recovery

2. **Throughput**
   - Data processed per second
   - Should scale with number of workers

3. **Recovery Time**
   - Time from failure detection to task reassignment
   - Should be < 35 seconds (WORKER_TIMEOUT + detection)

4. **Failure Impact**
   - Expected overhead: 10-30%
   - Depends on: when failure occurs, task size, number of workers

### Plot Interpretation

**execution_time_by_dataset.png:**
- Shows average execution time for each dataset
- Error bars show variability across runs
- Larger datasets should show proportional increase

**failure_comparison.png:**
- Compares normal vs. failure mode execution times
- Bars should be similar height (showing robust recovery)
- Failure mode bar slightly taller (overhead cost)

**time_series.png:**
- Shows consistency across multiple runs
- Low variance = stable system
- Outliers may indicate failure events

### Expected Performance Numbers

**Baseline (10MB dataset, 8 partitions, 4 workers):**
- Execution time: 30-60 seconds
- Tasks per worker: 2 (map) + 2 (reduce) = 4 total
- Failures: 0
- Reassignments: 0

**With Single Worker Failure:**
- Execution time: 40-75 seconds (20-30% increase)
- Failures: 2-4 (depends on task assignment)
- Reassignments: 2-4
- Recovery time: < 35 seconds per failure

## Troubleshooting Tests

### Issue: Tests Failing

1. Check all services are running:
```bash
docker-compose ps
```

2. Check for errors in logs:
```bash
docker-compose logs | grep -i error
```

3. Verify HDFS is accessible:
```bash
docker-compose exec hdfs hadoop fs -ls /
```

### Issue: Workers Not Failing As Expected

1. Verify FAIL_AFTER environment variable:
```bash
docker-compose exec worker1 env | grep FAIL_AFTER
```

2. Check worker logs for simulation message:
```bash
docker-compose logs worker1 | grep "SIMULATION"
```

### Issue: Inconsistent Performance

1. Ensure system has enough resources:
```bash
docker stats
```

2. Run tests when system is idle

3. Increase number of runs for more stable averages

## Advanced Testing

### Stress Test

```bash
# Large dataset, many partitions
docker-compose run client python create_test_data.py --size 500MB
docker-compose run client python benchmark.py \
    --datasets hdfs://nn:9000/data/test_500mb.parquet \
    --partitions 32 \
    --runs 3
```

### Chaos Testing

```bash
# Randomly kill and restart workers during execution
while true; do
    sleep $((RANDOM % 30 + 10))
    WORKER=$((RANDOM % 4 + 1))
    docker-compose restart worker$WORKER
done
```

### Load Testing

```bash
# Submit multiple jobs concurrently
for i in {1..5}; do
    docker-compose run -d client python client.py
done
```

## Reporting Results

When reporting benchmark results, include:

1. **System Configuration**
   - Number of workers
   - CPU/memory limits
   - Dataset sizes tested

2. **Test Conditions**
   - Failure scenarios tested
   - Number of runs per configuration
   - Environmental factors

3. **Quantitative Results**
   - Execution times (mean, std dev)
   - Failure counts and reassignments
   - Overhead percentages

4. **Visualizations**
   - Performance comparison plots
   - Time series showing consistency
   - Resource utilization graphs

5. **Observations**
   - Any unexpected behaviors
   - Bottlenecks identified
   - Recommendations for improvement

