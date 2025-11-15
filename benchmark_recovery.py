#!/usr/bin/env python3
"""
Enhanced MapReduce Benchmarking Script with Recovery Metrics

Runs MapReduce jobs and collects detailed metrics including:
- Recovery latency after node failures
- Throughput before, during, and after failures
- Worker count and CPU usage over time
"""

import grpc
import map_reduce_pb2
import map_reduce_pb2_grpc
import time
import json
import argparse
from datetime import datetime
import sys
import pandas as pd


def submit_job_with_metrics(dataset_path, master_service=None):
    """
    Submit a MapReduce job and collect detailed metrics.

    Args:
        dataset_path: Path to the dataset
        master_service: Reference to the master service (for direct access to metrics)

    Returns:
        dict: Complete job metrics including recovery data
    """
    try:
        channel = grpc.insecure_channel("localhost:50051", options=[
            ('grpc.max_receive_message_length', 100 * 1024 * 1024),
        ])
        stub = map_reduce_pb2_grpc.MasterStub(channel)

        request = map_reduce_pb2.SubmitJobRequest(
            dataset_path=dataset_path,
            map_function_path="/app/user_funcs/map_func.py",
            reduce_function_path="/app/user_funcs/reduce_func.py",
            num_map_tasks=4,
            num_reduce_tasks=2
        )

        print(f"[BENCHMARK] Submitting job for {dataset_path}")
        start_time = time.time()

        response = stub.SubmitJob(request)
        job_id = response.job_id

        print(f"[BENCHMARK] Job {job_id} submitted, waiting for completion...")

        # Poll for completion
        while True:
            time.sleep(3)
            status_req = map_reduce_pb2.JobStatusRequest(job_id=job_id)
            status_resp = stub.GetJobStatus(status_req)

            print(f"[BENCHMARK] Job {job_id} status: {status_resp.status}")

            if status_resp.status in ("COMPLETED", "FAILED", "ERROR"):
                end_time = time.time()
                duration = end_time - start_time

                # Collect metrics from master
                # Note: This requires accessing the master service directly
                # In production, we'd add a gRPC endpoint for this
                metrics = {
                    'job_id': job_id,
                    'status': status_resp.status,
                    'total_duration': duration,
                    'dataset_path': dataset_path,
                    'timestamp': datetime.now().isoformat(),
                }

                # If we have direct access to master service, get detailed metrics
                if master_service:
                    job_metrics = master_service.get_job_metrics(job_id)
                    if job_metrics:
                        metrics.update(extract_recovery_metrics(job_metrics))

                return metrics

    except Exception as e:
        print(f"[BENCHMARK] Error: {e}")
        return {
            'job_id': None,
            'status': 'ERROR',
            'total_duration': 0,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }


def extract_recovery_metrics(job_data):
    """
    Extract recovery and throughput metrics from job data.

    Args:
        job_data: Raw job metrics from master service

    Returns:
        dict: Processed metrics
    """
    metrics = {}

    # Phase durations
    if job_data.get('map_phase_start') and job_data.get('map_phase_end'):
        metrics['map_phase_duration'] = job_data['map_phase_end'] - job_data['map_phase_start']
    else:
        metrics['map_phase_duration'] = None

    if job_data.get('reduce_phase_start') and job_data.get('reduce_phase_end'):
        metrics['reduce_phase_duration'] = job_data['reduce_phase_end'] - job_data['reduce_phase_start']
    else:
        metrics['reduce_phase_duration'] = None

    # Failure counts
    metrics['total_failures'] = job_data.get('failures', 0)
    metrics['total_reassignments'] = job_data.get('reassignments', 0)

    # Recovery metrics
    recovery_events = job_data.get('recovery_events', [])
    if recovery_events:
        recovery_times = [event['recovery_duration'] for event in recovery_events]
        metrics['avg_recovery_latency'] = sum(recovery_times) / len(recovery_times)
        metrics['max_recovery_latency'] = max(recovery_times)
        metrics['min_recovery_latency'] = min(recovery_times)
        metrics['num_recoveries'] = len(recovery_events)
    else:
        metrics['avg_recovery_latency'] = 0
        metrics['max_recovery_latency'] = 0
        metrics['min_recovery_latency'] = 0
        metrics['num_recoveries'] = 0

    # Throughput analysis
    task_completions = job_data.get('task_completions', [])
    failure_events = job_data.get('failure_events', [])

    if task_completions:
        metrics['total_tasks_completed'] = len(task_completions)

        # Calculate throughput in different periods
        throughput_metrics = calculate_throughput_metrics(
            task_completions,
            failure_events,
            job_data.get('start_time', 0)
        )
        metrics.update(throughput_metrics)
    else:
        metrics['total_tasks_completed'] = 0
        metrics['throughput_before_failure'] = 0
        metrics['throughput_during_failure'] = 0
        metrics['throughput_after_recovery'] = 0

    # Worker count statistics
    worker_timeline = job_data.get('worker_count_timeline', [])
    if worker_timeline:
        worker_counts = [entry['worker_count'] for entry in worker_timeline]
        cpu_usages = [entry['cpu_percent'] for entry in worker_timeline]

        metrics['avg_worker_count'] = sum(worker_counts) / len(worker_counts)
        metrics['min_worker_count'] = min(worker_counts)
        metrics['max_worker_count'] = max(worker_counts)

        metrics['avg_cpu_percent'] = sum(cpu_usages) / len(cpu_usages)
        metrics['max_cpu_percent'] = max(cpu_usages)
    else:
        metrics['avg_worker_count'] = 0
        metrics['min_worker_count'] = 0
        metrics['max_worker_count'] = 0
        metrics['avg_cpu_percent'] = 0
        metrics['max_cpu_percent'] = 0

    # Save timeline data for plotting
    metrics['task_completion_timeline'] = task_completions
    metrics['failure_event_timeline'] = failure_events
    metrics['worker_count_timeline'] = worker_timeline

    return metrics


def calculate_throughput_metrics(task_completions, failure_events, job_start_time):
    """
    Calculate throughput before, during, and after failures.

    Args:
        task_completions: List of task completion events
        failure_events: List of failure events
        job_start_time: Job start timestamp

    Returns:
        dict: Throughput metrics
    """
    if not failure_events:
        # No failures - calculate overall throughput
        if len(task_completions) > 1:
            duration = task_completions[-1]['time'] - task_completions[0]['time']
            throughput = len(task_completions) / duration if duration > 0 else 0
            return {
                'throughput_before_failure': throughput,
                'throughput_during_failure': 0,
                'throughput_after_recovery': throughput,
                'overall_throughput': throughput
            }
        else:
            return {
                'throughput_before_failure': 0,
                'throughput_during_failure': 0,
                'throughput_after_recovery': 0,
                'overall_throughput': 0
            }

    # Find first failure and last recovery
    first_failure_time = min(event['time'] for event in failure_events)
    last_failure_time = max(event['time'] for event in failure_events)

    # Define recovery period (30 seconds after last failure as example)
    recovery_end_time = last_failure_time + 30

    # Partition task completions into three periods
    before_failure = [t for t in task_completions if t['time'] < first_failure_time]
    during_failure = [t for t in task_completions if first_failure_time <= t['time'] < recovery_end_time]
    after_recovery = [t for t in task_completions if t['time'] >= recovery_end_time]

    def calc_throughput(tasks):
        if len(tasks) > 1:
            duration = tasks[-1]['time'] - tasks[0]['time']
            return len(tasks) / duration if duration > 0 else 0
        return 0

    throughput_before = calc_throughput(before_failure) if before_failure else 0
    throughput_during = calc_throughput(during_failure) if during_failure else 0
    throughput_after = calc_throughput(after_recovery) if after_recovery else 0

    # Overall throughput
    if len(task_completions) > 1:
        total_duration = task_completions[-1]['time'] - task_completions[0]['time']
        overall_throughput = len(task_completions) / total_duration if total_duration > 0 else 0
    else:
        overall_throughput = 0

    return {
        'throughput_before_failure': throughput_before,
        'throughput_during_failure': throughput_during,
        'throughput_after_recovery': throughput_after,
        'overall_throughput': overall_throughput,
        'first_failure_time': first_failure_time,
        'last_failure_time': last_failure_time,
        'recovery_window_end': recovery_end_time
    }


def run_benchmark(dataset_path, output_file):
    """
    Run a single benchmark and save detailed metrics.

    Args:
        dataset_path: Path to the dataset
        output_file: JSON file to write results
    """
    print("=" * 60)
    print("Enhanced MapReduce Benchmark with Recovery Metrics")
    print("=" * 60)
    print(f"Dataset: {dataset_path}")
    print(f"Output file: {output_file}")
    print("=" * 60 + "\n")

    # Note: In a real implementation, we'd need to get the master service instance
    # For now, we collect what we can from the gRPC API
    metrics = submit_job_with_metrics(dataset_path)

    # Write results to JSON (preserves timeline data)
    with open(output_file, 'w') as f:
        json.dump(metrics, f, indent=2)

    print(f"\n[BENCHMARK] Results written to {output_file}")
    print_detailed_summary(metrics)


def print_detailed_summary(metrics):
    """Print a detailed summary of benchmark results"""
    print("\n" + "=" * 60)
    print("DETAILED BENCHMARK RESULTS")
    print("=" * 60)

    print(f"\nJob ID: {metrics.get('job_id', 'N/A')}")
    print(f"Status: {metrics.get('status', 'N/A')}")
    print(f"Total Duration: {metrics.get('total_duration', 0):.2f}s")

    if metrics.get('map_phase_duration'):
        print(f"\nMap Phase Duration: {metrics['map_phase_duration']:.2f}s")
    if metrics.get('reduce_phase_duration'):
        print(f"Reduce Phase Duration: {metrics['reduce_phase_duration']:.2f}s")

    print(f"\n--- Failure & Recovery ---")
    print(f"Total Failures: {metrics.get('total_failures', 0)}")
    print(f"Total Reassignments: {metrics.get('total_reassignments', 0)}")
    print(f"Number of Recoveries: {metrics.get('num_recoveries', 0)}")

    if metrics.get('num_recoveries', 0) > 0:
        print(f"Avg Recovery Latency: {metrics.get('avg_recovery_latency', 0):.2f}s")
        print(f"Min Recovery Latency: {metrics.get('min_recovery_latency', 0):.2f}s")
        print(f"Max Recovery Latency: {metrics.get('max_recovery_latency', 0):.2f}s")

    print(f"\n--- Throughput ---")
    print(f"Tasks Completed: {metrics.get('total_tasks_completed', 0)}")
    print(f"Before Failure: {metrics.get('throughput_before_failure', 0):.2f} tasks/sec")
    print(f"During Failure: {metrics.get('throughput_during_failure', 0):.2f} tasks/sec")
    print(f"After Recovery: {metrics.get('throughput_after_recovery', 0):.2f} tasks/sec")
    print(f"Overall: {metrics.get('overall_throughput', 0):.2f} tasks/sec")

    print(f"\n--- System Resources ---")
    print(f"Avg Workers: {metrics.get('avg_worker_count', 0):.1f}")
    print(f"Min Workers: {metrics.get('min_worker_count', 0)}")
    print(f"Max Workers: {metrics.get('max_worker_count', 0)}")
    print(f"Avg CPU: {metrics.get('avg_cpu_percent', 0):.1f}%")
    print(f"Max CPU: {metrics.get('max_cpu_percent', 0):.1f}%")


def main():
    parser = argparse.ArgumentParser(description='Run enhanced MapReduce benchmarks with recovery metrics')
    parser.add_argument('--dataset', type=str,
                        default='hdfs://namenode:9000/data/test_10mb.parquet',
                        help='Dataset path to benchmark')
    parser.add_argument('--output', type=str, default='benchmark_recovery_results.json',
                        help='Output JSON file for results')

    args = parser.parse_args()

    run_benchmark(args.dataset, args.output)


if __name__ == "__main__":
    main()