#!/usr/bin/env python3
"""
MapReduce Benchmarking Script

Runs MapReduce jobs with various configurations and measures performance.
Compares execution with and without worker failures.
"""

import grpc
import map_reduce_pb2
import map_reduce_pb2_grpc
import time
import csv
import argparse
from datetime import datetime
import sys

def submit_job(dataset_path):
    """
    Submit a MapReduce job and wait for completion.
    
    Returns:
        tuple: (job_id, status, duration_seconds)
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
                return job_id, status_resp.status, duration
        
    except Exception as e:
        print(f"[BENCHMARK] Error: {e}")
        return None, "ERROR", 0


def run_benchmark(dataset_sizes, num_runs, output_file):
    """
    Run benchmark tests.
    
    Args:
        dataset_sizes: List of dataset paths to test
        num_runs: Number of runs per configuration
        output_file: CSV file to write results
    """
    results = []
    
    for dataset_path in dataset_sizes:
        dataset_name = dataset_path.split('/')[-1].replace('.parquet', '')
        
        for run in range(num_runs):
            print(f"\n{'='*60}")
            print(f"Run {run+1}/{num_runs} for {dataset_name}")
            print(f"{'='*60}\n")
            
            job_id, status, duration = submit_job(dataset_path)
            
            result = {
                'timestamp': datetime.now().isoformat(),
                'dataset': dataset_name,
                'dataset_path': dataset_path,
                'run': run + 1,
                'job_id': job_id,
                'status': status,
                'duration_seconds': duration,
                'failure_mode': 'normal'
            }
            
            results.append(result)
            print(f"[BENCHMARK] Result: {status} in {duration:.2f}s")
    
    # Write results to CSV
    with open(output_file, 'w', newline='') as f:
        fieldnames = ['timestamp', 'dataset', 'dataset_path', 
                     'run', 'job_id', 'status', 'duration_seconds', 'failure_mode']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    
    print(f"\n[BENCHMARK] Results written to {output_file}")
    print_summary(results)


def print_summary(results):
    """Print a summary of benchmark results"""
    print("\n" + "="*60)
    print("BENCHMARK SUMMARY")
    print("="*60)
    
    # Group by dataset
    datasets = {}
    for r in results:
        ds = r['dataset']
        if ds not in datasets:
            datasets[ds] = []
        if r['status'] == 'COMPLETED':
            datasets[ds].append(r['duration_seconds'])
    
    for ds, durations in datasets.items():
        if durations:
            avg = sum(durations) / len(durations)
            min_d = min(durations)
            max_d = max(durations)
            print(f"\n{ds}:")
            print(f"  Runs: {len(durations)}")
            print(f"  Avg: {avg:.2f}s")
            print(f"  Min: {min_d:.2f}s")
            print(f"  Max: {max_d:.2f}s")


def main():
    parser = argparse.ArgumentParser(description='Run MapReduce benchmarks')
    parser.add_argument('--datasets', nargs='+', 
                       default=['hdfs://nn:9000/data/test_10mb.parquet'],
                       help='List of dataset paths to benchmark')
    parser.add_argument('--runs', type=int, default=3,
                       help='Number of runs per configuration')
    parser.add_argument('--output', type=str, default='benchmark_results.csv',
                       help='Output CSV file for results')
    
    args = parser.parse_args()
    
    print("="*60)
    print("MapReduce Benchmark")
    print("="*60)
    print(f"Datasets: {args.datasets}")
    print(f"Runs per config: {args.runs}")
    print(f"Output file: {args.output}")
    print("="*60)
    
    run_benchmark(args.datasets, args.runs, args.output)


if __name__ == "__main__":
    main()

