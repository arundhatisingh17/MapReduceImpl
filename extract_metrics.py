#!/usr/bin/env python3
"""
Helper script to extract detailed metrics from master service.

This script connects directly to the master service (not via gRPC)
to retrieve internal metrics for a completed job.
"""

import sys
import json
import pickle
import argparse


def extract_metrics_from_master(job_id, master_host='localhost', master_port=50051):
    """
    Extract detailed metrics from the master service.

    Note: This is a helper function. In production, you would:
    1. Add a gRPC endpoint to the master service to expose metrics
    2. Or run this script on the same machine as the master
    3. Or use shared storage for metrics

    For now, this demonstrates how to structure the metrics extraction.
    """
    # This would require direct access to the master service instance
    # For demonstration, we'll show the expected structure

    print(f"[EXTRACT] Attempting to extract metrics for job {job_id}")
    print(f"[EXTRACT] Master: {master_host}:{master_port}")

    # In a real implementation, you might:
    # 1. Import the master module and connect to its instance
    # 2. Use a metrics database
    # 3. Add a GetJobMetrics gRPC endpoint

    print("""
[EXTRACT] Note: Direct metrics extraction requires access to master service.
[EXTRACT]
[EXTRACT] To enable metrics extraction, you have two options:
[EXTRACT]
[EXTRACT] Option 1: Add a gRPC endpoint to master.proto:
[EXTRACT]   rpc GetJobMetrics(JobMetricsRequest) returns (JobMetricsResponse) {}
[EXTRACT]
[EXTRACT] Option 2: Write metrics to a shared file/database after job completion.
[EXTRACT]   In master.py _execute_job(), add:
[EXTRACT]   ```
[EXTRACT]   with open(f'/shared/metrics/{job_id}.json', 'w') as f:
[EXTRACT]       json.dump(self.jobs[job_id], f, default=str)
[EXTRACT]   ```
[EXTRACT]
[EXTRACT] For now, run benchmark_recovery.py with --save-metrics flag
[EXTRACT] to export metrics during job execution.
    """)

    return None


def save_metrics_to_file(metrics, output_file):
    """Save metrics to JSON file"""
    # Convert any non-JSON-serializable objects
    def convert_value(obj):
        if isinstance(obj, (list, dict, str, int, float, bool, type(None))):
            return obj
        return str(obj)

    serializable_metrics = {}
    for key, value in metrics.items():
        if isinstance(value, dict):
            serializable_metrics[key] = {k: convert_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            serializable_metrics[key] = [convert_value(v) if not isinstance(v, dict)
                                          else {k: convert_value(vv) for k, vv in v.items()}
                                          for v in value]
        else:
            serializable_metrics[key] = convert_value(value)

    with open(output_file, 'w') as f:
        json.dump(serializable_metrics, f, indent=2)

    print(f"[EXTRACT] Metrics saved to {output_file}")


def main():
    parser = argparse.ArgumentParser(description='Extract job metrics from master service')
    parser.add_argument('--job-id', type=str, required=True,
                        help='Job ID to extract metrics for')
    parser.add_argument('--output', type=str, default='job_metrics.json',
                        help='Output JSON file')
    parser.add_argument('--master-host', type=str, default='localhost',
                        help='Master service host')
    parser.add_argument('--master-port', type=int, default=50051,
                        help='Master service port')

    args = parser.parse_args()

    metrics = extract_metrics_from_master(args.job_id, args.master_host, args.master_port)

    if metrics:
        save_metrics_to_file(metrics, args.output)
    else:
        print("[EXTRACT] Failed to extract metrics. See notes above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
