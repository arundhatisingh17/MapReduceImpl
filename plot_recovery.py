#!/usr/bin/env python3
"""
Recovery Metrics Plotting Script

Creates visualizations for:
- Throughput over time with failure markers
- Recovery latency
- Worker count timeline
- CPU usage during job execution
"""

import json
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec
import numpy as np
import argparse
from datetime import datetime


def load_metrics(json_file):
    """Load metrics from JSON file"""
    with open(json_file, 'r') as f:
        return json.load(f)


def plot_throughput_and_recovery(metrics, output_file='recovery_analysis.png'):
    """
    Create a comprehensive plot showing:
    - Failure events markers
    - Recovery periods
    - Worker count
    - CPU usage

    Args:
        metrics: Dictionary containing job metrics
        output_file: Output filename for the plot
    """
    # Extract timeline data
    task_completions = metrics.get('task_completions', [])
    failure_events = metrics.get('failure_events', [])
    recovery_events = metrics.get('recovery_events', [])
    worker_timeline = metrics.get('worker_count_timeline', [])

    if not worker_timeline and not failure_events:
        print("No timeline data available for plotting.")
        return

    # Create figure with subplots
    fig = plt.figure(figsize=(16, 10))
    gs = GridSpec(3, 1, height_ratios=[2, 1, 1], hspace=0.3)

    # Get job start time as reference point
    start_time = metrics.get('start_time', 0)
    if worker_timeline:
        start_time = min(start_time, min(w['time'] for w in worker_timeline))

    # Convert absolute timestamps to relative (seconds from start)
    def to_relative_time(timestamp):
        return timestamp - start_time

    # ========== Subplot 1: Failure and Recovery Events Timeline ==========
    ax1 = fig.add_subplot(gs[0])

    # Mark failure events
    if failure_events:
        failure_times = [to_relative_time(f['time']) for f in failure_events]
        failure_phases = [f.get('phase', 'UNKNOWN') for f in failure_events]

        # Group failures by phase
        map_failures = [t for t, p in zip(failure_times, failure_phases) if p == 'MAP']
        reduce_failures = [t for t, p in zip(failure_times, failure_phases) if p == 'REDUCE']

        if map_failures:
            for i, ft in enumerate(map_failures):
                ax1.axvline(x=ft, color='red', linestyle='--', linewidth=2, alpha=0.7,
                           label='Node Failure' if i == 0 else '')

        if reduce_failures:
            for i, ft in enumerate(reduce_failures):
                ax1.axvline(x=ft, color='orange', linestyle='--', linewidth=2, alpha=0.7,
                           label='Reduce Failure' if i == 0 else '')

        # Mark recovery complete time if available
        if metrics.get('recovery_window_end'):
            recovery_end = to_relative_time(metrics['recovery_window_end'])
            ax1.axvline(x=recovery_end, color='green', linestyle=':', linewidth=2, alpha=0.7, label='Recovery Complete')

            # Shade recovery period
            if failure_times:
                first_failure = min(failure_times)
                ax1.axvspan(first_failure, recovery_end, alpha=0.2, color='yellow', label='Recovery Period')

    # Mark recovery events
    if recovery_events:
        recovery_times = [to_relative_time(r['time']) for r in recovery_events]
        for i, rt in enumerate(recovery_times):
            ax1.axvline(x=rt, color='blue', linestyle=':', linewidth=1.5, alpha=0.5,
                       label='Task Recovered' if i == 0 else '')

    ax1.set_xlabel('Time (seconds)', fontsize=12)
    ax1.set_ylabel('Event Timeline', fontsize=12)
    ax1.set_title('MapReduce Job: Failure & Recovery Timeline', fontsize=14, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    ax1.legend(loc='upper right', fontsize=10)
    ax1.set_ylim(0, 1)
    ax1.set_yticks([])

    # Add phase markers if available
    if metrics.get('map_phase_duration') and metrics.get('reduce_phase_duration'):
        map_end = metrics.get('map_phase_duration', 0)
        ax1.axvline(x=map_end, color='purple', linestyle='-.', linewidth=1.5, alpha=0.5, label='Map→Reduce')

    # ========== Subplot 2: Worker Count Over Time ==========
    ax2 = fig.add_subplot(gs[1], sharex=ax1)

    if worker_timeline:
        worker_times = [to_relative_time(w['time']) for w in worker_timeline]
        worker_counts = [w['worker_count'] for w in worker_timeline]

        ax2.plot(worker_times, worker_counts, 'g-', linewidth=2, marker='o', markersize=3, label='Active Workers')
        ax2.fill_between(worker_times, worker_counts, alpha=0.3, color='green')

        # Mark failures on worker count plot too
        if failure_events:
            for ft in failure_times:
                ax2.axvline(x=ft, color='red', linestyle='--', linewidth=1.5, alpha=0.5)

    ax2.set_ylabel('Worker Count', fontsize=12)
    ax2.set_title('Worker Availability Over Time', fontsize=12, fontweight='bold')
    ax2.grid(True, alpha=0.3)
    ax2.legend(loc='upper right', fontsize=10)
    ax2.set_ylim(bottom=0)

    # ========== Subplot 3: CPU Usage Over Time ==========
    ax3 = fig.add_subplot(gs[2], sharex=ax1)

    if worker_timeline:
        cpu_times = [to_relative_time(w['time']) for w in worker_timeline]
        cpu_usages = [w['cpu_percent'] for w in worker_timeline]

        ax3.plot(cpu_times, cpu_usages, 'orange', linewidth=2, label='CPU Usage')
        ax3.fill_between(cpu_times, cpu_usages, alpha=0.3, color='orange')

        # Mark failures
        if failure_events:
            for ft in failure_times:
                ax3.axvline(x=ft, color='red', linestyle='--', linewidth=1.5, alpha=0.5)

    ax3.set_xlabel('Time (seconds)', fontsize=12)
    ax3.set_ylabel('CPU Usage (%)', fontsize=12)
    ax3.set_title('CPU Consumption Over Time', fontsize=12, fontweight='bold')
    ax3.grid(True, alpha=0.3)
    ax3.legend(loc='upper right', fontsize=10)
    ax3.set_ylim(0, 100)

    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"[PLOT] Saved recovery analysis plot to {output_file}")

    plt.close()


def plot_recovery_latency_breakdown(metrics, output_file='recovery_latency.png'):
    """
    Create a bar chart showing recovery latency for each failed task.

    Args:
        metrics: Dictionary containing job metrics
        output_file: Output filename for the plot
    """
    recovery_events = metrics.get('recovery_events', [])

    if not recovery_events:
        print("No recovery events to plot.")
        return

    fig, ax = plt.subplots(figsize=(12, 6))

    task_ids = [event['task_id'].split('-')[-1] for event in recovery_events]
    recovery_times = [event['recovery_duration'] for event in recovery_events]

    bars = ax.bar(range(len(task_ids)), recovery_times, color='steelblue', alpha=0.7)

    # Color bars by duration (red for slow, green for fast)
    max_time = max(recovery_times)
    for bar, duration in zip(bars, recovery_times):
        intensity = duration / max_time
        bar.set_color(plt.cm.RdYlGn_r(intensity))

    ax.set_xlabel('Task ID', fontsize=12)
    ax.set_ylabel('Recovery Latency (seconds)', fontsize=12)
    ax.set_title('Recovery Latency per Failed Task', fontsize=14, fontweight='bold')
    ax.set_xticks(range(len(task_ids)))
    ax.set_xticklabels(task_ids, rotation=45, ha='right')
    ax.grid(True, alpha=0.3, axis='y')

    # Add average line
    avg_recovery = metrics.get('avg_recovery_latency', 0)
    ax.axhline(y=avg_recovery, color='red', linestyle='--', linewidth=2, label=f'Average: {avg_recovery:.2f}s')
    ax.legend()

    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"[PLOT] Saved recovery latency breakdown to {output_file}")
    plt.close()




def main():
    parser = argparse.ArgumentParser(description='Plot MapReduce recovery metrics')
    parser.add_argument('--input', type=str, required=True,
                        help='Input JSON file with benchmark metrics')
    parser.add_argument('--output-dir', type=str, default='.',
                        help='Output directory for plots')

    args = parser.parse_args()

    print("=" * 60)
    print("MapReduce Recovery Metrics Plotting")
    print("=" * 60)
    print(f"Input file: {args.input}")
    print(f"Output directory: {args.output_dir}")
    print("=" * 60 + "\n")

    # Load metrics
    metrics = load_metrics(args.input)

    # Create plots
    import os
    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)

    print("[PLOT] Generating recovery timeline plot...")
    plot_throughput_and_recovery(
        metrics,
        output_file=os.path.join(output_dir, 'recovery_timeline.png')
    )

    print("[PLOT] Generating recovery latency breakdown...")
    plot_recovery_latency_breakdown(
        metrics,
        output_file=os.path.join(output_dir, 'recovery_latency.png')
    )

    print("\n[PLOT] All plots generated successfully!")
    print(f"[PLOT] Check {output_dir}/ for the output files")


if __name__ == "__main__":
    main()
