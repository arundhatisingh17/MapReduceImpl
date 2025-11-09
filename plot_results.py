#!/usr/bin/env python3
"""
Plot MapReduce Benchmark Results

Creates visualizations comparing performance with and without worker failures.
"""

import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import argparse
import sys

def plot_benchmark_results(csv_file, output_dir='.'):
    """
    Create plots from benchmark results.
    
    Args:
        csv_file: Path to benchmark results CSV
        output_dir: Directory to save plots
    """
    try:
        df = pd.read_csv(csv_file)
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        sys.exit(1)
    
    # Filter only completed jobs
    df = df[df['status'] == 'COMPLETED']
    
    if len(df) == 0:
        print("No completed jobs found in results")
        return
    
    # Plot 1: Execution time by dataset
    plt.figure(figsize=(12, 6))
    
    datasets = df['dataset'].unique()
    
    avg_times = []
    std_times = []
    labels = []
    
    for dataset in sorted(datasets):
        dataset_df = df[df['dataset'] == dataset]
        avg_times.append(dataset_df['duration_seconds'].mean())
        std_times.append(dataset_df['duration_seconds'].std())
        labels.append(dataset)
    
    x_pos = range(len(labels))
    plt.bar(x_pos, avg_times, yerr=std_times, align='center', alpha=0.7, 
            ecolor='black', capsize=10, color='steelblue')
    plt.ylabel('Execution Time (seconds)')
    plt.xlabel('Dataset')
    plt.title('MapReduce Job Execution Time by Dataset')
    plt.xticks(x_pos, labels, rotation=45, ha='right')
    plt.tight_layout()
    
    output_file = f"{output_dir}/execution_time_by_dataset.png"
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Created {output_file}")
    plt.close()
    
    # Plot 2: Execution time comparison (with/without failures)
    if 'failure_mode' in df.columns and df['failure_mode'].nunique() > 1:
        plt.figure(figsize=(12, 6))
        
        failure_modes = df['failure_mode'].unique()
        width = 0.35
        x_pos = range(len(datasets))
        
        for i, mode in enumerate(failure_modes):
            mode_data = []
            for dataset in sorted(datasets):
                subset = df[(df['dataset'] == dataset) & (df['failure_mode'] == mode)]
                if len(subset) > 0:
                    mode_data.append(subset['duration_seconds'].mean())
                else:
                    mode_data.append(0)
            
            offset = width * i
            plt.bar([x + offset for x in x_pos], mode_data, width, 
                   label=mode.capitalize(), alpha=0.8)
        
        plt.ylabel('Execution Time (seconds)')
        plt.xlabel('Dataset')
        plt.title('MapReduce Performance: Normal vs With Failures')
        plt.xticks([x + width/2 for x in x_pos], sorted(datasets), rotation=45, ha='right')
        plt.legend()
        plt.tight_layout()
        
        output_file = f"{output_dir}/failure_comparison.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"✓ Created {output_file}")
        plt.close()
    
    # Plot 3: Time series of runs
    if len(df) > 1:
        plt.figure(figsize=(14, 6))
        
        for dataset in sorted(datasets):
            dataset_df = df[df['dataset'] == dataset].sort_values('run')
            plt.plot(dataset_df['run'], dataset_df['duration_seconds'], 
                    marker='o', label=dataset, linewidth=2, markersize=8)
        
        plt.ylabel('Execution Time (seconds)')
        plt.xlabel('Run Number')
        plt.title('MapReduce Execution Time Across Runs')
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        
        output_file = f"{output_dir}/time_series.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"✓ Created {output_file}")
        plt.close()
    
    # Print statistics
    print("\n" + "="*60)
    print("STATISTICS")
    print("="*60)
    
    for dataset in sorted(datasets):
        dataset_df = df[df['dataset'] == dataset]
        print(f"\n{dataset}:")
        print(f"  Runs: {len(dataset_df)}")
        print(f"  Mean time: {dataset_df['duration_seconds'].mean():.2f}s")
        print(f"  Std dev: {dataset_df['duration_seconds'].std():.2f}s")
        print(f"  Min time: {dataset_df['duration_seconds'].min():.2f}s")
        print(f"  Max time: {dataset_df['duration_seconds'].max():.2f}s")
    
    print("\n" + "="*60)


def main():
    parser = argparse.ArgumentParser(description='Plot MapReduce benchmark results')
    parser.add_argument('csv_file', help='CSV file with benchmark results')
    parser.add_argument('--output-dir', default='.', 
                       help='Directory to save plots (default: current directory)')
    
    args = parser.parse_args()
    
    print("="*60)
    print("Plotting MapReduce Benchmark Results")
    print("="*60)
    print(f"Input: {args.csv_file}")
    print(f"Output directory: {args.output_dir}")
    print("="*60 + "\n")
    
    plot_benchmark_results(args.csv_file, args.output_dir)
    
    print("\n✓ All plots generated successfully!")


if __name__ == "__main__":
    main()

