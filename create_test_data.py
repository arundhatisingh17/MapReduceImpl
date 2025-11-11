#!/usr/bin/env python3
"""
Test Dataset Generator for MapReduce System

Generates test datasets of various sizes for benchmarking purposes.
"""

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import argparse
import sys

def generate_dataset(size_mb, output_path):
    """
    Generate a test dataset of specified size.
    
    Args:
        size_mb: Target size in MB
        output_path: HDFS path to write the dataset
    """
    # Estimate number of rows needed
    # Each row: ~40 bytes (id:8, x:8, y:8, value:8, text:variable)
    bytes_per_row = 100  # Conservative estimate with text field
    target_bytes = size_mb * 1024 * 1024
    n_rows = int(target_bytes / bytes_per_row)
    
    print(f"Generating dataset with ~{n_rows:,} rows ({size_mb}MB target)")
    
    # Generate data in chunks to avoid memory issues
    chunk_size = 100000
    chunks = []
    
    for i in range(0, n_rows, chunk_size):
        chunk_n = min(chunk_size, n_rows - i)
        
        data = {
            "id": np.arange(i, i + chunk_n),
            "x": np.random.randint(0, 100, chunk_n),
            "y": np.random.randint(0, 100, chunk_n),
            "value": np.random.random(chunk_n) * 100,
            "category": np.random.choice(['A', 'B', 'C', 'D', 'E'], chunk_n),
            "text": [f"data_row_{j}_value_{np.random.randint(0,1000)}" for j in range(chunk_n)]
        }
        
        chunks.append(pd.DataFrame(data))
        
        if (i // chunk_size) % 10 == 0:
            print(f"  Generated {i:,} rows...")
    
    print(f"Combining {len(chunks)} chunks...")
    df = pd.concat(chunks, ignore_index=True)
    
    # Write to HDFS
    print(f"Writing to {output_path}...")
    try:
        fs = pa.fs.HadoopFileSystem("namenode", 9000)
        hdfs_path = output_path.replace("hdfs://namenode:9000", "").replace("hdfs://nn:9000", "")
        
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, hdfs_path, filesystem=fs)
        
        # Check actual size
        file_info = fs.get_file_info(hdfs_path)
        actual_size_mb = file_info.size / (1024 * 1024)
        
        print(f"✓ Dataset created successfully!")
        print(f"  Rows: {len(df):,}")
        print(f"  Size: {actual_size_mb:.2f} MB")
        print(f"  Path: {output_path}")
        
        return output_path
        
    except Exception as e:
        print(f"✗ Error writing to HDFS: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description='Generate test datasets for MapReduce benchmarking')
    parser.add_argument('--size', type=str, default='10MB', 
                       help='Dataset size (e.g., 1MB, 10MB, 100MB, 500MB)')
    parser.add_argument('--output', type=str, default=None,
                       help='Output path (default: hdfs://nn:9000/data/test_<size>.parquet)')
    
    args = parser.parse_args()
    
    # Parse size
    size_str = args.size.upper()
    if size_str.endswith('MB'):
        size_mb = float(size_str[:-2])
    elif size_str.endswith('GB'):
        size_mb = float(size_str[:-2]) * 1024
    else:
        print(f"Error: Invalid size format '{args.size}'. Use format like '10MB' or '1GB'")
        sys.exit(1)
    
    # Generate output path
    if args.output:
        output_path = args.output
    else:
        output_path = f"hdfs://namenode:9000/data/test_{args.size.lower()}.parquet"
    
    generate_dataset(size_mb, output_path)


if __name__ == "__main__":
    main()

