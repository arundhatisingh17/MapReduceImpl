#!/usr/bin/env python3
"""
Run HDFS server locally for testing (outside Docker).
This allows you to test the HDFS server before deploying the full cluster.

Usage:
    python run_hdfs_local.py
"""

import os
import sys

# Set local data directory (instead of /data in Docker)
LOCAL_DATA_DIR = os.path.join(os.path.dirname(__file__), "hdfs_data_local")
os.makedirs(LOCAL_DATA_DIR, exist_ok=True)

# Override ROOT in the server module
os.environ["HDFS_ROOT"] = LOCAL_DATA_DIR

# Add hdfs directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'hdfs'))

# Import and modify the server
import hdfs.server as hdfs_server

# Override ROOT
hdfs_server.ROOT = LOCAL_DATA_DIR

if __name__ == "__main__":
    print("=" * 60)
    print("HDFS Server - Local Test Mode")
    print("=" * 60)
    print(f"Data directory: {LOCAL_DATA_DIR}")
    print(f"Listening on: localhost:50051")
    print("\nPress Ctrl+C to stop the server")
    print("=" * 60 + "\n")
    
    hdfs_server.serve(host="127.0.0.1", port=50051)

