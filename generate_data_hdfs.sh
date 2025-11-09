#!/bin/bash
# Quick script to generate test data from HDFS container

# Copy the test data generator to HDFS container
docker cp create_test_data.py mapreduce-hdfs-1:/tmp/

# Install required Python packages in HDFS container
docker-compose exec hdfs pip3 install pandas pyarrow numpy

# Generate the test data
docker-compose exec hdfs python3 /tmp/create_test_data.py --size "$@"

