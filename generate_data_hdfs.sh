#!/bin/bash
# Helper script to generate test data directly from HDFS container

docker-compose exec hdfs bash -c "python3 /home/hdfs_user/create_test_data.py --size $1"