import grpc
from concurrent import futures
import boss_pb2
import boss_pb2_grpc
import sys
import time
import threading
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import importlib.util
import os
from collections import defaultdict
import traceback
import socket

class WorkerService(boss_pb2_grpc.WorkerServicer):
    def __init__(self, worker_id, worker_address):
        self.worker_id = worker_id
        self.worker_address = worker_address
        self.current_task = None
        
    def ExecuteTask(self, request, context):
        """Execute a map or reduce task"""
        task_id = request.task_id
        task_type = request.task_type
        
        print(f"[WORKER {self.worker_id}] Received task: {task_id} (type: {task_type})")
        
        try:
            if task_type == boss_pb2.MAP:
                self._execute_map_task(request)
            else:  # REDUCE
                self._execute_reduce_task(request)
            
            print(f"[WORKER {self.worker_id}] Task {task_id} completed successfully")
            return boss_pb2.TaskComplete(
                task_id=task_id,
                worker_id=self.worker_id,
                success=True,
                error_message="",
                output_path=request.output_path
            )
            
        except Exception as e:
            error_msg = f"Task failed: {str(e)}\n{traceback.format_exc()}"
            print(f"[WORKER {self.worker_id}] {error_msg}")
            return boss_pb2.TaskComplete(
                task_id=task_id,
                worker_id=self.worker_id,
                success=False,
                error_message=error_msg,
                output_path=""
            )
    
    def SendHeartbeat(self, request, context):
        """Respond to heartbeat from boss"""
        return boss_pb2.HeartbeatAck(ok=True)
    
    def _execute_map_task(self, task):
        """Execute a map task"""
        # Read input partition
        fs = pa.fs.HadoopFileSystem("nn", 9000)
        input_path = task.input_path.replace("hdfs://nn:9000", "")
        
        table = pq.read_table(input_path, filesystem=fs)
        df = table.to_pandas()
        
        # Dynamically load map function
        map_func = self._load_user_function('/app/user_funcs/map_func.py', 'map_function')
        
        # Execute map function on each record
        # Collect results partitioned by key hash
        partitioned_results = defaultdict(list)
        
        for idx, row in df.iterrows():
            # Call user's map function
            for key, value in map_func(idx, row.to_dict()):
                # Partition by key hash
                partition_id = hash(key) % task.num_reduce_tasks
                partitioned_results[partition_id].append({'key': key, 'value': value})
        
        # Write partitioned results to intermediate storage
        job_id = task.job_id
        for partition_id, records in partitioned_results.items():
            if records:
                intermediate_df = pd.DataFrame(records)
                intermediate_path = f"/intermediate/{job_id}/partition-{partition_id}/map-{task.partition_id}.parquet"
                
                # Ensure directory exists
                dir_path = os.path.dirname(intermediate_path)
                try:
                    fs.create_dir(dir_path, recursive=True)
                except:
                    pass  # Directory might already exist
                
                intermediate_table = pa.Table.from_pandas(intermediate_df, preserve_index=False)
                pq.write_table(intermediate_table, intermediate_path, filesystem=fs)
        
        print(f"[WORKER {self.worker_id}] Map task {task.task_id} output written")
    
    def _execute_reduce_task(self, task):
        """Execute a reduce task"""
        # Read all intermediate files for this partition
        fs = pa.fs.HadoopFileSystem("nn", 9000)
        job_id = task.job_id
        partition_id = task.partition_id
        
        # Collect all map outputs for this partition
        intermediate_dir = f"/intermediate/{job_id}/partition-{partition_id}"
        
        all_records = []
        try:
            # List all files in the intermediate directory
            file_info = fs.get_file_info(pa.fs.FileSelector(intermediate_dir, recursive=True))
            
            for info in file_info:
                if info.type == pa.fs.FileType.File and info.path.endswith('.parquet'):
                    table = pq.read_table(info.path, filesystem=fs)
                    df = table.to_pandas()
                    all_records.append(df)
        except Exception as e:
            print(f"[WORKER {self.worker_id}] Warning: Could not read intermediate files: {e}")
            # Create empty output if no intermediate data
            all_records = [pd.DataFrame({'key': [], 'value': []})]
        
        if all_records:
            combined_df = pd.concat(all_records, ignore_index=True)
        else:
            combined_df = pd.DataFrame({'key': [], 'value': []})
        
        # Group by key
        grouped = combined_df.groupby('key')['value'].apply(list).reset_index()
        
        # Dynamically load reduce function
        reduce_func = self._load_user_function('/app/user_funcs/reduce_func.py', 'reduce_function')
        
        # Execute reduce function on each key-values pair
        results = []
        for _, row in grouped.iterrows():
            key = row['key']
            values = row['value']
            
            for result_key, result_value in reduce_func(key, values):
                results.append({'key': result_key, 'value': result_value})
        
        # Write final output
        output_path = task.output_path.replace("hdfs://nn:9000", "")
        
        if results:
            output_df = pd.DataFrame(results)
        else:
            output_df = pd.DataFrame({'key': [], 'value': []})
        
        # Ensure output directory exists
        dir_path = os.path.dirname(output_path)
        try:
            fs.create_dir(dir_path, recursive=True)
        except:
            pass  # Directory might already exist
        
        output_table = pa.Table.from_pandas(output_df, preserve_index=False)
        pq.write_table(output_table, output_path, filesystem=fs)
        
        print(f"[WORKER {self.worker_id}] Reduce task {task.task_id} output written to {output_path}")
    
    def _load_user_function(self, filepath, function_name):
        """Dynamically load a user-defined function"""
        spec = importlib.util.spec_from_file_location("user_module", filepath)
        user_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(user_module)
        return getattr(user_module, function_name)


def register_with_boss(worker_id, worker_address):
    """Register this worker with the boss"""
    max_retries = 10
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            channel = grpc.insecure_channel("boss:50052")
            stub = boss_pb2_grpc.BossStub(channel)
            
            response = stub.RegisterWorker(
                boss_pb2.WorkerInfo(
                    worker_type="generic",
                    worker_address=worker_address
                ),
                timeout=5
            )
            
            if response.success:
                print(f"[WORKER {worker_id}] Successfully registered with boss")
                return True
                
        except Exception as e:
            retry_count += 1
            print(f"[WORKER {worker_id}] Failed to register with boss (attempt {retry_count}/{max_retries}): {e}")
            time.sleep(5)
    
    print(f"[WORKER {worker_id}] Failed to register with boss after {max_retries} attempts")
    return False


def get_worker_address():
    """Get the worker's address"""
    hostname = socket.gethostname()
    # In Docker, use hostname:port
    return f"{hostname}:50053"


def serve(worker_id):
    worker_address = get_worker_address()
    
    # Create and start gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    worker_service = WorkerService(worker_id, worker_address)
    boss_pb2_grpc.add_WorkerServicer_to_server(worker_service, server)
    server.add_insecure_port('0.0.0.0:50053')
    server.start()
    
    print(f"[WORKER {worker_id}] Server started on port 50053")
    
    # Register with boss
    if not register_with_boss(worker_id, worker_address):
        print(f"[WORKER {worker_id}] Failed to register, but continuing to serve")
    
    # Keep server running
    server.wait_for_termination()


if __name__ == '__main__':
    # Get worker ID from environment or generate one
    worker_id = os.environ.get('WORKER_ID', socket.gethostname())
    
    print(f"[WORKER] Starting worker {worker_id}")
    serve(worker_id)

