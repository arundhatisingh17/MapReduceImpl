import grpc
from concurrent import futures
import boss_pb2
import boss_pb2_grpc
import map_reduce_pb2
import map_reduce_pb2_grpc
import threading
import time
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from collections import defaultdict
import uuid
import traceback

class BossService(boss_pb2_grpc.BossServicer):
    def __init__(self):
        self.workers = {}  # worker_address -> {type, last_heartbeat, status}
        self.active_tasks = {}  # task_id -> {worker_address, assignment, status, retry_count}
        self.completed_tasks = set()
        self.job_status = {}  # job_id -> {status, map_done, reduce_done, etc}
        self.lock = threading.Lock()
        
        # Start heartbeat monitor thread
        self.monitor_thread = threading.Thread(target=self._monitor_workers, daemon=True)
        self.monitor_thread.start()
        
    def RegisterWorker(self, request, context):
        """Register a worker with the boss"""
        worker_address = request.worker_address
        worker_type = request.worker_type
        
        with self.lock:
            self.workers[worker_address] = {
                'type': worker_type,
                'last_heartbeat': time.time(),
                'status': 'ACTIVE',
                'current_task': None
            }
        
        print(f"[BOSS] Registered worker: {worker_address} (type: {worker_type})")
        return boss_pb2.Ack(success=True, message=f"Worker {worker_address} registered")
    
    def AssignJob(self, request, context):
        """Receive job assignment from scheduler and orchestrate execution"""
        job_id = request.job_id
        print(f"[BOSS] Received job assignment: {job_id}")
        
        with self.lock:
            self.job_status[job_id] = {
                'status': 'MAP_IN_PROGRESS',
                'request': request,
                'map_tasks_pending': [],
                'map_tasks_completed': 0,
                'reduce_tasks_pending': [],
                'reduce_tasks_completed': 0,
                'start_time': time.time()
            }
        
        # Start job execution in a separate thread
        job_thread = threading.Thread(target=self._execute_job, args=(request,), daemon=True)
        job_thread.start()
        
        return boss_pb2.AssignmentStatus(
            job_id=job_id,
            acknowledged=True,
            message="Job accepted and execution started",
            status_code=0
        )
    
    def _execute_job(self, job_request):
        """Execute the complete MapReduce job"""
        job_id = job_request.job_id
        
        try:
            # Phase 1: Map Phase
            print(f"[BOSS] Starting MAP phase for job {job_id}")
            self._execute_map_phase(job_request)
            
            # Phase 2: Shuffle Phase (implicit - data already partitioned by map tasks)
            print(f"[BOSS] MAP phase complete for job {job_id}")
            
            # Phase 3: Reduce Phase
            print(f"[BOSS] Starting REDUCE phase for job {job_id}")
            self._execute_reduce_phase(job_request)
            
            print(f"[BOSS] Job {job_id} completed successfully")
            self._update_scheduler_status(job_id, "COMPLETED", job_request.output_path)
            
        except Exception as e:
            error_msg = f"Job {job_id} failed: {str(e)}\n{traceback.format_exc()}"
            print(f"[BOSS] {error_msg}")
            self._update_scheduler_status(job_id, "FAILED", "", error_msg)
    
    def _execute_map_phase(self, job_request):
        """Execute map tasks across available workers"""
        job_id = job_request.job_id
        num_map_tasks = job_request.num_map_tasks
        
        # Partition the input data
        input_partitions = self._partition_input(job_request.input_path, num_map_tasks)
        
        # Create map task assignments
        map_tasks = []
        for partition_id, partition_path in enumerate(input_partitions):
            task_id = f"{job_id}-map-{partition_id}"
            task_assignment = boss_pb2.TaskAssignment(
                task_id=task_id,
                job_id=job_id,
                task_type=boss_pb2.MAP,
                input_path=partition_path,
                output_path=f"hdfs://nn:9000/intermediate/{job_id}/map-{partition_id}",
                map_function_path=job_request.input_path.replace('sample.parquet', 'map_func.py'),  # Temporary
                reduce_function_path="",
                partition_id=partition_id,
                num_reduce_tasks=job_request.num_reduce_tasks
            )
            map_tasks.append((task_id, task_assignment))
        
        # Assign and execute map tasks
        self._execute_tasks(map_tasks, job_id, 'map')
    
    def _execute_reduce_phase(self, job_request):
        """Execute reduce tasks across available workers"""
        job_id = job_request.job_id
        num_reduce_tasks = job_request.num_reduce_tasks
        
        # Create reduce task assignments
        reduce_tasks = []
        for reduce_id in range(num_reduce_tasks):
            task_id = f"{job_id}-reduce-{reduce_id}"
            task_assignment = boss_pb2.TaskAssignment(
                task_id=task_id,
                job_id=job_id,
                task_type=boss_pb2.REDUCE,
                input_path=f"hdfs://nn:9000/intermediate/{job_id}/partition-{reduce_id}",
                output_path=f"{job_request.output_path}/part-{reduce_id}.parquet",
                map_function_path="",
                reduce_function_path=job_request.input_path.replace('sample.parquet', 'reduce_func.py'),  # Temporary
                partition_id=reduce_id,
                num_reduce_tasks=num_reduce_tasks
            )
            reduce_tasks.append((task_id, task_assignment))
        
        # Assign and execute reduce tasks
        self._execute_tasks(reduce_tasks, job_id, 'reduce')
    
    def _execute_tasks(self, tasks, job_id, phase):
        """Execute a list of tasks using available workers"""
        task_queue = list(tasks)
        completed = 0
        total = len(tasks)
        
        while completed < total:
            # Get available workers
            available_workers = self._get_available_workers()
            
            if not available_workers:
                print(f"[BOSS] No workers available, waiting...")
                time.sleep(2)
                continue
            
            # Assign tasks to available workers
            while task_queue and available_workers:
                task_id, task_assignment = task_queue.pop(0)
                worker_address = available_workers.pop(0)
                
                # Execute task on worker
                success = self._execute_task_on_worker(worker_address, task_assignment)
                
                if success:
                    completed += 1
                    print(f"[BOSS] Task {task_id} completed ({completed}/{total})")
                else:
                    # Re-queue failed task
                    print(f"[BOSS] Task {task_id} failed, re-queuing")
                    task_queue.append((task_id, task_assignment))
            
            time.sleep(1)
    
    def _execute_task_on_worker(self, worker_address, task_assignment):
        """Execute a single task on a worker"""
        try:
            channel = grpc.insecure_channel(worker_address)
            stub = boss_pb2_grpc.WorkerStub(channel)
            
            # Mark worker as busy
            with self.lock:
                if worker_address in self.workers:
                    self.workers[worker_address]['current_task'] = task_assignment.task_id
                    self.workers[worker_address]['status'] = 'BUSY'
            
            # Execute task with timeout
            response = stub.ExecuteTask(task_assignment, timeout=300)
            
            # Mark worker as available
            with self.lock:
                if worker_address in self.workers:
                    self.workers[worker_address]['current_task'] = None
                    self.workers[worker_address]['status'] = 'ACTIVE'
            
            return response.success
            
        except Exception as e:
            print(f"[BOSS] Error executing task on {worker_address}: {e}")
            # Mark worker as available
            with self.lock:
                if worker_address in self.workers:
                    self.workers[worker_address]['current_task'] = None
                    self.workers[worker_address]['status'] = 'ACTIVE'
            return False
    
    def _get_available_workers(self):
        """Get list of available worker addresses"""
        with self.lock:
            available = [
                addr for addr, info in self.workers.items()
                if info['status'] == 'ACTIVE' and info['current_task'] is None
            ]
        return available
    
    def _partition_input(self, input_path, num_partitions):
        """Partition input data into chunks for map tasks"""
        try:
            # Read input parquet file from HDFS
            fs = pa.fs.HadoopFileSystem("nn", 9000)
            hdfs_path = input_path.replace("hdfs://nn:9000", "")
            
            table = pq.read_table(hdfs_path, filesystem=fs)
            df = table.to_pandas()
            
            # Split into partitions
            partition_size = len(df) // num_partitions
            partitions = []
            
            for i in range(num_partitions):
                start_idx = i * partition_size
                end_idx = start_idx + partition_size if i < num_partitions - 1 else len(df)
                
                partition_df = df.iloc[start_idx:end_idx]
                partition_path = f"/data/partitions/input-partition-{i}.parquet"
                
                # Write partition to HDFS
                partition_table = pa.Table.from_pandas(partition_df, preserve_index=False)
                pq.write_table(partition_table, partition_path, filesystem=fs)
                
                partitions.append(f"hdfs://nn:9000{partition_path}")
            
            print(f"[BOSS] Created {len(partitions)} input partitions")
            return partitions
            
        except Exception as e:
            print(f"[BOSS] Error partitioning input: {e}")
            raise
    
    def _monitor_workers(self):
        """Monitor worker health via heartbeats"""
        while True:
            time.sleep(5)
            current_time = time.time()
            
            with self.lock:
                for worker_address, info in list(self.workers.items()):
                    # Check if worker has timed out (no heartbeat in 30 seconds)
                    if current_time - info.get('last_heartbeat', 0) > 30:
                        print(f"[BOSS] Worker {worker_address} timed out")
                        info['status'] = 'FAILED'
                        
                        # Re-queue any tasks assigned to this worker
                        if info['current_task']:
                            print(f"[BOSS] Re-queuing task {info['current_task']}")
    
    def _update_scheduler_status(self, job_id, status, output_path="", error=""):
        """Update job status in scheduler"""
        try:
            channel = grpc.insecure_channel("scheduler:5440")
            stub = map_reduce_pb2_grpc.SchedulerStub(channel)
            
            # Note: This requires adding a callback RPC to the scheduler
            # For now, we'll just print the status
            print(f"[BOSS] Job {job_id} status update: {status}")
            
            # Update local job status
            with self.lock:
                if job_id in self.job_status:
                    self.job_status[job_id]['status'] = status
                    self.job_status[job_id]['output_path'] = output_path
                    self.job_status[job_id]['error'] = error
            
        except Exception as e:
            print(f"[BOSS] Error updating scheduler: {e}")


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    boss_pb2_grpc.add_BossServicer_to_server(BossService(), server)
    server.add_insecure_port('0.0.0.0:50052')
    server.start()
    print("[BOSS] Server started on port 50052")
    server.wait_for_termination()


if __name__ == '__main__':
    serve()

