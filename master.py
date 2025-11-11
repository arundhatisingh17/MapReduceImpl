import grpc
from concurrent import futures
import map_reduce_pb2
import map_reduce_pb2_grpc
import threading
import time
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import pyarrow.fs
import uuid
import traceback
from collections import defaultdict

class MasterService(map_reduce_pb2_grpc.MasterServicer):
    """Master service that coordinates MapReduce jobs"""
    
    def __init__(self):
        self.workers = {}  # worker_address -> {last_heartbeat, status, current_task}
        self.jobs = {}  # job_id -> {status, request, output_path, error, start_time, ...}
        self.active_tasks = {}  # task_id -> {worker_address, assignment, status, retry_count, start_time}
        self.completed_tasks = set()
        self.lock = threading.Lock()
        
        # Configuration
        self.MAX_RETRIES = 3
        self.TASK_TIMEOUT = 300  # seconds (5 minutes)
        self.WORKER_TIMEOUT = 60  # seconds
        
        # Start monitoring thread
        self.monitor_thread = threading.Thread(target=self._monitor_workers, daemon=True)
        self.monitor_thread.start()
        
        print("[MASTER] Master service initialized")
    
    def SubmitJob(self, request, context):
        """Handle job submission from client"""
        job_id = f"job-{uuid.uuid4().hex[:8]}"
        
        print(f"[MASTER] Received job submission: {job_id}")
        print(f"[MASTER]   Dataset: {request.dataset_path}")
        print(f"[MASTER]   Map tasks: {request.num_map_tasks}")
        print(f"[MASTER]   Reduce tasks: {request.num_reduce_tasks}")
        
        with self.lock:
            self.jobs[job_id] = {
                'status': 'PENDING',
                'request': request,
                'output_path': f"/output/{job_id}",
                'error': '',
                'start_time': time.time(),
                'map_tasks_completed': 0,
                'reduce_tasks_completed': 0,
                'failures': 0,
                'reassignments': 0
            }
        
        # Start job execution in a separate thread
        job_thread = threading.Thread(target=self._execute_job, args=(job_id, request), daemon=True)
        job_thread.start()
        
        return map_reduce_pb2.JobStatusResponse(
            job_id=job_id,
            status='PENDING',
            output_path='',
            error=''
        )
    
    def GetJobStatus(self, request, context):
        """Get status of a submitted job"""
        job_id = request.job_id
        
        with self.lock:
            if job_id not in self.jobs:
                return map_reduce_pb2.JobStatusResponse(
                    job_id=job_id,
                    status='NOT_FOUND',
                    output_path='',
                    error='Job not found'
                )
            
            job = self.jobs[job_id]
            return map_reduce_pb2.JobStatusResponse(
                job_id=job_id,
                status=job['status'],
                output_path=job['output_path'] if job['status'] == 'COMPLETED' else '',
                error=job['error']
            )
    
    def RegisterWorker(self, request, context):
        """Register a worker with the master"""
        worker_address = request.worker_address
        
        with self.lock:
            if worker_address in self.workers:
                print(f"[MASTER] ✓ Worker re-registered: {worker_address}")
                # Update heartbeat for existing worker
                self.workers[worker_address]['last_heartbeat'] = time.time()
                message = "Worker re-registered successfully"
            else:
                self.workers[worker_address] = {
                    'last_heartbeat': time.time(),
                    'status': 'idle',
                    'current_task': None,
                    'tasks_completed': 0
                }
                print(f"[MASTER] ✓ New worker registered: {worker_address}")
                print(f"[MASTER]   Total workers: {len(self.workers)}")
                message = "Worker registered successfully"
        
        return map_reduce_pb2.RegistrationAck(
            success=True,
            message=message
        )
    
    def _execute_job(self, job_id, request):
        """Execute a complete MapReduce job"""
        try:
            # Wait for workers to be available
            self._wait_for_workers(min_workers=1, timeout=60)
            
            # Phase 1: Map Phase
            self._update_job_status(job_id, 'MAP_IN_PROGRESS')
            print(f"\n[MASTER] ====== Starting MAP phase for job {job_id} ======")
            print(f"[MASTER]   Number of map tasks: {request.num_map_tasks}")
            self._execute_map_phase(job_id, request)
            print(f"[MASTER] ====== MAP phase completed for job {job_id} ======\n")
            
            # Phase 2: Reduce Phase
            self._update_job_status(job_id, 'REDUCE_IN_PROGRESS')
            print(f"\n[MASTER] ====== Starting REDUCE phase for job {job_id} ======")
            print(f"[MASTER]   Number of reduce tasks: {request.num_reduce_tasks}")
            self._execute_reduce_phase(job_id, request)
            print(f"[MASTER] ====== REDUCE phase completed for job {job_id} ======\n")
            
            # Job completed
            self._update_job_status(job_id, 'COMPLETED')
            duration = time.time() - self.jobs[job_id]['start_time']
            print(f"[MASTER] Job {job_id} COMPLETED in {duration:.2f}s")
            
            # Log statistics
            with self.lock:
                job = self.jobs[job_id]
                print(f"[MASTER]   Failures: {job.get('failures', 0)}")
                print(f"[MASTER]   Reassignments: {job.get('reassignments', 0)}")
            
        except Exception as e:
            error_msg = f"Job failed: {str(e)}\n{traceback.format_exc()}"
            print(f"[MASTER] Job {job_id} FAILED: {error_msg}")
            self._update_job_status(job_id, 'FAILED', error=error_msg)
    
    def _execute_map_phase(self, job_id, request):
        """Execute the map phase"""
        # Partition input data
        input_partitions = self._partition_input(request.dataset_path, request.num_map_tasks)
        
        # Create map task assignments
        map_tasks = []
        for partition_id, partition_path in enumerate(input_partitions):
            task_id = f"{job_id}-map-{partition_id}"
            task_assignment = map_reduce_pb2.TaskAssignment(
                task_id=task_id,
                job_id=job_id,
                task_type=map_reduce_pb2.MAP,
                input_path=partition_path,
                output_path=f"/intermediate/{job_id}",
                map_function_path=request.map_function_path,
                reduce_function_path='',
                partition_id=partition_id,
                num_reduce_tasks=request.num_reduce_tasks
            )
            map_tasks.append((task_id, task_assignment))
        
        # Execute all map tasks
        self._execute_tasks(map_tasks, job_id)
    
    def _execute_reduce_phase(self, job_id, request):
        """Execute the reduce phase"""
        # Create reduce task assignments
        reduce_tasks = []
        for reduce_id in range(request.num_reduce_tasks):
            task_id = f"{job_id}-reduce-{reduce_id}"
            task_assignment = map_reduce_pb2.TaskAssignment(
                task_id=task_id,
                job_id=job_id,
                task_type=map_reduce_pb2.REDUCE,
                input_path=f"/intermediate/{job_id}",
                output_path=f"/output/{job_id}/part-{reduce_id}.parquet",
                map_function_path='',
                reduce_function_path=request.reduce_function_path,
                partition_id=reduce_id,
                num_reduce_tasks=request.num_reduce_tasks
            )
            reduce_tasks.append((task_id, task_assignment))
        
        # Execute all reduce tasks
        self._execute_tasks(reduce_tasks, job_id)
    
    def _execute_tasks(self, tasks, job_id):
        """Execute a list of tasks using available workers"""
        task_queue = list(tasks)
        task_retry_counts = {}
        
        while task_queue or self._has_running_tasks(job_id):
            # Check for timed-out tasks
            self._check_task_timeouts(task_queue, task_retry_counts, job_id)
            
            # Get available workers
            available_workers = self._get_available_workers()
            
            # Assign tasks to available workers
            while task_queue and available_workers:
                task_id, task_assignment = task_queue.pop(0)
                worker_address = available_workers.pop(0)
                
                # Check retry limit
                retry_count = task_retry_counts.get(task_id, 0)
                if retry_count >= self.MAX_RETRIES:
                    raise Exception(f"Task {task_id} exceeded max retries ({self.MAX_RETRIES})")
                
                # Log task assignment
                task_type = "MAP" if task_assignment.task_type == map_reduce_pb2.MAP else "REDUCE"
                print(f"[MASTER] → Assigning {task_type} task {task_id} to worker {worker_address}")
                
                # Execute task asynchronously
                thread = threading.Thread(
                    target=self._execute_task_on_worker,
                    args=(worker_address, task_assignment, task_id, job_id, task_queue, task_retry_counts),
                    daemon=True
                )
                thread.start()
                
                # Track active task
                with self.lock:
                    self.active_tasks[task_id] = {
                        'worker_address': worker_address,
                        'assignment': task_assignment,
                        'status': 'RUNNING',
                        'retry_count': retry_count,
                        'start_time': time.time()
                    }
                    if worker_address in self.workers:
                        self.workers[worker_address]['current_task'] = task_id
            
            time.sleep(1)
        
        print(f"[MASTER] All tasks completed for job {job_id}")
    
    def _execute_task_on_worker(self, worker_address, task_assignment, task_id, job_id, task_queue, task_retry_counts):
        """Execute a single task on a worker"""
        task_type = "MAP" if task_assignment.task_type == map_reduce_pb2.MAP else "REDUCE"
        
        try:
            channel = grpc.insecure_channel(worker_address)
            stub = map_reduce_pb2_grpc.WorkerStub(channel)
            
            response = stub.ExecuteTask(task_assignment, timeout=self.TASK_TIMEOUT)
            
            with self.lock:
                if response.success:
                    self.active_tasks[task_id]['status'] = 'COMPLETED'
                    self.completed_tasks.add(task_id)
                    
                    # Update worker stats
                    if worker_address in self.workers:
                        self.workers[worker_address]['tasks_completed'] = self.workers[worker_address].get('tasks_completed', 0) + 1
                        self.workers[worker_address]['current_task'] = None
                    
                    # Update job stats
                    if job_id in self.jobs:
                        if task_type == "MAP":
                            self.jobs[job_id]['map_tasks_completed'] += 1
                        else:
                            self.jobs[job_id]['reduce_tasks_completed'] += 1
                    
                    print(f"[MASTER] ✓ {task_type} task {task_id} completed by {worker_address}")
                else:
                    # Task failed, re-queue it
                    self.active_tasks[task_id]['status'] = 'FAILED'
                    retry_count = task_retry_counts.get(task_id, 0) + 1
                    task_retry_counts[task_id] = retry_count
                    
                    if retry_count < self.MAX_RETRIES:
                        task_queue.append((task_id, task_assignment))
                        print(f"[MASTER] Task {task_id} failed, re-queuing (retry {retry_count}/{self.MAX_RETRIES})")
                        self.jobs[job_id]['failures'] += 1
                        self.jobs[job_id]['reassignments'] += 1
                
                # Mark worker as available
                if worker_address in self.workers:
                    self.workers[worker_address]['current_task'] = None
                    
        except Exception as e:
            print(f"[MASTER] Error executing task {task_id} on {worker_address}: {e}")
            with self.lock:
                if task_id in self.active_tasks:
                    self.active_tasks[task_id]['status'] = 'FAILED'
                
                # Mark worker as available
                if worker_address in self.workers:
                    self.workers[worker_address]['current_task'] = None
                
                # Re-queue task
                retry_count = task_retry_counts.get(task_id, 0) + 1
                task_retry_counts[task_id] = retry_count
                if retry_count < self.MAX_RETRIES:
                    task_queue.append((task_id, task_assignment))
                    self.jobs[job_id]['failures'] += 1
                    self.jobs[job_id]['reassignments'] += 1
    
    def _partition_input(self, input_path, num_partitions):
        """Partition input data for map tasks"""
        try:
            print(f"[MASTER] Partitioning input: {input_path} into {num_partitions} partitions")
            
            # Connect to HDFS
            fs = pyarrow.fs.HadoopFileSystem("namenode", 9000)
            
            # Remove hdfs:// prefix if present
            hdfs_path = input_path.replace("hdfs://namenode:9000", "")
            
            # Read input data
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
                
                partitions.append(f"hdfs://namenode:9000{partition_path}")
            
            print(f"[MASTER] Created {len(partitions)} input partitions")
            return partitions
            
        except Exception as e:
            print(f"[MASTER] Error partitioning input: {e}")
            raise
    
    def _check_task_timeouts(self, task_queue, task_retry_counts, job_id):
        """Check for timed-out tasks and re-queue them"""
        current_time = time.time()
        timed_out_tasks = []
        
        with self.lock:
            for task_id, task_info in list(self.active_tasks.items()):
                if (task_info.get('status') == 'RUNNING' and 
                    task_id.startswith(job_id)):
                    if current_time - task_info.get('start_time', current_time) > self.TASK_TIMEOUT:
                        print(f"[MASTER] Task {task_id} timed out")
                        timed_out_tasks.append((task_id, task_info['assignment']))
                        task_info['status'] = 'FAILED'
                        
                        # Mark worker as potentially failed
                        worker_addr = task_info['worker_address']
                        if worker_addr in self.workers:
                            self.workers[worker_addr]['current_task'] = None
                        
                        # Update retry count
                        task_retry_counts[task_id] = task_retry_counts.get(task_id, 0) + 1
                        self.jobs[job_id]['failures'] += 1
                        self.jobs[job_id]['reassignments'] += 1
        
        # Re-queue timed-out tasks
        for task_id, assignment in timed_out_tasks:
            task_queue.append((task_id, assignment))
    
    def _has_running_tasks(self, job_id):
        """Check if there are any running tasks for a job"""
        with self.lock:
            return any(
                task_info.get('status') == 'RUNNING' 
                for task_id, task_info in self.active_tasks.items() 
                if task_id.startswith(job_id)
            )
    
    def _get_available_workers(self):
        """Get list of available worker addresses"""
        with self.lock:
            current_time = time.time()
            available = [
                addr for addr, info in self.workers.items()
                if (info.get('current_task') is None and
                    current_time - info.get('last_heartbeat', 0) < self.WORKER_TIMEOUT)
            ]
        return available
    
    def _wait_for_workers(self, min_workers=1, timeout=60):
        """Wait for minimum number of workers to be available"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            available = self._get_available_workers()
            if len(available) >= min_workers:
                print(f"[MASTER] {len(available)} workers available")
                return
            print(f"[MASTER] Waiting for workers... ({len(available)}/{min_workers})")
            time.sleep(2)
        
        raise Exception(f"Timeout waiting for {min_workers} workers")
    
    def _monitor_workers(self):
        """Monitor worker health"""
        while True:
            time.sleep(30)  # Check every 30 seconds
            current_time = time.time()
            
            with self.lock:
                down_workers = []
                for worker_address, info in list(self.workers.items()):
                    if current_time - info.get('last_heartbeat', 0) > self.WORKER_TIMEOUT:
                        down_workers.append(worker_address)
                
                # Only log if there are down workers
                if down_workers:
                    for worker_address in down_workers:
                        print(f"[MASTER] ⚠ Worker {worker_address} appears to be down (last heartbeat > {self.WORKER_TIMEOUT}s ago)")
                        # Worker tasks will be handled by timeout mechanism
    
    def _update_job_status(self, job_id, status, error=''):
        """Update job status"""
        with self.lock:
            if job_id in self.jobs:
                self.jobs[job_id]['status'] = status
                if error:
                    self.jobs[job_id]['error'] = error

def serve():
    """Start the master server"""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    map_reduce_pb2_grpc.add_MasterServicer_to_server(MasterService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("[MASTER] Server started on port 50051")
    
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()

