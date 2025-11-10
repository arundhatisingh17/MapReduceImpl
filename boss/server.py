"""
Boss gRPC server implementing MapReduce SubmitJob and GetJobStatus.
Coordinates map and reduce task scheduling to worker HTTP endpoints.

Workers:
- Provided by docker-compose (worker1..worker4) exposing /run_task on port 8000.
HDFS:
- gRPC service running on hdfs:50051
"""

import uuid
import threading
import time
import grpc
import os
import requests
from concurrent import futures
from typing import List, Dict, Optional

import mapreduce_pb2
import mapreduce_pb2_grpc
import hdfs_pb2
import hdfs_pb2_grpc

# ============================================================================
# Configuration
# ============================================================================

WORKERS = os.environ.get(
    "WORKER_HOSTS", 
    "worker1:8000,worker2:8000,worker3:8000,worker4:8000"
).split(",")
HDFS_ADDR = os.environ.get("HDFS_ADDR", "hdfs:50051")
NUM_WORKERS = len(WORKERS)

# Task execution timeout (seconds)
TASK_TIMEOUT = 120
MAP_TASK_WAIT_TIME = 3  # Simulated wait time for map tasks

# Job status constants
STATUS_QUEUED = "QUEUED"
STATUS_RUNNING = "RUNNING"
STATUS_COMPLETED = "COMPLETED"
STATUS_NOT_FOUND = "NOT_FOUND"

# ============================================================================
# Logging Utilities
# ============================================================================

def info(msg: str) -> None:
    """Print an info message with blue color."""
    print(f"\033[94m[INFO]\033[0m {msg}")

def success(msg: str) -> None:
    """Print a success message with green color."""
    print(f"\033[92m[SUCCESS]\033[0m {msg}")

def warn(msg: str) -> None:
    """Print a warning message with yellow color."""
    print(f"\033[93m[WARN]\033[0m {msg}")

def error(msg: str) -> None:
    """Print an error message with red color."""
    print(f"\033[91m[ERROR]\033[0m {msg}")

# ============================================================================
# Helper Functions
# ============================================================================

def normalize_hdfs_path(path: str) -> str:
    """
    Normalize an HDFS path by removing hdfs: prefix and leading slashes.
    
    Args:
        path: Path that may contain hdfs: prefix or leading slashes
        
    Returns:
        Normalized path relative to HDFS root
    """
    return path.replace("hdfs:", "").lstrip("/")

def build_hdfs_path(base_path: str, filename: str) -> str:
    """
    Build a normalized HDFS path by joining base path and filename.
    
    Args:
        base_path: Base directory path (will be normalized)
        filename: Filename to append
        
    Returns:
        Normalized full path
    """
    normalized_base = normalize_hdfs_path(base_path)
    if normalized_base:
        return f"{normalized_base}/{filename}".replace("//", "/")
    return filename

def get_hdfs_client() -> hdfs_pb2_grpc.HdfsServiceStub:
    """
    Create and return an HDFS gRPC client stub.
    
    Returns:
        HDFS service stub
        
    Raises:
        Exception: If connection to HDFS fails
    """
    channel = grpc.insecure_channel(HDFS_ADDR)
    return hdfs_pb2_grpc.HdfsServiceStub(channel)

# ============================================================================
# Job Manager
# ============================================================================

class JobManager:
    """Manages MapReduce job lifecycle and state."""
    
    def __init__(self):
        """Initialize the job manager with empty job dictionary and lock."""
        self.jobs: Dict[str, Dict] = {}
        self.lock = threading.Lock()
    
    def submit(self, request: mapreduce_pb2.JobRequest) -> str:
        """
        Submit a new MapReduce job for execution.
        
        Args:
            request: Job request containing input/output paths and task counts
            
        Returns:
            Unique job ID
        """
        job_id = str(uuid.uuid4())
        job_record = {
            "request": request,
            "status": STATUS_QUEUED,
            "map_tasks": [],
            "reduce_tasks": [],
            "msg": ""
        }
        
        with self.lock:
            self.jobs[job_id] = job_record
        
        self._log_job_submission(job_id, request)
        
        # Start job execution in background thread
        threading.Thread(target=self._run_job, args=(job_id,), daemon=True).start()
        
        return job_id
    
    def get_status(self, job_id: str) -> Optional[Dict]:
        """
        Get the status of a job by ID.
        
        Args:
            job_id: Unique job identifier
            
        Returns:
            Job record dictionary or None if not found
        """
        with self.lock:
            return self.jobs.get(job_id)
    
    def _log_job_submission(self, job_id: str, request: mapreduce_pb2.JobRequest) -> None:
        """Log job submission details."""
        info(f"New job submitted → ID: {job_id}")
        info(f"  Input: {request.input_path}")
        info(f"  Output: {request.output_path}")
        info(f"  User code: {request.user_code_path}")
        info(f"  Map tasks: {request.num_map_tasks}, Reduce tasks: {request.num_reduce_tasks}")
    
    def _run_job(self, job_id: str) -> None:
        """
        Execute a MapReduce job (runs in background thread).
        
        Args:
            job_id: Unique job identifier
        """
        # Mark job as running
        with self.lock:
            job = self.jobs[job_id]
            job["status"] = STATUS_RUNNING
        
        request = job["request"]
        info(f"=== Starting Job {job_id} ===")
        
        try:
            # Connect to HDFS
            hdfs_stub = self._connect_to_hdfs()
            
            # Get input files
            input_files = self._discover_input_files(hdfs_stub, request.input_path)
            
            # Distribute files across map tasks
            map_task_buckets = self._distribute_files_to_map_tasks(
                input_files, 
                request.num_map_tasks
            )
            
            # Execute map phase
            job_tmpdir = f"jobs/{job_id}"
            self._execute_map_phase(
                map_task_buckets,
                request.user_code_path,
                request.num_reduce_tasks,
                job_tmpdir
            )
            
            # Prepare reduce inputs
            reduce_inputs = self._prepare_reduce_inputs(job_tmpdir, request.num_reduce_tasks)
            
            # Execute reduce phase
            self._execute_reduce_phase(
                reduce_inputs,
                request.user_code_path,
                request.output_path,
                request.num_reduce_tasks,
                job_tmpdir
            )
            
            # Mark job as completed
            self._mark_job_completed(job_id)
            
        except Exception as e:
            error(f"Job {job_id} failed: {e}")
            with self.lock:
                if job_id in self.jobs:
                    self.jobs[job_id]["status"] = "FAILED"
                    self.jobs[job_id]["msg"] = str(e)
    
    def _connect_to_hdfs(self) -> hdfs_pb2_grpc.HdfsServiceStub:
        """
        Establish connection to HDFS service.
        
        Returns:
            HDFS service stub
            
        Raises:
            Exception: If connection fails
        """
        try:
            stub = get_hdfs_client()
            info(f"Connected to HDFS at {HDFS_ADDR}")
            return stub
        except Exception as e:
            error(f"Failed to connect to HDFS: {e}")
            raise
    
    def _discover_input_files(
        self, 
        hdfs_stub: hdfs_pb2_grpc.HdfsServiceStub, 
        input_path: str
    ) -> List[str]:
        """
        Discover input files from HDFS path (directory or single file).
        
        Args:
            hdfs_stub: HDFS service stub
            input_path: Input path (may be directory or file)
            
        Returns:
            List of normalized input file paths
        """
        normalized_path = normalize_hdfs_path(input_path)
        info(f"Listing files under {input_path} in HDFS...")
        
        try:
            list_response = hdfs_stub.List(hdfs_pb2.ListRequest(path=normalized_path))
            
            if list_response.entries:
                # Directory: construct full paths for each file
                input_files = []
                for entry in list_response.entries:
                    full_path = build_hdfs_path(normalized_path, entry)
                    input_files.append(full_path)
                info(f"Found {len(input_files)} input files")
                return input_files
            else:
                # Single file or empty directory
                warn(f"No directory entries found; treating {input_path} as a single file")
                return [normalized_path]
                
        except Exception as e:
            error(f"Failed to list HDFS files: {e}")
            raise
    
    def _distribute_files_to_map_tasks(
        self, 
        input_files: List[str], 
        num_map_tasks: int
    ) -> List[List[str]]:
        """
        Distribute input files across map tasks using round-robin.
        
        Args:
            input_files: List of input file paths
            num_map_tasks: Number of map tasks to create
            
        Returns:
            List of buckets, each containing files for one map task
        """
        num_map = max(1, num_map_tasks)
        buckets = [[] for _ in range(num_map)]
        
        for index, file_path in enumerate(input_files):
            bucket_index = index % num_map
            buckets[bucket_index].append(file_path)
        
        return buckets
    
    def _execute_map_phase(
        self,
        map_task_buckets: List[List[str]],
        user_code_path: str,
        num_reducers: int,
        job_tmpdir: str
    ) -> None:
        """
        Execute all map tasks by dispatching them to workers.
        
        Args:
            map_task_buckets: List of file buckets, one per map task
            user_code_path: Path to user's map/reduce code in HDFS
            num_reducers: Number of reduce tasks (for partitioning)
            job_tmpdir: Temporary directory for intermediate files
        """
        num_map_tasks = len(map_task_buckets)
        info(f"Scheduling {num_map_tasks} map tasks across {NUM_WORKERS} workers")
        
        for map_index in range(num_map_tasks):
            worker = WORKERS[map_index % NUM_WORKERS]
            file_bucket = map_task_buckets[map_index]
            
            payload = {
                "task_type": "map",
                "input_paths": file_bucket,
                "output_path": None,
                "user_code_path": user_code_path,
                "num_reducers": num_reducers,
                "job_tmpdir": job_tmpdir
            }
            
            info(f"Dispatching map task {map_index} to {worker} with {len(file_bucket)} inputs")
            self._dispatch_task_to_worker(worker, payload, f"Map task {map_index}")
        
        # Wait for map tasks to complete
        info(f"Waiting for map tasks to finish (simulated delay {MAP_TASK_WAIT_TIME}s)...")
        time.sleep(MAP_TASK_WAIT_TIME)
    
    def _execute_reduce_phase(
        self,
        reduce_inputs: List[List[str]],
        user_code_path: str,
        output_path: str,
        num_reducers: int,
        job_tmpdir: str
    ) -> None:
        """
        Execute all reduce tasks by dispatching them to workers.
        
        Args:
            reduce_inputs: List of input file lists, one per reduce task
            user_code_path: Path to user's map/reduce code in HDFS
            output_path: Base output path in HDFS
            num_reducers: Number of reduce tasks
            job_tmpdir: Temporary directory for intermediate files
        """
        normalized_output = normalize_hdfs_path(output_path)
        info(f"Scheduling {num_reducers} reduce tasks across {NUM_WORKERS} workers")
        
        for reduce_index in range(num_reducers):
            worker = WORKERS[reduce_index % NUM_WORKERS]
            output_file = build_hdfs_path(normalized_output, f"part-{reduce_index}.txt")
            
            payload = {
                "task_type": "reduce",
                "input_paths": reduce_inputs[reduce_index],
                "output_path": output_file,
                "user_code_path": user_code_path,
                "num_reducers": num_reducers,
                "job_tmpdir": job_tmpdir
            }
            
            info(f"Dispatching reduce task {reduce_index} to {worker}")
            self._dispatch_task_to_worker(worker, payload, f"Reduce task {reduce_index}")
    
    def _prepare_reduce_inputs(
        self, 
        job_tmpdir: str, 
        num_reducers: int
    ) -> List[List[str]]:
        """
        Prepare input file paths for each reduce task.
        
        Args:
            job_tmpdir: Temporary directory for intermediate files
            num_reducers: Number of reduce tasks
            
        Returns:
            List of input file lists, one per reduce task
        """
        reduce_inputs = [[] for _ in range(num_reducers)]
        
        for reducer_index in range(num_reducers):
            intermediate_file = f"intermediate/{job_tmpdir}/map-part-{reducer_index}.txt"
            reduce_inputs[reducer_index].append(intermediate_file)
        
        return reduce_inputs
    
    def _dispatch_task_to_worker(
        self, 
        worker: str, 
        payload: Dict, 
        task_name: str
    ) -> None:
        """
        Dispatch a task to a worker via HTTP POST.
        
        Args:
            worker: Worker hostname:port
            payload: Task payload dictionary
            task_name: Human-readable task name for logging
        """
        url = f"http://{worker}/run_task"
        
        try:
            response = requests.post(url, json=payload, timeout=TASK_TIMEOUT)
            
            if response.ok:
                success(f"{task_name} completed successfully on {worker}")
            else:
                error(f"{task_name} failed on {worker}: {response.text}")
                
        except Exception as e:
            error(f"{task_name} HTTP error on {worker}: {e}")
    
    def _mark_job_completed(self, job_id: str) -> None:
        """
        Mark a job as completed.
        
        Args:
            job_id: Unique job identifier
        """
        with self.lock:
            job = self.jobs[job_id]
            job["status"] = STATUS_COMPLETED
            job["msg"] = "Job completed successfully"
        
        print("", flush=True)  # Empty line for readability
        success(f"=== Job {job_id} COMPLETED ===")

# ============================================================================
# Global Job Manager Instance
# ============================================================================

job_manager = JobManager()

# ============================================================================
# gRPC Service Implementation
# ============================================================================

class MapReduceServicer(mapreduce_pb2_grpc.MapReduceServiceServicer):
    """gRPC service implementation for MapReduce operations."""
    
    def SubmitJob(self, request: mapreduce_pb2.JobRequest, context) -> mapreduce_pb2.JobResponse:
        """
        Submit a new MapReduce job.
        
        Args:
            request: Job request with input/output paths and task counts
            context: gRPC context
            
        Returns:
            Job response with job ID and status
        """
        info("Received SubmitJob RPC")
        print("Received SubmitJob RPC", flush=True)
        
        # Validate required fields
        if not self._validate_job_request(request):
            error("SubmitJob missing required fields")
            return mapreduce_pb2.JobResponse(
                job_id="",
                success=False,
                message="Missing fields"
            )
        
        # Submit job
        job_id = job_manager.submit(request)
        success(f"Job {job_id} accepted")
        
        return mapreduce_pb2.JobResponse(
            job_id=job_id,
            success=True,
            message="Job submitted"
        )
    
    def GetJobStatus(
        self, 
        request: mapreduce_pb2.JobStatusRequest, 
        context
    ) -> mapreduce_pb2.JobStatusResponse:
        """
        Get the status of a job.
        
        Args:
            request: Job status request with job ID
            context: gRPC context
            
        Returns:
            Job status response with current status
        """
        info(f"Received GetJobStatus RPC for {request.job_id}")
        
        job_data = job_manager.get_status(request.job_id)
        
        if not job_data:
            warn(f"Job {request.job_id} not found")
            return mapreduce_pb2.JobStatusResponse(
                job_id=request.job_id,
                status=STATUS_NOT_FOUND,
                message="No such job"
            )
        
        return mapreduce_pb2.JobStatusResponse(
            job_id=request.job_id,
            status=job_data["status"],
            message=job_data.get("msg", "")
        )
    
    def _validate_job_request(self, request: mapreduce_pb2.JobRequest) -> bool:
        """
        Validate that a job request has all required fields.
        
        Args:
            request: Job request to validate
            
        Returns:
            True if valid, False otherwise
        """
        return bool(
            request.input_path and 
            request.output_path and 
            request.user_code_path
        )

# ============================================================================
# Server Startup
# ============================================================================

def serve() -> None:
    """Start the gRPC server and listen for requests."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mapreduce_pb2_grpc.add_MapReduceServiceServicer_to_server(
        MapReduceServicer(), 
        server
    )
    server.add_insecure_port("[::]:50052")
    server.start()
    
    success("Boss gRPC listening on :50052")
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        warn("Shutting down boss")

if __name__ == "__main__":
    serve()
