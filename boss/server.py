"""
Boss gRPC server that implements mapreduce.SubmitJob and GetJobStatus.
It schedules map tasks to worker HTTP endpoints and reduce tasks after map complete.

Constraints:
- Workers are provided by docker-compose (worker1..worker4) and expose /run_task on port 8000.
- HDFS is at hdfs:50052.
"""
import uuid
import threading
import time
import grpc
from concurrent import futures
import mapreduce_pb2
import mapreduce_pb2_grpc
import requests
import json
import os
import hdfs_pb2
import hdfs_pb2_grpc

WORKERS = os.environ.get("WORKER_HOSTS", "worker1:8000,worker2:8000,worker3:8000,worker4:8000").split(",")
HDFS_ADDR = os.environ.get("HDFS_ADDR", "hdfs:50052")
NUM_WORKERS = len(WORKERS)

class JobManager:
    def __init__(self):
        self.jobs = {}
        self.lock = threading.Lock()

    def submit(self, req):
        job_id = str(uuid.uuid4())
        record = {
            "request": req,
            "status": "QUEUED",
            "map_tasks": [],
            "reduce_tasks": [],
            "msg": ""
        }
        with self.lock:
            self.jobs[job_id] = record
        threading.Thread(target=self._run_job, args=(job_id,), daemon=True).start()
        return job_id

    def get_status(self, job_id):
        with self.lock:
            return self.jobs.get(job_id)

    def _run_job(self, job_id):
        with self.lock:
            job = self.jobs[job_id]
            job["status"] = "RUNNING"
        req = job["request"]
        # Step 1: ensure user code and input exists in HDFS - assume the client uploaded them already
        # Step 2: create job tmp dir name
        job_tmp = f"{job_id}"
        # Step 3: schedule map tasks across workers: we'll create N map tasks and distribute input files across them by simple round-robin
        # List input files from HDFS
        channel = grpc.insecure_channel(HDFS_ADDR)
        stub = hdfs_pb2_grpc.HdfsServiceStub(channel)
        list_resp = stub.List(hdfs_pb2.ListRequest(path=req.input_path.lstrip("/")))
        input_files = []
        if list_resp.entries:
            for e in list_resp.entries:
                input_files.append(os.path.join(req.input_path, e))
        else:
            # maybe input_path is a file
            input_files = [req.input_path]

        # split input files roughly into num_map_tasks buckets
        num_map = max(1, req.num_map_tasks)
        buckets = [[] for _ in range(num_map)]
        for idx, f in enumerate(input_files):
            buckets[idx % num_map].append(f)

        # send map tasks
        # we set intermediate destination as "intermediate/<job_tmp>/map-part-<r>.txt"
        for map_idx in range(num_map):
            worker = WORKERS[map_idx % NUM_WORKERS]
            payload = {
                "task_type": "map",
                "input_paths": buckets[map_idx],
                "output_path": None,
                "user_code_path": req.user_code_path,
                "num_reducers": req.num_reduce_tasks,
                "job_tmpdir": job_tmp
            }
            try:
                r = requests.post(f"http://{worker}/run_task", json=payload, timeout=120)
                if r.ok:
                    pass
                else:
                    print("Map task failed on worker", worker, r.text)
            except Exception as e:
                print("Map task HTTP error:", e)

        # wait a little for maps to finish (in production would poll)
        time.sleep(3)

        # Step 4: shuffle: for each reducer r, gather all intermediate map-part-r files
        num_reduce = max(1, req.num_reduce_tasks)
        reduce_inputs = [[] for _ in range(num_reduce)]
        for r in range(num_reduce):
            # intermediate files are at intermediate/<job_tmp>/map-part-<r>.txt
            # HDFS path:
            path = f"intermediate/{job_tmp}/map-part-{r}.txt"
            # We will give each reducer that single path (workers will read it)
            reduce_inputs[r].append(path)

        # Step 5: dispatch reduce tasks
        for ridx in range(num_reduce):
            worker = WORKERS[ridx % NUM_WORKERS]
            out_path = os.path.join(req.output_path, f"part-{ridx}.txt")
            payload = {
                "task_type": "reduce",
                "input_paths": reduce_inputs[ridx],
                "output_path": out_path,
                "user_code_path": req.user_code_path,
                "num_reducers": num_reduce,
                "job_tmpdir": job_tmp
            }
            try:
                r = requests.post(f"http://{worker}/run_task", json=payload, timeout=120)
                if r.ok:
                    pass
                else:
                    print("Reduce task failed on worker", worker, r.text)
            except Exception as e:
                print("Reduce task HTTP error:", e)

        # mark job complete
        with self.lock:
            job["status"] = "COMPLETED"
            job["msg"] = "Job completed (best-effort)"
        print("Job", job_id, "completed (boss)")

job_manager = JobManager()

class MapReduceServicer(mapreduce_pb2_grpc.MapReduceServiceServicer):
    def SubmitJob(self, request, context):
        # minimal validation
        if not request.input_path or not request.output_path or not request.user_code_path:
            return mapreduce_pb2.JobResponse(job_id="", success=False, message="Missing fields")
        job_id = job_manager.submit(request)
        return mapreduce_pb2.JobResponse(job_id=job_id, success=True, message="Job submitted")

    def GetJobStatus(self, request, context):
        info = job_manager.get_status(request.job_id)
        if not info:
            return mapreduce_pb2.JobStatusResponse(job_id=request.job_id, status="NOT_FOUND", message="No such job")
        return mapreduce_pb2.JobStatusResponse(job_id=request.job_id, status=info["status"], message=info.get("msg",""))

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mapreduce_pb2_grpc.add_MapReduceServiceServicer_to_server(MapReduceServicer(), server)
    server.add_insecure_port("[::]:50052")
    server.start()
    print("Boss gRPC listening on :50052")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("Shutting down boss")

if __name__ == "__main__":
    serve()
