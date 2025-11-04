from concurrent import futures
import traceback
import map_reduce_pb2_grpc
import map_reduce_pb2
import grpc
import numpy as np
import scheduler

class MapReduce(map_reduce_pb2_grpc.SchedulerServicer):
    def SubmitJob(self, request, context):
        try:
            print(f"Received schedule job request: {request}")
            job_id = scheduler.schedule_job(request)
            return map_reduce_pb2.JobStatus(job_id=job_id, status="SCHEDULED")
        except Exception:
            err = traceback.format_exc()
            print(f"Error in scheduleJob: {err}")
            return map_reduce_pb2.JobStatus(job_id="", status="ERROR", err=err)

    def getJobStatus(self, request, context):
        try:
            job_id = request.job_id
            print(f"Received status request for job ID: {job_id}")
            
            job = scheduler.fetch_job(job_id)
            if not job:
                return map_reduce_pb2.JobStatus(job_id=job_id, status="NOT_FOUND")

            if job['status'] == "COMPLETED":
                return map_reduce_pb2.JobStatus(job_id=job_id, status="COMPLETED", output_path=job['output_path'])
            
            return map_reduce_pb2.JobStatus(job_id=job_id, status=job['status'])
        except Exception:
            err = traceback.format_exc()
            print(f"Error in getJobStatus: {err}")
            return map_reduce_pb2.JobStatus(job_id=getattr(request, 'job_id', ''), status="ERROR", err=err)
 
server = grpc.server(futures.ThreadPoolExecutor(max_workers=10), options=[("grpc.so_reuseport", 0)])
map_reduce_pb2_grpc.add_SchedulerServicer_to_server(MapReduce(), server)
server.add_insecure_port('0.0.0.0:5440')
server.start()
print("started")

server.wait_for_termination()
