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
 
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    job_manager = JobManager()

    mapreduce_pb2_grpc.add_MapReduceServiceServicer_to_server(
        MapReduceServiceServicer(job_manager), server
    )

    print("Starting MapReduce Master gRPC server on port 50051...")
    server.add_insecure_port("[::]:50051")
    server.start()

    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        print("Shutting down server...")
        server.stop(0)


if __name__ == "__main__":
    serve()
