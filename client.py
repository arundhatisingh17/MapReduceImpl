import grpc
import map_reduce_pb2
import map_reduce_pb2_grpc
import time

def submit_job():
    """Submit a MapReduce job to the master"""
    # NOTE: Data must be pre-generated inside a container using create_test_data.py
    # See README.md "Uploading Example Data" section for instructions
    
    # Connect to master service
    channel = grpc.insecure_channel("localhost:50051")
    stub = map_reduce_pb2_grpc.MasterStub(channel)

    # Create job submission request
    request = map_reduce_pb2.SubmitJobRequest(
        dataset_path="hdfs://namenode:9000/data/test_10mb.parquet",
        num_map_tasks=8,
        num_reduce_tasks=4,
        map_function_path="/user_funcs/map_func.py",  # Path inside worker container
        reduce_function_path="/user_funcs/reduce_func.py"  # Path inside worker container
    )

    print("[CLIENT] Submitting job to master...")
    response = stub.SubmitJob(request)
    print(f"[CLIENT] Job submitted successfully!")
    print(f"[CLIENT]   Job ID: {response.job_id}")
    print(f"[CLIENT]   Status: {response.status}")
    
    # Poll for job status
    print("\n[CLIENT] Monitoring job status...")
    while True:
        time.sleep(3)
        status_req = map_reduce_pb2.JobStatusRequest(job_id=response.job_id)
        status_resp = stub.GetJobStatus(status_req)
        print(f"[CLIENT] Job {status_resp.job_id} status: {status_resp.status}")
        
        if status_resp.status == "COMPLETED":
            print(f"[CLIENT] Job completed successfully!")
            print(f"[CLIENT] Output path: {status_resp.output_path}")
            break
        elif status_resp.status == "FAILED":
            print(f"[CLIENT] Job failed: {status_resp.error}")
            break
        elif status_resp.status == "NOT_FOUND":
            print(f"[CLIENT] Job not found!")
            break


if __name__ == "__main__":
    submit_job()
