import grpc
import map_reduce_pb2
import map_reduce_pb2_grpc
import time
import dataset_generator

def submit_job():
    dataset_generator.generate_default_dataset()
    channel = grpc.insecure_channel("scheduler:5440")
    stub = map_reduce_pb2_grpc.SchedulerStub(channel)

    request = map_reduce_pb2.ScheduleJobRequest(
        dataset_path="hdfs://nn:9000/data/sample.parquet",
        num_partitions=4,
        map_function_path="/app/user_funcs/map_func.py",
        reduce_function_path="/app/user_funcs/reduce_func.py",
        repartition_threshold=0.1,
        custom_hash_func=""
    )

    print("[CLIENT] Submitting job...")
    response = stub.SubmitJob(request)
    print(f"[CLIENT] Job ID: {response.job_id} (status={response.status})")
    
    print("Printing status of job now.")
    while True:
        time.sleep(3)
        status_req = map_reduce_pb2.GetJobStatusRequest(job_id=response.job_id)
        status_resp = stub.GetJobStatus(status_req)
        print(f"[CLIENT] Job {status_resp.job_id} status: {status_resp.status}")
        if status_resp.status in ("COMPLETED", "FAILED", "ERROR", "NOT_FOUND"):
            break


if __name__ == "__main__":
    submit_job()
