import grpc
import map_reduce_pb2
import map_reduce_pb2_grpc
import time
import dataset_generator
from hdfs import InsecureClient


def upload_to_hdfs(local_path, hdfs_path):
    """Uploads a local file to HDFS."""
    try:
        # Assuming HDFS namenode WebHDFS is accessible on localhost:9870
        client = InsecureClient('http://localhost:9870', user='root')
        print(f"[CLIENT] Uploading {local_path} to HDFS at {hdfs_path}...")
        client.upload(hdfs_path, local_path, overwrite=True)
        print("[CLIENT] Upload complete.")
    except Exception as e:
        print(f"[CLIENT] Error uploading to HDFS: {e}")
        raise


def submit_job():
    dataset_generator.generate_default_dataset()
    upload_to_hdfs('sample.parquet', '/data/sample.parquet')

    channel = grpc.insecure_channel("localhost:5440")
    stub = map_reduce_pb2_grpc.SchedulerStub(channel)

    request = map_reduce_pb2.ScheduleJobRequest(
        dataset_path="hdfs://nn:9000/data/sample.parquet",
        output_path="hdfs://nn:9000/data/output",
        num_map_tasks=4,
        num_reduce_tasks=4,
        map_function_path="/app/user_funcs/map_func.py",
        reduce_function_path="/app/user_funcs/reduce_func.py",
        repartition_threshold=0.1,
        custom_hash_func=""
    )

    print("[CLIENT] Submitting job...")
    response = stub.SubmitJob(request)
    print(f"[CLIENT] Job ID: {response.job_id} (status={response.status})")
    
    print("Printint status of job now.")
    while True:
        time.sleep(3)
        status_req = map_reduce_pb2.GetJobStatusRequest(job_id=response.job_id)
        status_resp = stub.GetJobStatus(status_req)
        print(f"[CLIENT] Job {status_resp.job_id} status: {status_resp.status}")
        if status_resp.status in ("COMPLETED", "FAILED", "ERROR", "NOT_FOUND"):
            break


if __name__ == "__main__":
    submit_job()
