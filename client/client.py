"""
Client CLI:
- upload <localpath> <hdfspath>
- submit --input hdfs:/input/job1 --output hdfs:/output/job1 --usercode local_user_code.py --nmap 4 --nreduce 2
- status <jobid>
"""
import argparse
import os
import grpc
import mapreduce_pb2
import mapreduce_pb2_grpc
import hdfs_pb2
import hdfs_pb2_grpc

HDFS_ADDR = "localhost:50051"
BOSS_ADDR = "localhost:50052"
HDFS_ROOT = "/data" 

def upload(local_path, hdfs_path):
    channel = grpc.insecure_channel(HDFS_ADDR)
    stub = hdfs_pb2_grpc.HdfsServiceStub(channel)
    if os.path.isdir(local_path):
        # upload files in directory
        for name in os.listdir(local_path):
            lp = os.path.join(local_path, name)
            with open(lp, "rb") as f:
                data = f.read()
            resp = stub.Upload(hdfs_pb2.UploadRequest(path=os.path.join(hdfs_path, name), data=data))
            print("Uploaded", name, "->", resp.success, resp.message)
    else:
        with open(local_path, "rb") as f:
            data = f.read()
        resp = stub.Upload(hdfs_pb2.UploadRequest(path=hdfs_path, data=data))
        print("Uploaded", local_path, "->", resp.success, resp.message)

def submit(input_path, output_path, user_code_localpath, nmap, nreduce):
    """
    Upload user code to HDFS and submit a MapReduce job to the boss service.
    """

    # 1. Create a job name based on the user code filename
    jobname = "job-" + os.path.splitext(os.path.basename(user_code_localpath))[0]

    # --- Upload user code to HDFS ---
    try:
        channel = grpc.insecure_channel(HDFS_ADDR)
        stub = hdfs_pb2_grpc.HdfsServiceStub(channel)

        with open(user_code_localpath, "rb") as f:
            code = f.read()

        user_dest = f"jobs/{jobname}/user_code.py"
        upload_req = hdfs_pb2.UploadRequest(path=user_dest, data=code)
        upload_resp = stub.Upload(upload_req)
        print(f"Uploaded user code to HDFS at {user_dest}")
    except grpc.RpcError as e:
        print(f"❌ Failed to upload to HDFS: {e.details()}")
        return None
    except FileNotFoundError:
        print(f"❌ Local file not found: {user_code_localpath}")
        return None

    # --- Submit the job to the boss service ---
    try:
        channel2 = grpc.insecure_channel(BOSS_ADDR)
        stub2 = mapreduce_pb2_grpc.MapReduceServiceStub(channel2)

        req = mapreduce_pb2.JobRequest(
            input_path=input_path,
            output_path=output_path,
            user_code_path=user_dest,
            num_map_tasks=nmap,
            num_reduce_tasks=nreduce
        )

        resp = stub2.SubmitJob(req)
        print(f"✅ Job submitted: ID={resp.job_id}, Success={resp.success}, Message='{resp.message}'")
        return resp.job_id

    except grpc.RpcError as e:
        print(f"❌ Failed to submit job: {e.details()}")
        return None

def status(jobid):
    channel2 = grpc.insecure_channel(BOSS_ADDR)
    stub2 = mapreduce_pb2_grpc.MapReduceServiceStub(channel2)
    resp = stub2.GetJobStatus(mapreduce_pb2.JobStatusRequest(job_id=jobid))
    print("Status:", resp.job_id, resp.status, resp.message)


def main():
    parser = argparse.ArgumentParser(description="MapReduce Client")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Upload
    upload_p = subparsers.add_parser("upload", help="Upload data or code to HDFS")
    upload_p.add_argument("--local-path", required=True, help="Local file or directory")
    upload_p.add_argument("--hdfs-path", required=True, help="Destination path in HDFS")

    # Submit
    submit_p = subparsers.add_parser("submit", help="Submit MapReduce job to Boss")
    submit_p.add_argument("--input-path", required=True)
    submit_p.add_argument("--output-path", required=True)
    submit_p.add_argument("--code-path", required=True)
    submit_p.add_argument("--num-map-tasks", type=int, required=True)
    submit_p.add_argument("--num-reduce-tasks", type=int, required=True)

    # Status
    status_p = subparsers.add_parser("status", help="Check job status")
    status_p.add_argument("--job-id", required=True)

    args = parser.parse_args()

    if args.command == "upload":
        upload(args.local_path, args.hdfs_path)
    elif args.command == "submit":
        submit(args.input_path, args.output_path, args.code_path, args.num_map_tasks, args.num_reduce_tasks)
    elif args.command == "status":
        status(args.job_id)

if __name__ == "__main__":
    main()
