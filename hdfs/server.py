import os
from concurrent import futures
import grpc
import argparse

# Import generated grpc modules (run protoc to generate)
import hdfs_pb2
import hdfs_pb2_grpc

ROOT = os.environ.get("HDFS_ROOT", "/data")  # mount this volume from host, or use env var for local testing

class HdfsServicer(hdfs_pb2_grpc.HdfsServiceServicer):
    def Upload(self, request, context):
        print("Upload request received by hdfs server.", flush=True)
        path = os.path.join(ROOT, request.path.lstrip("/"))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        try:
            with open(path, "wb") as f:
                f.write(request.data)
            return hdfs_pb2.UploadResponse(success=True, message="Uploaded")
        except Exception as e:
            return hdfs_pb2.UploadResponse(success=False, message=str(e))

    def Download(self, request, context):
        print("Downloadrequest received by hdfs server.", flush=True)
        path = os.path.join(ROOT, request.path.lstrip("/"))
        if not os.path.exists(path):
            return hdfs_pb2.DownloadResponse(success=False, data=b"", message="Not found")
        try:
            with open(path, "rb") as f:
                data = f.read()
            return hdfs_pb2.DownloadResponse(success=True, data=data, message="OK")
        except Exception as e:
            return hdfs_pb2.DownloadResponse(success=False, data=b"", message=str(e))

    def List(self, request, context):
        path = os.path.join(ROOT, request.path.lstrip("/"))
        if not os.path.exists(path):
            return hdfs_pb2.ListResponse(entries=[], message="Not found")
        try:
            entries = os.listdir(path)
            return hdfs_pb2.ListResponse(entries=entries, message="OK")
        except Exception as e:
            return hdfs_pb2.ListResponse(entries=[], message=str(e))


def serve(host="0.0.0.0", port=50051):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    hdfs_pb2_grpc.add_HdfsServiceServicer_to_server(HdfsServicer(), server)
    server.add_insecure_port(f"{host}:{port}")
    server.start()
    print(f"HDFS gRPC server listening on {host}:{port}, ROOT={ROOT}")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("Shutting down HDFS")


if __name__ == "__main__":
    os.makedirs("/hdfs-root", exist_ok=True)
    serve()
