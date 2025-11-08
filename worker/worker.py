"""
Simple worker HTTP server that accepts POST /run_task
Body JSON:
{
  "task_type": "map" or "reduce",
  "input_paths": ["hdfs:/path/to/file1", ...],  # HDFS paths to read
  "output_path": "hdfs:/path/to/outfile",
  "user_code_path": "hdfs:/jobs/job1/user_code.py",
  "num_reducers": int    # for map tasks, used to partition intermediate outputs
}
"""
import os
import json
from flask import Flask, request, jsonify
import subprocess
import sys
import tempfile
import requests
import grpc
import hdfs_pb2
import hdfs_pb2_grpc
import importlib.util
from pathlib import Path

HDFS_ADDR = os.environ.get("HDFS_ADDR", "hdfs:50052")

app = Flask(__name__)

def hdfs_download(path):
    channel = grpc.insecure_channel(HDFS_ADDR)
    stub = hdfs_pb2_grpc.HdfsServiceStub(channel)
    resp = stub.Download(hdfs_pb2.DownloadRequest(path=path.lstrip("/")))
    if not resp.success:
        raise RuntimeError("HDFS download failed: " + resp.message)
    return resp.data

def hdfs_upload(path, data: bytes):
    channel = grpc.insecure_channel(HDFS_ADDR)
    stub = hdfs_pb2_grpc.HdfsServiceStub(channel)
    resp = stub.Upload(hdfs_pb2.UploadRequest(path=path.lstrip("/"), data=data))
    if not resp.success:
        raise RuntimeError("HDFS upload failed: " + resp.message)
    return resp

def load_user_module_from_bytes(bytes_data, tmp_dir):
    fn = os.path.join(tmp_dir, "user_code.py")
    with open(fn, "wb") as f:
        f.write(bytes_data)
    spec = importlib.util.spec_from_file_location("user_code", fn)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

def run_map(mapper, input_files, num_reducers, job_tmpdir):
    # Each map task reads all input files and writes partitioned intermediate files part-<r>.txt
    partitions = {i: [] for i in range(num_reducers)}
    for hpath in input_files:
        data = hdfs_download(hpath.lstrip("/"))
        text = data.decode("utf-8")
        for line in text.splitlines():
            pairs = mapper(line)
            for key, val in pairs:
                idx = (hash(key) & 0xffffffff) % num_reducers
                partitions[idx].append(f"{key}\t{val}\n")
    # write partitions to hdfs intermediate files
    for idx, lines in partitions.items():
        out_path = f"intermediate/{job_tmpdir}/map-part-{idx}.txt"
        hdfs_upload(out_path, "".join(lines).encode("utf-8"))

def run_reduce(reducer, input_files, output_hdfs_path):
    # aggregate by key
    agg = {}
    for hpath in input_files:
        data = hdfs_download(hpath.lstrip("/"))
        text = data.decode("utf-8")
        for line in text.splitlines():
            if not line.strip(): continue
            key, val = line.split("\t", 1)
            agg.setdefault(key, []).append(int(val))
    # reduce
    lines = []
    for k, vals in sorted(agg.items()):
        k2, outv = reducer(k, vals)
        lines.append(f"{k2}\t{outv}\n")
    hdfs_upload(output_hdfs_path.lstrip("/"), "".join(lines).encode("utf-8"))

@app.route("/run_task", methods=["POST"])
def run_task():
    body = request.get_json()
    try:
        task_type = body["task_type"]
        input_paths = body.get("input_paths", [])
        output_path = body.get("output_path")
        user_code_path = body.get("user_code_path")
        num_reducers = int(body.get("num_reducers", 1))
        job_tmpdir = body.get("job_tmpdir", "jobtmp")
        # load user code from HDFS
        code_bytes = hdfs_download(user_code_path.lstrip("/"))
        tmpdir = tempfile.mkdtemp()
        module = load_user_module_from_bytes(code_bytes, tmpdir)
        mapper = getattr(module, "map_func", None)
        reducer = getattr(module, "reduce_func", None)
        if task_type == "map":
            if mapper is None:
                return jsonify({"ok": False, "msg": "mapper not found"}), 400
            run_map(mapper, input_paths, num_reducers, job_tmpdir)
            return jsonify({"ok": True, "msg": "map done"})
        elif task_type == "reduce":
            if reducer is None:
                return jsonify({"ok": False, "msg": "reducer not found"}), 400
            run_reduce(reducer, input_paths, output_path)
            return jsonify({"ok": True, "msg": "reduce done"})
        else:
            return jsonify({"ok": False, "msg": "unknown task type"}), 400
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
