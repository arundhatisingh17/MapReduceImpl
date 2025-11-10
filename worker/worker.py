"""
Simple worker HTTP server that accepts POST /run_task
Body JSON:
{
  "task_type": "map" or "reduce",
  "input_paths": ["hdfs:/path/to/file1", ...],  # HDFS paths to read
  "output_path": "hdfs:/path/to/outfile",  # None for map tasks
  "user_code_path": "hdfs:/jobs/job1/user_code.py",  # Single file containing both mapper and reducer
  "num_reducers": int,  # for map tasks, used to partition intermediate outputs
  "job_tmpdir": "jobs/job-id"  # temporary directory for intermediate files
}

User code file (user_code.py) should contain:
- mapper function (or map_func): takes a line string, returns/yields (key, value) tuples
- reducer function (or reduce_func): takes (key, values_list), returns/yields (key, value) tuple

Example:
  def mapper(line):
      for word in line.strip().split():
          yield (word, 1)
  
  def reducer(key, values):
      yield (key, sum(values))
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

HDFS_ADDR = os.environ.get("HDFS_ADDR", "hdfs:50051")

app = Flask(__name__)

def hdfs_download(path):
    """
    Download a file from HDFS.
    Path should be relative to HDFS root (no leading slash, no hdfs: prefix).
    The HDFS server will join it with ROOT (/data).
    """
    channel = grpc.insecure_channel(HDFS_ADDR)
    stub = hdfs_pb2_grpc.HdfsServiceStub(channel)
    # HDFS server does: os.path.join(ROOT, request.path.lstrip("/"))
    # So we pass the path as-is (already normalized by caller)
    # The server will handle any leading slash stripping
    resp = stub.Download(hdfs_pb2.DownloadRequest(path=path))
    if not resp.success:
        raise RuntimeError("HDFS download failed: " + resp.message)
    return resp.data

def hdfs_upload(path, data: bytes):
    """
    Upload a file to HDFS.
    Path should be relative to HDFS root (no leading slash, no hdfs: prefix).
    The HDFS server will join it with ROOT (/data).
    """
    channel = grpc.insecure_channel(HDFS_ADDR)
    stub = hdfs_pb2_grpc.HdfsServiceStub(channel)
    # HDFS server does: os.path.join(ROOT, request.path.lstrip("/"))
    # So we pass the path as-is (already normalized by caller)
    # The server will handle any leading slash stripping
    resp = stub.Upload(hdfs_pb2.UploadRequest(path=path, data=data))
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
        # Normalize path: remove hdfs: prefix and leading slash
        print(f"[DEBUG] Downloading from HDFS: '{hpath}'", flush=True)
        data = hdfs_download(hpath)
        text = data.decode("utf-8")
        for line in text.splitlines():
            pairs = mapper(line)
            # Handle both list and generator returns
            if not isinstance(pairs, list):
                pairs = list(pairs)
            for key, val in pairs:
                idx = (hash(key) & 0xffffffff) % num_reducers
                partitions[idx].append(f"{key}\t{val}\n")
    # write partitions to hdfs intermediate files
    for idx, lines in partitions.items():
        out_path = f"intermediate/{job_tmpdir}/map-part-{idx}.txt"
        # Path is already normalized (no leading slash)
        print(f"[DEBUG] Uploading intermediate file to HDFS: '{out_path}'", flush=True)
        hdfs_upload(out_path, "".join(lines).encode("utf-8"))

def run_reduce(reducer, input_files, output_hdfs_path):
    # aggregate by key
    agg = {}
    for hpath in input_files:
        # Normalize path: remove hdfs: prefix and leading slash
        normalized_path = hpath.replace("hdfs:", "").lstrip("/")
        print(f"[DEBUG] Downloading from HDFS: '{hpath}' -> normalized: '{normalized_path}'", flush=True)
        data = hdfs_download(normalized_path)
        text = data.decode("utf-8")
        for line in text.splitlines():
            if not line.strip(): continue
            key, val = line.split("\t", 1)
            # Try to convert to int for compatibility with reducers expecting numbers
            # but keep as string if conversion fails (for flexibility)
            try:
                val_int = int(val)
                agg.setdefault(key, []).append(val_int)
            except ValueError:
                # Keep as string if not numeric
                agg.setdefault(key, []).append(val)
    # reduce
    lines = []
    for k, vals in sorted(agg.items()):
        result = reducer(k, vals)
        # Handle both tuple return and generator yield
        if isinstance(result, tuple) and len(result) == 2:
            # Direct tuple return: (key, value)
            k2, outv = result
            lines.append(f"{k2}\t{outv}\n")
        elif hasattr(result, '__iter__') and not isinstance(result, (str, bytes)):
            # Generator or iterator - iterate over all yielded results
            for item in result:
                if isinstance(item, tuple) and len(item) == 2:
                    k2, outv = item
                    lines.append(f"{k2}\t{outv}\n")
        else:
            # Single value or unexpected format
            print(f"[WARN] Reducer returned unexpected format for key {k}: {result}", flush=True)
            continue
    # Normalize output path: remove hdfs: prefix and leading slash
    normalized_output = output_hdfs_path.replace("hdfs:", "").lstrip("/")
    print(f"[DEBUG] Uploading to HDFS: '{output_hdfs_path}' -> normalized: '{normalized_output}'", flush=True)
    hdfs_upload(normalized_output, "".join(lines).encode("utf-8"))

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
        
        # Validate required fields
        if not user_code_path:
            return jsonify({"ok": False, "msg": "user_code_path is required"}), 400
        if not input_paths:
            return jsonify({"ok": False, "msg": "input_paths is required"}), 400
        
        # load user code from HDFS
        # Normalize path: remove hdfs: prefix and leading slash
        print(f"[DEBUG] Downloading user code from HDFS: '{user_code_path}' ", flush=True)
        code_bytes = hdfs_download(user_code_path)
        tmpdir = tempfile.mkdtemp()
        module = load_user_module_from_bytes(code_bytes, tmpdir)
        
        # Try both naming conventions: map_func/reduce_func and mapper/reducer
        mapper = getattr(module, "map_func", None) or getattr(module, "mapper", None)
        reducer = getattr(module, "reduce_func", None) or getattr(module, "reducer", None)
        
        if task_type == "map":
            if mapper is None:
                return jsonify({"ok": False, "msg": "mapper function (map_func or mapper) not found"}), 400
            if output_path is not None:
                # Boss sends output_path=None for map tasks, but handle if it's set
                pass
            run_map(mapper, input_paths, num_reducers, job_tmpdir)
            return jsonify({"ok": True, "msg": "map done"})
        elif task_type == "reduce":
            if reducer is None:
                return jsonify({"ok": False, "msg": "reducer function (reduce_func or reducer) not found"}), 400
            if not output_path:
                return jsonify({"ok": False, "msg": "output_path is required for reduce tasks"}), 400
            run_reduce(reducer, input_paths, output_path)
            return jsonify({"ok": True, "msg": "reduce done"})
        else:
            return jsonify({"ok": False, "msg": "unknown task type"}), 400
    except Exception as e:
        import traceback
        error_msg = f"{str(e)}\n{traceback.format_exc()}"
        return jsonify({"ok": False, "msg": error_msg}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
