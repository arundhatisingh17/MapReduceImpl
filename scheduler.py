import uuid
import json, os
import grpc
import boss_pb2
import boss_pb2_grpc
import threading

JOBS_PATH = "/tmp/jobs.json"
JOBS = {}

def save_jobs():
    with open(JOBS_PATH, "w") as f:
        json.dump(JOBS, f)

def load_jobs():
    global JOBS
    if os.path.exists(JOBS_PATH):
        with open(JOBS_PATH, "r") as f:
            JOBS = json.load(f)

# JOBS Format
# JOBS = {
 #   "job-001": {"status": "IN_PROGRESS"},
  #  "job-002": {"status": "COMPLETED", "output_path": "/tmp/outputs/job-002.txt"},
  #  "job-003": {"status": "FAILED"}
#}

def schedule_job(job_details):
    """
    This function would be responsible for receiving a new job,
    assigning it a job_id, adding it to the JOBS dictionary,
    and sending it to the boss node for scheduling on map nodes.
    """
    job_id = f"job-{uuid.uuid4()}"
    JOBS[job_id] = {"status": "SCHEDULED", "details": job_details, "output_path": ""}
    print(f"[SCHEDULER] Scheduled job {job_id}")
    save_jobs()
    
    # Send job to boss node for execution
    threading.Thread(target=_assign_job_to_boss, args=(job_id, job_details), daemon=True).start()

    return job_id


def _assign_job_to_boss(job_id, job_details):
    """
    Send job assignment to Boss node
    """
    try:
        # Wait a bit for boss to be ready
        import time
        time.sleep(2)
        
        channel = grpc.insecure_channel("boss:50052")
        stub = boss_pb2_grpc.BossStub(channel)
        
        # Create job assignment
        assignment = boss_pb2.JobAssignment(
            job_id=job_id,
            input_path=job_details.dataset_path,
            output_path=f"hdfs://nn:9000/output/{job_id}",
            num_map_tasks=job_details.num_partitions,  # Use num_partitions as num_map_tasks
            num_reduce_tasks=job_details.num_partitions,  # Same for reduce tasks
            job_type=boss_pb2.MAP  # Will handle both MAP and REDUCE
        )
        
        print(f"[SCHEDULER] Assigning job {job_id} to boss")
        response = stub.AssignJob(assignment, timeout=10)
        
        if response.acknowledged:
            print(f"[SCHEDULER] Boss acknowledged job {job_id}")
            update_job_status(job_id, "MAP_IN_PROGRESS", "")
        else:
            print(f"[SCHEDULER] Boss rejected job {job_id}: {response.message}")
            update_job_status(job_id, "FAILED", "")
            
    except Exception as e:
        print(f"[SCHEDULER] Failed to assign job {job_id} to boss: {e}")
        update_job_status(job_id, "FAILED", "")


def fetch_job(job_id):
    """
    Fetches job from JOBS dictionary
    """
    load_jobs()
    return JOBS.get(job_id)


def update_job_status(job_id, status, output_path):
    """
    Update job status in JOBS dictionary
    """
    load_jobs()
    if job_id in JOBS:
        JOBS[job_id]["status"] = status
        if output_path:
            JOBS[job_id]["output_path"] = output_path
        save_jobs()
        print(f"[SCHEDULER] Job {job_id} status updated to: {status}")

