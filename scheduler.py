import uuid
import json, os

JOBS_PATH = "/tmp/jobs.json"

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
    JOBS[job_id] = {"status": "SCHEDULED", "details": job_details}
    print(f"Scheduled job {job_id} with details: {job_details}")
    # In a real implementation, you would add logic to communicate with a boss node.
    save_jobs()

    return job_id


def fetch_job(job_id):
    """
    Fetches job from JOBS dictionary
    """
    load_jobs()
    return JOBS.get(job_id)

