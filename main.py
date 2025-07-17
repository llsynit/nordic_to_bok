
import os
import io
import sys
import traceback
from pathlib import Path
import uuid
import tempfile
import shutil
import subprocess
import time
import json
import threading
import datetime
import logging
import zipfile
import xml.etree.ElementTree as ET
from typing import Optional, Union
from datetime import datetime
from contextlib import asynccontextmanager

import httpx

from bs4 import BeautifulSoup

from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from fastapi.responses import JSONResponse
from fastapi.responses import FileResponse
from fastapi import BackgroundTasks

from dotenv import load_dotenv

from daisy_pipeline_light import RemoteDaisyPipelineJob  # your simplified class
from utils import remove_file, generate_reference_number
from incoming_nordic import create_epub_no_img  # your EPUB validation function
from nordic_to_nlbpub import get_nordic_guidelines_version, nordic_to_nlbpub_with_migrator

from jobHandler import JobStepHandler

load_dotenv()

# Registry and queue
db_lock = threading.Lock()
job_queue = []
job_done_events = {}
job_running = False
current_running_job_id = None
current_running_job_name = None
current_running_job_source = None


job_queue = []
db_lock = threading.Lock()
over_all_job_registry = {}

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def run_job_queue():
    while True:
        if not job_queue:
            logger.info("Job queue is empty. Waiting for tasks...")
            time.sleep(2)
            continue

        job = job_queue.pop(0)
        reference = job["reference_number"]
        handler = JobStepHandler(job)

        try:
            with db_lock:
                over_all_job_registry[reference]["status"] = "RUNNING"
                over_all_job_registry[reference]["start_time"] = now_utc()

            steps = [
                ("create-epub-no-img", handler.run_step_create_epub_no_img),
                ("incoming-nordic", lambda: handler.run_step_daisy(
                    "nordic-epub3-validate", "incoming-nordic")),
                ("nordic-epub3-to-html", lambda: handler.run_step_daisy(
                    "nordic-epub3-to-html", "nordic-epub3-to-html"))
            ]

            for step_name, step_fn in steps:
                started = now_utc()
                with db_lock:
                    over_all_job_registry[reference]["steps"][step_name]["status"] = "RUNNING"
                    over_all_job_registry[reference]["steps"][step_name]["start_time"] = started

                try:
                    success = step_fn()
                except Exception as e:
                    logger.exception(
                        f"Step '{step_name}' failed with exception")
                    success = False

                ended = now_utc()
                with db_lock:
                    step = over_all_job_registry[reference]["steps"][step_name]
                    step["end_time"] = ended
                    step["duration"] = iso_duration(started, ended)
                    step["status"] = "SUCCESS" if success else "ERROR"

                if not success:
                    with db_lock:
                        over_all_job_registry[reference]["status"] = "ERROR"
                        over_all_job_registry[reference]["end_time"] = ended
                        over_all_job_registry[reference]["duration"] = iso_duration(
                            over_all_job_registry[reference]["start_time"], ended
                        )
                    break
            else:
                end_time = now_utc()
                with db_lock:
                    over_all_job_registry[reference]["status"] = "SUCCESS"
                    over_all_job_registry[reference]["end_time"] = end_time
                    over_all_job_registry[reference]["duration"] = iso_duration(
                        over_all_job_registry[reference]["start_time"], end_time
                    )

        except Exception as e:
            logger.exception(
                f"Unexpected error in run_job_queue for job {reference}")
            with db_lock:
                over_all_job_registry[reference]["status"] = "ERROR"
                over_all_job_registry[reference]["end_time"] = now_utc()
# https://fastapi.tiangolo.com/advanced/events/


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting background job thread via lifespan...")
    thread = threading.Thread(target=run_job_queue, daemon=True)
    thread.start()
    yield  # This allows FastAPI to start serving
    logger.info("App is shutting down...")  # Optional cleanup

# Initialize the FastAPI app using lifespan
app = FastAPI(lifespan=lifespan)

SAXON_JAR = os.environ.get("SAXON_JAR")
XSLT_DIR = os.environ.get("XSLT")

print("SAXON_JAR:", SAXON_JAR)
print("XSLT_DIR:", XSLT_DIR)
pip_job_registry = {}
over_all_job_registry = {}


class InMemoryLogHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.log_stream = io.StringIO()
        # only log this thread's messages and avoid logs from other endpoints
        self.thread_id = threading.get_ident()

    def emit(self, record):
        if record.thread == self.thread_id:
            msg = self.format(record)
            self.log_stream.write(msg + "\n")

    def get_logs(self):
        return self.log_stream.getvalue()


def now_utc():
    return datetime.utcnow().isoformat()


def iso_duration(start, end):
    s = datetime.fromisoformat(start)
    e = datetime.fromisoformat(end)
    return str(e - s)


def prepare_final_output(job, job_status, result_zip_path):
    """
    Prepares the final zip output:
    - Always writes logs.txt
    - On SUCCESS, unzips the result.zip
    - Always zips everything in output_dir into a final zip
    """
    os.makedirs(job.dir_output, exist_ok=True)

    # 1. Write logs.txt
    combined_log = job.get_log()
    logs_txt_path = os.path.join(job.dir_output, "logs.txt")
    with open(logs_txt_path, "w", encoding="utf-8") as f:
        f.write(combined_log)

    # 2. Unzip job.zip into dir_output if SUCCESS
    # result_zip_path = os.path.join(job.dir_output, f"{job.job_id}.zip")
    if job_status == "SUCCESS" and os.path.exists(result_zip_path):
        try:
            with zipfile.ZipFile(result_zip_path, 'r') as z:
                z.extractall(job.dir_output)
            os.remove(result_zip_path)  # remove the original result zip
        except Exception as e:
            logger.warning(f"Failed to extract result ZIP: {e}")

    # 3. Zip all contents into final zip
    final_zip_path = os.path.join(job.dir_output, f"{job.job_id}_final.zip")
    with zipfile.ZipFile(final_zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(job.dir_output):
            for file in files:
                file_path = os.path.join(root, file)
                print(f"File -->: {file}")
                print(f"Zipping file -->: {file_path}")
                if file_path != final_zip_path:
                    arcname = os.path.relpath(file_path, job.dir_output)
                    zipf.write(file_path, arcname)

    return final_zip_path


def check_status_internal(reference_number: str) -> Union[dict, FileResponse]:
    with db_lock:
        entry = pip_job_registry.get(reference_number)

    if not entry:
        return {
            "status": "not_found",
            "message": "No job found for filename",
            "code": 404
        }

    status = entry.get("status")
    final_zip = entry.get("final_zip")

    if status in ("QUEUED", "RUNNING", "IDLE"):
        return {
            "status": status,
            "message": "Job is still processing",
            "code": 202
        }

    if not final_zip or not os.path.exists(final_zip):
        return {
            "status": status,
            "message": "Result ZIP not found",
            "code": 500
        }
    return FileResponse(
        path=final_zip,
        media_type="application/zip",
        filename=os.path.basename(final_zip)
    )


@app.post("/validate_nordic_epub/")
async def submit_pipeline_job(epub: UploadFile = File(...),
                              source: Optional[str] = Form(default="unknown")):

    # reference = generate_reference_number(epub.filename, source)
    # log_handler = InMemoryLogHandler()

    log_handler = InMemoryLogHandler()
    log_handler.setFormatter(logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(log_handler)
    logger.info("New job submission request received.")
    temp_dir = tempfile.mkdtemp()
    epub_path = os.path.join(temp_dir, epub.filename)
    with open(epub_path, "wb") as f:
        f.write(await epub.read())
    logger.info(f"Uploaded file saved: {epub.filename}")

    logger.info(
        f"Job added to queue: {epub.filename} from source: {source} (position)")
    production_number = os.path.splitext(epub.filename)[0]
    reference_number = f"{production_number}_{source}_{uuid.uuid4().hex[:6]}"

    job_data = {
        "reference_number": reference_number,
        "epub_path": epub_path,
        "filename": epub.filename,
        "source": source,
        "log_handler": log_handler,
    }

    with db_lock:
        over_all_job_registry[reference_number] = {
            "status": "QUEUED",
            "start_time": None,
            "end_time": None,
            "duration": None,
            "steps": {
                "create-epub-no-img": {"status": "PENDING"},
                "incoming-nordic": {"status": "PENDING"},
                "nordic-epub3-to-html": {"status": "PENDING"},
            }
        }
        job_queue.append(job_data)

    return JSONResponse({"status": "QUEUED", "reference_number": reference_number}, status_code=202)
