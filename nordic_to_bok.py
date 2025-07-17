
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

import httpx

from bs4 import BeautifulSoup

from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from fastapi.responses import JSONResponse
from fastapi.responses import FileResponse
from fastapi import BackgroundTasks

from dotenv import load_dotenv

from daisy_pipeline_light import RemoteDaisyPipelineJob  # your simplified class
from utils import remove_file  # your utility function
from incoming_nordic import create_epub_no_img  # your EPUB validation function
from nordic_to_nlbpub import get_nordic_guidelines_version, nordic_to_nlbpub_with_migrator


# Registry and queue
db_lock = threading.Lock()
job_queue = []
job_done_events = {}
job_running = False
current_running_job_id = None
current_running_job_name = None
current_running_job_source = None


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

load_dotenv()

app = FastAPI()

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


def run_validation(epub_path, reference_number, filename, source, log_handler):
    over_all_job_registry[reference_number] = {
        "status": "RUNNING",
        "start_time": datetime.datetime.utcnow().isoformat(),
        "filename": filename,
        "source": source,
        "subtasks": {
            "create-epub-without-img": "RUNNING",
            "incoming-nordic": "PENDING",
            "accessibility-report": "PENDING",
            "nordic-epub3-to-html": "PENDING",
            "insert-metadata": "PENDING",
        }
    }
    result = create_epub_no_img(epub_path)
    epub_noimages_file = result.get("file")
    print("Result from create_epub_no_img:", result)

    with db_lock:
        if result["status"] == "error":
            logger.error(result["message"])

            # Update subtask status
            over_all_job_registry[reference_number]["subtasks"]["create-epub-without-img"] = "ERROR"

            # Set overall job to ERROR
            over_all_job_registry[reference_number]["status"] = "ERROR"
            over_all_job_registry[reference_number]["error"] = result["message"]
            over_all_job_registry[reference_number]["end_time"] = datetime.datetime.utcnow(
            ).isoformat()
            return  # Exit early if the first step fails

        # If successful
        over_all_job_registry[reference_number]["subtasks"]["create-epub-without-img"] = "SUCCESS"

    pipeline_and_script_version = [
        # added 08.04.24 validate with Nordic EPUB3/DTBook Migrator. The Nordic EPUB3 Validator script can validate according to both 2015-1 and 2020-1 rulesets. Which ruleset will be applied is determined by the value of the <meta property="nordic:guidelines"> element in package.opf.
        ("1.14.3", "1.5.2-SNAPSHOT"),
        ("1.13.6", "1.4.6"),
        ("1.13.4", "1.4.5"),
        ("1.12.1", "1.4.2"),
        ("1.11.1-SNAPSHOT", "1.3.0"),
    ]

    arguments = {"epub": os.path.basename(epub_noimages_file)}
    context = {
        os.path.basename(epub_noimages_file): epub_noimages_file
    }
    job_entry = {
        "init_args": {
            "script_id": "nordic-epub3-validate",
            "arguments": arguments,
            "context": context,
            "versions": pipeline_and_script_version,
            "log_handler": log_handler,
        },
        "source": source,
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "filename": filename,
        "reference_number": reference_number
    }
    with db_lock:
        # update the overall job registry
        over_all_job_registry[reference_number]["subtasks"]["incoming-nordic"] = "RUNNING"
        over_all_job_registry[reference_number]["status"] = "RUNNING"
        job_queue.append(job_entry)
        job_done_events[reference_number] = threading.Event()

        queue_position = len(job_queue)
        pip_job_registry[reference_number] = {
            "status": "QUEUED",
            "source": source,
            "timestamp": job_entry["timestamp"],
            "reference_number": reference_number,
        }
    logger.info(
        f"Job {reference_number} added to queue. Current queue size: {queue_position}. Source: {source}")
    job_done_events[reference_number].wait()
    status = pip_job_registry[reference_number]["status"]

    if status == "SUCCESS":
        logger.info(f"Job {reference_number} completed successfully.")
        over_all_job_registry[reference_number]["subtasks"]["incoming-nordic"] = "SUCCESS"
        over_all_job_registry[reference_number]["status"] = "RUNNING"
        entry = pip_job_registry.get(reference_number)
        if entry and "final_zip" in entry:
            final_zip = entry["final_zip"]
            over_all_job_registry[reference_number]["final_zip"] = final_zip
            logger.info(
                f"Final zip for job {reference_number} is ready: {final_zip}")
        guidelines = get_nordic_guidelines_version(epub_path)
        if guidelines is False:
            logger.error("Error: Could not determine guidelines version.")
        elif guidelines == "2020-1":
            logger.info("EPUB follows the 2020-1 Nordic guidelines.")
            nordic_to_nlbpub_with_migrator(epub_path)
        else:
            logger.info("EPUB follows the 2015-1 Nordic guidelines.")
    elif status == "ERROR":
        logger.error(
            f"Job {reference_number} failed with error: {pip_job_registry[reference_number].get('error', 'Unknown error')}")
        over_all_job_registry[reference_number]["subtasks"]["incoming-nordic"] = "ERROR"
        over_all_job_registry[reference_number]["status"] = "ERROR"

    elif status == "QUEUED":
        logger.info(
            f"Job {reference_number} is still queued. Current status: {pip_job_registry[reference_number]['status']}")
        over_all_job_registry[reference_number]["subtasks"]["incoming-nordic"] = "RUNNING"
        over_all_job_registry[reference_number]["status"] = "RUNNING"
    else:
        over_all_job_registry[reference_number]["subtasks"]["incoming-nordic"] = "RUNNING"
        over_all_job_registry[reference_number]["status"] = "RUNNING"


def run_pipeline_queue():
    global job_running, current_running_job_id, current_running_job_name, current_running_job_source

    while True:
        if job_queue and not job_running:
            logger.info("Starting job from queue...")
            job_running = True
            job_data = job_queue.pop(0)
            job_id = None

            try:
                job = RemoteDaisyPipelineJob(**job_data["init_args"])
                result = job.run()
                job_id = result["job_id"]
                current_running_job_id = job_id
                current_running_job_name = job_data["init_args"]["arguments"].get(
                    "source")
                reference_number = job_data.get(
                    "reference_number")
                logger.info("Job reference number: %s",
                            reference_number)
                with db_lock:
                    pip_job_registry[reference_number] = {
                        "status": "RUNNING",
                        "log_file": None,
                        "output_dir": None,
                        "start_time": datetime.datetime.utcnow().isoformat(),
                        "filename": current_running_job_name,
                        "source": current_running_job_source,
                        "job_id": job_id
                    }

                status = "IDLE"
                timeout = time.time() + 3600  # 1 hour max
                while status in ("IDLE", "RUNNING") and time.time() < timeout:
                    logger.info(f"Polling status for job {job_id}: {status}")
                    time.sleep(5)
                    status = job.get_status(job_id)
                    logger.info(f"Job {job_id} status: {status}")
                    if status == "DONE":
                        logger.info(f"Job {job_id} status is SUCCESS.")
                        status = "SUCCESS"
                    elif status not in ("IDLE", "RUNNING", "SUCCESS"):
                        logger.info(
                            f"Job {job_id} has failed with status: {status}")
                        status = "FAIL"

                final_zip = prepare_final_output(
                    job, status, job.download_all(job_id))

                with db_lock:
                    pip_job_registry[reference_number].update({
                        "status": status,
                        "final_zip": final_zip
                    })
                    if status != "SUCCESS":
                        pip_job_registry[reference_number][
                            "error"] = f"Final job status: {status}"
                      # Signal the event that job is done
                    event = job_done_events.get(reference_number)
                    if event:
                        event.set()
                        del job_done_events[reference_number]  # cleanup
                if status == "SUCCESS":
                    logger.info(f"Job completed successfully: {job_id}")
                else:
                    logger.error(f"Job {job_id} failed with status: {status}")
                    print(job.get_log())
                    logger.warning(f"Job Failed. Final status: {status}")
            except Exception as e:
                logger.error(f"Job failed due to error: {str(e)}")
                if job_id:
                    with db_lock:
                        pip_job_registry[reference_number] = {
                            "status": "ERROR",
                            "error": str(e)

                        }

            current_running_job_id = None
            current_running_job_name = None
            current_running_job_source = None
            job_running = False
        else:
            if not job_queue:
                logger.info("Job queue is empty. Waiting for new jobs...")
            time.sleep(2)


def get_remote_endpoints():
    raw = os.getenv("REMOTE_PIPELINE2_WS_ENDPOINTS", "")
    return [e.strip() for e in raw.replace(",", " ").split() if e.strip()]


@app.get("/health", tags=["Health"])
async def health_check():
    internal_status = "ok"  # assume internal is always healthy here
    alive = []
    unavailable = []

    endpoints = get_remote_endpoints()

    async with httpx.AsyncClient(timeout=5.0) as client:
        for endpoint in endpoints:
            alive_url = endpoint.rstrip("/") + "/alive"
            try:
                resp = await client.get(alive_url)
                resp.raise_for_status()
                root = ET.fromstring(resp.text)
                version = root.attrib.get("version", "unknown")
                alive.append({
                    "endpoint": endpoint,
                    "version": version
                })
            except Exception as e:
                unavailable.append({
                    "endpoint": endpoint,
                    "error": str(e)
                })

    return JSONResponse(
        status_code=200,
        content={
            "status": "ok",  # reflects internal health only
            "internal": internal_status,
            "external": {
                "alive": alive,
                "unavailable": unavailable
            }
        }
    )


@app.post("/validate_nordic_epub/")
async def submit_pipeline_job(
    background_tasks: BackgroundTasks,
    epub: UploadFile = File(...),
    braille_arguments_from_queue: Optional[str] = Form(default="{}"),
    source: Optional[str] = Form(default="unknown")
):
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

    def threaded_validation():
        run_validation(epub_path, reference_number,
                       epub.filename, source, log_handler)

    threading.Thread(target=threaded_validation).start()
    return JSONResponse({
        "status": "QUEUED",
        "reference_number": reference_number
    }, status_code=202)


@app.get("/download/{reference_number}")
async def download_result_zip(reference_number: str):
    with db_lock:
        entry = pip_job_registry.get(reference_number)

    final_zip = entry.get("final_zip") if entry else None

    if not final_zip or not os.path.exists(final_zip):
        return JSONResponse({"error": "Result zip not found"}, status_code=404)

    return FileResponse(
        path=final_zip,
        media_type="application/zip",
        filename=os.path.basename(final_zip)
    )


@app.get("/checkstatus/{reference_number}")
async def check_status(reference_number: str):
    result = check_status_internal(reference_number)

    # If result is a FileResponse, return it directly
    if isinstance(result, FileResponse):
        return result

    # Otherwise, extract response content and status code
    content = {}
    for key, value in result.items():
        if key != "code":
            content[key] = value

    status_code = result.get("code", 200)
    return JSONResponse(content=content, status_code=status_code)


@app.get("/job-queue")
async def list_job_queue():
    with db_lock:
        queued = [job["init_args"]["arguments"]["source"] for job in job_queue]
        running = current_running_job_id
    return JSONResponse({"queued": queued, "running": running})


@app.get("/nlbpubtopef/status/{job_id}")
def get_job_status(job_id: str):
    job = pip_job_registry.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@app.get("/nlbpubtopef/log/{job_id}")
def get_job_log(job_id: str):
    job = pip_job_registry.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    log_content = None
    log_path = job.get("log_file")
    if log_path and Path(log_path).is_file():
        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            log_content = f.read()

    return JSONResponse({
        "job_id": job_id,
        "status": job["status"],
        "error": job.get("error"),
        "output_dir": job.get("output_dir"),
        "log": log_content
    })


threading.Thread(target=run_pipeline_queue, daemon=True).start()
