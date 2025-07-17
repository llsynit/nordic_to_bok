from datetime import datetime
import os
import time
import uuid
import sys
import tempfile
import shutil
import logging
from daisy_pipeline_light import RemoteDaisyPipelineJob
from incoming_nordic import create_epub_no_img
from utils import remove_file


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


class JobStepHandler:
    def __init__(self, job):
        self.job = job
        self.reference = job["reference_number"]
        self.epub_path = job["epub_path"]
        self.log_handler = job["log_handler"]
        self.filename = job["filename"]

    def run_step_create_epub_no_img(self):
        print(f"Running step: create-epub-no-img for job {self.reference}")
        result = create_epub_no_img(self.epub_path)
        if result.get("status") == "error":
            return False
        self.job["epub_path"] = result["file"]
        print(result["file"])
        return True

    def run_step_daisy(self, script_id, step_name):
        print("---")
        print(self.job["epub_path"])
        print(self.filename)
        print(os.path.basename(self.job["epub_path"]))
        # check if the epub file exists
        if not os.path.exists(self.job["epub_path"]):
            print(f"Error: Epub file {self.job['epub_path']} does not exist.")
            return False
        print(f"Ext --Running step: {step_name} for job {self.reference}")

        epub_file_path = self.job["epub_path"]
        epub_filename = os.path.basename(epub_file_path)

        temp_dir = tempfile.mkdtemp()
        copied_path = os.path.join(temp_dir, epub_filename)
        shutil.copy(epub_file_path, copied_path)

        init_args = {
            "script_id": script_id,
            "arguments": {"epub": os.path.basename(self.job["epub_path"])},
            "context": {self.filename: copied_path},
            "versions": [("1.11.1-SNAPSHOT", "1.3.0"), ("1.14.3", "1.5.2-SNAPSHOT"),  # added 08.04.24 validate with Nordic EPUB3/DTBook Migrator. The Nordic EPUB3 Validator script can validate according to both 2015-1 and 2020-1 rulesets. Which ruleset will be applied is determined by the value of the <meta property="nordic:guidelines"> element in package.opf.
                         ("1.13.6", "1.4.6"),
                         ("1.13.4", "1.4.5"),
                         ("1.12.1", "1.4.2"),
                         ],
            "log_handler": self.log_handler,
        }

        job = RemoteDaisyPipelineJob(**init_args)
        result = job.run()
        job_id = result.get("job_id")
        status = "RUNNING"
        timeout = time.time() + 600  # 10 min

        while status in ("RUNNING", "IDLE") and time.time() < timeout:
            status = job.get_status(job_id)
            logger.info(f"Job {job_id} status: {status}")
            if status == "DONE":
                job.download_all(job_id)
                return True
            elif status not in ("IDLE", "RUNNING"):
                return False
            time.sleep(5)
        """ try:
            epub_path = self.job["epub_path"]
            if os.path.exists(epub_path):
                os.remove(epub_path)
                print(f"Deleted temporary file: {epub_path}")
        except Exception as e:
            print(f"Warning: Failed to delete {epub_path}: {e}")"""
        return False
