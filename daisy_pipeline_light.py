import os
import io
import sys
import shutil
import zipfile
import tempfile
import logging
import requests
import datetime
import base64
import random
import hmac
import hashlib
from lxml import etree as ET
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import JSONResponse
from typing import Optional
from requests_toolbelt.multipart.encoder import MultipartEncoder

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


class InMemoryLogHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.log_stream = io.StringIO()

    def emit(self, record):
        msg = self.format(record)
        self.log_stream.write(msg + "\n")

    def get_logs(self):
        return self.log_stream.getvalue()


class RemoteDaisyPipelineJob:
    namespace = {"d": 'http://www.daisy.org/ns/pipeline/data'}

    def __init__(self, script_id, arguments, context, versions, log_handler=None):
        self.script_id = script_id
        self.arguments = arguments
        self.context = context
        self.versions = versions
        self.engine = None
        self.job_id = None
        self.dir_output = tempfile.mkdtemp()
        self.log_handler = log_handler

        self.logger = logging.getLogger(__name__)
        self.local_log_handler = InMemoryLogHandler()
        self.local_log_handler.setFormatter(logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"))
        self.logger.addHandler(self.local_log_handler)

    def run(self):
        self.logger.info(
            "Initializing and posting job to remote Daisy Pipeline...")
        self._init_engines()
        if not self._select_engine():
            self.logger.error("No suitable engine found")
            raise RuntimeError("No suitable remote engine found")

        self._post_job()
        logger.info(f"Job posted successfully with ID: {self.job_id}")
        return {
            "engine": self.engine,
            "job_id": self.job_id,
            "dir_output": self.dir_output
        }

    def get_status(self, job_id):
        r = requests.get(self._url(self.engine, f"/jobs/{job_id}"))
        root = ET.XML(r.content.split(b"?>")[-1])
        return root.attrib["status"]

    def download_all(self, job_id):
        self.job_id = job_id
        return self._download_result()
        # self._download_log()

    def _init_engines(self):
        self.engines = []
        endpoints = os.getenv("REMOTE_PIPELINE2_WS_ENDPOINTS", "").split()
        auth = os.getenv("REMOTE_PIPELINE2_WS_AUTHENTICATION", "").split()
        keys = os.getenv("REMOTE_PIPELINE2_WS_AUTHENTICATION_KEYS", "").split()
        secrets = os.getenv(
            "REMOTE_PIPELINE2_WS_AUTHENTICATION_SECRETS", "").split()
        for i, endpoint in enumerate(endpoints):
            self.engines.append({
                "endpoint": endpoint,
                "authentication": auth[i] if i < len(auth) else "false",
                "key": keys[i] if i < len(keys) else None,
                "secret": secrets[i] if i < len(secrets) else None
            })

    def _select_engine(self):
        logger.info("Available engines: " + str(self.engines))
        for pipeline_version, script_version in self.versions:
            logger.info(
                f"Trying version: {pipeline_version}, {script_version}")

            for engine in self.engines:
                # if self._script_available(engine, version):
                if self.script_available(engine, pipeline_version, script_version):
                    self.engine = engine
                    self.found_pipeline_version, self.found_script_version = version
                    return True
        return False

    def _script_available(self, engine, version):
        try:
            alive = requests.get(self._url(engine, "/alive"))
            if not alive.ok:
                logger.warning(
                    f"Engine {engine['endpoint']} is not alive or not reachable.")
                return False
            version_str = ET.XML(alive.content.split(
                b"?>")[-1]).attrib.get("version")
            if version_str != version[0]:
                logger.warning(
                    f"Engine {engine['endpoint']} version mismatch: expected {version[0]}, got {version_str}")
                return False
            logger.info(
                f"Engine {engine['endpoint']} is alive and version matches: {version_str}")
            scripts = requests.get(self._url(engine, "/scripts"))
            root = ET.XML(scripts.content.split(b"?>")[-1])
            script_el = root.xpath(
                f"/d:scripts/d:script[@id='{self.script_id}']",
                namespaces=self.namespace
            )
            if not script_el:
                return False
            script_ver = script_el[0].find(
                "d:version", namespaces=self.namespace).text
            return script_ver == version[1]
        except Exception as e:
            logger.warning(f"Script availability check failed: {e}")
            return False

    def script_available(self, engine, pipeline_version, script_version):
        alive = None
        scripts = None

        try:
            alive = requests.get(self._url(engine, "/alive", {}))
            if alive.ok:
                alive = str(alive.content, 'utf-8')
                alive = ET.XML(alive.split("?>")[-1])
            else:
                alive = None
        except Exception:
            alive = None

        if alive is None:
            logger.warning(
                "Pipeline 2 kjører ikke på: {}".format(engine["endpoint"]))
            return False

        # find engine version
        engine_pipeline_version = alive.attrib.get("version")

        # test for correct engine version
        if pipeline_version is not None and pipeline_version != engine_pipeline_version:
            logger.debug("Incorrect version of Pipeline 2. Looking for {} but found {}.".format(pipeline_version,
                                                                                                engine_pipeline_version))
            return False

        try:
            scripts = requests.get(self._url(engine, "/scripts", {}))
            if scripts.ok:
                scripts = str(scripts.content, 'utf-8')
                scripts = ET.XML(scripts.split("?>")[-1])
            else:
                scripts = None
        except Exception:
            scripts = None

        if scripts is None:
            logger.warning("Klarte ikke å hente liste over skript fra Pipeline 2 på: {}".format(
                engine["endpoint"]))
            return False

        # find script
        engine_script = scripts.xpath(
            "/d:scripts/d:script[@id='{}']".format(self.script), namespaces=self.namespace)
        engine_script = engine_script[0] if len(engine_script) else None

        # test if script was found
        if engine_script is None:
            logger.debug("Script not found: {}".format(self.script))
            return False

        # find script version
        engine_script_version = engine_script.xpath(
            "d:version", namespaces=self.namespace) if len(engine_script) else None
        engine_script_version = engine_script_version[0].text if len(
            engine_script_version) else None

        # test if script version is correct
        if script_version is not None and script_version != engine_script_version:
            logger.debug("Incorrect version of Pipeline 2. Looking for {} but found {}.".format(script_version,
                                                                                                engine_script_version))
            return False

        return True

    def _post_job(self):
        script_url = self._url(self.engine, f"/scripts/{self.script_id}")
        script_xml = ET.XML(requests.get(script_url).content.split(b"?>")[-1])

        job_req = ET.XML(
            "<jobRequest xmlns='http://www.daisy.org/ns/pipeline/data'/>")
        job_req.append(ET.XML(f"<priority>medium</priority>"))
        job_req.append(ET.XML(f"<script href='{script_url}'/>"))

        for input_ in script_xml.xpath("/d:script/d:input", namespaces=self.namespace):
            name = input_.attrib["name"]
            if name in self.arguments:
                values = self.arguments[name]
                if not isinstance(values, list):
                    values = [values]
                input_el = ET.Element("input", name=name)
                for val in values:
                    input_el.append(ET.Element("item", value=val))
                job_req.append(input_el)

        for opt in script_xml.xpath("/d:script/d:option", namespaces=self.namespace):
            name = opt.attrib["name"]
            if name in self.arguments:
                values = self.arguments[name]
                if not isinstance(values, list):
                    values = [values]
                option_el = ET.Element("option", name=name)
                for val in values:
                    option_el.append(ET.Element("item", value=val))
                job_req.append(option_el)

        job_xml_path = tempfile.mktemp(suffix=".xml")
        ET.ElementTree(job_req).write(
            job_xml_path, encoding="UTF-8", xml_declaration=True, pretty_print=True)
        multipart_fields = {
            "job-request": ("jobRequest.xml", open(job_xml_path, "rb"), "application/xml")
        }
        with open(job_xml_path) as f:
            logger.info(
                "Job request: " + "".join(f.readlines()))

        if self.context:
            ctx_path = tempfile.mktemp(suffix=".zip")
            with zipfile.ZipFile(ctx_path, 'w') as zipf:
                for href, path in self.context.items():
                    print(f"Adding context file: {href} -> {path}")
                    zipf.write(path, href)
            multipart_fields["job-data"] = ("context.zip",
                                            open(ctx_path, "rb"), "application/zip")
        m = MultipartEncoder(fields=multipart_fields)
        try:
            r = requests.post(self._url(self.engine, "/jobs"),
                              data=m, headers={"Content-Type": m.content_type})
            print(str(r.content, 'utf-8'))
            r.raise_for_status()
            response = str(r.content, 'utf-8')
            print(response)

            try:
                # job = ET.XML(response.split(b"?>")[-1])
                job = ET.XML(response.split("?>")[-1])

                print(r.status_code, r.reason)
                self.job_id = job.attrib["id"]
            except Exception as e:
                logging.debug(response)
                raise e
        except requests.exceptions.RequestException as e:
            logging.error("HTTP request failed: %s", e)
            raise e

    def _download_result(self):
        url = self._url(self.engine, f"/jobs/{self.job_id}/result")
        result_zip = os.path.join(self.dir_output, f"{self.job_id}.zip")
        with requests.get(url, stream=True) as r:
            with open(result_zip, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
        with zipfile.ZipFile(result_zip, 'r') as z:
            z.extractall(self.dir_output)
        return result_zip

    def get_log(self):
        url = self._url(self.engine, f"/jobs/{self.job_id}/log")
        try:
            r = requests.get(url)
            r.raise_for_status()
            remote_log = str(r.content, 'utf-8')
        except Exception as e:
            remote_log = f"[ERROR] Failed to fetch remote log: {e}"

        # Get local log if available
        local_log = ""
        if self.log_handler:
            local_log = self.log_handler.get_logs()

        class_log = self.local_log_handler.get_logs()
        # Combine logs with clear divider
        return (
            "===== FastAPI LOG =====\n" +
            class_log +
            "\n===== DAISY PIPELINE JOB POST LOG =====\n" +
            local_log +
            "==== DAISY PIPELINE CONVERSION LOG ====\n" +
            remote_log +
            "\n===== LOG END =====\n"
        )

    def _url(self, engine, path, params=None):
        base = engine["endpoint"] + path
        if params is None:
            params = {}

        if engine["authentication"] == "true":
            time_str = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            nonce = str(random.randint(10**29, 10**30 - 1))
            params["authid"] = engine["key"]
            params["time"] = time_str
            params["nonce"] = nonce

        if params:
            base += "?" + "&".join([f"{k}={v}" for k, v in params.items()])

        if engine["authentication"] == "true":
            h = hmac.new(engine["secret"].encode(),
                         base.encode(), hashlib.sha1)
            sig = base64.b64encode(h.digest()).decode().replace("+", "%2B")
            # In some implementations you might append sig to the URL
            # base += f"&sign={sig}"  # Uncomment if required by server

        return base
