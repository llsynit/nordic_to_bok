import os
import io
import sys
import shutil
import zipfile
import tempfile
import logging
import requests
import datetime
import urllib
import base64
import random
import hmac
import hashlib
from lxml import etree as ET
from lxml.etree import XPath
from lxml.etree import XPathEvaluator


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
            for engine in self.engines:

                logger.info(
                    f"Trying endpoint: {engine['endpoint']} looking for pipeline version {pipeline_version}, script: {script_version}")
                if self._script_available(engine, pipeline_version, script_version):
                    # if self.script_available(engine, pipeline_version, script_version):
                    self.engine = engine
                    # self.found_pipeline_version, self.found_script_version = version
                    self.found_pipeline_version = pipeline_version
                    self.found_script_version = script_version
                    return True
        return False

    def _script_available(self, engine, pipeline_version, script_version):
        scripts = None
        try:
            alive = requests.get(self._url(engine, "/alive"))
            if not alive.ok:
                logger.warning(
                    f"Engine {engine['endpoint']} is not alive or not reachable.")
                return False
            version_str = ET.XML(alive.content.split(
                b"?>")[-1]).attrib.get("version")
            if version_str != pipeline_version:
                logger.warning(
                    f"Engine {engine['endpoint']} version mismatch: expected {pipeline_version}, got {version_str}")
                return False
            logger.info(
                f"Engine {engine['endpoint']} is alive and version matches: {version_str}")
            try:
                scripts = requests.get(self.encode_url(engine, "/scripts", {}))
                """root = ET.XML(scripts.content.split(b"?>")[-1])
                    engine_script = root.xpath(
                        f"/d:scripts/d:script[@id='{self.script_id}']",
                        namespaces=self.namespace
                    )"""
                if scripts.ok:
                    scripts = str(scripts.content, 'utf-8')
                    scripts = ET.XML(scripts.split("?>")[-1])
                else:
                    scripts = None
            except Exception as e:
                scripts = None
                logger.warning(
                    f"Failed to fetch scripts from {engine['endpoint']}: {e}")
                return False
            # script_ver = script_el[0].find("d:version", namespaces=self.namespace).text
            # script_ver = engine_script[0].find("d:version", namespaces=self.namespace).text
            print(f"Scripts******@: {scripts}")
            print(f"Script ID----->: {self.script_id}")
            print(f"Version ID----->: {self.versions}")
            engine_script = scripts.xpath("/d:scripts/d:script[@id='{}']".format(
                self.script_id), namespaces=self.namespace)
            if engine_script is not None and len(engine_script) > 0:
                engine_script_version = engine_script[0]
            else:
                engine_script_version = None
            # test if script version is correct
            if script_version is not None and (engine_script_version is None or script_version != engine_script_version.text):
                logger.debug("Incorrect version of Pipeline 2. Looking for {} but found {}.".format(
                    script_version, engine_script_version))

            return engine_script_version
        except Exception as e:
            logger.warning(f"Script availability check failed: {e}")
            return False

    def _post_job(self):
        logger.info("Posting job to remote Daisy Pipeline...")
        script_url = self._url(self.engine, f"/scripts/{self.script_id}")
        script_xml = ET.XML(requests.get(script_url).content.split(b"?>")[-1])

        job_req = ET.XML(
            "<jobRequest xmlns='http://www.daisy.org/ns/pipeline/data'/>")
        job_req.append(ET.XML(f"<priority>medium</priority>"))
        job_req.append(ET.XML(f"<script href='{script_url}'/>"))

        for input in script_xml.xpath("/d:script/d:input", namespaces=self.namespace):
            if input.attrib["name"] in self.arguments:
                values = []
                argument = self.arguments[input.attrib["name"]]
                if not isinstance(argument, list):
                    argument = [argument]
                for argument_value in argument:
                    value = argument_value
                    values.append(value)

                input_xml = f'<input name="{input.attrib["name"]}" xmlns="http://www.daisy.org/ns/pipeline/data">'
                for value in values:
                    input_xml += f'<item value="{value}"/>'
                input_xml += "</input>"
                job_req.append(ET.XML(input_xml))

        for option in script_xml.xpath("/d:script/d:option", namespaces=self.namespace):
            if option.attrib["name"] in self.arguments:
                values = []
                argument = self.arguments[option.attrib["name"]]
                if not isinstance(argument, list):
                    argument = [argument]
                for argument_value in argument:
                    value = argument_value
                    values.append(value)

                option_xml = f'<option name="{option.attrib["name"]}" xmlns="http://www.daisy.org/ns/pipeline/data">'
                if len(values) == 1:
                    option_xml += values[0]
                else:
                    for value in values:
                        option_xml += f'<item value="{value}"/>'
                option_xml += "</option>"
                job_req.append(ET.XML(option_xml))

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

    def encode_url(self, engine, endpoint, parameters):
        if engine["authentication"] == "true":
            iso8601 = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            nonce = str(random.randint(10**29, 10**30-1))  # 30 digits

            parameters["authid"] = engine["key"]
            parameters["time"] = iso8601
            parameters["nonce"] = nonce

        url = engine["endpoint"] + endpoint
        if parameters:
            url += "?" + urllib.parse.urlencode(parameters)

        if engine["authentication"] == "true":
            # Use RFC 2104 HMAC for keyed hashing of the URL
            hash = hmac.new(engine["secret"].encode('utf-8'),
                            url.encode('utf-8'),
                            digestmod=hashlib.sha1)

            # Use base 64 encoding
            hash = base64.b64encode(hash.digest()).decode('utf-8')

            # Base64 encoding uses + which we have to encode in URL parameters.
            hash = hash.replace("+", "%2B")

            # Append hash as parameter to the end of the URL
            url += "&sign=" + hash

        return url
