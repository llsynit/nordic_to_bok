import os
import subprocess
import logging
import sys
import uuid

from lxml import etree as ElementTree
from bs4 import BeautifulSoup
from typing import List, Optional, Tuple

SAXON_JAR = os.environ.get("SAXON_JAR")
XSLT_DIR = os.environ.get("XSLT")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def generate_reference_number(production_number: str, source: str) -> str:

    reference_number = f"{production_number}_{source}_{uuid.uuid4().hex[:6]}"
    return reference_number


def remove_file(path: str):
    try:
        os.remove(path)
        logger.info(f"Removed temp file: {path}")
    except Exception as e:
        logger.warning(f"Could not remove temp file: {e}")


def xslt_transform(xhtml_file):
    stylesheets = {
        "prepare-for-braille.xsl": "Tilpasser innhold for punktskrift…",
        "pre-processing.xsl": "Bedre hefteinndeling, fjern tittelside og innholdsfortegnelse, flytte kolofon og opphavsrettside til slutten av boka…",
        "add-table-classes.xsl": "Bedre håndtering av tabeller…",
        "insert-boilerplate.xsl": "Lag ny tittelside og bokinformasjon…"
    }

    input_file = xhtml_file
    temp_files = []

    for sheet, description in stylesheets.items():
        xslt_path = os.path.join(XSLT_DIR, sheet)
        temp_output = input_file + f".{sheet}.tmp"
        command = [
            "java", "-jar", SAXON_JAR,
            "-s:" + input_file,
            "-xsl:" + xslt_path,
            "-o:" + temp_output
        ]
        logger.info(f"XSLT: {sheet} - {description}")
        logger.info("Running XSLT")
        logger.info("Processing  %s", command)

        try:
            result = subprocess.run(
                command, check=True, capture_output=True, text=True)
            logger.info(f"XSLT {sheet} output: {result.stdout}")
        except subprocess.TimeoutExpired:
            logger.error(f"XSLT {sheet} timed out.")
            return {"status": "fail", "error": f"Timeout on {sheet}"}
        except subprocess.CalledProcessError as e:
            logger.error(f"XSLT {sheet} failed: {e.stderr}")
            return {"status": "fail", "error": e.stderr}

        temp_files.append(temp_output)
        input_file = temp_output  # Next stylesheet uses previous output

    # After all stylesheets, return the final output file path
    return {"status": "success", "result": input_file}
