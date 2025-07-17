import os
import sys
import subprocess
import traceback
import tempfile
from pathlib import Path
import logging
from epub import Epub
from bs4 import BeautifulSoup

from filesystem import Filesystem

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def get_nordic_guidelines_version(epub_file):
    """
    Get the nordic guidelines version:  2015 or 2020 guidelines.
    """

    epub = Epub(logger, epub_file)
    epub_unzipped = epub.asDir()
    opf_path = Path(os.path.join(epub_unzipped, "EPUB", "package.opf"))
    try:

        if not opf_path.exists():
            logger.error(f"Error: OPF file not found at {opf_path}")
            return False
        xml_content = opf_path.read_text(encoding='utf-8')
        soup = BeautifulSoup(xml_content, 'xml')  # Use 'xml' parser

        # Extract content with property 'nordic:guidelines'
        meta_property = soup.find("meta", property="nordic:guidelines")
        meta_property_content = meta_property.text if meta_property else None
        logger.info(
            f"Content of meta with property 'nordic:guidelines':, {meta_property_content}")

        # Extract single content with name 'nordic:guidelines'
        meta_name = soup.find("meta", attrs={"name": "nordic:guidelines"})
        meta_name_content = meta_name['content'] if meta_name and "content" in meta_name.attrs else None
        logger.info(
            f"Content of meta with name 'nordic:guidelines':, {meta_name_content}")

    except Exception as e:
        logger.error(f"Error reading {opf_path}: {e}")
        return False
    if (meta_property_content is not None and meta_property_content == "2020-1") or (meta_name_content is not None and meta_name_content == "2020-1"):
        return "2020-1"
    return "2015-1"


def nordic_to_nlbpub_with_migrator(epub_file):
    """
    Convert Nordic EPUB to NLBPUB format.
    """
    success = False
    epub = Epub(logger, epub_file)
    epub_unzipped = epub.asDir()
    temp_xml_file_obj = tempfile.NamedTemporaryFile()
    temp_xml_file = temp_xml_file_obj.name
    html_dir_obj = tempfile.TemporaryDirectory()
    html_dir = html_dir_obj.name
    try:
        command = ["src/run.py", epub_unzipped,
                   html_dir, "--add-header-element=false"]

        epub_to_html_home = os.getenv("EPUB_TO_HTML_HOME")
        if not epub_to_html_home:
            logger.warning(
                "EPUB_TO_HTML_HOME is not set. Using default value: /opt/nordic-epub3-dtbook-migrator")
            epub_to_html_home = "/opt/nordic-epub3-dtbook-migrator"

        process = Filesystem.run_static(command, epub_to_html_home, logger)
        success = process.returncode == 0

    except subprocess.TimeoutExpired:
        logger.error("Converting took too long and were therefore stopped.")

    except Exception:
        logger.debug(traceback.format_exc(), preformatted=True)
        logger.report.error(
            "An error occured while running EPUB to HTML")

    if not success:
        logger.error("Klarte ikke å konvertere boken")
        return False

    logger.debug(
        "Output directory contains: " + str(os.listdir(html_dir)))
    html_dir = os.path.join(html_dir, epub.identifier())

    if not os.path.isdir(html_dir):
        logger.error(
            "Finner ikke den konverterte boken: {}".format(html_dir))
        return False
