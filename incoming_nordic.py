from epub import Epub
import os
import sys
import logging
import shutil
import subprocess
import sys
import tempfile


from lxml import etree as ElementTree
import xml.etree.ElementTree as ET

from filesystem import Filesystem

XSLT_DIR = os.environ.get("XSLT")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def create_epub_no_img(epub_file):
    uid = "incoming-nordic"
    title = "Validering av Nordisk EPUB 3"
    labels = ["EPUB", "Statped"]
    publication_format = None
    expected_processing_time = 1400
    logger.info("Starter validering av Nordisk EPUB 3----" + uid)
    epub = Epub(logger, epub_file)
    epubTitle = ""
    try:
        epubTitle = " (" + epub.meta("dc:title") + ") "
    except Exception:
        pass
    # sjekk at dette er en EPUB
    if not epub.isepub():
        logger.info(" feilet 😭👎" + epubTitle)
        return {
            "status": "error",
            "message": f"{epubTitle} er ikke en gyldig EPUB",
        }

    if not epub.identifier():
        logger.error(
            "Klarte ikke å bestemme boknummer basert på dc:identifier.")
        return {
            "status": "error",
            "message": "Mangler dc:identifier – kan ikke finne boknummer",
        }

    # Function to process attributes based on XSLT logic

    def process_attribute(value):
        logger.info("Processing attribute: " + value)
        if not value.startswith("images/"):
            return value  # Keep unchanged
        if "/cover.jpg" in value:
            return value  # Keep unchanged
        if "#" in value:
            fragment = value.split("#", 1)[1]
            return f"images/dummy.jpg#{fragment}"
        return "images/dummy.jpg"  # Replace with dummy image

    # Function to process the XHTML document
    def transform_xhtml(html_file):
        tree = ET.parse(html_file)
        root = tree.getroot()

        # Define attributes to modify
        attributes_to_modify = ["src", "href", "altimg", "longdesc"]

        # Traverse XML tree
        for elem in root.iter():
            for attr in attributes_to_modify:
                if attr in elem.attrib:
                    elem.attrib[attr] = process_attribute(elem.attrib[attr])

            # Special case: @data under <object>
            if elem.tag == "object" and "data" in elem.attrib:
                elem.attrib["data"] = process_attribute(elem.attrib["data"])

        # Write back changes
        tree.write(html_file, method="xml", encoding="UTF-8")

    logger.info("Lager en kopi av EPUBen med tomme bildefiler")
    temp_noimages_epubdir_obj = tempfile.TemporaryDirectory()
    temp_noimages_epubdir = temp_noimages_epubdir_obj.name
    Filesystem.copy(logger, epub.asDir(), temp_noimages_epubdir)
    if os.path.isdir(os.path.join(temp_noimages_epubdir, "EPUB", "images")):
        temp_xml_obj = tempfile.NamedTemporaryFile()
        temp_xml = temp_xml_obj.name
        opf_image_references = []
        html_image_references = {}
        for root, dirs, files in os.walk(os.path.join(temp_noimages_epubdir, "EPUB")):
            for file in files:
                if file.endswith(".opf"):
                    opf_file = os.path.join(root, file)
                    logger.info(
                        "Fjerner alle bildereferanser fra OPFen, og erstatter med en referanse til dummy.jpg...")
                    opf_xml_document = ElementTree.parse(opf_file)
                    opf_xml = opf_xml_document.getroot()
                    image_items = opf_xml.xpath(
                        "//*[local-name()='item' and starts-with(@media-type, 'image/')]")
                    replaced = False
                    for image_item in image_items:
                        if image_item.attrib["href"] not in opf_image_references:
                            opf_image_references.append(
                                image_item.attrib["href"])

                        if image_item.get("href") == "images/cover.jpg":
                            pass  # don't change the reference to cover.jpg

                        elif not replaced:
                            image_item.attrib["href"] = "images/dummy.jpg"
                            replaced = True

                        else:
                            image_item.getparent().remove(image_item)

                    opf_xml_document.write(
                        opf_file, method='XML', xml_declaration=True, encoding='UTF-8', pretty_print=False)

                if file.endswith(".xhtml"):
                    html_file = os.path.join(root, file)

                    html_xml_document = ElementTree.parse(html_file)
                    html_xml = html_xml_document.getroot()
                    image_references = html_xml.xpath(
                        "//@href | //@src | //@altimg")
                    for reference in image_references:
                        path = reference.split("#")[0]
                        if path.startswith("images/"):
                            if path not in html_image_references:
                                html_image_references[path] = []
                            html_image_references[path].append(file)

                    logger.info(
                        "Erstatter alle bildereferanser med images/dummy.jpg...")
                    logger.info(
                        "Erstatter alle bildereferanser med images/dummy.jpg... i" + html_file)
                    logger.debug("dummy-jpg.xsl")
                    logger.debug("    source = " + html_file)
                    logger.debug("    target = " + temp_xml)
                    transform_xhtml(html_file)
                    """xslt = Xslt(self,
                                stylesheet=os.path.join(Xslt.xslt_dir, IncomingNordic.uid, "dummy-jpg.xsl"),
                                source=html_file,
                                target=temp_xml)
                    if not xslt.success:
                        self.utils.report.title = self.title + ": " + epub.identifier() + " feilet 😭👎" + epubTitle
                        return False
                    shutil.copy(temp_xml, html_file)"""

        # validate for the presence of image files here, since epubcheck won't be able to do it anymore after we change the EPUB
        image_files_present = []
        for root, dirs, files in os.walk(os.path.join(temp_noimages_epubdir, "EPUB", "images")):
            for file in files:
                fullpath = os.path.join(root, file)
                relpath = os.path.relpath(
                    fullpath, os.path.join(temp_noimages_epubdir, "EPUB"))
                image_files_present.append(relpath)
        image_error = False
        for file in image_files_present:
            if file not in opf_image_references:
                logger.error("Bildefilen er ikke deklarert i OPFen: " + file)
                image_error = True
        for file in opf_image_references:
            if file not in image_files_present:
                logger.error(
                    "Bildefilen er deklarert i OPFen, men finnes ikke: " + file)
                image_error = True
        for file in html_image_references:
            if file not in opf_image_references:
                logger.error("Bildefilen er deklarert i HTMLen, men finnes ikke: " + file
                             + " (deklarert i: " + ", ".join(html_image_references[file]) + ")")
                image_error = True
        if image_error:
            logger.info(epub.identifier() + " feilet 😭👎" + epubTitle)
            return {
                "status": "error",
                "message": " Image error",
            }

        for root, dirs, files in os.walk(os.path.join(temp_noimages_epubdir, "EPUB", "images")):
            for file in files:
                if file == "cover.jpg":
                    continue  # don't delete the cover file
                fullpath = os.path.join(root, file)
                os.remove(fullpath)
        shutil.copy(os.path.join(XSLT_DIR, uid, "reference-files", "demobilde.jpg"),
                    os.path.join(temp_noimages_epubdir, "EPUB", "images", "dummy.jpg"))
    temp_noimages_epub = Epub(logger, temp_noimages_epubdir)

    logger.info("Validerer EPUB med epubcheck og nordiske retningslinjer...")
    epub_noimages_file = temp_noimages_epub.asFile()

    epub_file_path = os.path.join("/tmp", os.path.basename(epub_noimages_file))
    shutil.copy(epub_noimages_file, epub_file_path)

    return {
        "status": "ok",
        "file": epub_file_path,

    }


def generate_ace_report(epub_file):
    logger.info("Starter validering av Nordisk EPUB 3----")
