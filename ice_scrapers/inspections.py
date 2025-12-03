from bs4 import BeautifulSoup
import os
from pprint import pformat
import re
from utils import (
    logger,
    output_folder,
    session,
)
from .utils import download_file

root_url = "https://www.ice.gov/foia/odo-facility-inspections"
storage_dir = f"{output_folder}{os.sep}inspections{os.sep}"
text_re = re.compile(r"^(\w+\s)?(\d+)\s(.+)\s(-|–)\s(.+)$")


def find_inspections(download_reports: bool = False):
    os.makedirs(storage_dir, exist_ok=True)
    inspections: dict = {}
    logger.info("Collecting inspection reports from %s", root_url)
    resp = session.get(root_url, timeout=120)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.content, "html.parser")
    content = soup.select_one("div.facility-inspections")
    links = content.select("a")  # type: ignore [union-attr]
    for link in links:
        url = link["href"]
        file_name = url.split("/")[-1]  # type: ignore [union-attr]
        """
        example: 2011 Calhoun County Correctional Facility, Battle Creek, MI - Dec. 6-8, 2011
        example 2: 2024 Chippewa County, Sault Sainte Marie, MI – Apr. 23-25, 2024
        example 3: FY 2018 South Texas ICE Processing Center Compliance Inspection Report – Pearsall, TX - May 1-3, 2018
        There are inconsistent hyphens!
        """
        text = text_re.search(link.text.strip())
        # third capture group should be the facility name
        location: str = text[3]  # type: ignore [index]
        # fifth capture group should be the inspection date
        date: str = text[5]  # type: ignore [index]
        logger.debug("Facility: %s, date: %s, details: %s", location, date, url)
        if download_reports:
            download_file(str(url), f"{output_folder}{os.sep}inspections{os.sep}{file_name}")
        if location in inspections:
            inspections[location].append({"date": date, "details": url})
        else:
            inspections[location] = [{"date": date, "details": url}]
    logger.debug(pformat(inspections))
    return inspections
