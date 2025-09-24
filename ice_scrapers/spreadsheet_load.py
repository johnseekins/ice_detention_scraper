from bs4 import BeautifulSoup
import copy
import datetime
import os
import polars
import re
from schemas import (
    facility_schema,
    field_office_schema,
)
from ice_scrapers import (
    clean_street,
    facility_sheet_header,
    ice_facility_types,
    ice_inspection_types,
    repair_zip,
    repair_locality,
    ice_facility_group_mapping,
)
from typing import Tuple
from utils import (
    logger,
    session,
)

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
base_xlsx_url = "https://www.ice.gov/detain/detention-management"
filename = f"{SCRIPT_DIR}{os.sep}detentionstats.xlsx"


def _download_sheet(keep_sheet: bool = True, force_download: bool = True) -> Tuple[polars.DataFrame, str]:
    """Download the detention stats sheet from ice.gov"""
    resp = session.get(base_xlsx_url, timeout=120)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.content, "html.parser")
    links = soup.findAll("a", href=re.compile("^https://www.ice.gov/doclib.*xlsx"))
    if not links:
        raise Exception(f"Could not find any XLSX files on {base_xlsx_url}")
    fy_re = re.compile(r".+FY(\d{2}).+")
    # this is _usually_ the most recently uploaded sheet...
    actual_link = links[0]["href"]
    cur_year = int(datetime.datetime.now().strftime("%y"))
    # try to find the most recent
    for link in links:
        match = fy_re.search(link["href"])
        if not match:
            continue
        year = int(match.group(1))
        if year >= cur_year:
            actual_link = link["href"]
            # this seems like tracking into the future...
            cur_year = year
    logger.debug("Found sheet at: %s", actual_link)
    if force_download or not os.path.exists(filename):
        logger.info("Downloading detention stats sheet from %s", actual_link)
        resp = session.get(actual_link, timeout=120, stream=True)
        size = len(resp.content)
        with open(filename, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        logger.debug("Wrote %s byte sheet to %s", size, filename)
    df = polars.read_excel(
        drop_empty_rows=True,
        has_header=False,
        raise_if_empty=True,
        # because we're manually defining the header...
        read_options={"skip_rows": 7, "column_names": facility_sheet_header},
        sheet_name=f"Facilities FY{cur_year}",
        source=open(filename, "rb"),
    )
    if not keep_sheet:
        os.unlink(filename)
    return df, actual_link


def load_sheet(keep_sheet: bool = True, force_download: bool = True) -> dict:
    df, sheet_url = _download_sheet(keep_sheet, force_download)
    """Convert the detentionstats sheet data into something we can update our facilities with"""
    results: dict = {}
    # occassionally a phone number shows up in weird places in the spreadsheet.
    # let's capture it
    phone_re = re.compile(r".+(\d{3}\s\d{3}\s\d{4})$")
    for row in df.iter_rows(named=True):
        details = copy.deepcopy(facility_schema)
        zcode, cleaned = repair_zip(row["Zip"], row["City"])
        if cleaned:
            details["_repaired_record"] = True
        street, cleaned = clean_street(row["Address"], row["City"])
        if cleaned:
            details["_repaired_record"] = True
        match = phone_re.search(row["Address"])
        if match:
            details["phone"] = match.group(1)
            details["_repaired_record"] = True
        locality, cleaned = repair_locality(row["City"], row["State"])
        if cleaned:
            details["_repaired_record"] = True
        full_address = ",".join([street, locality, row["State"], zcode]).upper()
        details["address"]["administrative_area"] = row["State"]
        details["address"]["locality"] = locality
        details["address"]["postal_code"] = zcode
        details["address"]["street"] = street
        details["name"] = row["Name"]

        # population statistics
        details["population"]["male"]["criminal"] = row["Male Crim"]
        details["population"]["male"]["non_criminal"] = row["Male Non-Crim"]
        details["population"]["female"]["criminal"] = row["Female Crim"]
        details["population"]["female"]["non_criminal"] = row["Female Non-Crim"]
        details["population"]["total"] = (
            row["Male Crim"] + row["Male Non-Crim"] + row["Female Crim"] + row["Female Non-Crim"]
        )
        if row["Male/Female"]:
            if "/" in row["Male/Female"]:
                details["population"]["female"]["allowed"] = True
                details["population"]["male"]["allowed"] = True
            elif "Female" in row["Male/Female"]:
                details["population"]["female"]["allowed"] = True
            else:
                details["population"]["male"]["allowed"] = True
        details["population"]["ice_threat_level"] = {
            "level_1": row["ICE Threat Level 1"],
            "level_2": row["ICE Threat Level 2"],
            "level_3": row["ICE Threat Level 3"],
            "none": row["No ICE Threat Level"],
        }
        """
        # extracted from https://www.ice.gov/doclib/detention/FY25_detentionStats09112025.xlsx 2025-09-22
        Upon admission and periodically thereafter, detainees are categorized into a security level based on a variety of public safety factors, and are housed accordingly.  Factors include prior convictions, threat risk, disciplinary record, special vulnerabilities, and special management concerns.  Detainees are categorized into one of four classes of security risk: A/low, B/medium low, C/medium high, and D/high.
        """
        details["population"]["security_threat"]["low"] = row["Level A"]
        details["population"]["security_threat"]["medium_low"] = row["Level B"]
        details["population"]["security_threat"]["medium_high"] = row["Level C"]
        details["population"]["security_threat"]["high"] = row["Level D"]

        details["facility_type"] = {
            "id": row["Type Detailed"],
            "housing": {
                "mandatory": row["Mandatory"],
                "guaranteed_min": row["Guaranteed Minimum"],
            },
        }
        ft_details = ice_facility_types.get(row["Type Detailed"], {})
        if ft_details:
            details["facility_type"]["description"] = ft_details["description"]
            details["facility_type"]["expanded_name"] = ft_details["expanded_name"]
            for group, ids in ice_facility_group_mapping.items():
                if row["Type Detailed"] in ids:
                    details["facility_type"]["group"] = group
                    break
        details["avg_stay_length"] = row["FY25 ALOS"]
        details["inspection"] = {
            # fall back to type code
            "last_type": ice_inspection_types.get(row["Last Inspection Type"], row["Last Inspection Type"]),
            "last_date": row["Last Inspection End Date"],
            "last_rating": row["Last Final Rating"],
        }
        details["source_urls"].append(sheet_url)
        # details["field_office"] = self.field_offices["field_offices"][area_of_responsibility[row["AOR"]]]
        details["field_office"] = copy.deepcopy(field_office_schema)
        details["field_office"]["id"] = row["AOR"]
        details["address_str"] = full_address
        results[full_address] = details
    return results
