import copy
import os
import polars
from schemas import facility_schema
from typing import Tuple
from utils import (
    logger,
    session,
)

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
# Github can aggressively rate-limit requests, so this may fail in surprising ways!
base_url = (
    "https://raw.githubusercontent.com/vera-institute/ice-detention-trends/refs/heads/main/metadata/facilities.csv"
)
filename = f"{SCRIPT_DIR}{os.sep}vera_facilities.csv"


def _vera_name_fixes(name: str, city: str) -> Tuple[str, bool]:
    """Match Vera names with ice.gov names"""
    matches = [
        {"match": "Adams County", "replace": "Adams County Courthouse", "city": "Ritzville"},
        {"match": "Lemon Creek, Juneau,AK", "replace": "Lemon Creek Correctional Facility", "city": "Juneau"},
        {"match": "Dept Of Corrections-Hagatna", "replace": "Department of Corrections Hagatna", "city": "Hagatna"},
    ]
    fixed = False
    for m in matches:
        if m["match"] == name and m["city"] == city:
            fixed = True
            name = m["replace"]
            break
    return name, fixed


def collect_vera_facility_data(facilities_data: dict, keep_sheet: bool = True, force_download: bool = True) -> dict:
    logger.info("Collecting and extracting data from vera.org facility data...")
    if force_download or not os.path.exists(filename):
        res = session.get(base_url, timeout=120, stream=True)
        res.raise_for_status()
        size = len(res.content)
        with open(filename, "wb") as f:
            for chunk in res.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        logger.debug("Wrote %s byte sheet to %s", size, filename)
    df = polars.read_csv(has_header=True, raise_if_empty=True, source=filename, use_pyarrow=True)
    logger.debug("Extracted data: %s", df)
    """
    We retrieve the following columns
    detention_facility_code, detention_facility_name, latitude, longitude, city, state, type_detailed, type_grouped

    None of the data Vera provides on a facility is more accurate than data we already have, so the logic
    here should be _purely_ "if not exists, add".
    """
    matched_count = 0
    skipped_count = 0
    fixed_names = 0
    for row in df.iter_rows(named=True):
        if not row["state"] or not row["city"]:
            logger.warning("  Skipping Vera row with missing values: %s", row)
            skipped_count += 1
            continue
        found = False
        facility_name, fixed = _vera_name_fixes(row["detention_facility_name"], row["city"])
        if fixed:
            fixed_names += 1
        addr_str = f"{facility_name},{row['city']},{row['state']},United States"
        for k, v in facilities_data["facilities"].items():
            if (
                v["name"].upper() == facility_name.upper()
                and v["address"]["administrative_area"].upper() == row["state"].upper()
                and v["address"]["locality"].upper() == row["city"].upper()
            ):
                logger.debug("  Found matching facility %s...", v["name"])
                facilities_data["facilities"][k]["osm"]["latitude"] = row["latitude"]
                facilities_data["facilities"][k]["osm"]["longitude"] = row["longitude"]
                matched_count += 1
                found = True
                break
        if not found:
            facilities_data["facilities"][addr_str] = copy.deepcopy(facility_schema)
            facilities_data["facilities"][addr_str]["source_urls"].append(base_url)
            facilities_data["facilities"][addr_str]["name"] = facility_name
            facilities_data["facilities"][addr_str]["address"]["administrative_area"] = row["state"]
            facilities_data["facilities"][addr_str]["address"]["locality"] = row["city"]
            facilities_data["facilities"][addr_str]["address"]["country"] = "United States"
            facilities_data["facilities"][addr_str]["osm"]["latitude"] = row["latitude"]
            facilities_data["facilities"][addr_str]["osm"]["longitude"] = row["longitude"]
            facilities_data["facilities"][addr_str]["facility_type"]["id"] = row["type_detailed"]
            facilities_data["facilities"][addr_str]["facility_type"]["group"] = row["type_grouped"]
    logger.info(
        "  Found %s facilities: Skipped %s, Matched %s, corrected names on %s",
        df.height,
        skipped_count,
        matched_count,
        fixed_names,
    )
    if not keep_sheet:
        os.unlink(filename)
    return facilities_data
