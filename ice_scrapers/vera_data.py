import copy
import os
import polars
from schemas import facility_schema
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


def collect_vera_facility_data(facilities_data: dict, keep_sheet: bool = True, force_download: bool = True) -> dict:
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
    skipped_count = 0
    for row in df.iter_rows(named=True):
        found = False
        facility_name = row["detention_facility_name"]
        for k, v in facilities_data["facilities"].items():
            if (
                v["name"].upper() == facility_name.upper()
                and v["address"]["administrative_area"].upper() == row["state"].upper()
                and v["address"]["locality"].upper() == row["city"].upper()
            ):
                logger.debug("  Found matching facility %s...skipping", v["name"])
                skipped_count += 1
                found = True
                break
        if not found:
            facilities_data["facilities"][facility_name] = copy.deepcopy(facility_schema)
            facilities_data["facilities"][facility_name]["source_urls"].append(base_url)
            facilities_data["facilities"][facility_name]["name"] = facility_name
            facilities_data["facilities"][facility_name]["address"]["administrative_area"] = row["state"]
            facilities_data["facilities"][facility_name]["address"]["locality"] = row["city"]
            facilities_data["facilities"][facility_name]["address"]["country"] = "United States"
            facilities_data["facilities"][facility_name]["osm"]["latitude"] = row["latitude"]
            facilities_data["facilities"][facility_name]["osm"]["longitude"] = row["longitude"]
            facilities_data["facilities"][facility_name]["facility_type"]["id"] = row["type_detailed"]
            facilities_data["facilities"][facility_name]["facility_type"]["group"] = row["type_grouped"]
    logger.debug("  Skipped %s facilities", skipped_count)
    if not keep_sheet:
        os.unlink(filename)
    return facilities_data
