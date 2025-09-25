import copy
from ice_scrapers import ice_facility_types
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
        {"match": "Essex Co. Jail, Middleton", "replace": "Essex County Jail", "city": "Middleton"},
        {"match": "Etowah County Jail (AL)", "replace": "Etowah County Jail", "city": "Gadsden"},
        {"match": "Fairfax Co Jail", "replace": "Fairfax County Jail", "city": "Fairfax"},
        {
            "match": "Ft Lauderdale Behavor Hlth Ctr",
            "replace": "Fort Lauderdale Behavioral Health Center",
            "city": "Oakland Park",
        },
        {"match": "Marion Correctional Inst.", "replace": "Marion Correctional Institution", "city": "Ocala"},
        {"match": "Florida St. Pris.", "replace": "Florida State Prison", "city": "Raiford"},
        {"match": "Dade Correctional Inst", "replace": "Dade Correctional Institution", "city": "Florida City"},
        {"match": "Franklin County Jail, VT", "replace": "Franklin County Jail", "city": "Saint Albans"},
        {"match": "Frederick County Det. Cen", "replace": "Frederick County Detention Center", "city": "Frederick"},
        {"match": "Freeborn County Jail, MN", "replace": "Freeborn County Jail", "city": "Albert Lea"},
        {"match": "Fremont County Jail, CO", "replace": "Fremont County Jail", "city": "Canon City"},
        {"match": "Fremont County Jail, WY", "replace": "Fremont County Jail", "city": "Lander"},
        {"match": "Grand Forks County Correc", "replace": "Grand Forks County Corrections", "city": "Grand Forks"},
        {"match": "Grand Forks Co. Juvenile", "replace": "Grand Forks County Juvenile", "city": "Grand Forks"},
        {"match": "Haile Det. Center", "replace": "Haile Detention Center", "city": "Caldwell"},
        {"match": "Hampden Co.House Of Corr.", "replace": "Hampden County House of Corrections", "city": "Ludlow"},
        {
            "match": "Henderson County Det. Fac.",
            "replace": "Henderson County Detention Facility",
            "city": "Hendersonville",
        },
        {"match": "Hel District Custody", "replace": "Helena District Custody", "city": "Helena"},
        {"match": "Houston Contract Det.Fac.", "replace": "Houston Contract Detention Facility", "city": "Houston"},
        {"match": "Howard County Det Cntr", "replace": "Howard County Detention Center", "city": "Jessup"},
        {"match": "In Dept. Of Corrections", "replace": "Indiana Department of Corrections", "city": "Indianapolis"},
        {"match": "Beth Israel Hospital, Manhattan", "replace": "Beth Israel Hospital Manhattan", "city": "New York"},
        {"match": "Kent Co.,Grand Rapids,MI", "replace": "Kent County Jail", "city": "Grand Rapids"},
        {"match": "Kern County Jail (Lerdo)", "replace": "Kern County Jail", "city": "Bakersfield"},
        {"match": "Lackawana Cnty Jail, PA", "replace": "Lackawana County Jail", "city": "Scranton"},
        {"match": "Las Colinas Women Det Fac", "replace": "Las Colinas Women's Detention Facility", "city": "Santee"},
        {"match": "Lawrence Co. Jail, SD", "replace": "Lawrence County Jail", "city": "Deadwood"},
        {"match": "Lehigh County Jail, PA", "replace": "Lehigh County Jail", "city": "Allentown"},
        {"match": "Macomb Co.Mt.Clemens,MI.", "replace": "Macomb County Jail", "city": "Mount Clemens"},
        {"match": "Bwater St Hosp Bridgewate", "replace": "Bridgewater State Hospital", "city": "Bridgewater"},
        {"match": "Meade Co. Jail, SD", "replace": "Meade County Jail", "city": "Sturgis"},
        {"match": "Mecklenburg (NC) Co Jail", "replace": "Mecklenburg County Jail", "city": "Charlotte"},
        {"match": "Mountrail Co. Jail, ND", "replace": "Mountrail County Jail", "city": "Stanley"},
        {
            "match": "Saipan Department Of Corrections",
            "replace": "SAIPAN DEPARTMENT OF CORRECTIONS (SUSUPE)",
            "city": "Saipan",
        },
        {"match": "Sitka City Jail, Sitka AK", "replace": "Sitka City Jail", "city": "Sitka"},
        {"match": "Leavenworth USP", "replace": "Leavenworth US Penitentiary", "city": "Leavenworth"},
        {"match": "Limestone County Jail", "replace": "Limestone County Detention Center", "city": "Groesbeck"},
        {"match": "FCI Berlin", "replace": "Berlin Fed. Corr. Inst.", "city": "Berlin"},
        {"match": "Nassau Co Correc Center", "replace": "Nassau County Correctional Center", "city": "East Meadow"},
        {"match": "Riverside Reg Jail", "replace": "Riverside Regional Jail", "city": "Hopewell"},
        {"match": "T Don Hutto Residential Center", "replace": "T Don Hutto Detention Center", "city": "Taylor"},
        {"match": "Desert View", "replace": "Desert View Annex", "city": "Adelanto"},
        {"match": "Alamance Co. Det. Facility", "replace": "Alamance County Detention Facility", "city": "Graham"},
        {"match": "Hall County Sheriff", "replace": "Hall County Department of Corrections", "city": "Grand Island"},
        {"match": "Hall County Sheriff", "replace": "Hall County Department of Corrections", "city": "Grand Island"},
        {
            "match": "Dallas County Jail-Lew Sterrett",
            "replace": "Dallas County Jail - Lew Sterrett Justice Center",
            "city": "Dallas",
        },
        {"match": "Hardin Co Jail", "replace": "Hardin County Jail", "city": "Eldora"},
    ]
    fixed = False
    for m in matches:
        if m["match"] == name and m["city"] == city:
            fixed = True
            name = m["replace"]
            break
    return name, fixed


def _vera_city_fixes(city: str, state: str) -> Tuple[str, bool]:
    """There are a few cases where getting a state match requires some munging"""
    matches = [
        {"match": "Saipan", "replace": "Susupe, Saipan", "city": "MP"},
    ]
    fixed = False
    for m in matches:
        if m["match"] == city and m["city"] == state:
            fixed = True
            city = m["replace"]
            break
    return city, fixed


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
    fixed = 0
    for row in df.iter_rows(named=True):
        if not row["state"] or not row["city"]:
            logger.warning("  Skipping Vera row with missing values: %s", row)
            skipped_count += 1
            continue
        found = False
        facility_name, fixed_name = _vera_name_fixes(row["detention_facility_name"], row["city"])
        city, fixed_city = _vera_city_fixes(row["city"], row["state"])
        if fixed_name or fixed_city:
            fixed += 1
        addr_str = f"{facility_name},{city},{row['state']},United States"
        for k, v in facilities_data["facilities"].items():
            if (
                v["name"].upper() == facility_name.upper()
                and v["address"]["administrative_area"].upper() == row["state"].upper()
                and v["address"]["locality"].upper() == city.upper()
            ):
                logger.debug("  Found matching facility %s...", v["name"])
                facilities_data["facilities"][k]["osm"]["latitude"] = row["latitude"]
                facilities_data["facilities"][k]["osm"]["longitude"] = row["longitude"]
                facilities_data["facilities"][k]["vera_id"] = row["detention_facility_code"]
                matched_count += 1
                found = True
                break
        if not found:
            facilities_data["facilities"][addr_str] = copy.deepcopy(facility_schema)
            facilities_data["facilities"][addr_str]["source_urls"].append(base_url)
            facilities_data["facilities"][addr_str]["name"] = facility_name
            facilities_data["facilities"][addr_str]["address"]["administrative_area"] = row["state"]
            facilities_data["facilities"][addr_str]["address"]["locality"] = city
            facilities_data["facilities"][addr_str]["address"]["country"] = "United States"
            facilities_data["facilities"][addr_str]["address_str"] = addr_str
            facilities_data["facilities"][addr_str]["osm"]["latitude"] = row["latitude"]
            facilities_data["facilities"][addr_str]["osm"]["longitude"] = row["longitude"]
            facilities_data["facilities"][addr_str]["facility_type"]["id"] = row["type_detailed"]
            facilities_data["facilities"][addr_str]["facility_type"]["group"] = row["type_grouped"]
            facilities_data["facilities"][addr_str]["vera_id"] = row["detention_facility_code"]
            ft_details = ice_facility_types.get(row["type_detailed"], {})
            if ft_details:
                facilities_data["facilities"][addr_str]["facility_type"]["description"] = ft_details["description"]
                facilities_data["facilities"][addr_str]["facility_type"]["expanded_name"] = ft_details["expanded_name"]

    logger.info(
        "  Found %s facilities: Skipped %s, Matched %s, corrected names on %s",
        df.height,
        skipped_count,
        matched_count,
        fixed,
    )
    if not keep_sheet:
        os.unlink(filename)
    return facilities_data
