import copy
from schemas import facilities_schema
from .agencies import scrape_agencies
from .custom_facilities import insert_additional_facilities
from .facilities_scraper import scrape_facilities
from .field_offices import (
    merge_field_offices,
    scrape_field_offices,
)
from .spreadsheet_load import load_sheet
from .vera_data import collect_vera_facility_data


def facilities_scrape_wrapper(
    keep_sheet: bool = True, force_download: bool = True, skip_vera: bool = False
) -> tuple[dict, dict]:
    agencies = scrape_agencies(keep_sheet, force_download)
    facilities_data = copy.deepcopy(facilities_schema)
    facilities = load_sheet(keep_sheet, force_download)
    facilities_data["facilities"] = copy.deepcopy(facilities)
    facilities_data = scrape_facilities(facilities_data)
    if not skip_vera:
        facilities_data = collect_vera_facility_data(facilities_data, keep_sheet, force_download)
    field_offices = scrape_field_offices()
    facilities_data = merge_field_offices(facilities_data, field_offices)
    facilities_data = insert_additional_facilities(facilities_data)

    return facilities_data, agencies
