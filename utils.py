# For general helpers, regexes, or shared logic (e.g. phone/address parsing functions).

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

facility_schema = {
    "address": "",
    "administrative_area": "",
    "country": "",
    "facility_url": "",
    "field_office": "",
    "full_address": "",
    "image_url": "",
    "locality": "",
    "name": "",
    "phone": "",
    "postal_code": "",
    "raw_scrape": "",
    "source_url": "",
}
enrichment_schema = [
    "wikipedia_page_url",
    "wikidata_page_url",
    "osm_result_url",
]
debug_schema = [
    "wikipedia_search_query",
    "wikidata_search_query",
    "osm_search_query",
]
