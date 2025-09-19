from concurrent.futures import ProcessPoolExecutor
import copy
from enrichers import (
    wikidata,
    wikipedia,
    openstreetmap,
)
from schemas import facilities_schema
import time
from utils import logger


def enrich_facility_data(facilities_data: dict, workers: int = 3) -> dict:
    """wrapper function for multiprocessing of facility enrichment"""
    start_time = time.time()
    logger.info("Starting data enrichment with external sources...")
    enriched_data = copy.deepcopy(facilities_schema)
    total = len(facilities_data["facilities"])
    processed = 0

    with ProcessPoolExecutor(max_workers=workers) as pool:
        for res in pool.map(enrich_facility, facilities_data["facilities"].items()):
            enriched_data["facilities"][res[0]] = res[1]  # type: ignore [index]
            processed += 1
            logger.info("  -> Finished %s, %s/%s completed", res[1]["name"], processed, total)

    logger.info("Data enrichment completed!")
    enriched_data["enrich_runtime"] = time.time() - start_time
    logger.info(" Completed in %s seconds", enriched_data["enrich_runtime"])
    return enriched_data


def enrich_facility(facility_data: tuple) -> tuple:
    """enrich a single facility"""
    facility_id, facility = facility_data
    facility_name = facility["name"]
    logger.info("Enriching facility %s...", facility_name)
    enriched_facility = copy.deepcopy(facility)

    wiki = wikipedia.search(facility_name)
    wd = wikidata.search(facility_name)
    osm = openstreetmap.search(facility_name, facility.get("address", {}))
    enriched_facility["wikipedia_page_url"] = wiki.get("url", "")
    enriched_facility["wikipedia_search_query"] = wiki.get("search_query_steps", "")
    enriched_facility["wikidata_page_url"] = wd.get("url", "")
    enriched_facility["wikidata_search_query"] = wd.get("search_query_steps", "")
    enriched_facility["osm_result_url"] = osm.get("url", "")
    enriched_facility["osm_search_query"] = osm.get("search_query_steps", "")

    logger.debug(enriched_facility)
    return facility_id, enriched_facility
