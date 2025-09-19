import copy
from enrichers.utils import (
    clean_facility_name,
    WIKIDATA_DELAY,
)
from schemas import enrich_resp_schema
import time
from utils import (
    logger,
    session,
)


def search(facility_name: str) -> dict:
    # Fetches 3 results based on clean_facility_name (not exact name). todo: needs adjustment.
    # Falls back to first result (usually truncated, eg. county)
    search_name_fallback = clean_facility_name(facility_name)
    logger.debug("Searching wikidata for %s and %s", facility_name, search_name_fallback)
    search_url = "https://www.wikidata.org/w/api.php"
    params = {
        "action": "wbsearchentities",
        "search": facility_name,
        "language": "en",
        "format": "json",
        "limit": 3,
    }
    resp_info = copy.deepcopy(enrich_resp_schema)
    resp_info["query_type"] = "wikidata"
    data = {}
    try:
        response = session.get(search_url, params=params, timeout=10)  # type: ignore [arg-type]
        response.raise_for_status()
        data = response.json()
        time.sleep(WIKIDATA_DELAY)
    except Exception as e:
        logger.debug("  Wikidata search error for '%s': %s", facility_name, e)
        resp_info["search_query_steps"].append(f"(Failed -> {e})")  # type: ignore [attr-defined]
    if not data.get("search"):
        params["search"] = search_name_fallback
        resp_info["search_query_steps"].append(search_name_fallback)  # type: ignore [attr-defined]
        try:
            response = session.get(search_url, params=params, timeout=10)  # type: ignore [arg-type]
            response.raise_for_status()
            data = response.json()
            time.sleep(WIKIDATA_DELAY)
        except Exception as e:
            logger.debug("  Wikidata search error for '%s': %s", facility_name, e)
            resp_info["search_query_steps"].append(f"(Failed -> {e})")  # type: ignore [attr-defined]
    if not data.get("search"):
        return resp_info
    match_terms = ["prison", "detention", "correctional", "jail", "facility", "processing"]
    for result in data["search"]:
        description = result.get("description", "").lower()
        if any(term in description for term in match_terms):
            resp_info["url"] = f"https://www.wikidata.org/wiki/{result['id']}"
            resp_info["title"] = result.get("label", "")
            return resp_info
    # fallback to first result
    first = data["search"][0]
    logger.debug("   Closer matching failed, falling back to first result %s", first)
    resp_info["url"] = f"https://www.wikidata.org/wiki/{result['id']}"
    resp_info["title"] = result.get("label", "")
    return resp_info
