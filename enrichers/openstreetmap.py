import copy
from enrichers.utils import (
    clean_facility_name,
    NOMINATIM_DELAY,
)
from schemas import enrich_resp_schema
import time
from utils import (
    logger,
    session,
)


def search(facility_name: str, address: dict) -> dict:
    search_name = clean_facility_name(facility_name)
    search_url = "https://nominatim.openstreetmap.org/search"
    resp_info = copy.deepcopy(enrich_resp_schema)
    resp_info["enrichment_type"] = "openstreetmap"
    data = []
    if not address:
        logger.debug("No address for %s, simply searching for name", facility_name)
        params = {
            "q": search_name,
            "format": "json",
            "limit": 5,
            "dedupe": 1,
        }
        logger.debug("Searching OSM for %s", search_name)
        resp_info["search_query_steps"].append(search_name)  # type: ignore [attr-defined]
        try:
            response = session.get(search_url, params=params, timeout=15)  # type: ignore [arg-type]
            response.raise_for_status()
            data = response.json()
            time.sleep(NOMINATIM_DELAY)
        except Exception as e:
            logger.debug(" OSM search error for '%s': %s", facility_name, e)
            resp_info["search_query_steps"].append(f"(Failed -> {e})")  # type: ignore [attr-defined]
            return resp_info
    else:
        full_address = (
            f"{address['street']} {address['locality']}, {address['administrative_area']} {address['postal_code']}"
        )
        locality = f"{address['locality']}, {address['administrative_area']} {address['postal_code']}"
        search_url = "https://nominatim.openstreetmap.org/search"
        search_params = {
            "facility_name": {
                "q": f"{search_name} {full_address}",
                "format": "json",
                "limit": 5,
                "dedupe": 1,
            },
            "street_address": {
                "q": f"{full_address}",
                "format": "json",
                "limit": 5,
                "dedupe": 1,
            },
            "locality": {
                "q": f"{locality}",
                "format": "json",
                "limit": 5,
                "dedupe": 1,
            },
        }
        for search_name, params in search_params.items():
            logger.debug("Searching OSM for %s", params["q"])
            resp_info["search_query_steps"].append(params["q"])  # type: ignore [attr-defined]
            try:
                response = session.get(search_url, params=params, timeout=15)  # type: ignore [arg-type]
                response.raise_for_status()
                data = response.json()
                time.sleep(NOMINATIM_DELAY)
            except Exception as e:
                logger.debug(" OSM search error for '%s': %s", facility_name, e)
                resp_info["search_query_steps"].append(f"(Failed -> {e})")  # type: ignore [attr-defined]
                continue
            if data:
                return resp_info
    # when the URL result is a "way" this is usually correct.
    # checks top five results.
    match_terms = ["prison", "detention", "correctional", "jail"]
    for result in data:
        osm_type = result.get("type", "").lower()
        display_name = result.get("display_name", "").lower()
        if any(term in osm_type for term in match_terms) or any(term in display_name for term in match_terms):
            # todo courthouse could be added, or other tags such as "prison:for=migrant" as a clear positive search result.
            osm_id = result.get("osm_id", "")
            osm_type_prefix = result.get("osm_type", "")
            title = result.get("display_name", "")
            if osm_id and osm_type_prefix:
                resp_info["url"] = f"https://www.openstreetmap.org/{osm_type_prefix}/{osm_id}"
                resp_info["title"] = title
                return resp_info
    # fallback to first result
    first_result = data[0]
    logger.debug("Address searches didn't directly find anything, just using the first result: %s", first_result)
    # default to Washington, D.C.?
    lat = first_result.get("lat", "38.89511000")
    lon = first_result.get("lon", "-77.03637000")
    title = first_result.get("display_name", "")
    resp_info["search_query_steps"].append(f"{lat}&{lon}")  # type: ignore [attr-defined]
    if lat and lon:
        resp_info["url"] = f"https://www.openstreetmap.org/?mlat={lat}&mlon={lon}&zoom=15"
        resp_info["title"] = title
    return resp_info
