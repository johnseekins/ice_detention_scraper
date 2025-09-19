from concurrent.futures import ProcessPoolExecutor
import copy
from schemas import (
    facilities_schema,
    enrich_resp_schema,
)
import time
from urllib.parse import quote
from utils import (
    logger,
    session,
)
# ExternalDataEnricher class for enrichment logic

# Rate limiting for API calls
NOMINATIM_DELAY = 1.0  # 1 second between requests as per OSM policy
WIKIPEDIA_DELAY = 0.5  # Be respectful to Wikipedia
WIKIDATA_DELAY = 0.5  # Be respectful to Wikidata


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

    wiki = _search_wikipedia(facility_name)
    wd = _search_wikidata(facility_name)
    osm = _search_openstreetmap(facility_name, facility.get("address", {}))
    enriched_facility["wikipedia_page_url"] = wiki.get("url", "")
    enriched_facility["wikipedia_search_query"] = wiki.get("search_query_steps", "")
    enriched_facility["wikidata_page_url"] = wd.get("url", "")
    enriched_facility["wikidata_search_query"] = wd.get("search_query_steps", "")
    enriched_facility["osm_result_url"] = osm.get("url", "")
    enriched_facility["osm_search_query"] = osm.get("search_query_steps", "")

    logger.debug(enriched_facility)
    return facility_id, enriched_facility


def _search_wikipedia(facility_name: str) -> dict:
    """Search Wikipedia for facility and return final URL after redirects"""
    facility_terms: list = [
        "detention",
        "prison",
        "jail",
        "correctional",
        "penitentiary",
        "facility",
        "center",
        "complex",
        "institution",
        "processing",
    ]
    resp_info = copy.deepcopy(enrich_resp_schema)
    resp_info["query_type"] = "wikipedia"
    # Clean facility name for search
    search_name: str = _clean_facility_name(facility_name)
    logger.debug("Searching Wikipedia for %s", facility_name)
    # Try direct page access first (replace space with underscores is the only change)
    wiki_url = f"https://en.wikipedia.org/wiki/{quote(facility_name.replace(' ', '_').replace('|', '_'))}"
    resp_info["search_query_steps"].append(wiki_url)  # type: ignore [attr-defined]
    initial_response = False
    try:
        response = session.get(wiki_url, allow_redirects=True, timeout=10)
        response.raise_for_status()
        initial_response = True
        time.sleep(WIKIPEDIA_DELAY)
    except Exception as e:
        logger.debug("  Wikipedia search error for '%s': %s", wiki_url, e)
        resp_info["search_query_steps"].append(f"(Failed -> {e})")  # type: ignore [attr-defined]
    if response.status_code != 200:
        wiki_url = f"https://en.wikipedia.org/wiki/{quote(facility_name.replace(' ', '_').replace('|', '_'))}"
        resp_info["search_query_steps"].append(wiki_url)  # type: ignore [attr-defined]
        try:
            response = session.get(wiki_url, allow_redirects=True, timeout=10)
            response.raise_for_status()
            initial_response = True
            time.sleep(WIKIPEDIA_DELAY)
        except Exception as e:
            logger.debug("  Wikipedia search error for '%s': %s", wiki_url, e)
            resp_info["search_query_steps"].append(f"(Failed -> {e})")  # type: ignore [attr-defined]

    if initial_response:
        # Check if we got a real article (not a disambiguation or search page)
        page_text = response.text.lower()

        # Enhanced false positive detection
        false_positive_indicators = [
            "may refer to:",  # Disambiguation page
            "did you mean",  # Search suggestion
            "disambiguation)",  # Disambiguation in title
            "is a disambiguation",  # Disambiguation description
            "this article is about",  # Generic topic page
            "for other uses",  # Disambiguation header
        ]

        # Additional check: ensure result is actually about a detention facility
        is_false_positive = any(indicator in page_text for indicator in false_positive_indicators)
        has_facility_context = any(indicator in page_text for indicator in facility_terms)

        # Only accept if it's not a false positive AND has facility context
        # OR if the cleaned name still contains facility-related terms
        facility_terms_in_name = any(term in search_name.lower() for term in facility_terms)

        if not is_false_positive and (has_facility_context or facility_terms_in_name):
            resp_info["url"] = response.url
            resp_info["method"] = "direct_access"
            return resp_info
        else:
            resp_info["search_query_steps"].append("[REJECTED: false_positive or no_facility_context]")  # type: ignore [attr-defined]

    logger.debug("  Falling back to Wikipedia API searches for %s and %s", facility_name, search_name)
    # If direct access fails, try Wikipedia search API with original name first
    search_queries = [
        facility_name,  # Original full name
        search_name,  # Cleaned name
    ]

    # Add variations if the name contains common facility types
    if any(term in facility_name.lower() for term in ["county", "parish"]):
        # For county facilities, try with facility type intact
        minimal_clean = _minimal_clean_facility_name(facility_name)
        if minimal_clean != search_name:
            search_queries.append(minimal_clean)

    for query_attempt, search_query in enumerate(search_queries):
        resp_info["search_query_steps"].append(search_query)  # type: ignore [attr-defined]

        search_url = "https://en.wikipedia.org/w/api.php"
        params = {
            "action": "query",
            "list": "search",
            "srsearch": search_query,
            "format": "json",
            "srlimit": 5,
        }

        try:
            response = session.get(search_url, params=params, timeout=10)  # type: ignore [arg-type]
            response.raise_for_status()
            data = response.json()
            time.sleep(WIKIPEDIA_DELAY)
        except Exception as e:
            logger.debug("   Wikipedia search for %s failed: %s", search_url, e)
            resp_info["search_query_steps"].append(f"(Failed: {search_query} -> {e})")  # type: ignore [attr-defined]
            continue

        if data.get("query", {}).get("search"):
            # Examine each result for relevance
            # Arbitrary but sort of useful.
            for result in data["query"]["search"]:
                page_title = result["title"]
                snippet = result.get("snippet", "").lower()

                # Enhanced relevance scoring
                relevance_score = 0

                # Check if title contains facility-related terms
                title_lower = page_title.lower()

                for term in facility_terms:
                    if term in title_lower:
                        relevance_score += 2

                # Check if snippet mentions detention/prison context
                detention_context = [
                    "detention",
                    "prison",
                    "jail",
                    "correctional",
                    "inmates",
                    "custody",
                    "incarceration",
                    "processing",
                ]
                for term in detention_context:
                    if term in snippet:
                        relevance_score += 1

                # Penalize generic location pages without facility context
                generic_indicators = ["county", "city", "town", "village", "township"]
                if any(term in title_lower for term in generic_indicators) and not any(
                    term in title_lower for term in facility_terms
                ):
                    relevance_score -= 3

                # Check name similarity (basic heuristic)
                original_tokens = set(facility_name.lower().split())
                title_tokens = set(page_title.lower().split())
                common_tokens = original_tokens.intersection(title_tokens)

                if len(common_tokens) >= 2:
                    relevance_score += len(common_tokens)

                # Only accept results with positive relevance score
                if relevance_score > 0:
                    final_url = f"https://en.wikipedia.org/wiki/{quote(page_title.replace(' ', '_'))}"

                    # Verify the page exists and isn't a redirect to something unrelated
                    try:
                        verify_response = session.get(final_url, allow_redirects=True, timeout=10)
                        verify_response.raise_for_status()
                        time.sleep(WIKIPEDIA_DELAY)
                    except Exception as e:
                        logger.debug("    Wikipedia query for %s failed: %s", final_url, e)
                        resp_info["search_query_steps"].append(final_url)  # type: ignore [attr-defined]
                    else:
                        resp_info["url"] = verify_response.url
                        resp_info["method"] = f"api_search_attempt_{query_attempt + 1}_score_{relevance_score}"
                        resp_info["search_query_steps"].append(f"-> {page_title} (score: {relevance_score})")  # type: ignore [attr-defined]
                        return resp_info

        # If this search query didn't work, try the next one
        if query_attempt < len(search_queries) - 1:
            resp_info["search_query_steps"].append("[no_relevant_results] ->")  # type: ignore [attr-defined]

    # No results found
    resp_info["search_query_steps"].append("[no_results_found]")  # type: ignore [attr-defined]
    resp_info["method"] = "failed"

    return resp_info


def _minimal_clean_facility_name(name: str) -> str:
    """Minimal cleaning that preserves important context like 'County Jail'"""
    cleaned = name

    # Remove pipe separators and take the main name
    if "|" in cleaned:
        parts = cleaned.split("|")
        cleaned = max(parts, key=len).strip()

    # Only remove very generic suffixes, keep specific ones like "County Jail"
    generic_suffixes = [
        "Service Processing Center",
        "ICE Processing Center",
        "Immigration Processing Center",
        "Contract Detention Facility",
        "Adult Detention Facility",
    ]

    for suffix in generic_suffixes:
        if cleaned.endswith(suffix):
            cleaned = cleaned[: -len(suffix)].strip()
            break

    return cleaned


def _search_wikidata(facility_name: str) -> dict:
    # Fetches 3 results based on _clean_facility_name (not exact name). todo: needs adjustment.
    # Falls back to first result (usually truncated, eg. county)
    search_name_fallback = _clean_facility_name(facility_name)
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


def _search_openstreetmap(facility_name: str, address: dict) -> dict:
    search_name = _clean_facility_name(facility_name)
    search_url = "https://nominatim.openstreetmap.org/search"
    resp_info = copy.deepcopy(enrich_resp_schema)
    resp_info["query_type"] = "openstreetmap"
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
                break
            time.sleep(NOMINATIM_DELAY)
        if not data:
            resp_info["search_query_steps"].append("No results found")  # type: ignore [attr-defined]
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


def _clean_facility_name(name: str) -> str:
    """Clean facility name for better search results"""
    # Remove common suffixes and prefixes that might interfere with search
    # This function may not be helpful - may be counterproductive.
    cleaned = name

    # Remove pipe separators and take the main name
    if "|" in cleaned:
        parts = cleaned.split("|")
        # Take the longer part as it's likely the full name
        cleaned = max(parts, key=len).strip()

    # Remove common facility type suffixes for broader search
    suffixes_to_remove = [
        "Detention Center",
        "Processing Center",
        "Correctional Center",
        "Correctional Facility",
        "Detention Facility",
        "Service Processing Center",
        "ICE Processing Center",
        "Immigration Processing Center",
        "Adult Detention Facility",
        "Contract Detention Facility",
        "Regional Detention Center",
        "County Jail",
        "County Detention Center",
        "Sheriff's Office",
        "Justice Center",
        "Safety Center",
        "Jail Services",
        "Correctional Complex",
        "Public Safety Complex",
    ]

    for suffix in suffixes_to_remove:
        if cleaned.endswith(suffix):
            cleaned = cleaned[: -len(suffix)].strip()
            break
    return cleaned
