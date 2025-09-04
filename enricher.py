import requests
from requests.adapters import HTTPAdapter
import time
from urllib.parse import quote
import urllib3
from utils import logger

# ExternalDataEnricher class for enrichment logic

# Rate limiting for API calls
NOMINATIM_DELAY = 1.0  # 1 second between requests as per OSM policy
WIKIPEDIA_DELAY = 0.5  # Be respectful to Wikipedia
WIKIDATA_DELAY = 0.5  # Be respectful to Wikidata


class ExternalDataEnricher(object):
    def __init__(self, debug_wikipedia=False, debug_wikidata=False, debug_osm=False):
        _retry_strategy = urllib3.Retry(
            total=4,
            backoff_factor=1,
        )
        _adapter = HTTPAdapter(max_retries=_retry_strategy)
        self.session = requests.Session()
        self.session.mount("https://", _adapter)
        self.session.mount("http://", _adapter)
        self.session.headers.update(
            {"User-Agent": "ICE-Facilities-Research/1.0 (Educational Research Purpose)"}
        )
        self.debug_wikipedia = debug_wikipedia
        self.debug_wikidata = debug_wikidata
        self.debug_osm = debug_osm

    def enrich_facility_data(self, facilities_data):
        logger.info("Starting data enrichment with external sources...")
        enriched_data = []
        total = len(facilities_data)

        for i, facility in enumerate(facilities_data):
            logger.info(
                "Processing facility %s/%s: %s...", i + 1, total, facility["name"][:50]
            )
            enriched_facility = facility.copy()
            base_enrichment = {
                "wikipedia_page_url": False,
                "wikidata_page_url": False,
                "osm_result_url": False,
            }
            if self.debug_wikipedia:
                base_enrichment["wikipedia_search_query"] = ""  # todo expects bool.
            if self.debug_wikidata:
                base_enrichment["wikidata_search_query"] = ""  # todo expects bool.
            if self.debug_osm:
                base_enrichment["osm_search_query"] = ""  # todo expects bool.

            enriched_facility.update(base_enrichment)

            facility_name = facility["name"]

            # Wikipedia search # todo refactor to method
            try:
                wiki_result = self._search_wikipedia(facility_name)
                if isinstance(wiki_result, dict):  # Debug mode returns dict
                    enriched_facility["wikipedia_page_url"] = (
                        wiki_result["url"] if wiki_result["url"] else False
                    )
                    if self.debug_wikipedia:
                        enriched_facility["wikipedia_search_query"] = wiki_result[
                            "search_query"
                        ]
                else:
                    enriched_facility["wikipedia_page_url"] = (
                        wiki_result if wiki_result else False
                    )
                time.sleep(WIKIPEDIA_DELAY)
            except Exception as e:
                logger.error(" Wikipedia search error: %s", e)
                enriched_facility["wikipedia_page_url"] = False
                if self.debug_wikipedia:
                    enriched_facility["wikipedia_search_query"] = f"ERROR: {str(e)}"

            # Wikidata search # todo refactor to method
            try:
                wikidata_result = self._search_wikidata(facility_name)
                if isinstance(
                    wikidata_result, dict
                ):  # debug returns dict with url and title
                    enriched_facility["wikidata_page_url"] = wikidata_result.get(
                        "url", False
                    )
                    if self.debug_wikidata:
                        enriched_facility["wikidata_search_query"] = (
                            wikidata_result.get("title", "")
                        )
                else:
                    enriched_facility["wikidata_page_url"] = (
                        wikidata_result if wikidata_result else False
                    )
                time.sleep(WIKIDATA_DELAY)
            except Exception as e:
                logger.error(" Wikidata search error: %s", e)
                enriched_facility["wikidata_page_url"] = False
                if self.debug_wikidata:
                    enriched_facility["wikidata_search_query"] = f"ERROR: {str(e)}"

            # OpenStreetMap search # todo refactor to method
            try:
                osm_result = self._search_openstreetmap(
                    facility_name, facility.get("address", "")
                )
                if isinstance(
                    osm_result, dict
                ):  # debug returns dict with url and title
                    enriched_facility["osm_result_url"] = osm_result.get("url", False)
                    if self.debug_osm:
                        enriched_facility["osm_search_query"] = osm_result.get(
                            "title", ""
                        )
                else:
                    enriched_facility["osm_result_url"] = (
                        osm_result if osm_result else False
                    )
                time.sleep(NOMINATIM_DELAY)
            except Exception as e:
                logger.error(" OSM search error: %s", e)
                enriched_facility["osm_result_url"] = False
                if self.debug_osm:
                    enriched_facility["osm_search_query"] = f"ERROR: {str(e)}"

            enriched_data.append(enriched_facility)

            # do we need the "progress bar" if we show the count in the beginning message?
            # if (i + 1) % 10 == 0:
            #     logger.info(" Progress: %s/%s facilities processed", i + 1, total)

        logger.info("Data enrichment completed!")
        return enriched_data

    def _search_wikipedia(self, facility_name):
        """Search Wikipedia for facility and return final URL after redirects"""
        # Clean facility name for search
        search_name = self._clean_facility_name(facility_name)

        # Store original and cleaned names for debug
        debug_info = {
            "original_name": facility_name,
            "cleaned_name": search_name,
            "search_query": search_name,
            "url": None,
            "method": "none",
        }

        # Try direct page access first (replace space with underscores is the only change)
        wiki_url = (
            f"https://en.wikipedia.org/wiki/{quote(search_name.replace(' ', '_'))}"
        )
        try:
            response = self.session.get(wiki_url, allow_redirects=True, timeout=10)

            if response.status_code == 200:
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
                facility_indicators = [
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

                is_false_positive = any(
                    indicator in page_text for indicator in false_positive_indicators
                )
                has_facility_context = any(
                    indicator in page_text for indicator in facility_indicators
                )

                # Only accept if it's not a false positive AND has facility context
                # OR if the cleaned name still contains facility-related terms
                facility_terms_in_name = any(
                    term in search_name.lower() for term in facility_indicators
                )

                if not is_false_positive and (
                    has_facility_context or facility_terms_in_name
                ):
                    debug_info["url"] = response.url
                    debug_info["method"] = "direct_access"
                    if self.debug_wikipedia:
                        return debug_info
                    return response.url
                else:
                    if self.debug_wikipedia:
                        debug_info["search_query"] += (
                            " [REJECTED: false_positive or no_facility_context]"
                        )

            # If direct access fails, try Wikipedia search API with original name first
            search_queries = [
                facility_name,  # Original full name
                search_name,  # Cleaned name
            ]

            # Add variations if the name contains common facility types
            if any(term in facility_name.lower() for term in ["county", "parish"]):
                # For county facilities, try with facility type intact
                minimal_clean = self._minimal_clean_facility_name(facility_name)
                if minimal_clean != search_name:
                    search_queries.append(minimal_clean)

            for query_attempt, search_query in enumerate(search_queries):
                debug_info["search_query"] = search_query

                search_url = "https://en.wikipedia.org/w/api.php"
                params = {
                    "action": "query",
                    "list": "search",
                    "srsearch": search_query,
                    "format": "json",
                    "srlimit": 5,
                }

                response = self.session.get(search_url, params=params, timeout=10)
                data = response.json()

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
                        facility_terms = [
                            "detention",
                            "prison",
                            "jail",
                            "correctional",
                            "penitentiary",
                            "facility",
                            "center",
                            "complex",
                            "processing",
                        ]

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
                        generic_indicators = [
                            "county",
                            "city",
                            "town",
                            "village",
                            "township",
                        ]
                        if any(
                            term in title_lower for term in generic_indicators
                        ) and not any(term in title_lower for term in facility_terms):
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
                            verify_response = self.session.get(
                                final_url, allow_redirects=True, timeout=10
                            )
                            if verify_response.status_code == 200:
                                debug_info["url"] = verify_response.url
                                debug_info["method"] = (
                                    f"api_search_attempt_{query_attempt + 1}_score_{relevance_score}"
                                )
                                debug_info["search_query"] = (
                                    f"{search_query} -> {page_title} (score: {relevance_score})"
                                )

                                if self.debug_wikipedia:
                                    return debug_info
                                return verify_response.url

                # If this search query didn't work, try the next one
                if query_attempt < len(search_queries) - 1:
                    debug_info["search_query"] += " [no_relevant_results] -> "

            # No results found
            debug_info["search_query"] += " [no_results_found]"
            debug_info["method"] = "failed"

            if self.debug_wikipedia:
                return debug_info
            return None

        except Exception as e:
            error_msg = f"ERROR: {str(e)}"
            if self.debug_wikipedia:
                return {
                    "original_name": facility_name,
                    "search_query": error_msg,
                    "url": None,
                    "method": "error",
                }
            logger.error("    Wikipedia search error for '%s': %s", facility_name, e)
            return None

    def _minimal_clean_facility_name(self, name):
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

    def _search_wikidata(self, facility_name):
        # Fetches 3 results based on _clean_facility_name (not exact name). todo: needs adjustment.
        # Falls back to first result (usually truncated, eg. county)
        search_name = self._clean_facility_name(facility_name)
        search_url = "https://www.wikidata.org/w/api.php"
        params = {
            "action": "wbsearchentities",
            "search": search_name,
            "language": "en",
            "format": "json",
            "limit": 3,
        }
        try:
            response = self.session.get(search_url, params=params, timeout=10)
            data = response.json()
            if data.get("search"):
                for result in data["search"]:
                    description = result.get("description", "").lower()
                    if any(
                        term in description
                        for term in [
                            "prison",
                            "detention",
                            "correctional",
                            "jail",
                            "facility",
                            "processing",
                        ]
                    ):
                        if self.debug_wikidata:
                            return {
                                "url": f"https://www.wikidata.org/wiki/{result['id']}",
                                "title": result.get("label", ""),
                            }
                        return f"https://www.wikidata.org/wiki/{result['id']}"
                # fallback to first result
                first = data["search"][0]
                if self.debug_wikidata:
                    return {
                        "url": f"https://www.wikidata.org/wiki/{first['id']}",
                        "title": first.get("label", ""),
                    }
                return f"https://www.wikidata.org/wiki/{first['id']}"
            return None
        except Exception as e:
            logger.error(" Wikidata search error for '%s': %s", facility_name, e)
            if self.debug_wikidata:
                return {"url": None, "title": f"ERROR: {str(e)}"}
            return None

    def _search_openstreetmap(self, facility_name, address):
        # when the URL result is a "way" this is usually correct.
        # checks top five results.
        search_name = self._clean_facility_name(facility_name)
        location_context = ""
        if address:
            parts = address.split(
                ", "
            )  # todo the address between the number and street name should not have a comma.
            if len(parts) >= 3:
                location_context = f", {parts[-3]}, {parts[-2].split()[0]}"
        search_url = "https://nominatim.openstreetmap.org/search"
        params = {
            "q": f"{search_name}{location_context}",
            "format": "json",
            "limit": 5,
            "dedupe": 1,
        }
        try:
            response = self.session.get(search_url, params=params, timeout=15)
            if response.status_code == 200:
                data = response.json()
                if data:
                    for result in data:
                        osm_type = result.get("type", "").lower()
                        display_name = result.get("display_name", "").lower()
                        if any(
                            term in osm_type
                            for term in ["prison", "detention", "correctional"]
                        ) or any(
                            term in display_name
                            for term in ["prison", "detention", "correctional", "jail"]
                        ):
                            # todo courthouse could be added, or other tags such as "prison:for=migrant" as a clear positive search result.
                            osm_id = result.get("osm_id")
                            osm_type_prefix = result.get("osm_type", "")
                            title = result.get("display_name", "")
                            if osm_id and osm_type_prefix:
                                if self.debug_osm:
                                    return {
                                        "url": f"https://www.openstreetmap.org/{osm_type_prefix}/{osm_id}",
                                        "title": title,
                                    }
                                return f"https://www.openstreetmap.org/{osm_type_prefix}/{osm_id}"
                    # fallback to first result
                    first_result = data[0]
                    lat = first_result.get("lat")
                    lon = first_result.get("lon")
                    title = first_result.get("display_name", "")
                    if lat and lon:
                        if self.debug_osm:
                            return {
                                "url": f"https://www.openstreetmap.org/?mlat={lat}&mlon={lon}&zoom=15",
                                "title": title,
                            }
                        return f"https://www.openstreetmap.org/?mlat={lat}&mlon={lon}&zoom=15"
            return None
        except Exception as e:
            logger.error(" OSM search error for '%s': %s", facility_name, e)
            if self.debug_osm:
                return {"url": None, "title": f"ERROR: {str(e)}"}
            return None

    def _clean_facility_name(self, name):
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
