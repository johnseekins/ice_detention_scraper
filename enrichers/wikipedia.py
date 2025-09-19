import copy
from enrichers.utils import (
    clean_facility_name,
    minimal_clean_facility_name,
    WIKIPEDIA_DELAY,
)
from schemas import enrich_resp_schema
import time
from urllib.parse import quote
from utils import (
    logger,
    session,
)

root_search = "https://en.wikipedia.org/wiki/"


def search(facility_name: str) -> dict:
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
    resp_info["enrichment_type"] = "wikipedia"
    # Clean facility name for search
    search_name: str = clean_facility_name(facility_name)
    logger.debug("Searching Wikipedia for %s", facility_name)
    # Try direct page access first (replace space with underscores is the only change)
    wiki_url = f"{root_search}{quote(facility_name.replace(' ', '_').replace('|', '_'))}"
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
        wiki_url = f"{root_search}{quote(facility_name.replace(' ', '_').replace('|', '_'))}"
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
        minimal_clean = minimal_clean_facility_name(facility_name)
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
                    final_url = f"{root_search}{quote(page_title.replace(' ', '_'))}"

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
