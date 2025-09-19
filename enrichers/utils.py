# Rate limiting for API calls
NOMINATIM_DELAY = 1.0  # 1 second between requests as per OSM policy
WIKIDATA_DELAY = 0.5  # Be respectful to Wikidata
WIKIPEDIA_DELAY = 0.5  # Be respectful to Wikipedia


def minimal_clean_facility_name(name: str) -> str:
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


def clean_facility_name(name: str) -> str:
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
