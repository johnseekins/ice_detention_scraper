import datetime

facilities_schema = {
    "scraped_date": datetime.datetime.utcnow(),
    "page_updated_date": datetime.datetime.utcnow(),
    "scrape_runtime": 0,
    "enrich_runtime": 0,
    "facilities": [],
}

facility_schema = {
    "address": {
        "street": "",
        "administrative_area": "",
        "country": "",
        "locality": "",
        "postal_code": "",
    },
    "facility_url": "",
    "field_office": "",
    "image_url": "",
    "name": "",
    "phone": "",
    "raw_scrape": "",
    "source_url": "",
    "wikipedia_page_url": "",
    "wikidata_page_url": "",
    "osm_result_url": "",
    "wikipedia_search_query": "",
    "wikidata_search_query": "",
    "osm_search_query": "",
}

# enrichment response object
resp_info_schema = {
    "original_name": "",
    "cleaned_name": "",
    "search_query_steps": [],
    "url": "",
    "method": "none",
}

# enrichment print details
enrichment_print_schema = {
    "wiki_found": 0,
    "wikidata_found": 0,
    "osm_found": 0,
}
