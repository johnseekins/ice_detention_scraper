import copy
import datetime

facilities_schema = {
    "scraped_date": datetime.datetime.now(datetime.UTC),
    "scrape_runtime": 0,
    "enrich_runtime": 0,
    "facilities": {},
}

field_offices_schema: dict = {
    "field_offices": {},
    "scraped_date": datetime.datetime.now(datetime.UTC),
    "scrape_runtime": 0,
}

field_office_schema: dict = {
    "name": "",
    "field_office": "",
    "id": "",
    "address_str": "",
    "address": {
        "administrative_area": "",
        "country": "",
        "locality": "",
        "postal_code": "",
        "street": "",
    },
    "aor": "",
    "email": "",
    "source_urls": [],
}

# default keys to "false"-y values so we can merge easier
facility_schema: dict = {
    "address": {
        "administrative_area": "",
        "country": "",
        "locality": "",
        "postal_code": "",
        "street": "",
    },
    "address_str": "",
    "_repaired_record": False,
    "field_office": copy.deepcopy(field_office_schema),
    "image_url": "",
    "name": "",
    "phone": "",
    "source_urls": [],
    "wikipedia": {
        "page_url": "",
        "search_query": "",
    },
    "wikidata": {
        "page_url": "",
        "search_query": "",
    },
    "osm": {
        "url": "",
        "latitude": 0,
        "longitude": 0,
        "search_query": "",
    },
    "page_updated_date": None,
    "population": {
        "male": {
            "allowed": False,
            "criminal": 0,
            "non_criminal": 0,
        },
        "female": {
            "allowed": False,
            "criminal": 0,
            "non_criminal": 0,
        },
        "ice_threat_level": {
            "level_1": 0,
            "level_2": 0,
            "level_3": 0,
            "none": 0,
        },
    },
    "facility_type": {
        "id": "",
        "description": "",
        "expanded_name": "",
        "housing": {
            "mandatory": 0,
            "guaranteed_min": 0,
        },
    },
    "inspection": {
        "last_date": None,
        "last_rating": "",
    },
    "avg_stay_length": 0,
}


# enrichment response object
enrich_resp_schema = {
    "original_name": "",
    "cleaned_name": "",
    "search_query_steps": [],
    "url": "",
    "details": {},
    "method": "none",
    "enrichment_type": "",
}

# enrichment print details
enrichment_print_schema = {
    "wiki_found": 0,
    "wikidata_found": 0,
    "osm_found": 0,
}

supported_output_types = ["csv", "json", "xlsx", "parquet"]
