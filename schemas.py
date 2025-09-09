import datetime

facilities_schema = {
    "scraped_date": datetime.datetime.now(datetime.UTC),
    "scrape_runtime": 0,
    "enrich_runtime": 0,
    "facilities": {},
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
    "_repaired_record": False,
    "field_office": "",
    "image_url": "",
    "name": "",
    "phone": "",
    "raw_scrape": "",
    "source_urls": [],
    "wikipedia_page_url": "",
    "wikidata_page_url": "",
    "osm_result_url": "",
    "wikipedia_search_query": "",
    "wikidata_search_query": "",
    "osm_search_query": "",
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
    },
    "facility_type": "",
    "inspection_date": None,
    "avg_stay_length": 0,
}

# extracted from https://www.ice.gov/doclib/detention/FY25_detentionStats08292025.xlsx 2025-09-07
ice_facility_types = {
    "BOP": {
        "expanded_name": "Federal Bureau of Prisons",
        "description": "A facility operated by the Federal Bureau of Prisons",
    },
    "DIGSA": {
        "expanded_name": "Dedicated Intergovernmental Service Agreement",
        "decsription": "A publicly-owned facility operated by state/local government(s), or private contractors, in which ICE contracts to use all bed space via a Dedicated Intergovernmental Service Agreement; or facilities used by ICE pursuant to Inter-governmental Service Agreements, which house only ICE detainees – typically these are operated by private contractors pursuant to their agreements with local governments.",
    },
    "IGSA": {
        "expanded_name": "Intergovernmental Service Agreement",
        "description": "A publicly-owned facility operated by state/local government(s), or private contractors, in which ICE contracts for bed space via an Intergovernmental Service Agreement; or local jails used by ICE pursuant to Inter-governmental Service Agreements, which house both ICE and non-ICE detainees, typically county prisoners awaiting trial or serving short sentences, but sometimes also USMS prisoners.",
    },
    "SPC": {
        "expanded_name": "Service Processing Center",
        "description": "A facility owned by the government and staffed by a combination of federal and contract employees.",
    },
    "USMS": {
        "expanded_name": "United States Marshals Service",
        "description": "A facility primarily contracted with the USMS for housing of USMS detainees, in which ICE contracts with the USMS for bed space.",
    },
    "USMSIGA": {
        "expanded_name": "USMS Intergovernmental Agreement",
        "description": "A USMS Intergovernmental Agreement in which ICE agrees to utilize an already established US Marshal Service contract.",
    },
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
