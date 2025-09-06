import csv
import json
from schemas import (
    debug_schema,
    facility_schema,
    enrichment_schema,
)
from utils import logger


def export_to_file(
    facilities_data: dict,
    filename: str = "ice_detention_facilities_enriched",
    file_type: str = "csv",
) -> str:
    if not facilities_data or not facilities_data.get("facilities", []):
        logger.warning("No data to export!")
        return ""

    full_name = f"{filename}.{file_type}"
    try:
        with open(full_name, "w", newline="", encoding="utf-8") as f_out:
            if file_type == "csv":
                base_fields: list = list(facility_schema.keys())
                fieldnames: list = base_fields.copy()

                if any(field in facilities_data["facilities"][0] for field in enrichment_schema):
                    fieldnames.extend(enrichment_schema)

                if any(field in facilities_data["facilities"][0] for field in debug_schema):
                    fieldnames.extend(debug_schema)

                writer = csv.DictWriter(f_out, fieldnames=fieldnames)
                writer.writeheader()
                for facility in facilities_data["facilities"]:
                    row_data = {field: facility.get(field, "") for field in fieldnames}
                    writer.writerow(row_data)
            elif file_type == "json":
                json.dump(facilities_data, f_out, indent=2, sort_keys=True, default=str)
    except Exception as e:
        logger.error("Error writing %s file: %s", file_type, e)
        return ""

    logger.info(
        "%s file '%s.%s' created successfully with %s facilities.",
        file_type,
        filename,
        file_type,
        len(facilities_data["facilities"]),
    )
    return filename


def print_summary(facilities_data: dict) -> None:
    """Print summary statistics about the facilities"""
    if not facilities_data:
        logger.info("No data to summarize!")
        logger.info("\n=== ICE Detention Facilities Scraper: Run completed ===")
        return

    total_facilities = len(facilities_data["facilities"])
    logger.info("\n=== ICE Detention Facilities Scraper Summary ===")
    logger.info("Scraped data at %s", facilities_data["scraped_date"])
    logger.info("ice.gov data updated at %s", facilities_data["page_updated_date"])
    logger.info("Total facilities: %s", total_facilities)

    # Count by field office
    field_offices: dict = {}
    for facility in facilities_data["facilities"]:
        office = facility.get("field_office", "Unknown")
        field_offices[office] = field_offices.get(office, 0) + 1

    logger.info("\nFacilities by Field Office:")
    for office, count in sorted(field_offices.items(), key=lambda x: x[1], reverse=True):
        logger.info("  %s: %s", office, count)

    # Check enrichment data if available
    if "wikipedia_page_url" in facilities_data["facilities"][0]:
        wiki_found = sum(
            1 for f in facilities_data["facilities"] if f.get("wikipedia_page_url") and f["wikipedia_page_url"]
        )
        wikidata_found = sum(
            1 for f in facilities_data["facilities"] if f.get("wikidata_page_url") and f["wikidata_page_url"]
        )
        osm_found = sum(1 for f in facilities_data["facilities"] if f.get("osm_result_url") and f["osm_result_url"])

        logger.info("\n=== External Data Enrichment Results ===")
        logger.info(
            "Wikipedia pages found: %s/%s (%s%%)",
            wiki_found,
            total_facilities,
            wiki_found / total_facilities * 100,
        )
        logger.info(
            "Wikidata entries found: %s/%s (%s%%)",
            wikidata_found,
            total_facilities,
            wikidata_found / total_facilities * 100,
        )
        logger.info(
            "OpenStreetMap results found: %s/%s (%s%%)",
            osm_found,
            total_facilities,
            osm_found / total_facilities * 100,
        )

        # Debug information if available
        if "wikipedia_search_query" in facilities_data["facilities"][0]:
            logger.info("\n=== Wikipedia Debug Information ===")
            false_positives = 0
            errors = 0
            for facility in facilities_data["facilities"]:
                query = facility.get("wikipedia_search_query", "")
                if "REJECTED" in query:
                    false_positives += 1
                elif "ERROR" in query:
                    errors += 1

            logger.info("False positives detected and rejected: %s", false_positives)
            logger.info("Search errors encountered: %s", errors)
            logger.info("Note: Review 'wikipedia_search_query' column for detailed search information")

        if "wikidata_search_query" in facilities_data["facilities"][0]:
            logger.warning("Note: Review 'wikidata_search_query' column for detailed search information")

        if "osm_search_query" in facilities_data["facilities"][0]:
            logger.warning("Note: Review 'osm_search_query' column for detailed search information")

    logger.info("\n=== ICE Detention Facilities Scraper: Run completed ===")
