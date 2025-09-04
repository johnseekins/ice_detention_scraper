import csv
from utils import (
    debug_schema,
    facility_schema,
    enrichment_schema,
    logger,
)
#
# # CSVHandler class for CSV export and reporting


class CSVHandler(object):
    @staticmethod
    def export_to_csv(
        facilities_data: list, filename: str = "ice_detention_facilities_enriched.csv"
    ) -> str | None:
        if not facilities_data:
            logger.warning("No data to export!")
            return None

        base_fields: list = list(facility_schema.keys())
        fieldnames: list = base_fields.copy()

        if any(field in facilities_data[0] for field in enrichment_schema):
            fieldnames.extend(enrichment_schema)

        if any(field in facilities_data[0] for field in debug_schema):
            fieldnames.extend(debug_schema)

        try:
            with open(filename, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for facility in facilities_data:
                    row_data = {field: facility.get(field, "") for field in fieldnames}
                    writer.writerow(row_data)

            logger.info(
                "CSV file '%s' created successfully with %s facilities.",
                filename,
                len(facilities_data),
            )
            return filename

        except Exception as e:
            logger.error("Error writing CSV file: %s", e)
            return None

    @staticmethod
    def print_summary(facilities_data: list) -> None:
        """Print summary statistics about the facilities"""
        if not facilities_data:
            logger.info("No data to summarize!")
            logger.info("\n=== ICE Detention Facilities Scraper: Run completed ===")
            return

        total_facilities = len(facilities_data)
        logger.info("\n=== ICE Detention Facilities Scraper Summary ===")
        logger.info("Total facilities: %s", total_facilities)

        # Count by field office
        field_offices: dict = {}
        for facility in facilities_data:
            office = facility.get("field_office", "Unknown")
            field_offices[office] = field_offices.get(office, 0) + 1

        logger.info("\nFacilities by Field Office:")
        for office, count in sorted(
            field_offices.items(), key=lambda x: x[1], reverse=True
        ):
            logger.info("  %s: %s", office, count)

        # Check enrichment data if available
        if "wikipedia_page_url" in facilities_data[0]:
            wiki_found = sum(
                1
                for f in facilities_data
                if f.get("wikipedia_page_url") and f["wikipedia_page_url"]
            )
            wikidata_found = sum(
                1
                for f in facilities_data
                if f.get("wikidata_page_url") and f["wikidata_page_url"]
            )
            osm_found = sum(
                1
                for f in facilities_data
                if f.get("osm_result_url") and f["osm_result_url"]
            )

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
            if "wikipedia_search_query" in facilities_data[0]:
                logger.info("\n=== Wikipedia Debug Information ===")
                false_positives = 0
                errors = 0
                for facility in facilities_data:
                    query = facility.get("wikipedia_search_query", "")
                    if "REJECTED" in query:
                        false_positives += 1
                    elif "ERROR" in query:
                        errors += 1

                logger.info(
                    "False positives detected and rejected: %s", false_positives
                )
                logger.info("Search errors encountered: %s", errors)
                logger.info(
                    "Note: Review 'wikipedia_search_query' column for detailed search information"
                )

            if "wikidata_search_query" in facilities_data[0]:
                logger.warning(
                    "Note: Review 'wikidata_search_query' column for detailed search information"
                )

            if "osm_search_query" in facilities_data[0]:
                logger.warning(
                    "Note: Review 'osm_search_query' column for detailed search information"
                )

        logger.info("\n=== ICE Detention Facilities Scraper: Run completed ===")
