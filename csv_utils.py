import csv
#
# # CSVHandler class for CSV export and reporting


class CSVHandler:
    @staticmethod
    def export_to_csv(facilities_data, filename="ice_detention_facilities_enriched.csv"):
        if not facilities_data:
            print("No data to export!")
            return None

        base_fields = ["name", "field_office", "address", "phone", "image_url"]
        enrichment_fields = ["wikipedia_page_url", "wikidata_page_url", "osm_result_url"]
        debug_fields = ["wikipedia_search_query", "wikidata_search_query", "osm_search_query"]

        fieldnames = base_fields.copy()

        if any(field in facilities_data[0] for field in enrichment_fields):
            fieldnames.extend(enrichment_fields)

        if any(field in facilities_data[0] for field in debug_fields):
            fieldnames.extend(debug_fields)

        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for facility in facilities_data:
                    row_data = {field: facility.get(field, '') for field in fieldnames}
                    writer.writerow(row_data)

            print(f"CSV file '{filename}' created successfully with {len(facilities_data)} facilities.")
            return filename

        except Exception as e:
            print(f"Error writing CSV file: {e}")
            return None

    @staticmethod
    def print_summary(facilities_data):
        """Print summary statistics about the facilities"""
        if not facilities_data:
            print("No data to summarize!")
            print(f"\n=== ICE Detention Facilities Scraper: Run completed ===")
            return

        total_facilities = len(facilities_data)
        print(f"\n=== ICE Detention Facilities Scraper Summary ===")
        print(f"Total facilities: {total_facilities}")

        # Count by field office
        field_offices = {}
        for facility in facilities_data:
            office = facility.get('field_office', 'Unknown')
            field_offices[office] = field_offices.get(office, 0) + 1

        print(f"\nFacilities by Field Office:")
        for office, count in sorted(field_offices.items(), key=lambda x: x[1], reverse=True):
            print(f"  {office}: {count}")

        # Check enrichment data if available
        if 'wikipedia_page_url' in facilities_data[0]:
            wiki_found = sum(1 for f in facilities_data if f.get('wikipedia_page_url') and f['wikipedia_page_url'] != False)
            wikidata_found = sum(1 for f in facilities_data if f.get('wikidata_page_url') and f['wikidata_page_url'] != False)
            osm_found = sum(1 for f in facilities_data if f.get('osm_result_url') and f['osm_result_url'] != False)

            print(f"\n=== External Data Enrichment Results ===")
            print(f"Wikipedia pages found: {wiki_found}/{total_facilities} ({wiki_found / total_facilities * 100:.1f}%)")
            print(f"Wikidata entries found: {wikidata_found}/{total_facilities} ({wikidata_found / total_facilities * 100:.1f}%)")
            print(f"OpenStreetMap results found: {osm_found}/{total_facilities} ({osm_found / total_facilities * 100:.1f}%)")

            # Debug information if available
            if 'wikipedia_search_query' in facilities_data[0]:
                print(f"\n=== Wikipedia Debug Information ===")
                false_positives = 0
                errors = 0
                for facility in facilities_data:
                    query = facility.get('wikipedia_search_query', '')
                    if 'REJECTED' in query:
                        false_positives += 1
                    elif 'ERROR' in query:
                        errors += 1

                print(f"False positives detected and rejected: {false_positives}")
                print(f"Search errors encountered: {errors}")
                print(f"Note: Review 'wikipedia_search_query' column for detailed search information")

            if 'wikidata_search_query' in facilities_data[0]:
                print(f"Note: Review 'wikidata_search_query' column for detailed search information")

            if 'osm_search_query' in facilities_data[0]:
                print(f"Note: Review 'osm_search_query' column for detailed search information")

        print(f"\n=== ICE Detention Facilities Scraper: Run completed ===")