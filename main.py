#!/usr/bin/env python3
"""
ICE Detention Facilities Data Scraper and Enricher

This script scrapes ICE detention facility data from ICE.gov and enriches it
with information from Wikipedia, Wikidata, and OpenStreetMap.

Usage:
    python main.py --scrape          # Scrape fresh data from ICE website
    python main.py --enrich          # Enrich existing data with external sources
    python main.py --scrape --enrich # Do both operations
    python main.py --help            # Show help

    # Enable Wikipedia debugging
    python main.py --load-existing --enrich --debug-wikipedia

    # With custom output file
    python main.py --load-existing --enrich --debug-wikipedia -o debug_facilities.csv

Requirements:
    pip install requests beautifulsoup4 lxml
    or for globally managed environments, (e.g. Debian and Ubuntu)
    sudo apt install python3-requests python3-bs4 python3-lxml
"""

from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import logging
from file_utils import export_to_file, print_summary
from data_loader import load_existing_data
from enricher import ExternalDataEnricher
from scraper import ICEFacilityScraper
from utils import logger

# CLI, argument parsing, script orchestration


def main() -> None:
    parser = ArgumentParser(
        description="ICE Detention Facilities Data Scraper and Enricher",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--scrape",
        action="store_true",
        default=False,
        help="Scrape initial facility data",
    )
    parser.add_argument("--enrich", action="store_true", default=False, help="enrich collected data")
    parser.add_argument(
        "--load-existing",
        action="store_true",
        default=False,
        help="load data from local files",
    )
    parser.add_argument(
        "--file-type",
        default="csv",
        choices=["csv", "json"],
        help="type of file to export",
    )
    parser.add_argument(
        "--output-file-name",
        "-o",
        default="ice_detention_facilities",
        help="The file we'll write data out to (excluding the suffix)",
    )
    parser.add_argument("--debug", action="store_true", help="Full debug information and logging")
    parser.add_argument("--debug-wikipedia", action="store_true", help="debug wikipedia enrichment")
    parser.add_argument("--debug-wikidata", action="store_true", help="debug wikidata enrichment")
    parser.add_argument("--debug-osm", action="store_true", help="debug OpenStreetMap enrichment")

    args = parser.parse_args()
    if args.debug:
        logger.setLevel(logging.DEBUG)
        args.debug_wikipedia = True
        args.debug_wikidata = True
        args.debug_osm = True
    logger.info("ICE Detention Facilities Scraper by the Open Security Mapping Project. MIT License.")

    if not any([args.scrape, args.enrich, args.load_existing]):
        parser.print_help()
        return

    facilities_data = []

    if args.scrape and args.load_existing:
        logger.error("Can't scrape and load existing data!")
        exit(1)

    if args.scrape:
        scraper = ICEFacilityScraper()
        facilities_data = scraper.scrape_facilities()
    elif args.load_existing:
        facilities_data = load_existing_data()
        logger.info(f"Loaded {len(facilities_data)} existing facilities from local data. (Not scraping ICE.gov)")

    if args.enrich:
        if not facilities_data:
            logger.warning("No facility data available for enrichment.")
            return
        enricher = ExternalDataEnricher(
            debug_wikipedia=args.debug_wikipedia,
            debug_wikidata=args.debug_wikidata,
            debug_osm=args.debug_osm,
        )
        facilities_data = enricher.enrich_facility_data(facilities_data)

    if facilities_data:
        output_filename = args.output_file_name
        if args.enrich and not output_filename.endswith("_enriched"):
            base_name = output_filename.replace(".csv", "")
            output_filename = f"{base_name}_enriched"
        export_to_file(facilities_data, output_filename, args.file_type)
        print_summary(facilities_data)
    else:
        logger.warning("No data to export!")


if __name__ == "__main__":
    main()
