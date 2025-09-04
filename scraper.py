# ICEFacilityScraper class and scraping-related code
from bs4 import BeautifulSoup
import copy
import re
import requests
from requests.adapters import HTTPAdapter
import time
import urllib3
from utils import (
    facility_schema,
    logger,
)


class ICEFacilityScraper(object):
    # All methods for scraping ICE websites

    def __init__(self):
        self.base_url = "https://www.ice.gov/detention-facilities"
        self.facilities_data = []
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

    def scrape_facilities(self):
        """Scrape all ICE detention facility data from all 6 pages"""
        logger.info("Starting to scrape ICE detention facilities...")

        # URLs for all pages
        urls = [f"{self.base_url}?exposed_form_display=1&page={i}" for i in range(6)]

        for page_num, url in enumerate(urls):
            logger.info("Scraping page %s/6...", page_num + 1)
            try:
                facilities = self._scrape_page(url)
                self.facilities_data.extend(facilities)
                logger.debug(
                    "Found %s facilities on page %s", len(facilities), page_num + 1
                )
                time.sleep(1)  # Be respectful to the server
            except Exception as e:
                logger.error("Error scraping page %s: %s", page_num + 1, e)

        # self.facilities_data = all_facilities
        logger.info("Total facilities scraped: %s", len(self.facilities_data))
        return self.facilities_data

    def _scrape_page(self, url):
        """Scrape a single page of facilities using BeautifulSoup"""
        logger.debug("  Fetching: %s", url)
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()

            # Parse HTML with BeautifulSoup
            soup = BeautifulSoup(response.content, "html.parser")

            # Extract facilities from the parsed HTML
            facilities = self._extract_facilities_from_html(soup, url)

            return facilities
        except requests.RequestException as e:
            logger.error("  Error fetching %s: %s", url, e)
            return []
        except Exception as e:
            logger.error("  Error parsing %s: %s", url, e)
            return []

    def _extract_facilities_from_html(self, soup, page_url):
        """Extract facility data from BeautifulSoup parsed HTML"""
        facilities = []

        logger.debug("Searching %s for content", page_url)
        try:
            # Look for the main content area - ICE uses different possible containers
            content_selectors = [
                "div.view-content",  # Primary content container
                "div.views-rows",  # Alternative container
                "ul.views-rows",  # List-based container
                "div.region-content",  # Region content
                "main",  # HTML5 main element
                "div.content",  # Generic content
            ]
            content_container = None
            for selector in content_selectors:
                content_container = soup.select_one(selector)
                if content_container:
                    logger.debug("  Found content using selector: %s", selector)
                    break

            if not content_container:
                logger.warning(
                    "  Warning: Could not find content container, searching entire page"
                )
                content_container = soup

            # Look for facility entries - try multiple patterns
            facility_selectors = [
                "li.grid",  # List items with grid class
                "div.views-row",  # View rows
                "li.views-row",  # List-based view rows
                "div.facility-item",  # Custom facility items
                "article",  # Article elements
                "div.node",  # Drupal node containers
            ]
            facility_elements = []
            for selector in facility_selectors:
                elements = content_container.select(selector)
                if elements:
                    facility_elements = elements
                    logger.debug(
                        "  Found %s facility elements using selector: %s",
                        len(elements),
                        selector,
                    )
                    break

            if not facility_elements:
                # Fallback: look for any element containing facility-like text patterns
                logger.warning(
                    "  Using fallback: searching for facility patterns in text"
                )
                facility_elements = self._find_facility_patterns(content_container)

            # Extract data from each facility element
            for element in facility_elements:
                facility_data = self._extract_single_facility(element, page_url)
                if facility_data and facility_data.get("name"):
                    facilities.append(facility_data)

            logger.info("  Extracted %s facilities from page", len(facilities))

        except Exception as e:
            logger.error("  Error extracting facilities from HTML: %s", e)

        return facilities

    def _find_facility_patterns(self, container):
        """Fallback method to find facility data using text patterns"""
        facility_elements = []

        # Look for text patterns that indicate facilities
        facility_patterns = [
            r"([A-Z][^|]+(?:\|[^|]+)?)\s*([A-Z][^A-Z]*Field Office)",
            r"([^-]+)\s*-\s*([A-Z][^A-Z]*Field Office)",
        ]

        text_content = container.get_text()

        for pattern in facility_patterns:
            matches = re.findall(pattern, text_content, re.MULTILINE)
            for match in matches:
                # Create a pseudo-element with the matched text
                pseudo_element = type(
                    "Element",
                    (),
                    {
                        "text": f"{match[0]} {match[1]}",
                        "get_text": lambda: f"{match[0]} {match[1]}",
                        "select": lambda x: [],
                        "select_one": lambda x: None,
                        "find": lambda *args, **kwargs: None,
                        "find_all": lambda *args, **kwargs: [],
                    },
                )()
                facility_elements.append(pseudo_element)

        return facility_elements

    def _extract_single_facility(self, element, page_url):
        """Extract data from a single facility element"""
        facility = copy.deepcopy(facility_schema)
        facility["source_url"] = page_url
        try:
            # Get all text content from the element
            element_text = element.get_text(separator=" ", strip=True)

            # Method 1: Try structured extraction if element has proper HTML structure
            facility_name_elem = element.select_one(
                ".facility-name, .field-name, h3, h2, .title"
            )
            field_office_elem = element.select_one(".field-office, .office")
            address_elem = element.select_one(".address, .field-address")
            phone_elem = element.select_one(".phone, .field-phone")

            if facility_name_elem:
                facility["name"] = facility_name_elem.get_text(strip=True)
            if field_office_elem:
                facility["field_office"] = field_office_elem.get_text(strip=True)
            if address_elem:
                facility["address"] = address_elem.get_text(strip=True)
            if phone_elem:
                facility["phone"] = phone_elem.get_text(strip=True)

            # Method 2: If structured extraction failed, parse the text content
            if not facility["name"] or not facility["field_office"]:
                facility = self._parse_facility_text(element_text, facility)

            # Extract image URL using the specified nested structure
            image_element = element.findAll("img")
            if image_element:
                facility["image_url"] = f"https://www.ice.gov{image_element[0]['src']}"
            facility_url_element = element.findAll("a")
            if facility_url_element:
                facility["facility_url"] = (
                    f"https://www.ice.gov{facility_url_element[0]['href']}"
                )
            # Clean up extracted data
            facility = self._clean_facility_data(facility)

        except Exception as e:
            logger.error("    Error extracting facility data: %s", e)
        logger.debug("Returning %s", facility)
        return facility

    def _clean_facility_data(self, facility):
        """Clean up data (currently a no-op)"""
        return facility

    def _parse_facility_text(self, text, facility):
        """Parse facility information from raw text"""
        # Common patterns for ICE facility listings
        patterns = [
            # Pattern: "Facility Name | Other NameField Office"
            r"^([^|]+(?:\|[^|]+)?)\s*([A-Z][^A-Z]*Field Office)",
            # Pattern: "Facility Name Field Office"
            r"^([^A-Z]*[^-]+?)\s*([A-Z][^A-Z]*Field Office)",
            # Pattern: "- Facility NameField Office"
            r"^-\s*([^-]+?)\s*([A-Z][^A-Z]*Field Office)",
        ]

        facility_info = None
        for pattern in patterns:
            match = re.match(pattern, text, re.MULTILINE | re.DOTALL)
            if match:
                facility_info = match
                break

        if facility_info:
            facility["name"] = facility_info.group(1).strip()
            facility["field_office"] = facility_info.group(2).strip()
            if facility["name"].endswith(" St."):
                facility["name"] = facility["name"].rstrip(" St.")
                facility["field_office"] = f"St. {facility['field_office']}"
            if facility["name"].endswith(" San"):
                facility["name"] = facility["name"].rstrip(" San")
                facility["field_office"] = f"San {facility['field_office']}"
            # Extract address from remaining text
            remaining_text = text[facility_info.end() :].strip()
            address_lines = []
            phone = ""
            for line in remaining_text.split("\n"):
                line = line.strip()
                if not line:
                    continue
                line = re.search(r"(.*United States).*$", line).group(1)
                logger.debug("Processing %s", line)
                # Check if it's a phone number
                if re.match(r"^\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}$", line):
                    phone = line
                # Check if it looks like an address component
                elif (
                    any(
                        indicator in line.lower()
                        for indicator in [
                            "street",
                            "road",
                            "avenue",
                            "drive",
                            "blvd",
                            "highway",
                            "hwy",
                        ]
                    )
                    or re.match(r"^\d+\s", line)  # Starts with number
                    or re.search(r"\b[A-Z]{2}\s+\d{5}", line)  # Contains state and zip
                    or "United States" in line
                ):
                    address_lines.append(line)

            if address_lines:
                facility["address"] = ", ".join(address_lines)
            facility["phone"] = phone
        return facility

    pass
