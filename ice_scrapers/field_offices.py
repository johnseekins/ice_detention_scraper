# ICEFieldOfficeScraper class and scraping-related code
from bs4 import BeautifulSoup
import copy
import datetime
import re
from schemas import (
    field_offices_schema,
    field_office_schema,
)
import time
from utils import (
    logger,
    session,
)

base_scrape_url = "https://www.ice.gov/contact/field-offices"

# ICE AOR mappings
area_of_responsibility = {
    "ATL": "Atlanta Field Office",
    "BAL": "Baltimore Field Office",
    "BOS": "Boston Field Office",
    "BUF": "Buffalo Field Office",
    "CHI": "Chicago Field Office",
    "DAL": "Dallas Field Office",
    "DEN": "Denver Field Office",
    "DET": "Detroit Field Office",
    "ELP": "El Paso Field Office",
    "HLG": "Harlingen Field Office",
    "HOU": "Houston Field Office",
    "LOS": "Los Angeles Field Office",
    "MIA": "Miami Field Office",
    "NEW": "Newark Field Office",
    "NOL": "New Orleans Field Office",
    "NYC": "New York City Field Office",
    "PHI": "Philadelphia Field Office",
    "PHO": "Phoenix Field Office",
    "SEA": "Seattle Field Office",
    "SFR": "San Francisco Field Office",
    "SLC": "Salt Lake City Field Office",
    "SNA": "San Antonio Field Office",
    "SND": "San Diego Field Office",
    "SPM": "St Paul Field Office",
    "WAS": "Washington Field Office",
}
field_office_to_aor = {v: k for k, v in area_of_responsibility.items()}


def _get_scrape_pages() -> list:
    """Discover all facility pages"""
    resp = session.get(base_scrape_url, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.content, "html.parser")
    links = soup.findAll("a", href=re.compile(r"\?page="))
    if not links:
        raise Exception(f"{base_scrape_url} contains *no* links?!")
    pages = [
        f"{base_scrape_url}{link['href']}&exposed_form_display=1"
        for link in links
        if not any(k in link["aria-label"] for k in ["Next", "Last"])
    ]
    logger.debug("Pages discovered: %s", pages)
    return pages


def scrape_field_offices() -> dict:
    """Collect data on ICE field offices"""
    start_time = time.time()
    office_data = copy.deepcopy(field_offices_schema)
    office_data["scraped_date"] = datetime.datetime.now(datetime.UTC)
    logger.info("Starting to scrape ICE.gov field offices...")
    urls = _get_scrape_pages()
    for page_num, url in enumerate(urls):
        logger.info("Scraping page %s/%s...", page_num + 1, len(urls))
        offices = _scrape_page(url)
        logger.debug("Found %s offices on page %s", len(offices), page_num + 1)
        time.sleep(1)  # Be respectful to the server
        for office in offices:
            office_data["field_offices"][office["field_office"]] = office
    office_data["scrape_runtime"] = time.time() - start_time
    logger.info("Total field offices scraped: %s", len(office_data["field_offices"]))
    logger.info(" Completed in %s seconds", office_data["scrape_runtime"])
    return office_data


def _scrape_page(page_url: str) -> list:
    """Scrape a single page of facilities using BeautifulSoup"""
    logger.debug("  Fetching: %s", page_url)
    try:
        response = session.get(page_url, timeout=30)
        response.raise_for_status()
    except Exception as e:
        logger.error("  Error parsing %s: %s", page_url, e)
        return []
    # Parse HTML with BeautifulSoup
    soup = BeautifulSoup(response.content, "html.parser")
    offices = []

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
    logger.debug("Searching %s for content", page_url)
    for selector in content_selectors:
        content_container = soup.select_one(selector)
        if content_container:
            logger.debug("  Found content using selector: %s", selector)
            break

    if not content_container:
        logger.warning("  Warning: Could not find content container, searching entire page")
        content_container = soup

    # Look for facility entries - try multiple patterns
    office_selectors = [
        "li.grid",  # List items with grid class
        "div.views-row",  # View rows
        "li.views-row",  # List-based view rows
        "div.facility-item",  # Custom facility items
        "article",  # Article elements
        "div.node",  # Drupal node containers
    ]
    office_elements: list = []
    for selector in office_selectors:
        elements = content_container.select(selector)
        if elements:
            office_elements = elements
            logger.debug(
                "  Found %s office elements using selector: %s",
                len(elements),
                selector,
            )
            break

    # if not office_elements:
    #     # Fallback: look for any element containing office-like text patterns
    #     logger.warning("  Using fallback: searching for office patterns in text")
    #     office_elements = _find_office_patterns(content_container)

    # Extract data from each office element
    for element in office_elements:
        office_data = _extract_single_office(element, page_url)
        if office_data and office_data.get("name", None):
            offices.append(office_data)
    logger.info("  Extracted %s field offices from page", len(offices))

    return offices


def _extract_single_office(element: BeautifulSoup, page_url: str) -> dict:
    """Extract data from a single office element"""
    logger.debug("Trying to get office data from %s", element)
    office_name = element.select_one(".views-field-field-field-office-location")
    if not office_name or not office_name.text.strip().endswith("ERO"):
        logger.debug("  Skipping %s because it is not an ERO location", office_name.text)  # type: ignore [union-attr]
        # not a field office
        return {}
    office = copy.deepcopy(field_office_schema)
    office["source_urls"].append(page_url)
    office["name"] = office_name.text.strip()
    field_office = element.select_one(".views-field-title")
    if field_office:
        office["field_office"] = field_office.text.strip()
        office["id"] = field_office_to_aor[office["field_office"]]
    address = element.select_one(".address-line1")
    if address:
        office["address"]["street"] = address.text.strip()
    # optional line 2 of address
    address = element.select_one(".address-line2")
    if address:
        office["address"]["street"] = f"{office['address']['street']} {address.text.strip()}"
    locality = element.select_one(".locality")
    if locality:
        office["address"]["locality"] = locality.text.strip()
    administrative_area = element.select_one(".administrative-area")
    if administrative_area:
        office["address"]["administrative_area"] = administrative_area.text.strip()
    postal_code = element.select_one(".postal-code")
    if postal_code:
        office["address"]["postal_code"] = postal_code.text.strip()
    office["address_str"] = (
        f"{office['address']['street']} {office['address']['locality']}, {office['address']['administrative_area']} {office['address']['postal_code']}"
    )
    country = element.select_one(".country")
    if country:
        office["address"]["country"] = country.text.strip()
    phone = element.select_one(".ct-addr")
    if phone:
        office["phone"] = phone.text.strip()
    details = element.select_one(".views-field-body")
    email = details.findAll("a")  # type: ignore [union-attr]
    if email:
        office["email"] = email[0]["href"].split(":", 1)[1]
    detail_txt = details.text  # type: ignore [union-attr]
    logger.debug("Detail text: %s", detail_txt)
    aor_match = re.match(r"Area of Responsibility:(.+)\n?Email", detail_txt)
    if aor_match:
        office["aor"] = aor_match.group(1).strip().replace("\xa0", " ")

    logger.debug("Returning %s", office)
    return office


def merge_field_offices(facilities_data: dict, field_offices: dict) -> dict:
    final_facilities = copy.deepcopy(facilities_data["facilities"])
    for facility_id, facility in facilities_data["facilities"].items():
        office = field_offices["field_offices"].get(facility["field_office"]["field_office"], None)
        if office:
            final_facilities[facility_id]["field_office"] = office
            continue
        office_name = area_of_responsibility.get(facility["field_office"]["id"], None)
        office = field_offices["field_offices"].get(office_name, None)
        if office:
            final_facilities[facility_id]["field_office"] = office
    facilities_data["facilities"] = final_facilities
    return facilities_data
