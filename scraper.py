# ICEFacilityScraper class and scraping-related code
import base64
from bs4 import BeautifulSoup
import copy
import datetime
import os
import polars
import re
from schemas import (
    facilities_schema,
    facility_schema,
)
import time
from typing import Tuple
from utils import (
    default_timestamp,
    facility_sheet_header,
    logger,
    session,
    timestamp_format,
)

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


class ICEGovFacilityScraper(object):
    base_scrape_url = "https://www.ice.gov/detention-facilities"
    base_xlsx_url = "https://www.ice.gov/detain/detention-management"

    # All methods for scraping ice.gov websites
    def __init__(self):
        self.facilities_data = copy.deepcopy(facilities_schema)
        self.filename = f"{SCRIPT_DIR}{os.sep}detentionstats.xlsx"

    def _download_sheet(self) -> None:
        resp = session.get(self.base_xlsx_url, timeout=120)
        soup = BeautifulSoup(resp.content, "html.parser")
        links = soup.findAll("a", href=re.compile("^https://www.ice.gov/doclib.*xlsx"))
        # quick solution is first result
        self.sheet_url = links[0]["href"]
        if not os.path.isfile(self.filename) or os.path.getsize(self.filename) < 1:
            logger.info("Downloading detention stats sheet from %s", self.sheet_url)
            resp = session.get(self.sheet_url, timeout=120)
            with open(self.filename, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)

    def _clean_street(self, street: str, locality: str = "") -> Tuple[str, bool]:
        """Generally, we'll let the spreadsheet win arguments just to be consistent"""
        street_filters = [
            # address mismatch between site and spreadsheet
            {"match": "80 29th Street", "replace": "100 29th Street", "locality": "Brooklyn"},
            {"match": "2250 Laffoon Trl", "replace": "2250 Lafoon Trail", "locality": "Madisonville"},
            {"match": "560 Gum Springs Road", "replace": "560 Gum Spring Road", "locality": "Winnfield"},
            {
                "match": "Vincente Taman Building",
                "replace": "Vicente T Seman Bldg Civic Center",
                "locality": "Susupe, Saipan",
            },
            {"match": "209 County Road A049", "replace": "209 County Road 49", "locality": "Estancia"},
            {
                "match": "50140 US Highway 191 South",
                "replace": "50140 UNITED STATES HIGHWAY 191 SOUTH",
                "locality": "Rock Springs",
            },
            {"match": "5 Basler Drive", "replace": "5 BASLER DR", "locality": "Ste. Genevieve"},
            {"match": "3843 Stagg Ave", "replace": "3843 Stagg Avenue", "locality": "Basile"},
            {
                "match": "13880 Business Center Drive NW",
                "replace": "13880 Business Center Drive",
                "locality": "Elk River",
            },
            {"match": "3040 South State Route 100", "replace": "3040 SOUTH STATE HIGHWAY 100", "locality": "Tiffin"},
            {"match": "1001 San Rio Blvd", "replace": "1001 San Rio Boulevard", "locality": "Laredo"},
            {"match": "1209 Sunflower Lane", "replace": "1209 Sunflower Ln", "locality": "Alvarado"},
            {"match": "27991 Buena Vista Blvd.", "replace": "27991 BUENA VISTA BOULEVARD", "locality": "Los Fresnos"},
            {"match": "175 Pike County Blvd.", "replace": "175 PIKE COUNTY BOULEVARD", "locality": "Lords Valley"},
            {"match": "500 W. 2nd Street", "replace": "301 W. 2nd", "locality": "Rolla"},
            {"match": "307 Saint Joseph St", "replace": "300 KANSAS CITY STREET NONE", "locality": "Rapid City"},
            {"match": "3405 West Highway 146", "replace": "3405 W HWY 146", "locality": "LaGrange"},
            {"match": "1623 E J Street, Suite 2", "replace": "1623 E. J STREET", "locality": "Tacoma"},
            {"match": "1805 W 32nd Street", "replace": "1805 W 32ND ST", "locality": "Baldwin"},
            {"match": "500 Hilbig Road", "replace": "500 HILBIG RD", "locality": "Conroe"},
            {"match": "806 Hilbig Road", "replace": "806 HILBIG RD", "locality": "Conroe"},
            {"match": "425 Golden State Avenue", "replace": "425 Golden State Ave", "locality": "Bakersfield"},
            {"match": "832 East Texas HWY 44", "replace": "832 EAST TEXAS STATE HIGHWAY 44", "locality": "Encinal"},
            {"match": "18201 SW 12th Street", "replace": "18201 SW 12TH ST", "locality": "Miami"},
            {"match": "2190 E Mesquite Avenue", "replace": "2190 EAST MESQUITE AVENUE", "locality": "Pahrump"},
            {"match": "287 Industrial Drive", "replace": "327 INDUSTRIAL DRIVE", "locality": "Jonesboro"},
            {"match": "1572 Gateway Road", "replace": "1572 GATEWAY", "locality": "Calexico"},
            {"match": "203 Aspinall Avenue", "replace": "203 ASPINAL AVE. PO BOX 3236", "locality": "Hagatna"},
            {"match": "1199 N Haseltine Road", "replace": "1199 N HASELTINE RD", "locality": "Springfield"},
            {"match": "1701 North Washington", "replace": "1701 NORTH WASHINGTON ST", "locality": "Grand Forks"},
            {"match": "611 Frontage Road", "replace": "611 FRONTAGE RD", "locality": "McFarland"},
            {"match": "12450 Merritt Road", "replace": "12450 MERRITT DR", "locality": "Chardon"},
            {"match": "411 S. Broadway Avenue", "replace": "411 SOUTH BROADWAY AVENUE", "locality": "Albert Lea"},
            {"match": "3424 Hwy 252 E", "replace": "3424 HIGHWAY 252 EAST", "locality": "Folkston"},
            {"match": "3250 N. Pinal Parkway", "replace": "3250 NORTH PINAL PARKWAY", "locality": "Florence"},
            {"match": "351 Elliott Street", "replace": "351 ELLIOTT ST", "locality": "Honolulu"},
            {"match": "1 Success Loop Rd", "replace": "1 SUCCESS LOOP DR", "locality": "Berlin"},
            {"match": "700 Arch Street", "replace": "700 ARCH ST", "locality": "Philadelphia"},
            {"match": "33 NE 4th Street", "replace": "33 NE 4 STREET", "locality": "Miami"},
            {"match": "1300 Metropolitan", "replace": "1300 METROPOLITAN AVE", "locality": "Leavenworth"},
            {"match": "601 McDonough Blvd SE", "replace": "601 MCDONOUGH BOULEVARD SE", "locality": "Atlanta"},
            {"match": "1705 E Hanna Rd", "replace": "1705 EAST HANNA RD", "locality": "Eloy"},
            {"match": "2255 East 8th North", "replace": "2255 E 8TH NORTH", "locality": "Mountain Home"},
            {"match": "8915 Montana Avenue", "replace": "8915 MONTANA AVE", "locality": "El Paso"},
            {"match": "704 E Broadway Street", "replace": "702 E BROADWAY ST", "locality": "Eden"},
            {"match": "1300 E Hwy 107", "replace": "1330 HIGHWAY 107", "locality": "La Villa"},
            {"match": "216 W. Center Street", "replace": "215 WEST CENTRAL STREET", "locality": "Juneau"},
            {"match": "300 El Racho Way ", "replace": "300 EL RANCHO WAY", "locality": "Dilley"},
            {"match": "3130 North Oakland Street", "replace": "3130 OAKLAND ST", "locality": "Aurora"},
            {"match": "03151 Co. Rd. 24.2", "replace": "3151 ROAD 2425 ROUTE 1", "locality": "Stryker"},
            {"match": "20 Hobo Forks Road", "replace": "20 HOBO FORK RD", "locality": "Natchez"},
            {"match": "7340 Highway 26 W", "replace": "7340 HIGHWAY 26 WEST", "locality": "Oberlin"},
            {"match": "1400 E Fourth Ave", "replace": "1400 E 4TH AVE", "locality": "Anchorage"},
            {"match": "3900 N. Powerline Road", "replace": "3900 NORTH POWERLINE ROAD", "locality": "Pompano Beach"},
            {"match": "185 E. Michigan Street", "replace": "185 EAST MICHIGAN AVENUE", "locality": "Battle Creek"},
            {"match": "601 Central Avenue", "replace": "601 CENTRAL AVE", "locality": "Newport"},
            {"match": "501 E Court Avenue", "replace": "501 EAST COURT AVE", "locality": "Jeffersonville"},
            {"match": "3200 S. Kings Hwy", "replace": "3700 S KINGS HWY", "locality": "Cushing"},
            {"match": "301 South Walnut", "replace": "301 SOUTH WALNUT STREET", "locality": "Cottonwood Falls"},
            {"match": "830 Pine Hill Road", "replace": "830 PINEHILL ROAD", "locality": "Jena"},
            {
                "match": "11093 SW Lewis Memorial Dr",
                "replace": "11093 SW LEWIS MEMORIAL DRIVE",
                "locality": "Bowling Green",
            },
            {"match": "58 Pine Mountain Road", "replace": "58 PINE MOUNTAIN RD", "locality": "McElhattan"},
            # a unique one, 'cause the PHONE NUMBER IS IN THE ADDRESS?!
            {"match": "911 PARR BLVD 775 328 3308", "replace": "911 E Parr Blvd", "locality": "RENO"},
            # default matches should come last
            {"match": "'s", "replace": "", "locality": ""},
            {"match": ".", "replace": "", "locality": ""},
            {"match": ",", "replace": "", "locality": ""},
        ]
        stripped_street = street
        cleaned = False
        if any(f["match"] in stripped_street for f in street_filters):
            cleaned = True
        for f in street_filters:
            if (f["match"] in stripped_street) and ((f["locality"] and f["locality"] == locality) or not f["locality"]):
                stripped_street = stripped_street.replace(f["match"], f["replace"])
                cleaned = True
        return stripped_street, cleaned

    def _repair_zip(self, zip_code: int, locality: str) -> Tuple[str, bool]:
        """
        Excel does a cool thing where it strips leading 0s
        Also, many zip codes are mysteriously discordant
        """
        zcode = str(zip_code)
        cleaned = False
        if len(zcode) == 4:
            zcode = f"0{zcode}"
            cleaned = True
        # This address is an absolute mess
        if zcode == "89512" and locality == "Reno":
            zcode = "89506"
            cleaned = True
        if zcode == "82901" and locality == "Rock Springs":
            zcode = "82935"
            cleaned = True
        if zcode == "98421-1615" and locality == "Tacoma":
            zcode = "98421"
            cleaned = True
        if zcode == "89048" and locality == "Pahrump":
            zcode = "89060"
            cleaned = True
        if zcode == "85132" and locality == "Florence":
            zcode = "85232"
            cleaned = True
        return zcode, cleaned

    def _repair_locality(self, locality: str, administrative_area: str) -> Tuple[str, bool]:
        """
        There is no consistency with any address.
        How the post office ever successfully delivered a letter is beyond me
        """
        cleaned = False
        if locality == "LaGrange" and administrative_area == "KY":
            locality = "La Grange"
            cleaned = True
        if locality == "Leachfield" and administrative_area == "KY":
            locality = "LEITCHFIELD"
            cleaned = True
        if locality == "Susupe, Saipan" and administrative_area == "MP":
            locality = "SAIPAN"
            cleaned = True
        if locality == "Cottonwood Falls" and administrative_area == "KS":
            locality = "COTTONWOOD FALL"
            cleaned = True
        if locality == "Sault Ste. Marie" and administrative_area == "MI":
            locality = "SAULT STE MARIE"
            cleaned = True
        return locality, cleaned

    def _load_sheet(self) -> dict:
        """Convert the detentionstats sheet data into something we can update our facilities with"""
        self._download_sheet()
        df = polars.read_excel(
            drop_empty_rows=True,
            has_header=False,
            # because we're manually defining the header...
            read_options={"skip_rows": 7, "column_names": facility_sheet_header},
            sheet_name="Facilities FY25",
            source=open(self.filename, "rb"),
        )
        results: dict = {}
        # occassionally a phone number shows up in weird places in the spreadsheet.
        # let's capture it
        phone_re = re.compile(r".+(\d{3}\s\d{3}\s\d{4})$")
        for row in df.iter_rows(named=True):
            details = copy.deepcopy(facility_schema)
            zcode, cleaned = self._repair_zip(row["Zip"], row["City"])
            if cleaned:
                details["_repaired_record"] = True
            street, cleaned = self._clean_street(row["Address"], row["City"])
            if cleaned:
                details["_repaired_record"] = True
            match = phone_re.search(row["Address"])
            if match:
                details["phone"] = match.group(1)
                details["_repaired_record"] = True
            full_address = ",".join([street, row["City"], row["State"], zcode]).upper()
            details["address"]["administrative_area"] = row["State"]
            locality, cleaned = self._repair_locality(row["City"], row["State"])
            if cleaned:
                details["_repaired_record"] = True
            details["address"]["locality"] = row["City"]
            details["address"]["postal_code"] = row["Zip"]
            details["address"]["street"] = row["Address"]
            details["name"] = row["Name"]
            details["population"]["male"]["criminal"] = row["Male Crim"]
            details["population"]["male"]["non_criminal"] = row["Male Non-Crim"]
            details["population"]["female"]["criminal"] = row["Female Crim"]
            details["population"]["female"]["non_criminal"] = row["Female Non-Crim"]
            if row["Male/Female"]:
                if "/" in row["Male/Female"]:
                    details["population"]["female"]["allowed"] = True
                    details["population"]["male"]["allowed"] = True
                elif "Female" in row["Male/Female"]:
                    details["population"]["female"]["allowed"] = True
                else:
                    details["population"]["male"]["allowed"] = True

            details["facility_type"] = row["Type Detailed"]
            details["avg_stay_length"] = row["FY25 ALOS"]
            details["inspection_date"] = row["Last Inspection End Date"]
            details["source_urls"].append(self.sheet_url)
            results[full_address] = details
        return results

    def _update_facility(self, old: dict, new: dict) -> dict:
        for k, v in new.items():
            if isinstance(v, dict):
                old[k] = self._update_facility(old[k], new[k])
            if not old.get(k, None):
                old[k] = v
        return old

    def scrape_facilities(self):
        """Scrape all ICE detention facility data from all 6 pages"""
        start_time = time.time()
        logger.info("Starting to scrape ICE.gov detention facilities...")
        self.facilities_data["scraped_date"] = datetime.datetime.now(datetime.UTC)
        self.facilities_data["facilities"] = self._load_sheet()

        # URLs for all pages
        urls = [f"{self.base_scrape_url}?exposed_form_display=1&page={i}" for i in range(6)]

        for page_num, url in enumerate(urls):
            logger.info("Scraping page %s/6...", page_num + 1)
            try:
                facilities = self._scrape_page(url)
            except Exception as e:
                logger.error("Error scraping page %s: %s", page_num + 1, e)
            logger.debug("Found %s facilities on page %s", len(facilities), page_num + 1)
            time.sleep(1)  # Be respectful to the server
            for facility in facilities:
                addr = facility["address"]
                street, cleaned = self._clean_street(addr["street"], addr["locality"])
                if cleaned:
                    facility["_repaired_record"] = True
                zcode, cleaned = self._repair_zip(addr["postal_code"], addr["locality"])
                if cleaned:
                    facility["_repaired_record"] = True
                locality, cleaned = self._repair_locality(addr["locality"], addr["administrative_area"])
                if cleaned:
                    facility["_repaired_record"] = True
                full_address = ",".join([street, locality, addr["administrative_area"], zcode]).upper()
                if full_address in self.facilities_data["facilities"].keys():
                    self.facilities_data["facilities"][full_address] = self._update_facility(
                        self.facilities_data["facilities"][full_address], facility
                    )
                    # update to the frequently nicer address from ice.gov
                    self.facilities_data["facilities"][full_address]["address"] = addr
                    # add scraped urls
                    for url in facility["source_urls"]:
                        # no dupes
                        if url in self.facilities_data["facilities"][full_address]["source_urls"]:
                            continue
                        self.facilities_data["facilities"][full_address]["source_urls"].append(url)
                # this is likely to produce _some_ duplicates, but it's a reasonable starting place
                else:
                    self.facilities_data["facilities"][facility["name"]] = facility

        self.facilities_data["scrape_runtime"] = time.time() - start_time
        logger.info("Total facilities scraped: %s", len(self.facilities_data["facilities"]))
        logger.info(" Completed in %s seconds", self.facilities_data["scrape_runtime"])
        return self.facilities_data

    def _scrape_updated(self, url: str):
        """Scrape first page to get "last updated" time"""
        if not url:
            logger.error("Could not find a time block! Guessing wildly!")
            return datetime.datetime.strptime(default_timestamp, timestamp_format)
        logger.debug("  Fetching: %s", url)
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
        except Exception as e:
            logger.error("  Error parsing %s: %s", url, e)
            return []
        soup = BeautifulSoup(response.content, "html.parser")
        times = soup.findAll("time")
        if not times:
            logger.error("Could not find a time block! Guessing wildly!")
            return datetime.datetime.strptime(default_timestamp, timestamp_format)
        # RFC8601 "UTC offset in the form ±HH:MM[:SS[.ffffff]]" is not supported (2025-09-04) in python's strptime implementation
        timestamp = times[0].get("datetime", default_timestamp)
        timestamp, tz = timestamp.rsplit("-", 1)
        tz = tz.replace(":", "")
        # another hack...we'll assume a US timestamp here...
        timestamp = f"{timestamp}-+{tz}"
        return datetime.datetime.strptime(timestamp, timestamp_format)

    def _scrape_page(self, page_url: str) -> list:
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

        facilities = []

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
        facility_selectors = [
            "li.grid",  # List items with grid class
            "div.views-row",  # View rows
            "li.views-row",  # List-based view rows
            "div.facility-item",  # Custom facility items
            "article",  # Article elements
            "div.node",  # Drupal node containers
        ]
        facility_elements: list = []
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
            logger.warning("  Using fallback: searching for facility patterns in text")
            facility_elements = self._find_facility_patterns(content_container)

        # Extract data from each facility element
        for element in facility_elements:
            facility_data = self._extract_single_facility(element, page_url)
            if facility_data and facility_data.get("name"):
                facilities.append(facility_data)

        logger.info("  Extracted %s facilities from page", len(facilities))

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
        raw_scrape = str(element)
        facility["raw_scrape"] = base64.b64encode(raw_scrape.encode("utf-8")).decode("utf-8")
        facility["source_urls"].append(page_url)
        logger.debug("Trying to get facility data from %s", element)
        # Method 1: Try structured extraction if element has proper HTML structure
        name = element.select_one(".views-field-title")
        if name:
            facility["name"] = name.text
        field_office = element.select_one(".views-field-field-field-office-name")
        if field_office:
            facility["field_office"] = field_office.text
        address = element.select_one(".address-line1")
        if address:
            facility["address"]["street"] = address.text
        locality = element.select_one(".locality")
        if locality:
            facility["address"]["locality"] = locality.text
        administrative_area = element.select_one(".administrative-area")
        if administrative_area:
            facility["address"]["administrative_area"] = administrative_area.text
        postal_code = element.select_one(".postal-code")
        if postal_code:
            facility["address"]["postal_code"] = postal_code.text
        country = element.select_one(".country")
        if country:
            facility["address"]["country"] = country.text
        phone = element.select_one(".ct-addr")
        if phone:
            facility["phone"] = phone.text

        # Method 2: If structured extraction failed, parse the text content
        if not facility["name"] or not facility["field_office"]:
            logger.warning("Falling back to text scraping!")
            facility = self._parse_facility_text(raw_scrape, facility)

        # Extract image URL using the specified nested structure
        image_element = element.findAll("img")
        if image_element:
            facility["image_url"] = f"https://www.ice.gov{image_element[0]['src']}"
        facility_url_element = element.findAll("a")
        facility_url = ""
        if facility_url_element:
            facility_url = f"https://www.ice.gov{facility_url_element[0]['href']}"
            facility["source_urls"].append(facility_url)
        if facility_url:
            facility["page_updated_date"] = self._scrape_updated(facility_url)
        # Clean up extracted data
        facility = self._clean_facility_data(facility)

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
