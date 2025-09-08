# For general helpers, regexes, or shared logic (e.g. phone/address parsing functions).
import logging
import requests
from requests.adapters import HTTPAdapter
import urllib3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

_retry_strategy = urllib3.Retry(
    total=4,
    backoff_factor=1,
)
_adapter = HTTPAdapter(max_retries=_retry_strategy)
session = requests.Session()
session.mount("https://", _adapter)
session.mount("http://", _adapter)
session.headers.update({"User-Agent": "ICE-Facilities-Research/1.0 (Educational Research Purpose)"})

default_timestamp = "1970-01-01T00:00:00-+0000"
timestamp_format = "%Y-%m-%dT%H:%M:%S-%z"

# extracted ADP sheet header list 2025-09-07
facility_sheet_header = [
    "Name",
    "Address",
    "City",
    "State",
    "Zip",
    "AOR",
    "Type Detailed",
    "Male/Female",
    "FY25 ALOS",
    "Level A",
    "Level B",
    "Level C",
    "Level D",
    "Male Crim",
    "Male Non-Crim",
    "Female Crim",
    "Female Non-Crim",
    "ICE Threat Level 1",
    "ICE Threat Level 2",
    "ICE Threat Level 3",
    "No ICE Threat Level",
    "Mandatory",
    "Guaranteed Minimum",
    "Last Inspection Type",
    "Last Inspection End Date",
    "Pending FY25 Inspection",
    "Last Inspection Standard",
    "Last Final Rating",
]


def _flatdict(d: dict, parent_key: str = "", sep: str = ".") -> dict:
    """flatten a nested dictionary for nicer printing in CSV"""
    items: list = []
    for k, v in d.items():
        new_key = parent_key + sep + str(k) if parent_key else str(k)
        if isinstance(v, dict):
            items.extend(_flatdict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)
