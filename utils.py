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
