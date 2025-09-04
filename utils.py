# For general helpers, regexes, or shared logic (e.g. phone/address parsing functions).

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

facility_obj = {
    "name": "",
    "field_office": "",
    "address": "",
    "phone": "",
    "facility_url": "",
    "image_url": "",
    "source_url": "",
}
