# For general helpers, regexes, or shared logic (e.g. phone/address parsing functions).
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())
