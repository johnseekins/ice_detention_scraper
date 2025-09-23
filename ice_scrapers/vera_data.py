import os
import polars
from utils import (
    logger,
    session,
)

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
# Github can aggressively rate-limit requests, so this may fail in surprising ways!
base_url = (
    "https://raw.githubusercontent.com/vera-institute/ice-detention-trends/refs/heads/main/metadata/facilities.csv"
)
filename = f"{SCRIPT_DIR}{os.sep}vera_facilities.csv"


def collect_vera_facility_data(facilities_data: dict, keep_csv: bool = True) -> dict:
    res = session.get(base_url, timeout=120, stream=True)
    res.raise_for_status()
    size = len(res.content)
    with open(filename, "wb") as f:
        for chunk in res.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
    logger.debug("Wrote %s byte sheet to %s", size, filename)
    df = polars.read_csv(
        has_header=True,
        raise_if_empty=True,
        source=filename,
        use_pyarrow=True,
    )
    logger.info(df)
    exit(1)
