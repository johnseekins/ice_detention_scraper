"""
Import order here is a touch weird, but we need it so
types exist before attempting to import functions that
may call them
"""

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

# extracted from https://www.ice.gov/doclib/detention/FY25_detentionStats08292025.xlsx 2025-09-07
ice_facility_types = {
    "BOP": {
        "expanded_name": "Federal Bureau of Prisons",
        "description": "A facility operated by the Federal Bureau of Prisons",
    },
    "DIGSA": {
        "expanded_name": "Dedicated Intergovernmental Service Agreement",
        "description": "A publicly-owned facility operated by state/local government(s), or private contractors, in which ICE contracts to use all bed space via a Dedicated Intergovernmental Service Agreement; or facilities used by ICE pursuant to Inter-governmental Service Agreements, which house only ICE detainees – typically these are operated by private contractors pursuant to their agreements with local governments.",
    },
    "IGSA": {
        "expanded_name": "Intergovernmental Service Agreement",
        "description": "A publicly-owned facility operated by state/local government(s), or private contractors, in which ICE contracts for bed space via an Intergovernmental Service Agreement; or local jails used by ICE pursuant to Inter-governmental Service Agreements, which house both ICE and non-ICE detainees, typically county prisoners awaiting trial or serving short sentences, but sometimes also USMS prisoners.",
    },
    "SPC": {
        "expanded_name": "Service Processing Center",
        "description": "A facility owned by the government and staffed by a combination of federal and contract employees.",
    },
    "USMS": {
        "expanded_name": "United States Marshals Service",
        "description": "A facility primarily contracted with the USMS for housing of USMS detainees, in which ICE contracts with the USMS for bed space.",
    },
    # two keys for the same thing as it isn't consistently defined
    "USMSIGA": {
        "expanded_name": "United States Marshal Service Intergovernmental Agreement",
        "description": "A USMS Intergovernmental Agreement in which ICE agrees to utilize an already established US Marshal Service contract.",
    },
    "USMS IGA": {
        "expanded_name": "United States Marshal Service Intergovernmental Agreement",
        "description": "A USMS Intergovernmental Agreement in which ICE agrees to utilize an already established US Marshal Service contract.",
    },
    "USMS CDF": {
        "expanded_name": "United States Marshal Service Contract Detention Facility",
        "description": "Name derived from listing at https://www.vera.org/ice-detention-trends",
    },
    "CDF": {
        "expanded_name": "Contract Detention Facility",
        "description": "Name derived from listing at https://www.vera.org/ice-detention-trends",
    },
}

from .utils import (  # noqa: E402
    clean_street,  # noqa: F401
    repair_zip,  # noqa: F401
    repair_locality,  # noqa: F401
    update_facility,  # noqa: F401
)
from .page_load import scrape_facilities  # noqa: F401,E402
from .spreadsheet_load import load_sheet  # noqa: F401,E402
from .field_offices import (  # noqa: E402
    merge_field_offices,  # noqa: F401
    scrape_field_offices,  # noqa: F401
)
