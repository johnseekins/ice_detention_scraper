# ICE Facility scrapers

----

These files maintain the code to collect (and collate) ICE facility data
from a number of sources.

## utils.py

Contains most of our collating functions and shared functions that scrapers
may need.

## __init__.py

Contains some static objects and import declarations (so we can `from ice_scrapers import`
successfully)...

## spreadsheet_load.py

ICE is required by law to produce regular custody data. We can pull that data from
here `https://www.ice.gov/detain/detention-management`. Because this spreadsheet is
more "complete" than other sources we've found, we use it as our base scrape.

## facilities_scraper.py

Pulls information about ICE detention facilities from
`https://www.ice.gov/detention-facilities`. This can add additional (or corrected)
data about facilities locations, contact information, and provides facility images.

## field_offices.py

Collects additional data about ICE/DHS field offices from
`https://www.ice.gov/contact/field-offices`. Largely basic areas of responsibility
and contact info for the field office.

> The field-offices page shows information about a number of different offices. As we
> are largely focused on detention, ERO (Enforcement and Removal Operations) centers
> are the most interesting.

## custom_facilities.py

Some facilities we may discover manually. Or they may be "pending" classification, but we discover them early on. These facilities are defined here.
