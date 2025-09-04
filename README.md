# ICE Detention Facilities Scraper

_ICE Detention Facilities Data Scraper and Enricher_, a Python script managed by the [Open Security Mapping Project](https://github.com/Open-Security-Mapping-Project).

In short this will help identify the online profile of each ICE detention facility. Please see the [project home page](https://github.com/Open-Security-Mapping-Project)
for more about mapping these facilities and other detailed info sources.

This script scrapes ICE detention facility data from ICE.gov and enriches it
with information from [Wikipedia](https://en.wikipedia.org), [Wikidata](https://wikidata.org), and
[OpenStreetMap](https://openstreetmap.org).

The main purpose right now is to identify if the detention facilities have data on Wikipedia, Wikidata and OpenStreetMap,
which will help with documenting the facilities appropriately. As these entries get fixed up, you should be able to see
your CSV results change almost immediately.

You can also use `--load-existing` to leverage an existing
scrape of the data from ICE.gov. This is stored in data_loader.py and includes the official current addresses of facilities.
(Note ICE has been renaming known "detention center" sites to "processing center", and so on.)

It also shows the ICE "field office" managing each detention facility.

On the OpenStreetMap (OSM) CSV results, if the URL includes a "way" then it has probably identified the correctly tagged
polygon. If you visit that URL you should see the courthouse or "prison grounds" way / area info. (This info can always
be improved, but at least it exists.)

On Wikipedia results the result will tend to be the first hit on the list of suggested pages, if it can't find the page
directly.

The script is MIT license, please feel free to fork it and/or submit patches.

The script should be compliant with these websites' rate limiting for queries.

At this point of development you probably want "enable all debugging" to see the results below.

## Usage:

Run the script and by default it will put a CSV file called `ice_detention_facilities_enriched.csv` in the same
directory.

```bash
    uv run python main.py --scrape          # Scrape fresh data from ICE website
    uv run python main.py --enrich          # Enrich existing data with external sources
    uv run python main.py --scrape --enrich # Do both operations
    uv run python main.py --help            # Show help

    # Enable Wikipedia debugging (extra col in CSV)
    uv run python main.py --load-existing --enrich --debug-wikipedia
    # Enable all debugging (extra cols in CSV) - this is recommended right now:
    uv run python main.py --load-existing --enrich --debug-wikipedia --debug-wikidata --debug-osm

    # With custom output file
    uv run python main.py --load-existing --enrich --debug-wikipedia -o debug_facilities.csv
```

## Requirements:

* Install [and enable mise](https://mise.jdx.dev/getting-started.html)
* Install dependencies

### Linux:

```bash
    # easiest command, but you may prefer using your package manager: https://mise.jdx.dev/installing-mise.html
    curl https://mise.run | bash
    if ! grep -q 'eval "$(mise activate bash --shims)"' "$HOME/.bashrc"; then echo 'eval "$(mise activate bash --shims)"' >> ~/.bashrc; fi
    mise install
    eval "$(mise activate bash)"
    pip install --upgrade pip wheel uv
    uv sync
```

### OS X:

```zsh
    brew install mise
    if ! grep -q 'eval "$(mise activate zsh --shims)"' "$HOME/.zshrc"; then echo 'eval "$(mise activate zsh --shims)"' >> ~/.zshrc; fi
    mise install
    eval "$(mise activate zsh)"
    pip install --upgrade pip wheel uv
    uv sync
```

## Todo / Known Issues:

* The enrichment on both Wikidata and Wikipedia is pretty messy & inaccurate right now. It tries to truncate common words
in hopes of finding similarly named pages but this is too aggressive, and it veers way off. (That is, it's looking for places
that have simpler names, like the county name instead of `county + detention center`). Use the debug mode to see what
it is doing.
* ICE scraping is not robustly tested. The image URL extraction needs some work. (should be able to get the detention center image URLs.)
* OSM enrichment submits to OSM Nominatim API search with an extra comma between address number and street name.
* The user-agent for running ice.gov scrape web requests calls itself `'User-Agent': 'ICE-Facilities-Research/1.0 (Educational Research Purpose)'`.
You can change this in scraper.py and enricher.py.
* It tells some pretty inaccurate percentages in the final summary - a lot of false positives, the Wikipedia debug percent
seems wrong.
* The remote query rate limiting is (I think) done in series but would go faster with parallel/async processing.
* This is only targeted at English (EN) Wikipedia currently, but  multi-lingual page checks would help a wider audience.

## Credit:

Original version by Dan Feidt ([@HongPong](https://github.com/HongPong)), with assistance from various AI gizmos. (My
first real Python program, please clap.)

## License:

MIT License.
