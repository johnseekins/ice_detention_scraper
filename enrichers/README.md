# Facility enrichment scrapers

These functions let us collect data about facilities from additional sources.

## Enrichment class

The base class we can build enrichment tools from. Largely ensures some consistent
in functionality between enrichment tools.

### Available functions

Sub-classing `Enrichment` provides the following functions/objects:

* `self.resp_info`
    * Pre-created response object following our expected schema
* `self._wait_time`
    * simple rate-limiting through `time.sleep()` calls, `wait_time` tells us how long we should sleep between calls to an individual API/site.
    * Defaults to `1` (seconds)
* `self._req(...)`
    * Wrapper function around a call to `requests.get` (using a properly configured `session` object)
    * handles redirects
    * supports most normal requests function calls (`params`, `timeout`, `stream`, custom headers)
    * raises for non-2xx/3xx status
    * returns the entire `requests.Response` object for manipulation
* `_minimal_clean_facility_name(str)`
    * standardizes facility name for searching
* `_clean_facility_name(str)`
    * standardizes facility name for searching
    * more aggressive formatting than `_minimal_...` above

> All child functions should implement the `search()` function, which should return a dictionary using the `enrich_resp_schema` schema.
