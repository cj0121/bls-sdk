# BLS SDK (Python)

A friendly Python wrapper for the Bureau of Labor Statistics APIs.

- Public Data API v2: time series across CPI, CES, JOLTS, LAUS, Productivity, etc.
- QCEW: table-level CSV access with automatic `.csv.gz` fallback and helpers.
- Built-in retries, client-side rate limiting, and clear error types.

## Features

- Public Data API v2
	- Get single or multiple series (auto-chunk to API limits)
	- Options: `startyear`, `endyear`, `annualaverage`, `calculations`, `catalog`
	- Latest data and popular series
	- Surveys listing and single-survey metadata
- QCEW
	- Fetch table-level CSV for `area`, `industry`, `own`, `size` with `.gz` fallback
	- Convenience filter: `get_area_rows(year, quarter, area_code)`
- Resilience
	- Retries with exponential backoff
	- Token-bucket rate limiter (default: 5 rps; configurable)
	- Typed exceptions: `HttpError`, `ApiError`, `ValidationError`

## Install

```bash
python -m pip install -r requirements.txt
python -m pip install -e .
```

## Configure

Create a `.env` file (see `.env.example`):

```
BLS_API_KEY=your_key_here
# Optional overrides
# BLS_PUBLIC_API_BASE=https://api.bls.gov/publicAPI/v2
# BLS_QCEW_API_BASE=https://data.bls.gov/cew/data/api
# BLS_REQUEST_TIMEOUT_SECONDS=30
# BLS_MAX_RETRIES=3
# BLS_BACKOFF_INITIAL_SECONDS=0.5
# BLS_BACKOFF_MAX_SECONDS=5
# BLS_USER_AGENT=bls-sdk/0.1 (+your-link)
# BLS_RATE_LIMIT_PER_SECOND=5
```

## Quickstart

```python
from bls_sdk.public_data import PublicDataClient
from bls_sdk.http_client import HttpClient

# Respect rate limits (defaults to 5 rps). You can lower during development.
client = PublicDataClient(HttpClient(rate_limit_per_second=1))

# Fetch CPI-U all items, 2023
resp = client.get_series("CUUR0000SA0", startyear="2023", endyear="2023")
print(resp["status"], len(resp["Results"]["series"][0]["data"]))

# List surveys
surveys = client.list_surveys()
print(surveys["status"], len(surveys["Results"].get("surveys", [])))
```

### Multiple series with options

```python
from bls_sdk.public_data import PublicDataClient
from bls_sdk.http_client import HttpClient

pdc = PublicDataClient(HttpClient(rate_limit_per_second=2))
series = ["CUUR0000SA0", "CEU0000000001"]  # CPI all items, CES total nonfarm employment
result = pdc.get_many_series(series, startyear="2022", endyear="2023", annualaverage=False, calculations=False)
print(len(result))  # merged list of series dicts
```

### QCEW CSV access

The QCEW JSON endpoints can return 404 for many routes. This SDK fetches CSV tables first and falls back to `.csv.gz` automatically.

#### QCEW with titles (one call)

```python
from bls_sdk.qcew import QCEWClient
from bls_sdk.http_client import HttpClient

q = QCEWClient(HttpClient(rate_limit_per_second=1))
rows = q.get_table_with_titles(2024, "1", "area")
# rows now include area_title, industry_title, own_title, size_title when available
print(rows[0].get("area_title"))
```

Notes:
- Year must be within roughly the last 5 years (per QCEW Open Data hosting window).
- Quarter accepts `1`–`4` or `a` (also `q1`..`q4` / `annual`).
- The client tries `.csv` first, then `.csv.gz`. 404s are returned as empty results.
- Title CSVs are cached in-memory to avoid extra requests.


```python
from bls_sdk.qcew import QCEWClient
from bls_sdk.http_client import HttpClient

q = QCEWClient(HttpClient(rate_limit_per_second=1))
# Table-level CSV (returns list[dict]) — you can filter rows by your own criteria
rows = q.get_table_csv(2022, "a", "industry")  # year, quarter (1/2/3/4 or 'a'), level
print(len(rows))

# Convenience: filter by area code (FIPS) after loading the area table
la_rows = q.get_area_rows(2022, "a", "06000")  # Los Angeles-Long Beach-Anaheim MSA (example)
print(len(la_rows))
```

Note: Valid QCEW table URLs and codes vary by year/quarter. If a plain CSV returns 404, the client will try the `.csv.gz` variant automatically.

## API limits (BLS Public Data v2)

- Daily: 500 queries (registered key)
- Series per query: up to 50
- Years per query: up to 20
- Rate: 50 requests per 10 seconds

This SDK defaults to a conservative rate limit (5 rps). Tune via `BLS_RATE_LIMIT_PER_SECOND` or `HttpClient(rate_limit_per_second=...)`.

## Errors

- `HttpError` — HTTP status >= 400. Includes `status_code`, `url`, and response body.
- `ApiError` — BLS API returned non-success status; includes messages array.
- `ValidationError` — Local argument validation failure.

## Development & Tests

- Run all tests:

```bash
pytest -q
```

- Live integration tests (skipped unless `BLS_API_KEY` is set):

```bash
export BLS_API_KEY=your_key_here
pytest -q tests/integration
```

The integration suite uses a conservative 1 rps rate and only a few requests by default.

## Roadmap

- Public Data convenience: series ID helpers and catalogs
- QCEW: add more targeted helpers and lookups (areas, NAICS, ownership, size)
- CLI: quick `bls ts` and `bls qcew` commands for ad-hoc queries
- Typed models and optional pandas integrations

## License

MIT
