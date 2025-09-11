# BLS SDK (Python)

A friendly Python wrapper for the BLS Public Data API v2 (time series).

- Public Data API v2: time series across CPI, CES, JOLTS, LAUS, Productivity, etc.
- Built-in retries, client-side rate limiting, and clear error types.

Note: QCEW is distributed via separate Open Data CSV/ZIP files and is not covered by this client.

## Features

- Public Data API v2
	- Get single or multiple series (auto-chunk to API limits)
	- Options: `startyear`, `endyear`, `annualaverage`, `calculations`, `catalog`, `latest`
	- Latest data and popular series
	- Surveys listing and single-survey metadata
- Resilience
	- Retries with exponential backoff
	- Token-bucket rate limiter (default: 5 rps; configurable)
	- Typed exceptions: `HttpError`, `ApiError`, `ValidationError`

### New: Archived Release Schedule Scraper (Selenium)

- Scrapes `https://www.bls.gov/bls/archived_sched.htm` yearly List View pages
- Returns a pandas DataFrame by default; optionally JSON
- Extracts: `date` (YYYY-MM-DD), `time` (24h HH:MM), `release_title`, period fields, and notes
  - `period_year`, `period_month` (1–12), `period_quarter` (1–4)
  - `notes`: stripped tags like `Monthly`, `Annual`, `P`, `R`, or `Biennial`
- Handles edge cases (e.g., titles with `(Monthly)`, period-only `for Biennial`, and a.m./p.m. variants)

## Install

```bash
python -m pip install -r requirements.txt
python -m pip install -e .
```

Requires Google Chrome installed. Selenium Manager will automatically fetch a matching ChromeDriver.

## Configure

Create a `.env` file (see `.env.example`):

```
BLS_API_KEY=your_key_here
# Optional overrides
# BLS_PUBLIC_API_BASE=https://api.bls.gov/publicAPI/v2
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

client = PublicDataClient(HttpClient(rate_limit_per_second=1))

# Fetch CPI-U all items, 2023
resp = client.get_series("CUUR0000SA0", startyear="2023", endyear="2023")
print(resp["status"], len(resp["Results"]["series"][0]["data"]))

# List surveys (helper returns a list)
surveys = client.list_surveys_list()
print(len(surveys), surveys[0]["survey_abbreviation"] if surveys else None)
```

### Popular and latest

```python
# Popular CPI series
popular = client.get_popular(survey="cu")
ids = [s["seriesID"] for s in popular["Results"]["series"]]

# Latest observation(s)
latest = client.get_latest(ids[:3])
print(latest["status"])  # REQUEST_SUCCEEDED
```

### Multiple series with options

```python
from bls_sdk.public_data import PublicDataClient
from bls_sdk.http_client import HttpClient

pdc = PublicDataClient(HttpClient(rate_limit_per_second=2))
series = ["CUUR0000SA0", "CEU0000000001"]
result = pdc.get_many_series(series, startyear="2022", endyear="2023", annualaverage=False, calculations=True)
print(len(result))
```

### Scrape Archived Release Schedule

```python
from bls_sdk import scrape_archived_schedule

# One year as DataFrame
df_2024 = scrape_archived_schedule([2024])
print(df_2024.head())

# Multiple years combined and saved to CSV
years = [2021, 2022, 2023, 2024]
df = scrape_archived_schedule(years)
df.to_csv('releases_2021_2024.csv', index=False)

# As JSON
records = scrape_archived_schedule([2023, 2024], output="json")
```

DataFrame columns:

- `date` — normalized `YYYY-MM-DD`
- `time` — normalized `HH:MM` (24-hour)
- `release_title` — e.g., `Employment Situation`
- `period_year`, `period_month`, `period_quarter`
- `notes` — e.g., `Monthly`, `Annual`, `Biennial`, `P`, `R`
- `source_year_page` — the year page parsed
- `year_page_url` — final URL used (List View when available)

### Manual (pre-2008) schedules

The Selenium scraper intentionally skips years prior to 2008 (older pages differ and are often blocked). For those years, copy the schedule text files locally (e.g., `data/manual_scrapes/2007.txt`) and parse with the manual converter.

```python
from bls_sdk import parse_manual_schedule_txt, parse_manual_batch

# Parse a single year text file (returns DataFrame by default)
df_2007 = parse_manual_schedule_txt('data/manual_scrapes/2007.txt', 2007)

# Batch-parse multiple years from a directory (default: data/manual_scrapes)
df_pre = parse_manual_batch([2006, 2007])

# Or get JSON records
recs_2006 = parse_manual_schedule_txt('data/manual_scrapes/2006.txt', 2006, output='json')
```

Manual parsing returns the same schema as the Selenium scraper. For manual rows, `year_page_url` is `None`.

### Combine manual and live scraped schedules

```python
from bls_sdk import scrape_archived_schedule, parse_manual_batch
import pandas as pd

# Pre-2008 via manual files
pre_df = parse_manual_batch([2006, 2007])

# 2008+ via Selenium
post_df = scrape_archived_schedule([2021, 2022, 2023, 2024])

combined = pd.concat([pre_df, post_df], ignore_index=True)
combined.to_csv('bls_release_calendar.csv', index=False)
```

### Catalog metadata (titles)

```python
info = client.get_series("CUUR0000SA0", startyear="2023", endyear="2023", catalog=True)
series = info["Results"]["series"][0]
print(series.get("catalog", {}).get("series_title"))
```

## API limits (BLS Public Data v2)

- Daily: 500 queries (registered key)
- Series per query: up to 50
- Years per query: up to 20
- Rate: 50 requests per 10 seconds

## Errors

- `HttpError` — HTTP status >= 400
- `ApiError` — BLS API returned non-success status
- `ValidationError` — local argument validation

## Examples

See `examples/public_data_quickstart.ipynb` for an end-to-end walkthrough.

## Development & Tests

```bash
pytest -q
```

## License

MIT
