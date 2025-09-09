import csv
import io
from typing import Dict, List, Optional

import requests
import re

from .config import USER_AGENT
from .rate_limiter import RateLimiter

_BASE = "https://download.bls.gov/pub/time.series/"


def _http_get(url: str, timeout_seconds: int, headers: Dict[str, str]) -> requests.Response:
	resp = requests.get(url, timeout=timeout_seconds, headers=headers)
	return resp


def _fetch_text(url: str, timeout_seconds: int = 30, user_agent: Optional[str] = None) -> str:
	headers = {
		"User-Agent": user_agent or USER_AGENT or "bls-sdk/0.1 (series-catalog)",
		"Accept": "text/plain, text/tab-separated-values, */*; q=0.8",
		"Accept-Language": "en-US,en;q=0.9",
		"Referer": "https://www.bls.gov/",
	}
	# Try HTTPS first
	resp = _http_get(url, timeout_seconds, headers)
	if resp.status_code in (403, 406):
		alt = url.replace("https://", "http://", 1)
		resp = _http_get(alt, timeout_seconds, headers)
	resp.raise_for_status()
	try:
		return resp.content.decode("utf-8")
	except UnicodeDecodeError:
		return resp.content.decode("latin-1", errors="replace")


def fetch_series_for_survey(survey: str, rate_limit_per_second: float = 2.0) -> List[Dict[str, str]]:
	"""Fetch and parse the .series TSV for a given survey (e.g., 'cu')."""
	survey = survey.strip("/ ").lower()
	url = f"{_BASE}{survey}/{survey}.series"
	# Light client-side rate limit
	RateLimiter(rate_limit_per_second).acquire()
	text = _fetch_text(url)
	# Collapse multiple tabs to a single tab, then parse
	text = re.sub(r"\t+", "\t", text)
	reader = csv.DictReader(io.StringIO(text), delimiter='\t')
	# Normalize header names by stripping whitespace
	if reader.fieldnames:
		reader.fieldnames = [fn.strip() if fn is not None else fn for fn in reader.fieldnames]
	rows = []
	for row in reader:
		norm = {}
		for k, v in row.items():
			key = k.strip() if isinstance(k, str) else k
			val = v.strip() if isinstance(v, str) else v
			norm[key] = val
		rows.append(norm)
	return rows


def fetch_cu_series(rate_limit_per_second: float = 2.0) -> List[Dict[str, str]]:
	return fetch_series_for_survey("cu", rate_limit_per_second=rate_limit_per_second)
