from typing import Dict, Optional, List, Iterable

from .http_client import HttpClient
from .config import QCEW_API_BASE

import csv
import io
import gzip
import requests
import datetime


class QCEWClient:
	def __init__(self, http: Optional[HttpClient] = None):
		self.http = http or HttpClient()
		self._titles_cache: Dict[str, List[Dict]] = {}

	def _get(self, path: str) -> Dict:
		url = f"{QCEW_API_BASE}/{path}.json"
		return self.http.get_json(url)

	def get_by_area(self, year: int, quarter: str, area_code: str) -> Dict:
		return self._get(f"{year}/{quarter}/area/{area_code}")

	def get_by_industry(self, year: int, quarter: str, naics: str) -> Dict:
		return self._get(f"{year}/{quarter}/industry/{naics}")

	def get_by_ownership(self, year: int, quarter: str, own_code: str) -> Dict:
		return self._get(f"{year}/{quarter}/own/{own_code}")

	def get_by_size(self, year: int, quarter: str, size_code: str) -> Dict:
		return self._get(f"{year}/{quarter}/size/{size_code}")

	def get_table_csv(self, year: int, quarter: str, level: str, gz: bool = False) -> List[Dict]:
		self._validate_year_window(year)
		q = self._normalize_quarter(quarter)
		ext = 'csv.gz' if gz else 'csv'
		url = f"{QCEW_API_BASE}/{year}/{q}/{level}.{ext}"
		r = requests.get(url, timeout=30)
		if r.status_code == 404 and not gz:
			# try gz fallback automatically
			return self.get_table_csv(year, quarter, level, gz=True)
		import requests as _rq
		if r.status_code == 404:
			return []
		r.raise_for_status()
		content = r.content
		if gz:
			content = gzip.decompress(content)
		text = content.decode("utf-8", errors="replace")
		reader = csv.DictReader(io.StringIO(text))
		return list(reader)

	def get_area_rows(self, year: int, quarter: str, area_code: str) -> List[Dict]:
		rows = self.get_table_csv(year, quarter, 'area')
		key = 'area_fips'
		return [r for r in rows if r.get(key) == area_code]


	def _normalize_quarter(self, quarter: str) -> str:
		q = str(quarter).strip().lower()
		if q in {"a", "annual"}:
			return "a"
		if q in {"1", "q1"}:
			return "1"
		if q in {"2", "q2"}:
			return "2"
		if q in {"3", "q3"}:
			return "3"
		if q in {"4", "q4"}:
			return "4"
		raise ValueError(f"Invalid quarter: {quarter}")

	def _validate_year_window(self, year: int) -> None:
		cur = datetime.datetime.utcnow().year
		min_year = cur - 4
		if year < min_year or year > cur:
			from .errors import ValidationError
			raise ValidationError(f"QCEW open data hosts approximately last 5 years ({min_year}-{cur}); got {year}")


	def _fetch_title_csv(self, name: str) -> List[Dict]:
		# Try a few known patterns; many sites host only gz
		paths = [
			name + '.csv',
			name + '.csv.gz',
		]
		for suffix in paths:
			url = f"{QCEW_API_BASE}/{suffix}"
			r = requests.get(url, timeout=30)
			if r.status_code == 404:
				continue
			r.raise_for_status()
			content = r.content
			if suffix.endswith('.gz'):
				content = gzip.decompress(content)
			text = content.decode('utf-8', errors='replace')
			return list(csv.DictReader(io.StringIO(text)))
		return []

	def get_area_titles(self) -> List[Dict]:
		return self._fetch_title_csv('area_titles')

	def get_industry_titles(self) -> List[Dict]:
		return self._fetch_title_csv('industry_titles')

	def get_ownership_titles(self) -> List[Dict]:
		return self._fetch_title_csv('ownership_titles')

	def get_size_titles(self) -> List[Dict]:
		return self._fetch_title_csv('size_titles')

	def join_titles(self, rows: List[Dict],
			areas: Optional[List[Dict]] = None,
			industries: Optional[List[Dict]] = None,
			ownerships: Optional[List[Dict]] = None,
			sizes: Optional[List[Dict]] = None,
		) -> List[Dict]:
		# Build maps if provided
		area_map = {r.get('area_fips'): r.get('area_title') for r in (areas or [])}
		ind_map = {r.get('industry_code'): r.get('industry_title') for r in (industries or [])}
		own_map = {r.get('own_code'): r.get('own_title') for r in (ownerships or [])}
		size_map = {r.get('size_code'): r.get('size_title') for r in (sizes or [])}
		out: List[Dict] = []
		for r in rows:
			copy = dict(r)
			if area_map:
				code = r.get('area_fips')
				if code in area_map:
					copy['area_title'] = area_map[code]
			if ind_map:
				code = r.get('industry_code')
				if code in ind_map:
					copy['industry_title'] = ind_map[code]
			if own_map:
				code = r.get('own_code')
				if code in own_map:
					copy['own_title'] = own_map[code]
			if size_map:
				code = r.get('size_code')
				if code in size_map:
					copy['size_title'] = size_map[code]
			out.append(copy)
		return out


	def _get_titles_cached(self, name: str) -> List[Dict]:
		if name in self._titles_cache:
			return self._titles_cache[name]
		fetcher = {
			'area_titles': self.get_area_titles,
			'industry_titles': self.get_industry_titles,
			'ownership_titles': self.get_ownership_titles,
			'size_titles': self.get_size_titles,
		}.get(name)
		if not fetcher:
			return []
		rows = fetcher()
		self._titles_cache[name] = rows
		return rows

	def get_table_with_titles(self, year: int, quarter: str, level: str) -> List[Dict]:
		rows = self.get_table_csv(year, quarter, level)
		areas = self._get_titles_cached('area_titles')
		inds = self._get_titles_cached('industry_titles')
		owns = self._get_titles_cached('ownership_titles')
		sizes = self._get_titles_cached('size_titles')
		return self.join_titles(rows, areas=areas, industries=inds, ownerships=owns, sizes=sizes)
