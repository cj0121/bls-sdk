import re
from typing import Iterable, List, Dict, Union, Optional
from pathlib import Path

_MONTH_TO_NUM = {
	"january": 1, "jan": 1, "jan.": 1,
	"february": 2, "feb": 2, "feb.": 2,
	"march": 3, "mar": 3, "mar.": 3,
	"april": 4, "apr": 4, "apr.": 4,
	"may": 5,
	"june": 6, "jun": 6, "jun.": 6,
	"july": 7, "jul": 7, "jul.": 7,
	"august": 8, "aug": 8, "aug.": 8,
	"september": 9, "sep": 9, "sep.": 9, "sept": 9, "sept.": 9,
	"october": 10, "oct": 10, "oct.": 10,
	"november": 11, "nov": 11, "nov.": 11,
	"december": 12, "dec": 12, "dec.": 12,
}

_TIME_RE = re.compile(r"\b(\d{1,2})(?::(\d{2}))?\s*(a\.?m\.?|p\.?m\.?)\b", re.I)


def _normalize_time_24h(text: str) -> Optional[str]:
	text = (text or "").replace("\xa0", " ")
	m = _TIME_RE.search(text)
	if not m:
		return None
	h = int(m.group(1))
	mins = int(m.group(2) or 0)
	ampm = m.group(3).lower()
	if ampm.startswith("a"):
		if h == 12:
			h = 0
	else:
		if h != 12:
			h += 12
	return f"{h:02d}:{mins:02d}"


def _strip_notes(title: str) -> (str, Optional[str]):
	# Extract trailing parenthetical notes and normalize
	m = re.search(r"(\s*\([^)]+\)\s*)+$", title)
	if not m:
		return title.strip(), None
	inner = re.findall(r"\(([^)]+)\)", m.group(0))
	tokens = []
	for raw in inner:
		t = raw.strip().replace(".", "")
		if not t:
			continue
		if t.lower() in {"p", "r"}:
			tokens.append(t.upper())
		else:
			tokens.append(t.title())
	notes = ", ".join(tokens) or None
	return title[: m.start()].strip(), notes


def parse_manual_schedule_txt(path: Union[str, Path], source_year: int, output: str = "dataframe") -> Union[List[Dict[str, Union[str, int, None]]], "pd.DataFrame"]:
	"""Parse a manually saved schedule text file into release_schedule format.

	Expected line shape: '<Release Title>\t<Month> <day>[, <year>]\t<time>'
	We tolerate multiple spaces or tabs as separators.
	"""
	path = Path(path)
	lines = [ln.rstrip("\n") for ln in path.read_text(encoding="utf-8").splitlines()]
	records: List[Dict[str, Union[str, int, None]]] = []
	for raw in lines:
		raw = raw.replace("\xa0", " ")
		if not raw.strip() or raw.strip().lower().startswith("release name"):
			continue
		# Split by two or more spaces or tabs
		parts = re.split(r"\s{2,}|\t+", raw.strip())
		if len(parts) < 2:
			continue
		title = parts[0].strip()
		# date may be parts[1]; time may be parts[2] or embedded in parts[1]
		date_part = parts[1].strip()
		# Always scan full line for time to avoid misses
		time_part = _normalize_time_24h(raw) or ""

		# Parse date like 'Jan.  5, 2007' or 'Jan.  5' (assume year from context)
		m = re.search(r"(Jan\.?|Feb\.?|Mar\.?|Apr\.?|May|Jun\.?|Jul\.?|Aug\.?|Sep\.?|Sept\.?|Oct\.?|Nov\.?|Dec\.?)\s+(\d{1,2})(?:,\s*(\d{4}))?", date_part, re.I)
		if not m:
			# Try to find elsewhere on the line
			m = re.search(r"(Jan\.?|Feb\.?|Mar\.?|Apr\.?|May|Jun\.?|Jul\.?|Aug\.?|Sep\.?|Sept\.?|Oct\.?|Nov\.?|Dec\.?)\s+(\d{1,2})(?:,\s*(\d{4}))?", raw, re.I)
		if not m:
			continue
		month = _MONTH_TO_NUM[m.group(1).lower()]
		day = int(m.group(2))
		year = int(m.group(3) or source_year)
		date_iso = f"{year:04d}-{month:02d}-{day:02d}"

		clean_title, notes = _strip_notes(title)
		# Drop leading 'The ' for Employment Situation
		if clean_title.startswith("The Employment Situation"):
			clean_title = clean_title[4:]

		# Period extraction from title like ", Fourth Quarter 2006", ", December 2006" or trailing ", 2006"
		period_year = None
		period_month = None
		period_quarter = None
		# Tail ', <Quarter> Quarter <YYYY>'
		m_q = re.search(r",\s*(First|Second|Third|Fourth)\s+Quarter\s+(\d{4})\s*$", clean_title, re.I)
		if m_q:
			quarter_word = m_q.group(1).lower()
			period_quarter = {"first": 1, "second": 2, "third": 3, "fourth": 4}[quarter_word]
			period_year = int(m_q.group(2))
			clean_title = clean_title[: m_q.start()].rstrip(', ').strip()
		else:
			# Tail ', <Month> <YYYY>'
			m_tail = re.search(r",\s*(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})\s*$", clean_title, re.I)
			if m_tail:
				period_month = _MONTH_TO_NUM[m_tail.group(1).lower()]
				period_year = int(m_tail.group(2))
				clean_title = clean_title[: m_tail.start()].rstrip(', ').strip()
			else:
				m_tail_y = re.search(r",\s*(\d{4})\s*$", clean_title)
				if m_tail_y:
					period_year = int(m_tail_y.group(1))
					clean_title = clean_title[: m_tail_y.start()].rstrip(', ').strip()

		records.append({
			"date": date_iso,
			"time": time_part or "",
			"release_title": clean_title,
			"period_year": period_year,
			"period_month": period_month,
			"period_quarter": period_quarter,
			"notes": notes,
			"source_year_page": source_year,
			"year_page_url": None,
		})

	if output == "json":
		return records
	# Default to DataFrame
	import pandas as pd  # type: ignore
	return pd.DataFrame.from_records(records, columns=[
		"date",
		"time",
		"release_title",
		"period_year",
		"period_month",
		"period_quarter",
		"notes",
		"source_year_page",
		"year_page_url",
	])


def parse_manual_batch(years: Iterable[int], directory: Union[str, Path] = "data/manual_scrapes", output: str = "dataframe") -> Union[List[Dict[str, Union[str, int, None]]], "pd.DataFrame"]:
	"""Parse multiple manual schedule text files (one per year) and combine.

	Parameters:
	- years: iterable of ints (e.g., [2004, 2005])
	- directory: root folder containing '<year>.txt'
	- output: 'dataframe' (default) or 'json'

	Returns a combined pandas DataFrame or list of dicts in the same schema as parse_manual_schedule_txt.
	"""
	directory = Path(directory)
	all_records: List[Dict[str, Union[str, int, None]]] = []
	for y in years:
		p = directory / f"{int(y)}.txt"
		if not p.exists():
			continue
		records_or_df = parse_manual_schedule_txt(p, int(y), output="json")
		# ensure list
		all_records.extend(records_or_df)  # type: ignore[arg-type]

	if output == "json":
		return all_records
	import pandas as pd  # type: ignore
	return pd.DataFrame.from_records(all_records, columns=[
		"date",
		"time",
		"release_title",
		"period_year",
		"period_month",
		"period_quarter",
		"notes",
		"source_year_page",
		"year_page_url",
	])


