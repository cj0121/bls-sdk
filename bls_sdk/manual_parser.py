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
_DATE_RE = re.compile(
    r"(January|February|March|April|May|June|July|August|September|October|November|December|Jan\.?|Feb\.?|Mar\.?|Apr\.?|Jun\.?|Jul\.?|Aug\.?|Sep\.?|Sept\.?|Oct\.?|Nov\.?|Dec\.?)\s+(\d{1,2})(?:\s*,\s*(\d{4}))?(?!\d)",
    re.I,
)


def _normalize_time_24h(text: str) -> Optional[str]:
	text = (text or "").replace("\xa0", " ")
	# Prefer the rightmost time occurrence in case of stray tokens earlier
	match_list = list(_TIME_RE.finditer(text))
	if not match_list:
		return None
	m = match_list[-1]
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


def _filter_cross_year_records(records: List[Dict[str, Union[str, int, None]]]) -> List[Dict[str, Union[str, int, None]]]:
	"""Keep only rows where year(date) == source_year_page and drop duplicates.

	This mirrors the reference filtering that removes the first releases of the
	next year when they appear at the bottom of the prior year's manual page.
	Duplicates are identified on (date, release_title).
	"""
	filtered: List[Dict[str, Union[str, int, None]]] = []
	seen = set()
	for r in records:
		date_str = r.get("date")  # type: ignore[assignment]
		source_year = r.get("source_year_page")  # type: ignore[assignment]
		if not date_str or source_year is None:
			continue
		try:
			date_year = int(str(date_str)[:4])
			page_year = int(str(source_year))
		except Exception:
			continue
		if date_year != page_year:
			continue
		key = (str(date_str), str(r.get("release_title")))
		if key in seen:
			continue
		seen.add(key)
		filtered.append(r)
	return filtered


def parse_manual_schedule_txt(path: Union[str, Path], source_year: int, output: str = "dataframe") -> Union[List[Dict[str, Union[str, int, None]]], object]:
	"""Parse a manually saved schedule text file into release_schedule format.

	Expected line shape: '<Release Title>\t<Month> <day>[, <year>]\t<time>'
	We tolerate multiple spaces or tabs as separators.
	"""
	path = Path(path)
	lines = [ln.rstrip("\n") for ln in path.read_text(encoding="utf-8").splitlines()]
	records: List[Dict[str, Union[str, int, None]]] = []
	# Some BLS pages append a block of next-year January/February releases at the end.
	# Detect entry into that block when we first see an explicit Jan/Feb date labeled
	# with source_year+1, and from then on infer missing years on Jan/Feb rows as +1.
	cross_year_active = False
	for raw in lines:
		raw = raw.replace("\xa0", " ")
		if not raw.strip():
			continue
		low = raw.strip().lower()
		if low.startswith("release name") or low.startswith("schedule for ") or low.startswith("last modified date"):
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
		# Skip lines without a time (avoid footer/headers like 'Last Modified Date')
		if not time_part:
			continue

		# Parse date with strong preference for separated tokens (Month in parts[1], Day[, Year] in parts[2])
		m = None
		if len(parts) >= 3:
			candidate = f"{parts[1]} {parts[2]}".strip()
			m = _DATE_RE.search(candidate)
		if not m and len(parts) >= 4:
			candidate = f"{parts[1]} {parts[2]} {parts[3]}".strip()
			m = _DATE_RE.search(candidate)
		if not m:
			# Fallback: scan entire line but avoid false match on '<Month> 2007' (no day)
			m = next((_m for _m in _DATE_RE.finditer(raw)), None)
		if not m:
			continue
		month = _MONTH_TO_NUM[m.group(1).lower()]
		day = int(m.group(2))
		captured_year = m.group(3)
		if captured_year is not None:
			year = int(captured_year)
			# Toggle cross-year mode when we encounter explicit Jan/Feb of next year
			if (month in (1, 2)) and year == (source_year + 1):
				cross_year_active = True
		else:
			# No explicit year printed; infer from position/context
			year = source_year
			if (month in (1, 2)) and cross_year_active:
				year = source_year + 1
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

		# Normalize plural 'Indexes' to singular 'Index' for consistency
		clean_title = re.sub(r"\bIndexes\b", "Index", clean_title, flags=re.I)

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

	# Remove rows whose release year doesn't match the page year; also de-dupe
	records = _filter_cross_year_records(records)

	if output == "json":
		return records
	# Default to DataFrame
	import pandas as pd  # type: ignore
	df = pd.DataFrame.from_records(records, columns=[
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
	# Ensure strict de-duplication on (date, release_title)
	if not df.empty:
		df = df.drop_duplicates(subset=["date", "release_title"], keep="first").reset_index(drop=True)
	return df


def parse_manual_batch(years: Iterable[int], directory: Union[str, Path] = "data/manual_scrapes", output: str = "dataframe") -> Union[List[Dict[str, Union[str, int, None]]], object]:
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

	# Apply cross-year filter and global de-duplication across combined years
	all_records = _filter_cross_year_records(all_records)
	if output == "json":
		return all_records
	import pandas as pd  # type: ignore
	df = pd.DataFrame.from_records(all_records, columns=[
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
	if not df.empty:
		df = df.drop_duplicates(subset=["date", "release_title"], keep="first").reset_index(drop=True)
	return df


