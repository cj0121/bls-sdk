from typing import Iterable, List, Dict, Union
import re
import time

from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup

from .config import USER_AGENT


_ARCHIVE_URL = "https://www.bls.gov/bls/archived_sched.htm"

_MONTH_TO_NUM = {
	"january": 1,
	"february": 2,
	"march": 3,
	"april": 4,
	"may": 5,
	"june": 6,
	"july": 7,
	"august": 8,
	"september": 9,
	"october": 10,
	"november": 11,
	"december": 12,
}

_QUARTER_WORD_TO_NUM = {
	"first": 1,
	"second": 2,
	"third": 3,
	"fourth": 4,
}


def _new_driver(headless: bool = True) -> webdriver.Chrome:
	opts = ChromeOptions()
	if headless:
		opts.add_argument("--headless=new")
	opts.add_argument(f"--user-agent={USER_AGENT}")
	opts.add_argument("--no-sandbox")
	opts.add_argument("--disable-dev-shm-usage")
	return webdriver.Chrome(options=opts)


def _scrape_year_page_html(driver: webdriver.Chrome, year_url: str) -> str:
	driver.get(year_url)
	# Prefer List View if present
	links = driver.find_elements(By.LINK_TEXT, "List View")
	if links:
		links[0].click()
		time.sleep(0.5)
	return driver.page_source


def _extract_rows_with_selenium(driver: webdriver.Chrome) -> List[Dict[str, str]]:
	# Parse the page source with BeautifulSoup to avoid Selenium grabbing nested text
	html = driver.page_source
	soup = BeautifulSoup(html, "html.parser")
	rows: List[Dict[str, str]] = []

	def norm(s: str) -> str:
		return " ".join((s or "").split())

	weekday = r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)"
	month = r"(?:January|February|March|April|May|June|July|August|September|October|November|December)"
	date_re = re.compile(rf"^{weekday},\s+{month}\s+\d{{1,2}},\s+\d{{4}}$", re.I)
	time_re = re.compile(r"^\d{1,2}:\d{2}\s*(?:AM|PM)$", re.I)

	# Iterate all tables; inside each table, locate the header row, then scan remaining rows
	for table in soup.find_all("table"):
		trs = table.find_all("tr")
		header_index = -1
		for i, tr in enumerate(trs):
			ths = tr.find_all("th")
			if not ths:
				continue
			headers = [norm(th.get_text(" ")).lower().rstrip(":") for th in ths]
			if headers[:3] == ["date", "time", "release"]:
				header_index = i
				break
		if header_index == -1:
			continue
		for tr in trs[header_index+1:]:
			if tr.find("th"):
				break
			cells = tr.find_all("td")
			if len(cells) != 3:
				continue
			# Preserve original spacing in the date cell (no trimming), but use a trimmed copy for validation
			date_cell_raw = (cells[0].get_text(" ", strip=False) or "").replace("\xa0", " ")
			date_cell_trim = date_cell_raw.strip()
			time_str = norm((cells[1].get_text(" ", strip=False) or "").replace("\xa0", " "))
			release_str = norm((cells[2].get_text(" ", strip=False) or "").replace("\xa0", " "))
			if not date_re.match(date_cell_trim):
				continue
			if not time_re.match(time_str):
				continue
			if not release_str:
				continue
			rows.append({"date_raw": date_cell_raw, "time": time_str, "release_raw": release_str})
	return rows


def scrape_archived_schedule(years: Iterable[int], output: str = "dataframe") -> Union["pd.DataFrame", List[Dict[str, Union[str, int, None]]]]:
	"""Selenium-based scraper for BLS Archived Release Schedule.

	Returns pandas DataFrame by default, or list[dict] when output="json".
	"""
	records: List[Dict[str, Union[str, int, None]]] = []
	driver = _new_driver(headless=True)
	try:
		# Load archive to discover year links
		driver.get(_ARCHIVE_URL)
		year_elems = driver.find_elements(By.PARTIAL_LINK_TEXT, "20") + driver.find_elements(By.PARTIAL_LINK_TEXT, "19")
		text_to_href = {e.text.strip(): e.get_attribute("href") for e in year_elems if e.get_attribute("href")}
		for y in years:
			y_int = int(y)
			# Find a link that contains the year
			candidate = None
			for k, v in text_to_href.items():
				if str(y_int) in k or str(y_int) in (v or ""):
					candidate = v
					break
			if not candidate:
				candidate = f"https://www.bls.gov/bls/schedule/archives/all_{y_int}_sched.htm"
			# Open year page (prefer List View)
			driver.get(candidate)
			time.sleep(0.5)
			links = driver.find_elements(By.LINK_TEXT, "List View")
			if links:
				links[0].click()
				time.sleep(0.5)
			# Extract table rows
			record_rows = _extract_rows_with_selenium(driver)
			for r in record_rows:
				title, p_year, p_month, p_quarter = _parse_release_text(r["release_raw"])
				records.append({
					"date": r["date_raw"],
					"time": r["time"],
					"release_title": title,
					"period_year": p_year,
					"period_month": p_month,
					"period_quarter": p_quarter,
					"source_year_page": y_int,
					"year_page_url": driver.current_url,
				})
	finally:
		driver.quit()

	if output == "json":
		return records
	import pandas as pd  # type: ignore
	return pd.DataFrame.from_records(records, columns=[
		"date",
		"time",
		"release_title",
		"period_year",
		"period_month",
		"period_quarter",
		"source_year_page",
		"year_page_url",
	])


__all__ = ["scrape_archived_schedule"]


def _parse_release_text(release_text: str) -> (str, Union[int, None], Union[int, None], Union[int, None]):
	"""Split release into clean title and period components.

	Rules handled:
	- Title cleanup: remove trailing parentheticals like (Monthly), (Annual), (P), (R)
	- Period detection after the first 'for':
	  - '<Month> <YYYY>' -> month + year
	  - '<First|Second|Third|Fourth> Quarter <YYYY>' -> quarter + year
	  - 'Annual <YYYY>' or 'Biennial <YYYY>' -> year only
	  - Fallback: first 4-digit year in the period string
	"""
	s = " ".join((release_text or "").split())
	parts = re.split(r"\s+for\s+", s, maxsplit=1, flags=re.I)
	title_raw = parts[0]
	# Strip frequency or revision tags at the end of title
	title = re.sub(r"\s*\((?:Monthly|Annual|Biennial|P|R)\)\s*$", "", title_raw, flags=re.I)
	period_year = None
	period_month = None
	period_quarter = None
	if len(parts) > 1:
		period_str = parts[1]
		# Quarter
		m_q = re.search(r"(First|Second|Third|Fourth)\s+Quarter\s+(\d{4})", period_str, re.I)
		if m_q:
			period_quarter = _QUARTER_WORD_TO_NUM[m_q.group(1).lower()]
			period_year = int(m_q.group(2))
			return title, period_year, period_month, period_quarter
		# Month
		m_m = re.search(r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})", period_str, re.I)
		if m_m:
			period_month = _MONTH_TO_NUM[m_m.group(1).lower()]
			period_year = int(m_m.group(2))
			return title, period_year, period_month, period_quarter
		# Annual/Biennial
		m_a = re.search(r"(Annual|Biennial)\s+(\d{4})", period_str, re.I)
		if m_a:
			period_year = int(m_a.group(2))
			return title, period_year, period_month, period_quarter
		# General fallback: first year
		m_y = re.search(r"\b(\d{4})\b", period_str)
		if m_y:
			period_year = int(m_y.group(1))
	return title, period_year, period_month, period_quarter


