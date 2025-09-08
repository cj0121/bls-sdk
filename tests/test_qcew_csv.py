import gzip
import io
import csv
import responses

from bls_sdk.qcew import QCEWClient
from bls_sdk import config
from bls_sdk.errors import ValidationError


@responses.activate
def test_qcew_csv_fallback_to_gz_and_parse():
	client = QCEWClient()
	# First try plain CSV -> 404
	responses.add(
		responses.GET,
		f"{config.QCEW_API_BASE}/2024/1/industry.csv",
		status=404,
	)
	# Then .csv.gz -> 200 with minimal CSV content
	buf = io.StringIO()
	writer = csv.writer(buf)
	writer.writerow(["industry_code", "own_code", "qtr", "year", "area_fips"])  # header
	writer.writerow(["10", "0", "1", "2024", "00000"])  # one row
	gz = gzip.compress(buf.getvalue().encode("utf-8"))
	responses.add(
		responses.GET,
		f"{config.QCEW_API_BASE}/2024/1/industry.csv.gz",
		body=gz,
		status=200,
		content_type="application/gzip",
	)
	rows = client.get_table_csv(2024, "Q1", "industry")
	assert isinstance(rows, list) and len(rows) == 1
	assert rows[0]["industry_code"] == "10"


def test_qcew_year_outside_window_raises():
	client = QCEWClient()
	import datetime
	year = datetime.datetime.utcnow().year - 6
	try:
		client.get_table_csv(year, "a", "area")
	except ValidationError:
		return
	assert False, "Expected ValidationError for year outside window"
