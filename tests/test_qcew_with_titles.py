import io
import csv
import gzip
import responses

from bls_sdk.qcew import QCEWClient
from bls_sdk import config


@responses.activate
def test_get_table_with_titles_joins_and_caches_titles():
	client = QCEWClient()
	# Mock table CSV
	table_buf = io.StringIO()
	w = csv.writer(table_buf)
	w.writerow(["area_fips", "industry_code", "own_code", "size_code"])  # header
	w.writerow(["06000", "10", "0", "1"])  # one row
	responses.add(responses.GET, f"{config.QCEW_API_BASE}/2024/1/area.csv", body=table_buf.getvalue(), status=200, content_type="text/csv")
	# Mock titles (use gz so we exercise the gz path)
	def gz_csv(header, rows):
		buf = io.StringIO(); w = csv.writer(buf); w.writerow(header); [w.writerow(r) for r in rows]; return gzip.compress(buf.getvalue().encode("utf-8"))
	responses.add(responses.GET, f"{config.QCEW_API_BASE}/area_titles.csv", status=404)
	responses.add(responses.GET, f"{config.QCEW_API_BASE}/area_titles.csv.gz", body=gz_csv(["area_fips","area_title"], [["06000","Los Angeles"]]), status=200)
	responses.add(responses.GET, f"{config.QCEW_API_BASE}/industry_titles.csv", status=404)
	responses.add(responses.GET, f"{config.QCEW_API_BASE}/industry_titles.csv.gz", body=gz_csv(["industry_code","industry_title"], [["10","Agriculture"]]), status=200)
	responses.add(responses.GET, f"{config.QCEW_API_BASE}/ownership_titles.csv", status=404)
	responses.add(responses.GET, f"{config.QCEW_API_BASE}/ownership_titles.csv.gz", body=gz_csv(["own_code","own_title"], [["0","Total Covered"]]), status=200)
	responses.add(responses.GET, f"{config.QCEW_API_BASE}/size_titles.csv", status=404)
	responses.add(responses.GET, f"{config.QCEW_API_BASE}/size_titles.csv.gz", body=gz_csv(["size_code","size_title"], [["1","0-4"]]), status=200)

	rows = client.get_table_with_titles(2024, "1", "area")
	assert rows[0]["area_title"] == "Los Angeles"
	assert rows[0]["industry_title"] == "Agriculture"
	assert rows[0]["own_title"] == "Total Covered"
	assert rows[0]["size_title"] == "0-4"

	# Clear mocks to ensure cache is used (no new HTTP calls)
	responses.reset()
	# titles cached; re-mock only the table
	responses.add(responses.GET, f"{config.QCEW_API_BASE}/2024/1/area.csv", body=table_buf.getvalue(), status=200, content_type="text/csv")
	rows2 = client.get_table_with_titles(2024, "1", "area")
	assert rows2 and rows2[0]["area_title"] == "Los Angeles"
