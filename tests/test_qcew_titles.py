import io
import csv
import gzip
import responses

from bls_sdk.qcew import QCEWClient
from bls_sdk import config


@responses.activate
def test_get_area_titles_csv_then_gz():
	client = QCEWClient()
	responses.add(responses.GET, f"{config.QCEW_API_BASE}/area_titles.csv", status=404)
	buf = io.StringIO()
	w = csv.writer(buf)
	w.writerow(["area_fips", "area_title"])  # header
	w.writerow(["06000", "Los Angeles-Long Beach-Anaheim, CA (MSA)"])
	gz = gzip.compress(buf.getvalue().encode("utf-8"))
	responses.add(responses.GET, f"{config.QCEW_API_BASE}/area_titles.csv.gz", body=gz, status=200, content_type="application/gzip")
	titles = client.get_area_titles()
	assert len(titles) == 1 and titles[0]["area_fips"] == "06000"


def test_join_titles_maps_fields():
	client = QCEWClient()
	rows = [
		{"area_fips": "06000", "industry_code": "10", "own_code": "0", "size_code": "1"},
	]
	areas = [{"area_fips": "06000", "area_title": "Los Angeles"}]
	inds = [{"industry_code": "10", "industry_title": "Agriculture"}]
	owns = [{"own_code": "0", "own_title": "Total Covered"}]
	sizes = [{"size_code": "1", "size_title": "0-4"}]
	out = client.join_titles(rows, areas=areas, industries=inds, ownerships=owns, sizes=sizes)
	assert out[0]["area_title"] == "Los Angeles"
	assert out[0]["industry_title"] == "Agriculture"
	assert out[0]["own_title"] == "Total Covered"
	assert out[0]["size_title"] == "0-4"
