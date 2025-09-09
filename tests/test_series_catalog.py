import responses
from bls_sdk.series_catalog import fetch_series_for_survey, _BASE


def test_fetch_series_for_survey_https_403_fallback_http():
	# First HTTPS returns 403, then HTTP returns valid TSV
	with responses.RequestsMock() as rsps:
		rsps.add(responses.GET, _BASE + "cu/cu.series", status=403)
		rsps.add(responses.GET, _BASE.replace("https://","http://") + "cu/cu.series", body="series_id\tseries_title\nX\tTitle\n", status=200, content_type="text/plain")
		rows = fetch_series_for_survey("cu")
		assert rows and rows[0]["series_id"] == "X"
