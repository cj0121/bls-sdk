from bls_sdk.series_catalog import fetch_series_for_survey, _fetch_text


def test_header_and_value_trimming(monkeypatch):
	# Simulate padded header and values
	sample = "series_id\t\t\tseries_title\nCUUR0000SA0\t  All items  \n"
	monkeypatch.setattr('bls_sdk.series_catalog._fetch_text', lambda url: sample)
	rows = fetch_series_for_survey('cu')
	assert rows[0]['series_id'] == 'CUUR0000SA0'
	assert rows[0]['series_title'] == 'All items'
