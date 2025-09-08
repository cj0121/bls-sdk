import os
import time
import pytest

from bls_sdk.http_client import HttpClient
from bls_sdk.public_data import PublicDataClient


def requires_key():
	return bool(os.getenv("BLS_API_KEY"))


@pytest.mark.skipif(not requires_key(), reason="BLS_API_KEY not set")
def test_live_public_timeseries_cpi_small_range():
	client = PublicDataClient(HttpClient(rate_limit_per_second=1))
	resp = client.get_series("CUUR0000SA0", startyear="2023", endyear="2023", annualaverage=False, calculations=False)
	assert resp["status"] == "REQUEST_SUCCEEDED"
	series = resp["Results"]["series"][0]
	assert series["seriesID"] == "CUUR0000SA0"
	assert any(d.get("year") == "2023" for d in series["data"]) 

