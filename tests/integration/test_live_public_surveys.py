import os
import pytest

from bls_sdk.http_client import HttpClient
from bls_sdk.public_data import PublicDataClient


def requires_key():
	return bool(os.getenv("BLS_API_KEY"))


@pytest.mark.skipif(not requires_key(), reason="BLS_API_KEY not set")
def test_live_public_surveys_list():
	client = PublicDataClient(HttpClient(rate_limit_per_second=1))
	resp = client.list_surveys()
	assert resp.get("status", "").upper() == "REQUEST_SUCCEEDED"
	assert "Results" in resp
