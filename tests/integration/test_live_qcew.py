import os
import pytest

from bls_sdk.http_client import HttpClient
from bls_sdk.qcew import QCEWClient
from bls_sdk import config


def requires_key():
	return bool(os.getenv("BLS_API_KEY"))


@pytest.mark.skip(reason="QCEW endpoint example pending verification")
def test_live_qcew_industry_small():
	client = QCEWClient(HttpClient(rate_limit_per_second=1))
	resp = client.get_by_industry(2022, "a", "10")
	assert isinstance(resp, dict)
	assert "Results" in resp or "data" in resp
