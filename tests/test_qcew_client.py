import responses
from bls_sdk.qcew import QCEWClient
from bls_sdk import config


@responses.activate
def test_qcew_get_by_area():
	client = QCEWClient()
	url = f"{config.QCEW_API_BASE}/2023/1/area/06000.json"
	responses.add(
		responses.GET,
		url,
		json={"Results": {"series": []}},
		status=200,
	)
	resp = client.get_by_area(2023, "1", "06000")
	assert "Results" in resp
