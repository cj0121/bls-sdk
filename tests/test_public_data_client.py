import responses
from bls_sdk.public_data import PublicDataClient
from bls_sdk import config


@responses.activate
def test_get_latest_multiple_series():
	client = PublicDataClient()
	mock = {"status": "REQUEST_SUCCEEDED", "Results": {"series": []}}
	responses.add(
		responses.GET,
		config.PUBLIC_API_LATEST_ENDPOINT,
		json=mock,
		status=200,
	)
	resp = client.get_latest(["S1", "S2"]) 
	assert resp["status"] == "REQUEST_SUCCEEDED"


@responses.activate
def test_get_popular_default():
	client = PublicDataClient()
	mock = {"status": "REQUEST_SUCCEEDED", "Results": {"series": []}}
	responses.add(
		responses.GET,
		config.PUBLIC_API_POPULAR_ENDPOINT,
		json=mock,
		status=200,
	)
	resp = client.get_popular()
	assert resp["status"] == "REQUEST_SUCCEEDED"


@responses.activate
def test_list_surveys():
	client = PublicDataClient()
	mock = {"status": "REQUEST_SUCCEEDED", "Results": {"surveys": []}}
	responses.add(
		responses.GET,
		config.PUBLIC_API_SURVEYS_ENDPOINT,
		json=mock,
		status=200,
	)
	resp = client.list_surveys()
	assert resp["status"] == "REQUEST_SUCCEEDED"
