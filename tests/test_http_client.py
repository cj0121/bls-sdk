import json
import responses

from bls_sdk.http_client import HttpClient
from bls_sdk.errors import ApiError, HttpError
from bls_sdk import config


@responses.activate
def test_post_public_timeseries_success():
	client = HttpClient()
	mock_resp = {
		"status": "REQUEST_SUCCEEDED",
		"message": [],
		"Results": {
			"series": [
				{
					"seriesID": "CUUR0000SA0",
					"data": [
						{"year": "2021", "period": "M01", "value": "261.582", "footnotes": [{}]}
					]
				}
			]
		}
	}
	responses.add(
		method=responses.POST,
		url=config.PUBLIC_API_TS_DATA_ENDPOINT,
		json=mock_resp,
		status=200,
	)
	body = {"seriesid": ["CUUR0000SA0"], "startyear": "2021", "endyear": "2021"}
	data = client.post_public_timeseries(body)
	assert data["status"] == "REQUEST_SUCCEEDED"
	assert "Results" in data and "series" in data["Results"]


@responses.activate
def test_post_public_timeseries_api_error():
	client = HttpClient(max_retries=1)
	mock_resp = {"status": "REQUEST_FAILED", "message": ["bad params"]}
	responses.add(
		method=responses.POST,
		url=config.PUBLIC_API_TS_DATA_ENDPOINT,
		json=mock_resp,
		status=200,
	)
	try:
		client.post_public_timeseries({"seriesid": []})
	except ApiError as e:
		assert "REQUEST_FAILED" in str(e)
	else:
		assert False, "Expected ApiError"


@responses.activate
def test_post_public_timeseries_http_error():
	client = HttpClient(max_retries=1)
	responses.add(
		method=responses.POST,
		url=config.PUBLIC_API_TS_DATA_ENDPOINT,
		body="server error",
		status=500,
	)
	try:
		client.post_public_timeseries({"seriesid": ["X"]})
	except HttpError as e:
		assert e.status_code == 500
	else:
		assert False, "Expected HttpError"
