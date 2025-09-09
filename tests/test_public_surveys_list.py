import responses
from bls_sdk.public_data import PublicDataClient
from bls_sdk import config


@responses.activate
def test_list_surveys_list_returns_list():
	client = PublicDataClient()
	mock = {"status": "REQUEST_SUCCEEDED", "Results": {"survey": [{"survey_abbreviation": "CU", "survey_name": "Consumer Price Index"}]}}
	responses.add(responses.GET, config.PUBLIC_API_SURVEYS_ENDPOINT, json=mock, status=200)
	lst = client.list_surveys_list()
	assert isinstance(lst, list) and lst[0]["survey_abbreviation"] == "CU"
