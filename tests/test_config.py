from bls_sdk import __version__
from bls_sdk import config


def test_version_present():
	assert isinstance(__version__, str) and len(__version__) > 0


def test_config_defaults():
	assert config.PUBLIC_API_BASE.startswith("https://")
	assert config.PUBLIC_API_TS_DATA_ENDPOINT.endswith("/timeseries/data")
	assert config.REQUEST_TIMEOUT_SECONDS > 0
	assert config.MAX_RETRIES >= 1
	assert config.BACKOFF_INITIAL_SECONDS > 0
	assert config.BACKOFF_MAX_SECONDS >= config.BACKOFF_INITIAL_SECONDS
	assert config.DEFAULT_RATE_LIMIT_PER_SECOND > 0
