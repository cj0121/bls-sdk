import json
from typing import Any, Dict, Optional
import requests
from tenacity import Retrying, stop_after_attempt, wait_exponential, retry_if_exception_type

from .config import (
	BLS_API_KEY,
	PUBLIC_API_TS_DATA_ENDPOINT,
	REQUEST_TIMEOUT_SECONDS,
	MAX_RETRIES,
	BACKOFF_INITIAL_SECONDS,
	BACKOFF_MAX_SECONDS,
	USER_AGENT,
	DEFAULT_RATE_LIMIT_PER_SECOND,
)
from .errors import HttpError, ApiError
from .rate_limiter import RateLimiter


class HttpClient:
	def __init__(self,
			timeout_seconds: Optional[int] = None,
			max_retries: Optional[int] = None,
			backoff_initial_seconds: Optional[float] = None,
			backoff_max_seconds: Optional[float] = None,
			rate_limit_per_second: Optional[float] = None,
	):
		self.session = requests.Session()
		self.timeout_seconds = timeout_seconds or REQUEST_TIMEOUT_SECONDS
		self.max_retries = max_retries or MAX_RETRIES
		self.backoff_initial_seconds = backoff_initial_seconds or BACKOFF_INITIAL_SECONDS
		self.backoff_max_seconds = backoff_max_seconds or BACKOFF_MAX_SECONDS
		self.headers = {
			"User-Agent": USER_AGENT,
			"Accept": "application/json",
			"Content-Type": "application/json",
		}
		self.rate_limiter = RateLimiter(rate_limit_per_second or DEFAULT_RATE_LIMIT_PER_SECOND)

	def _do_request(self, method: str, url: str, **kwargs) -> requests.Response:
		self.rate_limiter.acquire()
		response = self.session.request(method=method, url=url, headers=self.headers, timeout=self.timeout_seconds, **kwargs)
		if response.status_code >= 400:
			raise HttpError(response.status_code, url, body=response.text)
		return response

	def _request_with_retries(self, method: str, url: str, **kwargs) -> requests.Response:
		for attempt in Retrying(
			stop=stop_after_attempt(self.max_retries),
			wait=wait_exponential(multiplier=self.backoff_initial_seconds, max=self.backoff_max_seconds),
			retry=retry_if_exception_type((requests.RequestException, HttpError)),
			reraise=True,
		):
			with attempt:
				return self._do_request(method, url, **kwargs)

	def post_public_timeseries(self, body: Dict[str, Any]) -> Dict[str, Any]:
		payload = dict(body)
		if BLS_API_KEY and "registrationKey" not in payload:
			payload["registrationKey"] = BLS_API_KEY
		resp = self._request_with_retries("POST", PUBLIC_API_TS_DATA_ENDPOINT, data=json.dumps(payload))
		data = resp.json()
		status = (data.get("status") or "").upper()
		if status != "REQUEST_SUCCEEDED":
			raise ApiError(status=status or "UNKNOWN_STATUS", messages=data.get("message") or [])
		return data

	def get_json(self, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
		resp = self._request_with_retries("GET", url, params=params)
		try:
			return resp.json()
		except Exception as e:
			raise HttpError(resp.status_code, url, body=resp.text)
