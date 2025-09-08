from typing import Optional, List, Union


class BlsError(Exception):
	"""Base exception for BLS SDK errors."""


class HttpError(BlsError):
	"""Raised for non-successful HTTP responses from the server."""

	def __init__(self, status_code: int, url: str, body: Optional[Union[str, bytes]] = None):
		self.status_code = status_code
		self.url = url
		self.body = body
		super().__init__(f"HTTP {status_code} for {url}")


class ApiError(BlsError):
	"""Raised when BLS API returns REQUEST_FAILED or similar status."""

	def __init__(self, status: str, messages: Optional[List[str]] = None):
		self.status = status
		self.messages = messages or []
		joined = "; ".join(self.messages) if self.messages else ""
		super().__init__(f"BLS API error: {status}{(': ' + joined) if joined else ''}")


class RateLimitError(BlsError):
	"""Raised when a local rate limit prevents a request from being sent."""


class ValidationError(BlsError):
	"""Raised for invalid arguments before making a request."""
