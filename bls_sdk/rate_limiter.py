import threading
import time
from typing import Optional


class RateLimiter:
	"""Simple token-bucket rate limiter.

	Allows up to `capacity` tokens to accumulate. Tokens refill at `rate_per_second`.
	"""

	def __init__(self, rate_per_second: float, capacity: Optional[int] = None):
		if rate_per_second <= 0:
			raise ValueError("rate_per_second must be positive")
		self.rate_per_second = float(rate_per_second)
		self.capacity = int(capacity if capacity is not None else max(1, int(rate_per_second)))
		self._tokens = float(self.capacity)
		self._last_refill = time.monotonic()
		self._lock = threading.Lock()

	def _refill(self) -> None:
		now = time.monotonic()
		elapsed = now - self._last_refill
		if elapsed <= 0:
			return
		added = elapsed * self.rate_per_second
		self._tokens = min(self.capacity, self._tokens + added)
		self._last_refill = now

	def try_acquire(self) -> bool:
		with self._lock:
			self._refill()
			if self._tokens >= 1.0:
				self._tokens -= 1.0
				return True
			return False

	def acquire(self, timeout: Optional[float] = None) -> None:
		end_time = None if timeout is None else (time.monotonic() + timeout)
		while True:
			if self.try_acquire():
				return
			if end_time is not None and time.monotonic() >= end_time:
				raise TimeoutError("RateLimiter.acquire timed out")
			time.sleep(max(0.0, 1.0 / self.rate_per_second / 2))
