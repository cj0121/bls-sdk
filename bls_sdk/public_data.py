from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from .http_client import HttpClient
from .config import (
	PUBLIC_API_TS_DATA_ENDPOINT,
	PUBLIC_API_LATEST_ENDPOINT,
	PUBLIC_API_POPULAR_ENDPOINT,
	PUBLIC_API_SURVEYS_ENDPOINT,
)


_MAX_SERIES_PER_REQUEST = 50


class PublicDataClient:
	def __init__(self, http: Optional[HttpClient] = None):
		self.http = http or HttpClient()

	def get_series(self, series_id: str, **options: Any) -> Dict[str, Any]:
		body: Dict[str, Any] = {"seriesid": [series_id]}
		body.update(options)
		return self.http.post_public_timeseries(body)

	def get_many_series(self, series_ids: Sequence[str], **options: Any) -> List[Dict[str, Any]]:
		if not series_ids:
			return []
		merged_series: List[Dict[str, Any]] = []
		for i in range(0, len(series_ids), _MAX_SERIES_PER_REQUEST):
			chunk = list(series_ids[i:i + _MAX_SERIES_PER_REQUEST])
			body: Dict[str, Any] = {"seriesid": chunk}
			body.update(options)
			resp = self.http.post_public_timeseries(body)
			results = resp.get("Results", {})
			series_list = results.get("series", [])
			merged_series.extend(series_list)
		return merged_series

	def get_latest(self, series_ids: Union[Sequence[str], str]) -> Dict[str, Any]:
		ids: List[str] = [series_ids] if isinstance(series_ids, str) else list(series_ids)
		params: List[Tuple[str, str]] = [("seriesid", sid) for sid in ids]
		return self.http.get_json(PUBLIC_API_LATEST_ENDPOINT, params=params)  # type: ignore[arg-type]

	def get_popular(self, survey: Optional[str] = None) -> Dict[str, Any]:
		params = {"survey": survey} if survey else None
		return self.http.get_json(PUBLIC_API_POPULAR_ENDPOINT, params=params)

	def list_surveys(self) -> Dict[str, Any]:
		return self.http.get_json(PUBLIC_API_SURVEYS_ENDPOINT)

	def get_survey(self, survey_abbr: str) -> Dict[str, Any]:
		return self.http.get_json(f"{PUBLIC_API_SURVEYS_ENDPOINT}/{survey_abbr}")

	def list_surveys_list(self) -> List[Dict[str, Any]]:
		resp = self.list_surveys()
		results = resp.get("Results", {})
		return list(results.get("survey", []))
