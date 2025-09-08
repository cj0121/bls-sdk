import os
from dotenv import load_dotenv

load_dotenv()

BLS_API_KEY = os.getenv("BLS_API_KEY")

PUBLIC_API_BASE = os.getenv("BLS_PUBLIC_API_BASE", "https://api.bls.gov/publicAPI/v2")
PUBLIC_API_TS_DATA_ENDPOINT = f"{PUBLIC_API_BASE}/timeseries/data"
PUBLIC_API_LATEST_ENDPOINT = f"{PUBLIC_API_BASE}/timeseries/latest"
PUBLIC_API_POPULAR_ENDPOINT = f"{PUBLIC_API_BASE}/timeseries/popular"
PUBLIC_API_SURVEYS_ENDPOINT = f"{PUBLIC_API_BASE}/surveys"

QCEW_API_BASE = os.getenv("BLS_QCEW_API_BASE", "https://data.bls.gov/cew/data/api")

REQUEST_TIMEOUT_SECONDS = int(os.getenv("BLS_REQUEST_TIMEOUT_SECONDS", "30"))
MAX_RETRIES = int(os.getenv("BLS_MAX_RETRIES", "3"))
BACKOFF_INITIAL_SECONDS = float(os.getenv("BLS_BACKOFF_INITIAL_SECONDS", "0.5"))
BACKOFF_MAX_SECONDS = float(os.getenv("BLS_BACKOFF_MAX_SECONDS", "5"))
USER_AGENT = os.getenv("BLS_USER_AGENT", "bls-sdk/0.1 (+local)")

DEFAULT_RATE_LIMIT_PER_SECOND = float(os.getenv("BLS_RATE_LIMIT_PER_SECOND", "5"))
