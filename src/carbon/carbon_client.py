import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


class CarbonClient:
    """Small HTTP client with connection pooling and retries for the Carbon Intensity API.

    Uses requests.Session with urllib3 Retry mounted on the HTTPS adapter. Intended for
    use with ThreadPoolExecutor for concurrent fetches.
    """

    def __init__(self, retries=3, backoff_factor=0.5, max_pool=12):
        self.session = requests.Session()
        retry = Retry(total=retries,
                      backoff_factor=backoff_factor,
                      status_forcelist=(429, 500, 502, 503, 504),
                      allowed_methods=frozenset(["GET", "HEAD"]))
        adapter = HTTPAdapter(max_retries=retry, pool_maxsize=max_pool)
        self.session.mount("https://", adapter)
        self.session.headers.update({"Accept": "application/json"})

    def fetch_json(self, url, timeout=20):
        """Fetch URL and return parsed JSON. Raises requests.HTTPError on bad status."""
        resp = self.session.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
