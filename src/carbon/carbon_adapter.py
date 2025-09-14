from functools import lru_cache
import pytz

from src.carbon.carbon_intensity_api import CarbonIntensityAPI


class CarbonAdapter:
    """Adapter around CarbonIntensityAPI that normalizes inputs and caches results.

    Accepts an injectable `api` for testing. The cache key is the normalized ISO strings
    for start/end plus the type and region/postcode.
    """

    def __init__(self, api=None, retries=5, max_workers=6):
        # allow dependency injection for tests
        self.api = api or CarbonIntensityAPI(retries=retries, max_workers=max_workers)

    @staticmethod
    def _normalize_dt(dt):
        """Return an ISO-formatted UTC timestamp string for caching."""
        import pandas as pd

        ts = pd.to_datetime(dt)
        # make timezone-aware in UTC if naive
        if ts.tzinfo is None:
            ts = ts.tz_localize(pytz.utc)
        else:
            ts = ts.tz_convert(pytz.utc)
        return ts.isoformat()

    @lru_cache(maxsize=256)
    def _fetch_cached(self, start_iso, end_iso, type_, region_id, postcode):
        import pandas as pd

        start = pd.to_datetime(start_iso)
        end = pd.to_datetime(end_iso)
        return self.api.between(start, end, type=type_, region_id=region_id, postcode=postcode)

    def fetch(self, start, end, type_, region_id=None, postcode=None):
        start_iso = self._normalize_dt(start)
        end_iso = self._normalize_dt(end)
        return self._fetch_cached(start_iso, end_iso, type_, region_id, postcode)
