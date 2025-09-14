"""Backward-compatible shim for the refactored Carbon Intensity service.

This module preserves the original public entrypoints but delegates to the new
`CarbonService` implementation in `carbon_service.py`.
"""

from datetime import datetime
from src.carbon.carbon_service import CarbonService
import pandas as pd


class CarbonIntensityAPI:
    """Compatibility wrapper around the refactored CarbonService.

    The original code created a CarbonIntensityAPI() and called between(...).
    This class keeps that surface but delegates to the new service which
    supports parallel fetching and a pluggable client/parser.
    """

    def __init__(self, retries=5, max_workers=6):
        # keep the simple constructor signature but wire up components
        self.service = CarbonService(max_workers=max_workers)

    def between(self, start, end, type="national", region_id=None, postcode=None):
        try:
            return self.service.between(start, end, type=type, region_id=region_id, postcode=postcode)

        except Exception as e:
            print(f"Error fetching carbon intensity data for {region_id}/{postcode} {start}:{end} %s", e)
            return pd.DataFrame(), pd.DataFrame()


def main(region_id=None):
    import pytz
    api = CarbonIntensityAPI()
    print(f"Demo query for region/postcode: {region_id}")
    carbon_data, gen_mix_data = api.between(datetime(2022, 10, 1, 0, 0, tzinfo=pytz.utc),
                                            datetime(2022, 10, 31, 23, 30, tzinfo=pytz.utc),
                                            type="postcode", postcode='G5')
    print(carbon_data.head())


if __name__ == "__main__":
    main()
