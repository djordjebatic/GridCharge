from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

from src.carbon.carbon_client import CarbonClient
from src.carbon.carbon_parser import CarbonParser


class CarbonService:
    """High-level service to orchestrate fetching and parsing of Carbon Intensity data.

    Provides a between(start, end, type, region_id/postcode) method similar to the old API but
    implemented with smaller components and optional parallel fetches.
    """

    def __init__(self, base_url="https://api.carbonintensity.org.uk", max_range=None, client=None, parser=None, max_workers=6):
        self.base_url = base_url
        self.max_range = max_range or {"national": timedelta(days=14), "regional": timedelta(days=14), "postcode": timedelta(days=14)}
        self.client = client or CarbonClient()
        self.parser = parser or CarbonParser()
        self.max_workers = max_workers

    def between(self, start, end, type="national", region_id=None, postcode=None):
        type = type.lower()
        if type not in ("national", "regional", "postcode"):
            raise ValueError("Type must be 'national', 'regional' or 'postcode'.")

        if type == "regional":
            endpoint = "/regional/intensity/{}/{}"
            region_suffix = f"/regionid/{str(region_id)}"
        elif type == "postcode":
            endpoint = "/regional/intensity/{}/{}"
            region_suffix = f"/postcode/{str(postcode)}"
        else:
            endpoint = "/intensity/{}/{}"
            region_suffix = ""

        # build list of URLs to fetch in chunks
        urls = []
        request_start = start
        while request_start < end:
            request_end = min(end, request_start + self.max_range[type] - timedelta(minutes=30))
            request_endpoint = endpoint.format(request_start.isoformat(), request_end.isoformat())
            url = f"{self.base_url}{request_endpoint}{region_suffix}"
            urls.append(url)
            request_start += self.max_range[type]

        carbon_parts = []
        gen_mix_parts = []

        # parallel fetches
        with ThreadPoolExecutor(max_workers=min(self.max_workers, max(1, len(urls)))) as ex:
            future_to_url = {ex.submit(self.client.fetch_json, u): u for u in urls}
            for fut in as_completed(future_to_url):
                url = future_to_url[fut]
                print(f"Fetched {url}")
                resp = fut.result()
                carbon_part, genmix_part = self.parser.parse_fromto_json(resp, type)
                if carbon_part is not None and not carbon_part.empty:
                    carbon_parts.append(carbon_part)
                if genmix_part is not None and not genmix_part.empty:
                    gen_mix_parts.append(genmix_part)

        carbon = pd.concat(carbon_parts, ignore_index=True) if carbon_parts else pd.DataFrame()
        genmix = pd.concat(gen_mix_parts, ignore_index=True) if gen_mix_parts else None

        return carbon, genmix
