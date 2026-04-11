"""OpenFDA API client with rate limiting, pagination, and retry logic."""

import logging
import time
from collections.abc import Generator

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.config import settings

logger = logging.getLogger(__name__)

ENDPOINTS = {
    "Drugs": "drug/enforcement.json",
    "Devices": "device/recall.json",
    "Food": "food/enforcement.json",
}


class FDAClient:
    """Client for the OpenFDA API with rate limiting and automatic pagination."""

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        requests_per_minute: int | None = None,
    ):
        self.api_key = api_key or settings.FDA_API_KEY
        self.base_url = (base_url or settings.FDA_BASE_URL).rstrip("/")
        self.rpm = requests_per_minute or settings.REQUESTS_PER_MINUTE
        self._min_interval = 60.0 / self.rpm
        self._last_request_time = 0.0
        self._client = httpx.Client(timeout=30.0)

    def close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def _rate_limit(self):
        """Enforce rate limiting between requests."""
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_request_time = time.monotonic()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=8),
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.TransportError)),
        reraise=True,
    )
    def _request(self, endpoint: str, params: dict) -> dict:
        """Make a single GET request to the FDA API with retries."""
        self._rate_limit()
        url = f"{self.base_url}/{endpoint}"
        if self.api_key:
            params["api_key"] = self.api_key

        response = self._client.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def fetch_page(
        self, endpoint: str, search: str = "", skip: int = 0, limit: int = 1000
    ) -> dict:
        """Fetch a single page of results from an endpoint."""
        params = {"limit": limit, "skip": skip}
        if search:
            params["search"] = search
        return self._request(endpoint, params)

    def fetch_all_pages(
        self, endpoint: str, search: str = ""
    ) -> Generator[list[dict], None, None]:
        """Yield batches of results, handling pagination automatically.

        Stops when all results are fetched or when skip exceeds the API cap (26000).
        """
        skip = 0
        limit = 1000
        max_skip = 26000

        while True:
            try:
                data = self.fetch_page(endpoint, search, skip, limit)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    logger.info("No results for %s search=%s", endpoint, search)
                    return
                raise

            results = data.get("results", [])
            if not results:
                return

            total = data.get("meta", {}).get("results", {}).get("total", 0)
            logger.info(
                "Fetched %d/%d records from %s (skip=%d)",
                len(results), total, endpoint, skip,
            )
            yield results

            skip += limit
            if skip >= total or skip >= max_skip:
                if skip < total:
                    logger.warning(
                        "Reached skip cap (%d) with %d remaining for %s. "
                        "Use narrower date ranges.",
                        max_skip, total - skip, endpoint,
                    )
                return

    def get_drug_enforcements(
        self, date_from: str, date_to: str
    ) -> Generator[list[dict], None, None]:
        """Fetch drug enforcement records for a date range.

        Args:
            date_from: Start date in YYYYMMDD format.
            date_to: End date in YYYYMMDD format.
        """
        search = f"report_date:[{date_from} TO {date_to}]"
        yield from self.fetch_all_pages(ENDPOINTS["Drugs"], search)

    def get_device_recalls(
        self, date_from: str, date_to: str
    ) -> Generator[list[dict], None, None]:
        """Fetch device recall records for a date range."""
        search = f"event_date_posted:[{date_from} TO {date_to}]"
        yield from self.fetch_all_pages(ENDPOINTS["Devices"], search)

    def get_food_enforcements(
        self, date_from: str, date_to: str
    ) -> Generator[list[dict], None, None]:
        """Fetch food enforcement records for a date range."""
        search = f"report_date:[{date_from} TO {date_to}]"
        yield from self.fetch_all_pages(ENDPOINTS["Food"], search)

    def get_records(
        self, product_type: str, date_from: str, date_to: str
    ) -> Generator[list[dict], None, None]:
        """Dispatch to the correct endpoint based on product type."""
        dispatch = {
            "Drugs": self.get_drug_enforcements,
            "Devices": self.get_device_recalls,
            "Food": self.get_food_enforcements,
        }
        fetcher = dispatch.get(product_type)
        if fetcher is None:
            raise ValueError(f"Unknown product_type: {product_type}")
        yield from fetcher(date_from, date_to)
