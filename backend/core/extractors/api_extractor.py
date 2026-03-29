"""
Enterprise ETL Platform - REST API Extractor

Extracts data from REST APIs with pagination support,
authentication, and retry logic via tenacity.
"""
import time
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from tenacity import retry, stop_after_attempt, wait_exponential
from urllib3.util.retry import Retry

from backend.core.extractors.base import BaseExtractor, ExtractionResult
from backend.utils.logger import get_logger

logger = get_logger(__name__)


class APIExtractor(BaseExtractor):
    """
    Extracts data from REST APIs.

    Config keys:
        url (str): Base endpoint URL.
        method (str): HTTP method. Default: GET.
        headers (dict): Request headers.
        auth_type (str): none | bearer | basic | api_key.
        token (str): Bearer token (if auth_type=bearer).
        api_key (str): API key value.
        api_key_header (str): Header name for API key. Default: X-API-Key.
        username (str): Basic auth username.
        password (str): Basic auth password.
        pagination (dict): Pagination config:
            type: page | cursor | offset
            page_param: Query param name for page number.
            size_param: Query param name for page size.
            size: Page size.
            results_key: JSON key containing records.
            next_key: JSON key with next cursor/page URL.
        params (dict): Static query parameters.
        data_key (str): JSON key to extract records from. Default: root.
        timeout (int): Request timeout in seconds. Default: 30.
    """

    def get_required_config_keys(self) -> List[str]:
        return ["url"]

    def connect(self) -> None:
        """Initialize requests Session with retry adapter."""
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self._session = requests.Session()
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)

        # Apply authentication
        auth_type = self.config.get("auth_type", "none")
        if auth_type == "bearer":
            token = self.config.get("token", "")
            self._session.headers.update({"Authorization": f"Bearer {token}"})
        elif auth_type == "basic":
            self._session.auth = (
                self.config.get("username", ""),
                self.config.get("password", ""),
            )
        elif auth_type == "api_key":
            key_header = self.config.get("api_key_header", "X-API-Key")
            self._session.headers.update({key_header: self.config.get("api_key", "")})

        # Apply custom headers
        extra_headers = self.config.get("headers", {})
        self._session.headers.update(extra_headers)
        self._connected = True

    def disconnect(self) -> None:
        """Close the requests session."""
        if hasattr(self, "_session"):
            self._session.close()
        self._connected = False

    def extract(
        self,
        checkpoint_value: Optional[str] = None,
    ) -> ExtractionResult:
        """
        Fetch data from API endpoint with pagination handling.

        Args:
            checkpoint_value: Cursor or since-date for incremental fetch.

        Returns:
            ExtractionResult containing all fetched records.
        """
        if not self._connected:
            self.connect()

        start = time.monotonic()
        all_records: List[Dict] = []
        pagination_cfg = self.config.get("pagination", {})
        params = dict(self.config.get("params", {}))

        if checkpoint_value:
            params["since"] = checkpoint_value

        if pagination_cfg:
            all_records = self._paginated_fetch(params, pagination_cfg)
        else:
            records = self._single_fetch(params)
            all_records = records

        df = pd.DataFrame(all_records) if all_records else pd.DataFrame()
        elapsed = time.monotonic() - start

        self.logger.info(
            "API extraction complete",
            url=self.config["url"],
            records=len(df),
            duration_seconds=round(elapsed, 3),
        )

        return ExtractionResult(
            data=df,
            source_name=self.source_name,
            records_count=len(df),
            extraction_time=elapsed,
            metadata={"url": self.config["url"], "total_pages_fetched": self._pages_fetched},
        )

    def _single_fetch(self, params: Dict) -> List[Dict]:
        """Make a single API request."""
        response = self._make_request(params)
        return self._extract_records(response.json())

    def _paginated_fetch(
        self,
        params: Dict,
        pagination_cfg: Dict,
    ) -> List[Dict]:
        """Handle paginated API responses."""
        all_records: List[Dict] = []
        self._pages_fetched = 0
        page = 1
        page_param = pagination_cfg.get("page_param", "page")
        size_param = pagination_cfg.get("size_param", "per_page")
        page_size = pagination_cfg.get("size", 100)
        next_key = pagination_cfg.get("next_key", "next")

        params[size_param] = page_size

        while True:
            params[page_param] = page
            response = self._make_request(params)
            body = response.json()

            records = self._extract_records(body)
            if not records:
                break

            all_records.extend(records)
            self._pages_fetched += 1
            self.logger.debug(f"Fetched page {page}", records_so_far=len(all_records))

            # Check for next page
            if isinstance(body, dict) and body.get(next_key):
                page += 1
            else:
                break

            # Safety cap
            if self._pages_fetched >= 500:
                self.logger.warning("Pagination cap reached (500 pages)")
                break

        return all_records

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _make_request(self, params: Dict) -> requests.Response:
        """Make HTTP request with retry."""
        method = self.config.get("method", "GET").upper()
        url = self.config["url"]
        timeout = self.config.get("timeout", 30)

        response = self._session.request(method, url, params=params, timeout=timeout)
        response.raise_for_status()
        return response

    def _extract_records(self, body: Any) -> List[Dict]:
        """Extract list of records from JSON response."""
        data_key = self.config.get("data_key")
        if data_key and isinstance(body, dict):
            records = body.get(data_key, [])
        elif isinstance(body, list):
            records = body
        else:
            records = [body] if body else []

        return records if isinstance(records, list) else []
