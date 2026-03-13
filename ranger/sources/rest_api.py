"""REST API source connector — paginated HTTP requests via httpx.

Supports offset, cursor, link-header, and page-number pagination strategies,
bearer/basic/api-key authentication, rate limiting, and JMESPath-based
response data extraction.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any

import structlog

from ranger.core.models import (
    ColumnDefinition,
    ColumnType,
    DiscoveredSchema,
    HealthStatus,
    PaginationConfig,
    Record,
    Schema,
)
from ranger.sources.base import BaseSource

logger = structlog.get_logger()

try:
    import httpx
except ImportError as _err:
    raise ImportError(
        "httpx is required for RestAPISource. "
        "Install it with: pip install ranger-core[rest_api]"
    ) from _err

try:
    import jmespath  # optional — used for response_data_path
except ImportError:
    jmespath = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Python type → Ranger ColumnType heuristic
# ---------------------------------------------------------------------------

def _infer_column_type(value: Any) -> ColumnType:
    """Infer a :class:`ColumnType` from a Python value."""
    if isinstance(value, bool):
        return ColumnType.BOOLEAN
    if isinstance(value, int):
        return ColumnType.INT64
    if isinstance(value, float):
        return ColumnType.FLOAT64
    if isinstance(value, list):
        return ColumnType.ARRAY
    if isinstance(value, dict):
        return ColumnType.JSON
    return ColumnType.STRING


class RestAPISource(BaseSource):
    """Read data from REST APIs with automatic pagination and rate limiting.

    Config keys
    -----------
    base_url : str
        Base URL for the API (e.g. ``https://api.example.com``).
    endpoint : str
        Path appended to *base_url* (e.g. ``/v2/records``).
    method : str
        HTTP method — ``GET`` (default) or ``POST``.
    headers : dict
        Extra HTTP headers.
    params : dict
        Query-string parameters.
    body : dict
        JSON body (used with POST).
    auth_type : str
        ``none`` | ``bearer`` | ``basic`` | ``api_key``.
    auth_config : dict
        Credentials — keys depend on *auth_type* (``token``, ``username``/
        ``password``, ``header_name``/``api_key``).
    pagination : dict
        ``strategy`` (offset / cursor / link_header / page_number),
        ``page_size``, ``cursor_field``, ``next_url_field``.
    rate_limit : float
        Maximum requests per second (default: ``10``).
    response_data_path : str
        JMESPath expression to extract the record array from the response.
    timeout : float
        Request timeout in seconds (default: ``30``).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._client: httpx.Client | None = None
        self._async_client: httpx.AsyncClient | None = None

    # ------------------------------------------------------------------
    # Metadata properties
    # ------------------------------------------------------------------

    @property
    def source_type(self) -> str:
        return "rest_api"

    @property
    def supports_streaming(self) -> bool:
        return False

    @property
    def supports_incremental(self) -> bool:
        return True

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        base_url = self._config.get("base_url", "")
        timeout = self._config.get("timeout", 30.0)
        headers = dict(self._config.get("headers", {}))

        # Apply authentication headers
        self._apply_auth_headers(headers)

        try:
            self._client = httpx.Client(
                base_url=base_url,
                headers=headers,
                timeout=httpx.Timeout(timeout),
            )
            self._async_client = httpx.AsyncClient(
                base_url=base_url,
                headers=headers,
                timeout=httpx.Timeout(timeout),
            )
            self._connected = True
            logger.info("rest_api_source.connected", base_url=base_url)
        except Exception as exc:
            self._connected = False
            logger.error("rest_api_source.connect_failed", error=str(exc))
            raise ConnectionError(f"Failed to create HTTP client: {exc}") from exc

    def close(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None

        if self._async_client is not None:
            try:
                # AsyncClient.close() is a coroutine; best-effort sync teardown
                import asyncio

                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    loop = None

                if loop and loop.is_running():
                    loop.create_task(self._async_client.aclose())
                else:
                    asyncio.run(self._async_client.aclose())
            except Exception:
                pass
            self._async_client = None

        self._connected = False
        logger.info("rest_api_source.closed")

    def health_check(self) -> HealthStatus:
        try:
            if self._client is None:
                self.connect()
            assert self._client is not None
            endpoint = self._config.get("endpoint", "/")
            resp = self._client.request(self._config.get("method", "GET"), endpoint)
            resp.raise_for_status()
            return HealthStatus.HEALTHY
        except Exception as exc:
            logger.warning("rest_api_source.health_check_failed", error=str(exc))
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Authentication helpers
    # ------------------------------------------------------------------

    def _apply_auth_headers(self, headers: dict[str, str]) -> None:
        """Mutate *headers* to include authentication credentials."""
        auth_type = self._config.get("auth_type", "none").lower()
        auth_cfg: dict[str, Any] = self._config.get("auth_config", {})

        if auth_type == "bearer":
            token = auth_cfg.get("token", "")
            headers["Authorization"] = f"Bearer {token}"
        elif auth_type == "basic":
            import base64

            username = auth_cfg.get("username", "")
            password = auth_cfg.get("password", "")
            encoded = base64.b64encode(f"{username}:{password}".encode()).decode()
            headers["Authorization"] = f"Basic {encoded}"
        elif auth_type == "api_key":
            header_name = auth_cfg.get("header_name", "X-API-Key")
            api_key = auth_cfg.get("api_key", "")
            headers[header_name] = api_key

    # ------------------------------------------------------------------
    # Response extraction
    # ------------------------------------------------------------------

    def _extract_records(self, response_json: Any) -> list[dict[str, Any]]:
        """Extract the record array from a JSON response body."""
        data_path = self._config.get("response_data_path")
        if data_path and jmespath is not None:
            extracted = jmespath.search(data_path, response_json)
            if isinstance(extracted, list):
                return extracted
            if isinstance(extracted, dict):
                return [extracted]
            return []
        if data_path and jmespath is None:
            # Fallback: simple dot-notation traversal
            parts = data_path.split(".")
            current: Any = response_json
            for part in parts:
                if isinstance(current, dict):
                    current = current.get(part)
                else:
                    break
            if isinstance(current, list):
                return current
            if isinstance(current, dict):
                return [current]
            return []

        # No path configured — try common patterns
        if isinstance(response_json, list):
            return response_json
        if isinstance(response_json, dict):
            for key in ("data", "results", "items", "records", "rows"):
                if key in response_json and isinstance(response_json[key], list):
                    return response_json[key]
            return [response_json]
        return []

    # ------------------------------------------------------------------
    # Rate-limiting helper
    # ------------------------------------------------------------------

    def _rate_limit_sleep(self, last_request_time: float) -> None:
        rps = self._config.get("rate_limit", 10.0)
        if rps <= 0:
            return
        min_interval = 1.0 / rps
        elapsed = time.monotonic() - last_request_time
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)

    # ------------------------------------------------------------------
    # Reading
    # ------------------------------------------------------------------

    def read(self) -> Iterator[Record]:
        """Yield records from the REST API, handling full pagination."""
        if self._client is None:
            raise RuntimeError("Source not connected — call connect() first")

        endpoint: str = self._config.get("endpoint", "/")
        method: str = self._config.get("method", "GET").upper()
        params: dict[str, Any] = dict(self._config.get("params", {}))
        body: dict[str, Any] | None = self._config.get("body")

        pagination = self._config.get("pagination", {})
        strategy = pagination.get("strategy", "none")
        page_size = pagination.get("page_size", 100)
        cursor_field = pagination.get("cursor_field", "cursor")
        next_url_field = pagination.get("next_url_field", "next")

        page_count = 0
        total_records = 0
        next_url: str | None = None

        # Seed pagination parameters
        if strategy == "offset":
            params["offset"] = params.get("offset", 0)
            params["limit"] = page_size
        elif strategy == "page_number":
            params["page"] = params.get("page", 1)
            params["page_size"] = page_size
        elif strategy == "cursor":
            params["limit"] = page_size

        logger.info(
            "rest_api_source.read_start",
            endpoint=endpoint,
            method=method,
            pagination_strategy=strategy,
        )

        while True:
            last_req = time.monotonic()

            if next_url is not None:
                resp = self._client.request(method, next_url)
            else:
                if method == "POST":
                    resp = self._client.post(endpoint, params=params, json=body)
                else:
                    resp = self._client.get(endpoint, params=params)

            resp.raise_for_status()
            response_json = resp.json()
            records = self._extract_records(response_json)

            for item in records:
                yield Record(
                    data=item if isinstance(item, dict) else {"value": item},
                    event_time=datetime.now(timezone.utc),
                    source_metadata={
                        "source_type": "rest_api",
                        "endpoint": endpoint,
                        "page": page_count,
                    },
                )
                total_records += 1

            page_count += 1

            # Determine if there is a next page
            if strategy == "none" or not records:
                break
            if len(records) < page_size:
                break

            if strategy == "offset":
                params["offset"] = params["offset"] + page_size
            elif strategy == "page_number":
                params["page"] = params["page"] + 1
            elif strategy == "cursor":
                cursor_value = None
                if isinstance(response_json, dict):
                    cursor_value = response_json.get(cursor_field)
                if not cursor_value and records:
                    cursor_value = records[-1].get(cursor_field)
                if not cursor_value:
                    break
                params[cursor_field] = cursor_value
            elif strategy == "link_header":
                link = resp.headers.get("Link", "")
                next_url = self._parse_link_header_next(link)
                if not next_url:
                    # Also check response body
                    if isinstance(response_json, dict):
                        next_url = response_json.get(next_url_field)
                if not next_url:
                    break
            else:
                break

            self._rate_limit_sleep(last_req)

        logger.info(
            "rest_api_source.read_complete",
            total_records=total_records,
            pages=page_count,
        )

    def read_api(self, pagination: PaginationConfig | None = None) -> Iterator[Record]:
        """Read with explicit :class:`PaginationConfig` parameter.

        Merges the provided pagination config into the source config and
        delegates to :meth:`read`.
        """
        if pagination is not None:
            merged_pagination = dict(self._config.get("pagination", {}))
            merged_pagination["strategy"] = pagination.strategy.value
            merged_pagination["page_size"] = pagination.page_size

            original = self._config.get("pagination", {})
            self._config["pagination"] = merged_pagination
            try:
                yield from self.read()
            finally:
                self._config["pagination"] = original
        else:
            yield from self.read()

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def get_schema(self) -> Schema:
        """Infer schema by making one request and inspecting the response."""
        if self._client is None:
            raise RuntimeError("Source not connected — call connect() first")

        endpoint = self._config.get("endpoint", "/")
        method = self._config.get("method", "GET").upper()
        params = dict(self._config.get("params", {}))
        body = self._config.get("body")

        # Limit to a small page for schema inference
        pagination = self._config.get("pagination", {})
        page_size = pagination.get("page_size", 100)
        if "limit" not in params:
            params["limit"] = min(page_size, 10)

        if method == "POST":
            resp = self._client.post(endpoint, params=params, json=body)
        else:
            resp = self._client.get(endpoint, params=params)

        resp.raise_for_status()
        response_json = resp.json()
        records = self._extract_records(response_json)

        if not records:
            return Schema(columns=[])

        # Gather all keys from the sample
        all_keys: set[str] = set()
        for rec in records:
            if isinstance(rec, dict):
                all_keys.update(rec.keys())

        columns: list[ColumnDefinition] = []
        for key in sorted(all_keys):
            # Infer from first non-None value
            col_type = ColumnType.STRING
            for rec in records:
                if isinstance(rec, dict) and rec.get(key) is not None:
                    col_type = _infer_column_type(rec[key])
                    break
            columns.append(ColumnDefinition(name=key, type=col_type, nullable=True))

        return Schema(columns=columns)

    def discover_schema(self) -> DiscoveredSchema:
        """Discover schema by making a sample request."""
        schema = self.get_schema()
        endpoint = self._config.get("endpoint", "/")
        return DiscoveredSchema(
            columns=schema.columns,
            primary_key=schema.primary_key,
            source_name="rest_api",
            object_name=endpoint,
            object_type="endpoint",
            source_metadata={
                "base_url": self._config.get("base_url", ""),
                "method": self._config.get("method", "GET"),
            },
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_link_header_next(link_header: str) -> str | None:
        """Parse an RFC 5988 ``Link`` header and return the ``next`` URL."""
        for part in link_header.split(","):
            part = part.strip()
            if 'rel="next"' in part or "rel='next'" in part:
                url_part = part.split(";")[0].strip()
                if url_part.startswith("<") and url_part.endswith(">"):
                    return url_part[1:-1]
        return None
