"""GraphQL source connector — execute GraphQL queries over HTTP via httpx.

Supports cursor-based and offset-based pagination, variable injection,
bearer/basic/api-key authentication, and rate limiting.
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
    Record,
    Schema,
)
from ranger.sources.base import BaseSource

logger = structlog.get_logger()

try:
    import httpx
except ImportError as _err:
    raise ImportError(
        "httpx is required for GraphQLSource. "
        "Install it with: pip install ranger-core[graphql]"
    ) from _err


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_path(data: Any, path: str) -> Any:
    """Resolve a dot-separated path against a nested dict/list structure.

    Example: ``_resolve_path({"data": {"users": {"edges": [...]}}}, "data.users.edges")``
    """
    parts = path.split(".")
    current = data
    for part in parts:
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list) and part.isdigit():
            current = current[int(part)]
        else:
            return None
    return current


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


class GraphQLSource(BaseSource):
    """Read data from a GraphQL endpoint with automatic pagination.

    Config keys
    -----------
    endpoint : str
        Full URL to the GraphQL endpoint.
    query : str
        GraphQL query string.
    variables : dict
        Variables to inject into the query.
    headers : dict
        Extra HTTP headers.
    auth_type : str
        ``none`` | ``bearer`` | ``basic`` | ``api_key``.
    auth_config : dict
        Credentials dict matching *auth_type*.
    pagination : dict
        Keys: ``strategy`` (``cursor`` | ``offset``),
        ``cursor_variable``, ``page_size_variable``,
        ``has_next_path``, ``cursor_path``, ``data_path``.
    rate_limit : float
        Max requests per second (default: ``10``).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._client: httpx.Client | None = None

    # ------------------------------------------------------------------
    # Metadata properties
    # ------------------------------------------------------------------

    @property
    def source_type(self) -> str:
        return "graphql"

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
        headers = dict(self._config.get("headers", {}))
        headers.setdefault("Content-Type", "application/json")
        self._apply_auth_headers(headers)

        try:
            self._client = httpx.Client(
                headers=headers,
                timeout=httpx.Timeout(self._config.get("timeout", 30.0)),
            )
            self._connected = True
            logger.info(
                "graphql_source.connected",
                endpoint=self._config.get("endpoint"),
            )
        except Exception as exc:
            self._connected = False
            logger.error("graphql_source.connect_failed", error=str(exc))
            raise ConnectionError(f"Failed to create HTTP client: {exc}") from exc

    def close(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None
        self._connected = False
        logger.info("graphql_source.closed")

    def health_check(self) -> HealthStatus:
        try:
            if self._client is None:
                self.connect()
            assert self._client is not None
            endpoint = self._config.get("endpoint", "")
            # Simple introspection query as health probe
            resp = self._client.post(
                endpoint,
                json={"query": "{ __typename }"},
            )
            resp.raise_for_status()
            return HealthStatus.HEALTHY
        except Exception as exc:
            logger.warning("graphql_source.health_check_failed", error=str(exc))
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    def _apply_auth_headers(self, headers: dict[str, str]) -> None:
        auth_type = self._config.get("auth_type", "none").lower()
        auth_cfg: dict[str, Any] = self._config.get("auth_config", {})

        if auth_type == "bearer":
            headers["Authorization"] = f"Bearer {auth_cfg.get('token', '')}"
        elif auth_type == "basic":
            import base64

            cred = base64.b64encode(
                f"{auth_cfg.get('username', '')}:{auth_cfg.get('password', '')}".encode()
            ).decode()
            headers["Authorization"] = f"Basic {cred}"
        elif auth_type == "api_key":
            header_name = auth_cfg.get("header_name", "X-API-Key")
            headers[header_name] = auth_cfg.get("api_key", "")

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
        """Execute the GraphQL query with pagination and yield Records."""
        if self._client is None:
            raise RuntimeError("Source not connected — call connect() first")

        endpoint: str = self._config.get("endpoint", "")
        query: str = self._config.get("query", "")
        variables: dict[str, Any] = dict(self._config.get("variables", {}))

        pagination = self._config.get("pagination", {})
        strategy = pagination.get("strategy", "none")
        cursor_variable = pagination.get("cursor_variable", "after")
        page_size_variable = pagination.get("page_size_variable", "first")
        has_next_path = pagination.get("has_next_path", "data.pageInfo.hasNextPage")
        cursor_path = pagination.get("cursor_path", "data.pageInfo.endCursor")
        data_path = pagination.get("data_path", "data")
        page_size = pagination.get("page_size", 100)

        # Seed pagination variables
        if strategy in ("cursor", "offset"):
            if page_size_variable:
                variables[page_size_variable] = page_size
        if strategy == "offset":
            variables.setdefault("offset", 0)

        page_count = 0
        total_records = 0

        logger.info(
            "graphql_source.read_start",
            endpoint=endpoint,
            pagination_strategy=strategy,
        )

        while True:
            last_req = time.monotonic()

            payload: dict[str, Any] = {"query": query, "variables": variables}
            resp = self._client.post(endpoint, json=payload)
            resp.raise_for_status()
            response_json = resp.json()

            # Check for GraphQL errors
            if "errors" in response_json and response_json["errors"]:
                logger.error(
                    "graphql_source.query_errors",
                    errors=response_json["errors"],
                )
                break

            # Extract data
            raw_data = _resolve_path(response_json, data_path) if data_path else response_json
            records: list[dict[str, Any]]
            if isinstance(raw_data, list):
                records = raw_data
            elif isinstance(raw_data, dict):
                # Handle Relay-style edges/node
                if "edges" in raw_data:
                    records = [
                        edge.get("node", edge) if isinstance(edge, dict) else edge
                        for edge in raw_data["edges"]
                    ]
                elif "nodes" in raw_data:
                    records = raw_data["nodes"]
                else:
                    records = [raw_data]
            else:
                records = []

            for item in records:
                yield Record(
                    data=item if isinstance(item, dict) else {"value": item},
                    event_time=datetime.now(timezone.utc),
                    source_metadata={
                        "source_type": "graphql",
                        "endpoint": endpoint,
                        "page": page_count,
                    },
                )
                total_records += 1

            page_count += 1

            # Determine next page
            if strategy == "none" or not records:
                break
            if len(records) < page_size:
                break

            if strategy == "cursor":
                has_next = _resolve_path(response_json, has_next_path)
                if not has_next:
                    break
                cursor_value = _resolve_path(response_json, cursor_path)
                if not cursor_value:
                    break
                variables[cursor_variable] = cursor_value
            elif strategy == "offset":
                variables["offset"] = variables.get("offset", 0) + page_size
            else:
                break

            self._rate_limit_sleep(last_req)

        logger.info(
            "graphql_source.read_complete",
            total_records=total_records,
            pages=page_count,
        )

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def get_schema(self) -> Schema:
        """Infer schema by executing the query once and inspecting the data."""
        if self._client is None:
            raise RuntimeError("Source not connected — call connect() first")

        endpoint = self._config.get("endpoint", "")
        query = self._config.get("query", "")
        variables = dict(self._config.get("variables", {}))

        pagination = self._config.get("pagination", {})
        data_path = pagination.get("data_path", "data")
        page_size_variable = pagination.get("page_size_variable", "first")
        if page_size_variable:
            variables[page_size_variable] = 10  # small sample

        resp = self._client.post(endpoint, json={"query": query, "variables": variables})
        resp.raise_for_status()
        response_json = resp.json()

        raw_data = _resolve_path(response_json, data_path) if data_path else response_json
        records: list[dict[str, Any]] = []
        if isinstance(raw_data, list):
            records = raw_data
        elif isinstance(raw_data, dict):
            if "edges" in raw_data:
                records = [
                    edge.get("node", edge) if isinstance(edge, dict) else edge
                    for edge in raw_data["edges"]
                ]
            elif "nodes" in raw_data:
                records = raw_data["nodes"]
            else:
                records = [raw_data]

        if not records:
            return Schema(columns=[])

        all_keys: set[str] = set()
        for rec in records:
            if isinstance(rec, dict):
                all_keys.update(rec.keys())

        columns: list[ColumnDefinition] = []
        for key in sorted(all_keys):
            col_type = ColumnType.STRING
            for rec in records:
                if isinstance(rec, dict) and rec.get(key) is not None:
                    col_type = _infer_column_type(rec[key])
                    break
            columns.append(ColumnDefinition(name=key, type=col_type, nullable=True))

        return Schema(columns=columns)

    def discover_schema(self) -> DiscoveredSchema:
        schema = self.get_schema()
        endpoint = self._config.get("endpoint", "")
        return DiscoveredSchema(
            columns=schema.columns,
            primary_key=schema.primary_key,
            source_name="graphql",
            object_name=endpoint,
            object_type="endpoint",
            source_metadata={
                "endpoint": endpoint,
                "query_preview": self._config.get("query", "")[:200],
            },
        )
