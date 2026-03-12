"""Elasticsearch source — reads documents via the Scroll API.

Uses the official ``elasticsearch-py`` client to execute queries against one
or more Elasticsearch indices, with support for DSL queries, scroll-based
pagination, and incremental reads.
"""

from __future__ import annotations

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

# ---------------------------------------------------------------------------
# Elasticsearch field type → Ranger ColumnType mapping
# ---------------------------------------------------------------------------

_ES_TYPE_MAP: dict[str, ColumnType] = {
    "text": ColumnType.STRING,
    "keyword": ColumnType.STRING,
    "long": ColumnType.INT64,
    "integer": ColumnType.INT32,
    "short": ColumnType.INT16,
    "byte": ColumnType.INT8,
    "double": ColumnType.FLOAT64,
    "float": ColumnType.FLOAT32,
    "half_float": ColumnType.FLOAT32,
    "scaled_float": ColumnType.FLOAT64,
    "boolean": ColumnType.BOOLEAN,
    "date": ColumnType.TIMESTAMP,
    "date_nanos": ColumnType.TIMESTAMP,
    "binary": ColumnType.BINARY,
    "ip": ColumnType.STRING,
    "object": ColumnType.JSON,
    "nested": ColumnType.JSON,
    "geo_point": ColumnType.POINT,
    "geo_shape": ColumnType.GEOMETRY,
    "flattened": ColumnType.JSON,
    "unsigned_long": ColumnType.INT64,
}


def _map_es_type(es_type: str) -> ColumnType:
    """Map an Elasticsearch field type to a Ranger :class:`ColumnType`."""
    return _ES_TYPE_MAP.get(es_type, ColumnType.STRING)


class ElasticsearchSource(BaseSource):
    """Read documents from an Elasticsearch index via the Scroll API.

    Config keys:
        hosts: List of Elasticsearch host URLs
            (e.g. ``["https://localhost:9200"]``).
        index: Index name or pattern (e.g. ``logs-*``).
        query: Elasticsearch Query DSL body (default: ``{"match_all": {}}``).
        scroll_timeout: Scroll context keep-alive
            (default: ``"5m"``).
        batch_size: Number of documents per scroll page (default: ``1_000``).
        api_key: Optional API key for authentication (tuple or string).
        basic_auth: Optional basic auth tuple ``(username, password)``.
        ca_certs: Optional path to CA bundle for TLS verification.
        verify_certs: Whether to verify TLS certificates (default: ``True``).
        incremental_column: Field name used for incremental reads.
        last_value: Last-seen value for the incremental field.
        request_timeout: Per-request timeout in seconds (default: ``30``).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._client: Any = None  # elasticsearch.Elasticsearch

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    @property
    def source_type(self) -> str:
        return "elasticsearch"

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
        try:
            from elasticsearch import Elasticsearch
        except ImportError as exc:
            raise ImportError(
                "elasticsearch-py is required for ElasticsearchSource. "
                "Install it with: pip install ranger-core[elasticsearch]"
            ) from exc

        hosts = self._config.get("hosts")
        if not hosts:
            raise ConnectionError("Missing required config key 'hosts'")

        client_kwargs: dict[str, Any] = {
            "hosts": hosts,
            "verify_certs": self._config.get("verify_certs", True),
            "request_timeout": self._config.get("request_timeout", 30),
        }

        # Authentication
        api_key = self._config.get("api_key")
        basic_auth = self._config.get("basic_auth")
        if api_key:
            client_kwargs["api_key"] = api_key
        elif basic_auth:
            client_kwargs["basic_auth"] = tuple(basic_auth) if isinstance(basic_auth, list) else basic_auth

        ca_certs = self._config.get("ca_certs")
        if ca_certs:
            client_kwargs["ca_certs"] = ca_certs

        try:
            self._client = Elasticsearch(**client_kwargs)
            # Verify connectivity
            info = self._client.info()
            self._connected = True
            logger.info(
                "elasticsearch_source.connected",
                cluster_name=info.get("cluster_name"),
                version=info.get("version", {}).get("number"),
            )
        except Exception as exc:
            self._connected = False
            logger.error("elasticsearch_source.connect_failed", error=str(exc))
            raise ConnectionError(f"Elasticsearch connection failed: {exc}") from exc

    def close(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None
        self._connected = False
        logger.info("elasticsearch_source.closed")

    def health_check(self) -> HealthStatus:
        """Ping the cluster for a lightweight health check."""
        try:
            if self._client is None:
                self.connect()
            if self._client.ping():
                return HealthStatus.HEALTHY
            return HealthStatus.DEGRADED
        except Exception as exc:
            logger.warning("elasticsearch_source.health_check_failed", error=str(exc))
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Query building
    # ------------------------------------------------------------------

    def _build_query_body(self) -> dict[str, Any]:
        """Build the Elasticsearch query body from config."""
        query = self._config.get("query", {"match_all": {}})

        # Incremental support: wrap in a bool filter with a range clause
        inc_col = self._config.get("incremental_column")
        last_value = self._config.get("last_value")

        if inc_col and last_value is not None:
            range_clause = {"range": {inc_col: {"gt": last_value}}}
            # Wrap existing query in bool.must + range filter
            return {
                "bool": {
                    "must": [query],
                    "filter": [range_clause],
                }
            }

        return query

    # ------------------------------------------------------------------
    # Reading
    # ------------------------------------------------------------------

    def read(self) -> Iterator[Record]:
        """Read documents using the Scroll API for memory-safe iteration."""
        if self._client is None:
            raise RuntimeError("Source not connected — call connect() first")

        index = self._config.get("index")
        if not index:
            raise ValueError("Config must include 'index'")

        scroll_timeout = self._config.get("scroll_timeout", "5m")
        batch_size: int = self._config.get("batch_size", 1_000)
        query_body = self._build_query_body()

        logger.info(
            "elasticsearch_source.read_start",
            index=index,
            batch_size=batch_size,
        )

        # Initial search with scroll
        response = self._client.search(
            index=index,
            query=query_body,
            scroll=scroll_timeout,
            size=batch_size,
        )

        scroll_id = response.get("_scroll_id")
        hits = response.get("hits", {}).get("hits", [])
        total = response.get("hits", {}).get("total", {})
        total_value = total.get("value", 0) if isinstance(total, dict) else total

        logger.info(
            "elasticsearch_source.scroll_started",
            total_hits=total_value,
        )

        row_count = 0

        try:
            while hits:
                for hit in hits:
                    data = hit.get("_source", {})
                    data["_id"] = hit.get("_id")
                    data["_index"] = hit.get("_index")

                    yield Record(
                        data=data,
                        event_time=datetime.now(timezone.utc),
                        source_metadata={
                            "source_type": "elasticsearch",
                            "index": hit.get("_index"),
                            "doc_id": hit.get("_id"),
                            "score": hit.get("_score"),
                        },
                    )
                    row_count += 1

                # Fetch next scroll page
                if not scroll_id:
                    break

                response = self._client.scroll(scroll_id=scroll_id, scroll=scroll_timeout)
                scroll_id = response.get("_scroll_id")
                hits = response.get("hits", {}).get("hits", [])

        finally:
            # Clean up scroll context
            if scroll_id:
                try:
                    self._client.clear_scroll(scroll_id=scroll_id)
                except Exception:
                    pass

        logger.info("elasticsearch_source.read_complete", rows=row_count)

    def read_batch(self, batch_size: int = 1_000) -> Iterator[list[Record]]:
        """Yield batches of records using the Scroll API."""
        if self._client is None:
            raise RuntimeError("Source not connected — call connect() first")

        index = self._config.get("index")
        if not index:
            raise ValueError("Config must include 'index'")

        scroll_timeout = self._config.get("scroll_timeout", "5m")
        query_body = self._build_query_body()

        response = self._client.search(
            index=index,
            query=query_body,
            scroll=scroll_timeout,
            size=batch_size,
        )

        scroll_id = response.get("_scroll_id")
        hits = response.get("hits", {}).get("hits", [])

        try:
            while hits:
                batch: list[Record] = []
                for hit in hits:
                    data = hit.get("_source", {})
                    data["_id"] = hit.get("_id")
                    data["_index"] = hit.get("_index")

                    batch.append(
                        Record(
                            data=data,
                            event_time=datetime.now(timezone.utc),
                            source_metadata={
                                "source_type": "elasticsearch",
                                "index": hit.get("_index"),
                                "doc_id": hit.get("_id"),
                            },
                        )
                    )

                yield batch

                if not scroll_id:
                    break
                response = self._client.scroll(scroll_id=scroll_id, scroll=scroll_timeout)
                scroll_id = response.get("_scroll_id")
                hits = response.get("hits", {}).get("hits", [])

        finally:
            if scroll_id:
                try:
                    self._client.clear_scroll(scroll_id=scroll_id)
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Schema operations
    # ------------------------------------------------------------------

    def get_schema(self) -> Schema:
        """Derive schema from the Elasticsearch index mapping."""
        if self._client is None:
            raise RuntimeError("Source not connected — call connect() first")

        index = self._config.get("index")
        if not index:
            return Schema(columns=[])

        try:
            mapping_resp = self._client.indices.get_mapping(index=index)
        except Exception as exc:
            logger.warning("elasticsearch_source.get_mapping_failed", error=str(exc))
            return Schema(columns=[])

        # The mapping response is keyed by concrete index name(s)
        columns: list[ColumnDefinition] = []
        seen_fields: set[str] = set()

        for _index_name, index_data in mapping_resp.items():
            properties = index_data.get("mappings", {}).get("properties", {})
            self._flatten_properties(properties, columns, seen_fields, prefix="")

        return Schema(columns=columns)

    def _flatten_properties(
        self,
        properties: dict[str, Any],
        columns: list[ColumnDefinition],
        seen: set[str],
        prefix: str,
    ) -> None:
        """Recursively flatten nested ES mapping properties into column defs."""
        for field_name, field_meta in properties.items():
            full_name = f"{prefix}{field_name}" if not prefix else f"{prefix}.{field_name}"
            if full_name in seen:
                continue
            seen.add(full_name)

            es_type = field_meta.get("type", "object")

            # If it has sub-properties, recurse
            sub_props = field_meta.get("properties")
            if sub_props:
                self._flatten_properties(sub_props, columns, seen, prefix=full_name)
                continue

            col_type = _map_es_type(es_type)
            columns.append(
                ColumnDefinition(
                    name=full_name,
                    type=col_type,
                    nullable=True,
                    description=f"ES type: {es_type}",
                )
            )

    def discover_schema(self) -> DiscoveredSchema:
        """Discover schema from the index mapping with catalog metadata."""
        schema = self.get_schema()
        index = self._config.get("index", "unknown")

        row_count: int | None = None
        if self._client:
            try:
                count_resp = self._client.count(index=index)
                row_count = count_resp.get("count")
            except Exception:
                pass

        return DiscoveredSchema(
            columns=schema.columns,
            primary_key=["_id"],
            source_name="elasticsearch",
            object_name=index,
            object_type="index",
            row_count_estimate=row_count,
            source_metadata={
                "hosts": self._config.get("hosts"),
            },
        )
