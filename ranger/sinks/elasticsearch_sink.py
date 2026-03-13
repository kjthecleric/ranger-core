"""Elasticsearch sink — indexes records into Elasticsearch.

Uses the elasticsearch-py client with the bulk helpers API for efficient
batch indexing, and supports explicit index mappings and ingest pipelines.
"""

from __future__ import annotations

import json
from typing import Any

import structlog

from ranger.core.models import ColumnType, HealthStatus, Record, Schema
from ranger.sinks.base import BaseSink

try:
    from elasticsearch import Elasticsearch, helpers as es_helpers

    _HAS_ES = True
except ImportError:  # pragma: no cover
    _HAS_ES = False

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Ranger ColumnType → Elasticsearch field type mapping
# ---------------------------------------------------------------------------

_ES_TYPE_MAP: dict[ColumnType, str] = {
    ColumnType.BOOLEAN: "boolean",
    ColumnType.INT8: "byte",
    ColumnType.INT16: "short",
    ColumnType.INT32: "integer",
    ColumnType.INT64: "long",
    ColumnType.FLOAT32: "float",
    ColumnType.FLOAT64: "double",
    ColumnType.DECIMAL: "scaled_float",
    ColumnType.STRING: "text",
    ColumnType.BINARY: "binary",
    ColumnType.DATE: "date",
    ColumnType.TIME: "keyword",
    ColumnType.TIMESTAMP: "date",
    ColumnType.TIMESTAMP_TZ: "date",
    ColumnType.JSON: "object",
    ColumnType.ARRAY: "nested",
    ColumnType.MAP: "object",
    ColumnType.STRUCT: "object",
    ColumnType.UUID: "keyword",
}


class ElasticsearchSink(BaseSink):
    """Index records into an Elasticsearch cluster.

    Config keys:
        hosts: List of Elasticsearch host URLs (required).
        index: Target index name (required).
        api_key: API key for authentication (optional).
        basic_auth: Dict with ``username`` and ``password`` (optional).
        id_field: Record field to use as the document ``_id`` (optional).
        pipeline: Ingest pipeline name to apply on indexing (optional).
        refresh: Index refresh policy — ``true``, ``false``, or ``wait_for``
            (default ``false``).
        batch_size: Number of documents per bulk request (default ``500``).
        mapping: Explicit index mapping dict (optional).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        if not _HAS_ES:
            raise ImportError(
                "elasticsearch is required for ElasticsearchSink. "
                "Install it with: pip install elasticsearch"
            )
        super().__init__(config)

        self._hosts: list[str] = config["hosts"]
        self._index: str = config["index"]
        self._api_key: str | None = config.get("api_key")
        self._basic_auth: dict[str, str] | None = config.get("basic_auth")
        self._id_field: str | None = config.get("id_field")
        self._pipeline: str | None = config.get("pipeline")
        self._refresh: str = config.get("refresh", "false")
        self._batch_size: int = config.get("batch_size", 500)
        self._mapping: dict[str, Any] | None = config.get("mapping")

        self._client: Elasticsearch | None = None
        self._schema: Schema | None = None

    @property
    def sink_type(self) -> str:
        return "elasticsearch"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def open(self, schema: Schema) -> None:
        self._schema = schema

        # Build client kwargs
        client_kwargs: dict[str, Any] = {"hosts": self._hosts}
        if self._api_key:
            client_kwargs["api_key"] = self._api_key
        if self._basic_auth:
            client_kwargs["basic_auth"] = (
                self._basic_auth["username"],
                self._basic_auth["password"],
            )

        self._client = Elasticsearch(**client_kwargs)

        # Create index if it doesn't exist
        if not self._client.indices.exists(index=self._index):
            body: dict[str, Any] = {}
            if self._mapping:
                body["mappings"] = self._mapping
            else:
                body["mappings"] = self._build_mapping_from_schema(schema)
            self._client.indices.create(index=self._index, body=body)
            logger.info(
                "elasticsearch_sink.index_created",
                index=self._index,
            )

        self._opened = True
        logger.info(
            "elasticsearch_sink.opened",
            index=self._index,
            hosts=self._hosts,
        )

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
        self._opened = False
        logger.info("elasticsearch_sink.closed", index=self._index)

    def health_check(self) -> HealthStatus:
        try:
            if self._client is None:
                return HealthStatus.UNHEALTHY
            info = self._client.cluster.health()
            status = info.get("status", "red")
            if status == "green":
                return HealthStatus.HEALTHY
            if status == "yellow":
                return HealthStatus.DEGRADED
            return HealthStatus.UNHEALTHY
        except Exception:
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Writing
    # ------------------------------------------------------------------

    def write_batch(self, records: list[Record]) -> int:
        if not self._opened or self._client is None:
            raise RuntimeError("Sink not opened. Call open() first.")
        if not records:
            return 0

        written = 0
        for i in range(0, len(records), self._batch_size):
            batch = records[i : i + self._batch_size]
            actions = self._build_bulk_actions(batch)
            success, errors = es_helpers.bulk(
                self._client,
                actions,
                refresh=self._refresh,
                raise_on_error=False,
            )
            written += success
            if errors:
                logger.warning(
                    "elasticsearch_sink.bulk_errors",
                    error_count=len(errors),
                    sample=errors[:3],
                )

        logger.info("elasticsearch_sink.batch_written", count=written, index=self._index)
        return written

    def _build_bulk_actions(self, records: list[Record]) -> list[dict[str, Any]]:
        """Convert records into Elasticsearch bulk action dicts."""
        actions: list[dict[str, Any]] = []
        for rec in records:
            action: dict[str, Any] = {
                "_index": self._index,
                "_source": rec.data,
            }
            if self._id_field and self._id_field in rec.data:
                action["_id"] = rec.data[self._id_field]
            if self._pipeline:
                action["pipeline"] = self._pipeline
            actions.append(action)
        return actions

    # ------------------------------------------------------------------
    # Schema evolution
    # ------------------------------------------------------------------

    def evolve_schema(self, new_schema: Schema) -> None:
        if self._client is None:
            raise RuntimeError("Sink not opened. Call open() first.")

        existing_names: set[str] = set()
        if self._schema:
            existing_names = set(self._schema.column_names())

        new_properties: dict[str, Any] = {}
        for col in new_schema.columns:
            if col.name not in existing_names:
                es_type = _ES_TYPE_MAP.get(col.type, "text")
                new_properties[col.name] = {"type": es_type}

        if new_properties:
            self._client.indices.put_mapping(
                index=self._index,
                body={"properties": new_properties},
            )
            logger.info(
                "elasticsearch_sink.mapping_updated",
                index=self._index,
                new_fields=list(new_properties.keys()),
            )

        self._schema = new_schema

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_mapping_from_schema(self, schema: Schema) -> dict[str, Any]:
        """Generate an Elasticsearch mapping from a Ranger Schema."""
        properties: dict[str, Any] = {}
        for col in schema.columns:
            es_type = _ES_TYPE_MAP.get(col.type, "text")
            field_def: dict[str, Any] = {"type": es_type}
            # Add keyword sub-field for text types
            if es_type == "text":
                field_def["fields"] = {"keyword": {"type": "keyword", "ignore_above": 256}}
            if es_type == "scaled_float":
                field_def["scaling_factor"] = 100
            properties[col.name] = field_def
        return {"properties": properties}
