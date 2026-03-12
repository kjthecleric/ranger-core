"""Change Data Capture source — Debezium CDC events via Kafka.

Consumes Debezium-formatted CDC events from Kafka topics and yields them
as :class:`Record` objects with operation metadata (insert / update / delete).
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Iterator
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
    StreamConfig,
)
from ranger.sources.base import BaseSource

logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# Debezium operation code → human-readable label
# ---------------------------------------------------------------------------

_DEBEZIUM_OPS: dict[str, str] = {
    "c": "insert",
    "r": "snapshot",  # read (initial snapshot)
    "u": "update",
    "d": "delete",
}


class DebeziumCDCSource(BaseSource):
    """Consume Debezium CDC events from a Kafka topic.

    Config keys:
        bootstrap_servers: Kafka bootstrap server(s) (comma-separated).
        topic: Kafka topic to consume from.
        group_id: Consumer group ID (default: ``ranger-cdc``).
        schema_registry_url: Optional Confluent Schema Registry URL for
            Avro/Protobuf envelope deserialization.
        auto_offset_reset: Where to start consuming when no committed offset
            exists — ``earliest`` (default) or ``latest``.
        poll_timeout: Seconds to wait per poll (default: ``1.0``).
        max_messages: Maximum messages to consume before stopping (``None``
            for unlimited — streaming mode).
        consumer_extra: Dict of extra ``confluent_kafka.Consumer`` config.
    """

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._consumer: Any = None  # confluent_kafka.Consumer
        self._deserializer: Any = None  # optional Avro/JSON deserializer

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    @property
    def source_type(self) -> str:
        return "cdc"

    @property
    def supports_streaming(self) -> bool:
        return True

    @property
    def supports_incremental(self) -> bool:
        return True

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        try:
            from confluent_kafka import Consumer
        except ImportError as exc:
            raise ImportError(
                "confluent-kafka is required for DebeziumCDCSource. "
                "Install it with: pip install ranger-core[cdc]"
            ) from exc

        bootstrap = self._config.get("bootstrap_servers")
        topic = self._config.get("topic")
        if not bootstrap or not topic:
            raise ConnectionError("Config must include 'bootstrap_servers' and 'topic'")

        group_id = self._config.get("group_id", "ranger-cdc")
        auto_offset_reset = self._config.get("auto_offset_reset", "earliest")

        consumer_conf: dict[str, Any] = {
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": True,
        }
        # Merge any extra consumer config
        consumer_conf.update(self._config.get("consumer_extra", {}))

        try:
            self._consumer = Consumer(consumer_conf)
            self._consumer.subscribe([topic])
            self._connected = True
            logger.info(
                "cdc_source.connected",
                bootstrap_servers=bootstrap,
                topic=topic,
                group_id=group_id,
            )
        except Exception as exc:
            self._connected = False
            logger.error("cdc_source.connect_failed", error=str(exc))
            raise ConnectionError(f"Kafka consumer creation failed: {exc}") from exc

        # Optional: schema-registry deserializer
        registry_url = self._config.get("schema_registry_url")
        if registry_url:
            self._setup_deserializer(registry_url)

    def _setup_deserializer(self, registry_url: str) -> None:
        """Set up an Avro/JSON deserializer backed by Schema Registry."""
        try:
            from confluent_kafka.schema_registry import SchemaRegistryClient
            from confluent_kafka.schema_registry.avro import AvroDeserializer

            sr_client = SchemaRegistryClient({"url": registry_url})
            self._deserializer = AvroDeserializer(sr_client)
            logger.info("cdc_source.schema_registry_connected", url=registry_url)
        except ImportError:
            logger.warning(
                "cdc_source.schema_registry_unavailable",
                msg="confluent_kafka.schema_registry not installed; falling back to JSON",
            )
        except Exception as exc:
            logger.warning("cdc_source.schema_registry_failed", error=str(exc))

    def close(self) -> None:
        if self._consumer is not None:
            try:
                self._consumer.close()
            except Exception:
                pass
            self._consumer = None
        self._deserializer = None
        self._connected = False
        logger.info("cdc_source.closed")

    def health_check(self) -> HealthStatus:
        """Check consumer assignment and broker connectivity."""
        try:
            if self._consumer is None:
                self.connect()
            # list_topics will raise if unreachable
            cluster_meta = self._consumer.list_topics(timeout=5)
            topic = self._config.get("topic")
            if topic and topic not in cluster_meta.topics:
                logger.warning("cdc_source.topic_not_found", topic=topic)
                return HealthStatus.DEGRADED
            return HealthStatus.HEALTHY
        except Exception as exc:
            logger.warning("cdc_source.health_check_failed", error=str(exc))
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Message parsing
    # ------------------------------------------------------------------

    def _deserialize_value(self, raw: bytes | None) -> dict[str, Any] | None:
        """Deserialize a Kafka message value."""
        if raw is None:
            return None
        if self._deserializer is not None:
            try:
                return self._deserializer(raw, None)
            except Exception:
                pass
        # Fallback: plain JSON
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return {"_raw": raw.decode("utf-8", errors="replace")}

    @staticmethod
    def _parse_cdc_envelope(payload: dict[str, Any]) -> Record:
        """Transform a Debezium envelope into a :class:`Record`.

        Debezium envelope structure::

            {
                "before": { ... },
                "after":  { ... },
                "source": { "ts_ms": ..., ... },
                "op":     "c" | "u" | "d" | "r",
                "ts_ms":  ...
            }
        """
        op_code = payload.get("op", "r")
        operation = _DEBEZIUM_OPS.get(op_code, op_code)

        before = payload.get("before")
        after = payload.get("after")
        source_info = payload.get("source", {})

        # Use the *after* image for inserts/updates, *before* for deletes
        if operation == "delete":
            data = dict(before) if before else {}
        else:
            data = dict(after) if after else {}

        # Source timestamp (Debezium provides millis)
        ts_ms = payload.get("ts_ms") or source_info.get("ts_ms")
        event_time: datetime | None = None
        if ts_ms is not None:
            event_time = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)

        return Record(
            data=data,
            event_time=event_time,
            source_metadata={
                "source_type": "cdc",
                "operation": operation,
                "before": before,
                "after": after,
                "source_ts": ts_ms,
                "debezium_source": source_info,
            },
        )

    # ------------------------------------------------------------------
    # Reading (batch / pull)
    # ------------------------------------------------------------------

    def read(self) -> Iterator[Record]:
        """Poll Kafka for CDC events up to *max_messages* (or until timeout).

        This is the synchronous pull-mode interface suitable for batch
        pipelines.  For continuous streaming use :meth:`read_stream`.
        """
        if self._consumer is None:
            raise RuntimeError("Source not connected — call connect() first")

        poll_timeout: float = self._config.get("poll_timeout", 1.0)
        max_messages: int | None = self._config.get("max_messages")
        topic = self._config.get("topic", "unknown")

        logger.info(
            "cdc_source.read_start",
            topic=topic,
            max_messages=max_messages,
        )

        count = 0
        empty_polls = 0
        max_empty = self._config.get("max_empty_polls", 10)

        while True:
            msg = self._consumer.poll(timeout=poll_timeout)
            if msg is None:
                empty_polls += 1
                if empty_polls >= max_empty:
                    logger.debug("cdc_source.no_more_messages", empty_polls=empty_polls)
                    break
                continue

            if msg.error():
                from confluent_kafka import KafkaError

                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("cdc_source.consumer_error", error=str(msg.error()))
                continue

            empty_polls = 0
            payload = self._deserialize_value(msg.value())
            if payload is None:
                continue

            # Debezium wraps the envelope in a "payload" key when using
            # the JSON converter with schemas enabled.
            if "payload" in payload and "op" in payload.get("payload", {}):
                payload = payload["payload"]

            record = self._parse_cdc_envelope(payload)
            record.source_metadata["kafka_topic"] = msg.topic()
            record.source_metadata["kafka_partition"] = msg.partition()
            record.source_metadata["kafka_offset"] = msg.offset()
            yield record

            count += 1
            if max_messages is not None and count >= max_messages:
                break

        logger.info("cdc_source.read_complete", messages=count)

    # ------------------------------------------------------------------
    # Streaming (async)
    # ------------------------------------------------------------------

    async def read_stream(  # type: ignore[override]
        self,
        config: StreamConfig | None = None,
    ) -> AsyncIterator[Record]:
        """Continuously consume CDC events as an async iterator.

        This never terminates on its own — callers should break out of the
        loop or cancel the task when done.
        """
        if self._consumer is None:
            raise RuntimeError("Source not connected — call connect() first")

        stream_cfg = config or StreamConfig()
        poll_timeout: float = self._config.get("poll_timeout", 0.5)
        topic = self._config.get("topic", "unknown")

        logger.info("cdc_source.stream_start", topic=topic)

        idle_seconds = 0.0
        while True:
            msg = self._consumer.poll(timeout=poll_timeout)

            if msg is None:
                idle_seconds += poll_timeout
                if idle_seconds >= stream_cfg.idle_timeout_seconds:
                    logger.info("cdc_source.idle_timeout", seconds=idle_seconds)
                    break
                await asyncio.sleep(0.01)  # yield to event loop
                continue

            if msg.error():
                from confluent_kafka import KafkaError

                if msg.error().code() == KafkaError._PARTITION_EOF:
                    await asyncio.sleep(0.01)
                    continue
                logger.error("cdc_source.stream_error", error=str(msg.error()))
                await asyncio.sleep(0.1)
                continue

            idle_seconds = 0.0
            payload = self._deserialize_value(msg.value())
            if payload is None:
                continue

            if "payload" in payload and "op" in payload.get("payload", {}):
                payload = payload["payload"]

            record = self._parse_cdc_envelope(payload)
            record.source_metadata["kafka_topic"] = msg.topic()
            record.source_metadata["kafka_partition"] = msg.partition()
            record.source_metadata["kafka_offset"] = msg.offset()
            yield record

    # ------------------------------------------------------------------
    # Schema operations
    # ------------------------------------------------------------------

    def get_schema(self) -> Schema:
        """Return an empty schema — CDC schemas are dynamic.

        For a concrete schema, connect the Schema Registry or use
        :meth:`discover_schema` after consuming a few events.
        """
        return Schema(columns=[])

    def discover_schema(self) -> DiscoveredSchema:
        """Discover schema by consuming a small sample of events."""
        sample_records: list[Record] = []
        for record in self.read():
            sample_records.append(record)
            if len(sample_records) >= 50:
                break

        if not sample_records:
            return DiscoveredSchema(
                source_name="cdc",
                object_name=self._config.get("topic", "unknown"),
                object_type="topic",
            )

        all_keys: set[str] = set()
        for rec in sample_records:
            all_keys.update(rec.data.keys())

        columns: list[ColumnDefinition] = []
        for key in sorted(all_keys):
            col_type = self._infer_type(key, [r.data for r in sample_records])
            columns.append(ColumnDefinition(name=key, type=col_type, nullable=True))

        return DiscoveredSchema(
            columns=columns,
            source_name="cdc",
            object_name=self._config.get("topic", "unknown"),
            object_type="topic",
            row_count_estimate=len(sample_records),
            source_metadata={
                "bootstrap_servers": self._config.get("bootstrap_servers"),
                "group_id": self._config.get("group_id", "ranger-cdc"),
            },
        )

    @staticmethod
    def _infer_type(key: str, sample: list[dict[str, Any]]) -> ColumnType:
        types_seen: set[str] = set()
        for row in sample:
            val = row.get(key)
            if val is not None:
                types_seen.add(type(val).__name__)
        types_seen.discard("NoneType")
        if not types_seen:
            return ColumnType.STRING
        if types_seen == {"int"}:
            return ColumnType.INT64
        if "float" in types_seen:
            return ColumnType.FLOAT64
        if types_seen == {"bool"}:
            return ColumnType.BOOLEAN
        if "dict" in types_seen:
            return ColumnType.JSON
        if "list" in types_seen:
            return ColumnType.ARRAY
        return ColumnType.STRING
