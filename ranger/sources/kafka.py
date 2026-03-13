"""Apache Kafka source connector.

Reads messages from Kafka topics using the confluent-kafka consumer.
Supports both bounded (batch) reads and continuous streaming consumption.
"""

from __future__ import annotations

import asyncio
import json
import time
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

try:
    from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
    from confluent_kafka.admin import AdminClient
except ImportError as _ck_err:
    raise ImportError(
        "confluent-kafka is required for KafkaSource. "
        "Install it with: pip install ranger-core[kafka]"
    ) from _ck_err


class KafkaSource(BaseSource):
    """Read messages from Apache Kafka topics.

    Config keys:
        bootstrap_servers: Comma-separated broker addresses
            (e.g. ``"broker1:9092,broker2:9092"``).
        topics: List of topic names to subscribe to.
        group_id: Consumer group identifier (default: ``"ranger-consumer"``).
        auto_offset_reset: Where to start reading if no committed offset
            exists — ``"earliest"`` or ``"latest"`` (default: ``"earliest"``).
        batch_size: Maximum number of messages to consume in bounded
            ``read()`` mode (default: ``1000``).
        poll_timeout: Seconds to wait for a message on each poll
            (default: ``1.0``).
        security_protocol: Protocol used to communicate with brokers
            (default: ``"PLAINTEXT"``).
        sasl_mechanism: SASL mechanism when security_protocol is SASL_*
            (e.g. ``"PLAIN"``, ``"SCRAM-SHA-256"``).
        sasl_username: SASL username.
        sasl_password: SASL password.
    """

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._consumer: Consumer | None = None
        self._admin: AdminClient | None = None

    # ------------------------------------------------------------------
    # Metadata properties
    # ------------------------------------------------------------------

    @property
    def source_type(self) -> str:  # noqa: D401
        return "kafka"

    @property
    def supports_streaming(self) -> bool:
        return True

    @property
    def supports_incremental(self) -> bool:
        return True

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def _build_consumer_config(self) -> dict[str, Any]:
        """Assemble the confluent-kafka consumer configuration dict."""
        bootstrap = self._config.get("bootstrap_servers", "localhost:9092")
        group_id = self._config.get("group_id", "ranger-consumer")
        auto_offset = self._config.get("auto_offset_reset", "earliest")
        security = self._config.get("security_protocol", "PLAINTEXT")

        conf: dict[str, Any] = {
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "auto.offset.reset": auto_offset,
            "enable.auto.commit": True,
            "security.protocol": security,
        }

        sasl_mechanism = self._config.get("sasl_mechanism")
        if sasl_mechanism:
            conf["sasl.mechanism"] = sasl_mechanism
            sasl_user = self._config.get("sasl_username")
            sasl_pass = self._config.get("sasl_password")
            if sasl_user:
                conf["sasl.username"] = sasl_user
            if sasl_pass:
                conf["sasl.password"] = sasl_pass

        return conf

    def connect(self) -> None:
        try:
            conf = self._build_consumer_config()
            self._consumer = Consumer(conf)

            topics: list[str] = self._config.get("topics", [])
            if topics:
                self._consumer.subscribe(topics)

            # Build admin client for health checks / schema discovery
            admin_conf = {
                "bootstrap.servers": conf["bootstrap.servers"],
                "security.protocol": conf["security.protocol"],
            }
            if "sasl.mechanism" in conf:
                admin_conf["sasl.mechanism"] = conf["sasl.mechanism"]
            if "sasl.username" in conf:
                admin_conf["sasl.username"] = conf["sasl.username"]
            if "sasl.password" in conf:
                admin_conf["sasl.password"] = conf["sasl.password"]
            self._admin = AdminClient(admin_conf)

            self._connected = True
            logger.info(
                "kafka_source.connected",
                bootstrap_servers=conf["bootstrap.servers"],
                topics=topics,
                group_id=conf["group.id"],
            )
        except Exception as exc:
            self._connected = False
            logger.error("kafka_source.connect_failed", error=str(exc))
            raise ConnectionError(f"Failed to connect to Kafka: {exc}") from exc

    def close(self) -> None:
        if self._consumer is not None:
            try:
                self._consumer.close()
            except Exception:
                pass
            self._consumer = None
        self._admin = None
        self._connected = False
        logger.info("kafka_source.closed")

    def health_check(self) -> HealthStatus:
        """List topics to verify broker connectivity."""
        try:
            if self._admin is None:
                self.connect()
            assert self._admin is not None
            metadata = self._admin.list_topics(timeout=10)
            topic_count = len(metadata.topics)
            logger.info("kafka_source.health_check_ok", topic_count=topic_count)
            return HealthStatus.HEALTHY
        except Exception as exc:
            logger.warning("kafka_source.health_check_failed", error=str(exc))
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Reading
    # ------------------------------------------------------------------

    @staticmethod
    def _message_to_record(msg: Any) -> Record:
        """Convert a confluent-kafka Message to a Ranger :class:`Record`."""
        value = msg.value()
        if isinstance(value, bytes):
            try:
                data = json.loads(value.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                data = {"raw": value.decode("utf-8", errors="replace")}
        elif isinstance(value, str):
            try:
                data = json.loads(value)
            except json.JSONDecodeError:
                data = {"raw": value}
        else:
            data = {"raw": str(value)}

        if not isinstance(data, dict):
            data = {"value": data}

        key = msg.key()
        if isinstance(key, bytes):
            key = key.decode("utf-8", errors="replace")

        # Kafka timestamp: tuple (type, ms-since-epoch)
        ts_type, ts_ms = msg.timestamp()
        event_time: datetime | None = None
        if ts_ms and ts_ms > 0:
            event_time = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)

        return Record(
            data=data,
            event_time=event_time,
            source_metadata={
                "source_type": "kafka",
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "key": key,
                "timestamp": ts_ms,
            },
        )

    def read(self) -> Iterator[Record]:
        """Bounded consumption — consume up to *batch_size* messages, then stop.

        Yields:
            Record objects, one per Kafka message.
        """
        if self._consumer is None:
            raise RuntimeError("Source not connected — call connect() first")

        batch_size: int = self._config.get("batch_size", 1000)
        poll_timeout: float = self._config.get("poll_timeout", 1.0)

        logger.info("kafka_source.read_start", batch_size=batch_size)
        consumed = 0

        while consumed < batch_size:
            msg = self._consumer.poll(timeout=poll_timeout)
            if msg is None:
                # No more messages within the timeout window — stop
                break
            if msg.error():
                err = msg.error()
                if err.code() == KafkaError._PARTITION_EOF:
                    logger.debug("kafka_source.partition_eof", partition=msg.partition())
                    continue
                logger.error("kafka_source.consumer_error", error=str(err))
                raise KafkaException(err)

            yield self._message_to_record(msg)
            consumed += 1

        logger.info("kafka_source.read_complete", messages=consumed)

    async def read_stream(self, config: StreamConfig | None = None) -> AsyncIterator[Record]:
        """Continuous async consumption — yields records indefinitely.

        Args:
            config: Optional streaming configuration.

        Yields:
            Record objects as they arrive from Kafka.
        """
        if self._consumer is None:
            raise RuntimeError("Source not connected — call connect() first")

        poll_timeout: float = self._config.get("poll_timeout", 1.0)
        idle_timeout: float = config.idle_timeout_seconds if config else 300.0
        last_msg_time = time.monotonic()

        logger.info("kafka_source.stream_start")

        while True:
            msg = self._consumer.poll(timeout=poll_timeout)

            if msg is None:
                if time.monotonic() - last_msg_time > idle_timeout:
                    logger.info("kafka_source.stream_idle_timeout")
                    break
                await asyncio.sleep(0.01)
                continue

            if msg.error():
                err = msg.error()
                if err.code() == KafkaError._PARTITION_EOF:
                    await asyncio.sleep(0.01)
                    continue
                logger.error("kafka_source.stream_error", error=str(err))
                raise KafkaException(err)

            last_msg_time = time.monotonic()
            yield self._message_to_record(msg)

    # ------------------------------------------------------------------
    # Schema operations
    # ------------------------------------------------------------------

    def get_schema(self) -> Schema:
        """Return a generic schema for Kafka message payloads.

        Kafka topics are schema-less by default — this returns a baseline
        schema representing the metadata fields always present on every
        consumed message.
        """
        return Schema(
            columns=[
                ColumnDefinition(name="key", type=ColumnType.STRING, nullable=True),
                ColumnDefinition(name="value", type=ColumnType.JSON, nullable=False),
                ColumnDefinition(name="topic", type=ColumnType.STRING, nullable=False),
                ColumnDefinition(name="partition", type=ColumnType.INT32, nullable=False),
                ColumnDefinition(name="offset", type=ColumnType.INT64, nullable=False),
                ColumnDefinition(name="timestamp", type=ColumnType.TIMESTAMP_TZ, nullable=True),
            ],
        )

    def discover_schema(self) -> DiscoveredSchema:
        """Discover available topics and return schema metadata."""
        schema = self.get_schema()
        topics = self._config.get("topics", [])
        topic_name = ",".join(topics) if topics else "unknown"

        metadata: dict[str, Any] = {}
        if self._admin is not None:
            try:
                cluster_meta = self._admin.list_topics(timeout=10)
                metadata["available_topics"] = list(cluster_meta.topics.keys())
                if topics:
                    for t in topics:
                        topic_meta = cluster_meta.topics.get(t)
                        if topic_meta:
                            metadata[f"{t}_partitions"] = len(topic_meta.partitions)
            except Exception:
                pass

        return DiscoveredSchema(
            columns=schema.columns,
            primary_key=None,
            source_name="kafka",
            object_name=topic_name,
            object_type="topic",
            source_metadata=metadata,
        )
