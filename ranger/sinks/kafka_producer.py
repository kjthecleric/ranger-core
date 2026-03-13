"""Kafka producer sink — publishes records as messages to Kafka topics.

Uses confluent-kafka Producer for high-throughput message delivery with
configurable serialization, compression, partitioning, and acknowledgements.
"""

from __future__ import annotations

import json
from typing import Any

import structlog

from ranger.core.models import HealthStatus, Record, Schema
from ranger.sinks.base import BaseSink

try:
    from confluent_kafka import KafkaError, KafkaException, Producer

    _HAS_CONFLUENT = True
except ImportError:  # pragma: no cover
    _HAS_CONFLUENT = False

logger = structlog.get_logger(__name__)


class KafkaProducerSink(BaseSink):
    """Produce records as messages to a Kafka topic.

    Config keys:
        bootstrap_servers: Kafka broker addresses (required).
        topic: Target topic name (required).
        key_field: Record field to use as the message key (optional).
        partition_field: Record field for explicit partitioning (optional).
        security_protocol: ``PLAINTEXT``, ``SSL``, ``SASL_PLAINTEXT``, or
            ``SASL_SSL`` (default ``PLAINTEXT``).
        sasl_mechanism: SASL mechanism — ``PLAIN``, ``SCRAM-SHA-256``, etc. (optional).
        sasl_username: SASL username (optional).
        sasl_password: SASL password (optional).
        compression: Compression codec — ``gzip``, ``snappy``, ``lz4``, or
            ``zstd`` (default ``none``).
        acks: Acknowledgement level — ``0``, ``1``, or ``all`` (default ``all``).
        serialization: Message value serialization — ``json`` or ``avro``
            (default ``json``).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        if not _HAS_CONFLUENT:
            raise ImportError(
                "confluent-kafka is required for KafkaProducerSink. "
                "Install it with: pip install confluent-kafka"
            )
        super().__init__(config)

        self._bootstrap_servers: str = config["bootstrap_servers"]
        self._topic: str = config["topic"]
        self._key_field: str | None = config.get("key_field")
        self._partition_field: str | None = config.get("partition_field")
        self._security_protocol: str = config.get("security_protocol", "PLAINTEXT")
        self._sasl_mechanism: str | None = config.get("sasl_mechanism")
        self._sasl_username: str | None = config.get("sasl_username")
        self._sasl_password: str | None = config.get("sasl_password")
        self._compression: str = config.get("compression", "none")
        self._acks: str = str(config.get("acks", "all"))
        self._serialization: str = config.get("serialization", "json")

        self._producer: Producer | None = None
        self._schema: Schema | None = None
        self._delivery_errors: int = 0

    @property
    def sink_type(self) -> str:
        return "kafka_producer"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def open(self, schema: Schema) -> None:
        self._schema = schema

        producer_config: dict[str, Any] = {
            "bootstrap.servers": self._bootstrap_servers,
            "acks": self._acks,
            "security.protocol": self._security_protocol,
        }

        if self._compression != "none":
            producer_config["compression.type"] = self._compression

        if self._sasl_mechanism:
            producer_config["sasl.mechanism"] = self._sasl_mechanism
        if self._sasl_username:
            producer_config["sasl.username"] = self._sasl_username
        if self._sasl_password:
            producer_config["sasl.password"] = self._sasl_password

        self._producer = Producer(producer_config)
        self._delivery_errors = 0
        self._opened = True
        logger.info(
            "kafka_producer_sink.opened",
            topic=self._topic,
            bootstrap_servers=self._bootstrap_servers,
        )

    def close(self) -> None:
        if self._producer is not None:
            remaining = self._producer.flush(timeout=30)
            if remaining > 0:
                logger.warning(
                    "kafka_producer_sink.flush_incomplete",
                    remaining_messages=remaining,
                )
            self._producer = None
        self._opened = False
        logger.info("kafka_producer_sink.closed", topic=self._topic)

    def health_check(self) -> HealthStatus:
        try:
            if self._producer is None:
                return HealthStatus.UNHEALTHY
            # list_topics is a lightweight metadata call
            metadata = self._producer.list_topics(timeout=10)
            if self._topic in metadata.topics:
                return HealthStatus.HEALTHY
            return HealthStatus.DEGRADED
        except Exception:
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Writing
    # ------------------------------------------------------------------

    def write_batch(self, records: list[Record]) -> int:
        if not self._opened or self._producer is None:
            raise RuntimeError("Sink not opened. Call open() first.")
        if not records:
            return 0

        self._delivery_errors = 0
        produced = 0

        for rec in records:
            value = self._serialize(rec.data)
            key = self._extract_key(rec)
            kwargs: dict[str, Any] = {
                "topic": self._topic,
                "value": value,
                "callback": self._delivery_callback,
            }
            if key is not None:
                kwargs["key"] = key

            try:
                self._producer.produce(**kwargs)
                produced += 1
            except BufferError:
                # Local queue full — poll to free space and retry
                self._producer.poll(1.0)
                try:
                    self._producer.produce(**kwargs)
                    produced += 1
                except Exception:
                    logger.warning("kafka_producer_sink.produce_failed", exc_info=True)

            # Periodic poll to trigger delivery reports
            if produced % 1000 == 0:
                self._producer.poll(0)

        # Final flush for this batch
        self._producer.flush(timeout=30)

        written = produced - self._delivery_errors
        logger.info(
            "kafka_producer_sink.batch_written",
            produced=produced,
            delivery_errors=self._delivery_errors,
            written=written,
            topic=self._topic,
        )
        return written

    def _delivery_callback(self, err: Any, msg: Any) -> None:
        """Called once per message by confluent-kafka on delivery (or failure)."""
        if err is not None:
            self._delivery_errors += 1
            logger.warning(
                "kafka_producer_sink.delivery_error",
                error=str(err),
                topic=msg.topic() if msg else self._topic,
            )

    def _serialize(self, data: dict[str, Any]) -> bytes:
        """Serialize record data to bytes."""
        if self._serialization == "json":
            return json.dumps(data, default=str).encode("utf-8")
        elif self._serialization == "avro":
            # Avro serialization would require a schema registry client.
            # For now, fall back to JSON with a warning.
            logger.warning(
                "kafka_producer_sink.avro_fallback",
                message="Avro serialization requires schema registry integration; using JSON.",
            )
            return json.dumps(data, default=str).encode("utf-8")
        else:
            return json.dumps(data, default=str).encode("utf-8")

    def _extract_key(self, record: Record) -> bytes | None:
        """Extract and serialize the message key from a record."""
        if self._key_field and self._key_field in record.data:
            key_value = record.data[self._key_field]
            if isinstance(key_value, bytes):
                return key_value
            return str(key_value).encode("utf-8")
        return None

    # ------------------------------------------------------------------
    # Schema evolution
    # ------------------------------------------------------------------

    def evolve_schema(self, new_schema: Schema) -> None:
        """Kafka topics are schema-agnostic at the broker level.

        Schema evolution is typically managed by a Schema Registry.
        This is a no-op for the producer itself.
        """
        self._schema = new_schema
        logger.debug("kafka_producer_sink.evolve_schema_noop", topic=self._topic)

    def supports_schema_evolution(self) -> bool:
        return False
