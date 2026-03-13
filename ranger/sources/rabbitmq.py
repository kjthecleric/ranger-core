"""RabbitMQ source connector.

Reads messages from a RabbitMQ queue using the pika library.
Supports both bounded (batch) reads and continuous async consumption.
"""

from __future__ import annotations

import asyncio
import json
import queue
import threading
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
    import pika
    from pika.exceptions import AMQPConnectionError, AMQPError
except ImportError as _pika_err:
    raise ImportError(
        "pika is required for RabbitMQSource. "
        "Install it with: pip install ranger-core[rabbitmq]"
    ) from _pika_err


class RabbitMQSource(BaseSource):
    """Read messages from a RabbitMQ queue.

    Config keys:
        host: RabbitMQ broker hostname (default: ``"localhost"``).
        port: AMQP port (default: ``5672``).
        virtual_host: Virtual host (default: ``"/"``).
        queue: Queue name to consume from.
        exchange: Optional exchange to bind the queue to.
        routing_key: Optional routing key for the exchange binding.
        username: Auth username (default: ``"guest"``).
        password: Auth password (default: ``"guest"``).
        prefetch_count: QoS prefetch count — how many un-acked messages
            the broker may deliver at once (default: ``100``).
        batch_size: Maximum messages for bounded ``read()``
            (default: ``1000``).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._connection: pika.BlockingConnection | None = None
        self._channel: Any | None = None  # pika channel

    # ------------------------------------------------------------------
    # Metadata properties
    # ------------------------------------------------------------------

    @property
    def source_type(self) -> str:  # noqa: D401
        return "rabbitmq"

    @property
    def supports_streaming(self) -> bool:
        return True

    @property
    def supports_incremental(self) -> bool:
        return False

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def _build_connection_params(self) -> pika.ConnectionParameters:
        """Assemble pika connection parameters from config."""
        host = self._config.get("host", "localhost")
        port = int(self._config.get("port", 5672))
        virtual_host = self._config.get("virtual_host", "/")
        username = self._config.get("username", "guest")
        password = self._config.get("password", "guest")

        credentials = pika.PlainCredentials(username, password)
        return pika.ConnectionParameters(
            host=host,
            port=port,
            virtual_host=virtual_host,
            credentials=credentials,
            heartbeat=600,
            blocked_connection_timeout=300,
        )

    def connect(self) -> None:
        try:
            params = self._build_connection_params()
            self._connection = pika.BlockingConnection(params)
            self._channel = self._connection.channel()

            # Set QoS
            prefetch = int(self._config.get("prefetch_count", 100))
            self._channel.basic_qos(prefetch_count=prefetch)

            # Ensure the queue exists (passive declare to check)
            queue_name = self._config.get("queue", "")
            if queue_name:
                self._channel.queue_declare(queue=queue_name, passive=False, durable=True)

            # Optionally bind to an exchange
            exchange = self._config.get("exchange")
            routing_key = self._config.get("routing_key", "")
            if exchange and queue_name:
                self._channel.exchange_declare(exchange=exchange, exchange_type="topic", durable=True)
                self._channel.queue_bind(
                    queue=queue_name,
                    exchange=exchange,
                    routing_key=routing_key,
                )

            self._connected = True
            logger.info(
                "rabbitmq_source.connected",
                host=params.host,
                port=params.port,
                queue=queue_name,
            )
        except Exception as exc:
            self._connected = False
            logger.error("rabbitmq_source.connect_failed", error=str(exc))
            raise ConnectionError(f"Failed to connect to RabbitMQ: {exc}") from exc

    def close(self) -> None:
        if self._channel is not None:
            try:
                self._channel.close()
            except Exception:
                pass
            self._channel = None

        if self._connection is not None:
            try:
                self._connection.close()
            except Exception:
                pass
            self._connection = None

        self._connected = False
        logger.info("rabbitmq_source.closed")

    def health_check(self) -> HealthStatus:
        """Verify the broker is reachable and the queue exists."""
        try:
            if self._connection is None or self._connection.is_closed:
                self.connect()
            assert self._channel is not None
            queue_name = self._config.get("queue", "")
            if queue_name:
                result = self._channel.queue_declare(queue=queue_name, passive=True)
                msg_count = result.method.message_count
                logger.info(
                    "rabbitmq_source.health_check_ok",
                    message_count=msg_count,
                )
            return HealthStatus.HEALTHY
        except Exception as exc:
            logger.warning("rabbitmq_source.health_check_failed", error=str(exc))
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _delivery_to_record(
        method: Any,
        properties: Any,
        body: bytes | str,
    ) -> Record:
        """Convert a RabbitMQ delivery to a Ranger :class:`Record`."""
        if isinstance(body, bytes):
            try:
                data = json.loads(body.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                data = {"raw": body.decode("utf-8", errors="replace")}
        elif isinstance(body, str):
            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                data = {"raw": body}
        else:
            data = {"raw": str(body)}

        if not isinstance(data, dict):
            data = {"value": data}

        # Extract timestamp from properties
        event_time: datetime | None = None
        if properties and properties.timestamp:
            try:
                event_time = datetime.fromtimestamp(properties.timestamp, tz=timezone.utc)
            except (ValueError, OSError):
                pass

        headers = {}
        if properties and properties.headers:
            headers = {str(k): str(v) for k, v in properties.headers.items()}

        return Record(
            data=data,
            event_time=event_time,
            source_metadata={
                "source_type": "rabbitmq",
                "delivery_tag": method.delivery_tag if method else None,
                "exchange": method.exchange if method else None,
                "routing_key": method.routing_key if method else None,
                "redelivered": method.redelivered if method else None,
                "content_type": properties.content_type if properties else None,
                "message_id": properties.message_id if properties else None,
                "correlation_id": properties.correlation_id if properties else None,
                "headers": headers,
            },
        )

    # ------------------------------------------------------------------
    # Reading
    # ------------------------------------------------------------------

    def read(self) -> Iterator[Record]:
        """Bounded consumption — consume up to *batch_size* messages, then stop.

        Messages are acknowledged after being yielded.

        Yields:
            Record objects, one per RabbitMQ message.
        """
        if self._channel is None:
            raise RuntimeError("Source not connected — call connect() first")

        queue_name = self._config.get("queue", "")
        if not queue_name:
            raise ValueError("Config must include 'queue'")

        batch_size: int = int(self._config.get("batch_size", 1000))

        logger.info(
            "rabbitmq_source.read_start",
            queue=queue_name,
            batch_size=batch_size,
        )

        consumed = 0
        while consumed < batch_size:
            method, properties, body = self._channel.basic_get(
                queue=queue_name,
                auto_ack=False,
            )
            if method is None:
                # Queue is empty
                break

            record = self._delivery_to_record(method, properties, body)
            yield record
            consumed += 1

            # Acknowledge
            self._channel.basic_ack(delivery_tag=method.delivery_tag)

        logger.info("rabbitmq_source.read_complete", messages=consumed)

    async def read_stream(self, config: StreamConfig | None = None) -> AsyncIterator[Record]:
        """Continuous async consumption using a callback-based consumer.

        Runs the pika consumer in a background thread and bridges records
        into the async iterator via a :class:`queue.Queue`.

        Args:
            config: Optional streaming configuration.

        Yields:
            Record objects as they arrive from RabbitMQ.
        """
        if self._connection is None or self._channel is None:
            raise RuntimeError("Source not connected — call connect() first")

        queue_name = self._config.get("queue", "")
        if not queue_name:
            raise ValueError("Config must include 'queue'")

        idle_timeout: float = config.idle_timeout_seconds if config else 300.0

        record_queue: queue.Queue[Record | None] = queue.Queue(maxsize=1000)
        stop_event = threading.Event()

        def _on_message(
            ch: Any,
            method: Any,
            properties: Any,
            body: bytes,
        ) -> None:
            """Push records from the consumer thread into the bridge queue."""
            try:
                record = self._delivery_to_record(method, properties, body)
                record_queue.put(record, timeout=5)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as exc:
                logger.warning("rabbitmq_source.callback_error", error=str(exc))
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        def _consume() -> None:
            """Run the blocking consumer in a background thread."""
            try:
                # Create a fresh connection for the consumer thread
                params = self._build_connection_params()
                conn = pika.BlockingConnection(params)
                ch = conn.channel()
                prefetch = int(self._config.get("prefetch_count", 100))
                ch.basic_qos(prefetch_count=prefetch)
                ch.basic_consume(
                    queue=queue_name,
                    on_message_callback=_on_message,
                    auto_ack=False,
                )

                while not stop_event.is_set():
                    conn.process_data_events(time_limit=1)
            except Exception as exc:
                logger.error("rabbitmq_source.consumer_thread_error", error=str(exc))
            finally:
                record_queue.put(None)
                try:
                    ch.close()
                    conn.close()
                except Exception:
                    pass

        logger.info("rabbitmq_source.stream_start")
        consumer_thread = threading.Thread(target=_consume, daemon=True)
        consumer_thread.start()

        try:
            last_record_time = time.monotonic()

            while True:
                try:
                    record = record_queue.get(timeout=1.0)
                    if record is None:
                        break
                    last_record_time = time.monotonic()
                    yield record
                except queue.Empty:
                    if time.monotonic() - last_record_time > idle_timeout:
                        logger.info("rabbitmq_source.stream_idle_timeout")
                        break
                    await asyncio.sleep(0.01)
        finally:
            stop_event.set()
            consumer_thread.join(timeout=10)

    # ------------------------------------------------------------------
    # Schema operations
    # ------------------------------------------------------------------

    def get_schema(self) -> Schema:
        """Return a generic schema for RabbitMQ message payloads."""
        return Schema(
            columns=[
                ColumnDefinition(name="body", type=ColumnType.JSON, nullable=False),
                ColumnDefinition(name="delivery_tag", type=ColumnType.INT64, nullable=False),
                ColumnDefinition(name="exchange", type=ColumnType.STRING, nullable=True),
                ColumnDefinition(name="routing_key", type=ColumnType.STRING, nullable=True),
                ColumnDefinition(name="message_id", type=ColumnType.STRING, nullable=True),
                ColumnDefinition(name="timestamp", type=ColumnType.TIMESTAMP_TZ, nullable=True),
                ColumnDefinition(name="headers", type=ColumnType.JSON, nullable=True),
            ],
        )

    def discover_schema(self) -> DiscoveredSchema:
        """Discover queue info and return schema metadata."""
        schema = self.get_schema()
        queue_name = self._config.get("queue", "unknown")

        metadata: dict[str, Any] = {}
        if self._channel is not None:
            try:
                result = self._channel.queue_declare(queue=queue_name, passive=True)
                metadata["message_count"] = result.method.message_count
                metadata["consumer_count"] = result.method.consumer_count
            except Exception:
                pass

        return DiscoveredSchema(
            columns=schema.columns,
            primary_key=None,
            source_name="rabbitmq",
            object_name=queue_name,
            object_type="queue",
            source_metadata=metadata,
        )
