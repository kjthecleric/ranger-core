"""RabbitMQ sink — publishes records as messages via AMQP.

Uses pika for connectivity with configurable exchange, routing key, queue
declaration/binding, and persistent/transient delivery modes.
"""

from __future__ import annotations

import json
from typing import Any

import structlog

from ranger.core.models import HealthStatus, Record, Schema
from ranger.sinks.base import BaseSink

try:
    import pika
    from pika import BasicProperties
    from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection

    _HAS_PIKA = True
except ImportError:  # pragma: no cover
    _HAS_PIKA = False

logger = structlog.get_logger(__name__)


class RabbitMQSink(BaseSink):
    """Publish records as messages to a RabbitMQ exchange.

    Config keys:
        host: RabbitMQ hostname (required).
        port: RabbitMQ port (default ``5672``).
        virtual_host: Virtual host (default ``/``).
        exchange: Exchange name to publish to (required).
        routing_key: Routing key for published messages (required).
        queue: Optional queue name — if provided the sink will declare the
            queue and bind it to the exchange with the routing key.
        username: Authentication username (default ``guest``).
        password: Authentication password (default ``guest``).
        delivery_mode: ``1`` (transient) or ``2`` (persistent) (default ``2``).
        content_type: MIME content type for messages
            (default ``application/json``).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        if not _HAS_PIKA:
            raise ImportError(
                "pika is required for RabbitMQSink. "
                "Install it with: pip install pika"
            )
        super().__init__(config)

        self._host: str = config["host"]
        self._port: int = int(config.get("port", 5672))
        self._virtual_host: str = config.get("virtual_host", "/")
        self._exchange: str = config["exchange"]
        self._routing_key: str = config["routing_key"]
        self._queue: str | None = config.get("queue")
        self._username: str = config.get("username", "guest")
        self._password: str = config.get("password", "guest")
        self._delivery_mode: int = int(config.get("delivery_mode", 2))
        self._content_type: str = config.get("content_type", "application/json")

        self._connection: BlockingConnection | None = None
        self._channel: BlockingChannel | None = None
        self._schema: Schema | None = None

    @property
    def sink_type(self) -> str:
        return "rabbitmq"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def open(self, schema: Schema) -> None:
        self._schema = schema

        credentials = pika.PlainCredentials(self._username, self._password)
        params = pika.ConnectionParameters(
            host=self._host,
            port=self._port,
            virtual_host=self._virtual_host,
            credentials=credentials,
        )

        self._connection = pika.BlockingConnection(params)
        self._channel = self._connection.channel()

        # Declare exchange (idempotent)
        self._channel.exchange_declare(
            exchange=self._exchange,
            exchange_type="topic",
            durable=True,
        )

        # Optionally declare queue and bind
        if self._queue:
            self._channel.queue_declare(queue=self._queue, durable=True)
            self._channel.queue_bind(
                queue=self._queue,
                exchange=self._exchange,
                routing_key=self._routing_key,
            )
            logger.info(
                "rabbitmq_sink.queue_bound",
                queue=self._queue,
                exchange=self._exchange,
                routing_key=self._routing_key,
            )

        self._opened = True
        logger.info(
            "rabbitmq_sink.opened",
            host=self._host,
            exchange=self._exchange,
            routing_key=self._routing_key,
        )

    def close(self) -> None:
        if self._channel is not None:
            try:
                self._channel.close()
            except Exception:
                logger.warning("rabbitmq_sink.channel_close_error", exc_info=True)
            self._channel = None

        if self._connection is not None:
            try:
                self._connection.close()
            except Exception:
                logger.warning("rabbitmq_sink.connection_close_error", exc_info=True)
            self._connection = None

        self._opened = False
        logger.info("rabbitmq_sink.closed", host=self._host)

    def health_check(self) -> HealthStatus:
        try:
            if self._connection is None or self._connection.is_closed:
                return HealthStatus.UNHEALTHY
            if self._channel is None or self._channel.is_closed:
                return HealthStatus.UNHEALTHY
            return HealthStatus.HEALTHY
        except Exception:
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Writing
    # ------------------------------------------------------------------

    def write_batch(self, records: list[Record]) -> int:
        if not self._opened or self._channel is None:
            raise RuntimeError("Sink not opened. Call open() first.")
        if not records:
            return 0

        properties = BasicProperties(
            delivery_mode=self._delivery_mode,
            content_type=self._content_type,
        )

        written = 0
        for rec in records:
            body = json.dumps(rec.data, default=str).encode("utf-8")
            try:
                self._channel.basic_publish(
                    exchange=self._exchange,
                    routing_key=self._routing_key,
                    body=body,
                    properties=properties,
                )
                written += 1
            except Exception:
                logger.warning(
                    "rabbitmq_sink.publish_error",
                    exchange=self._exchange,
                    routing_key=self._routing_key,
                    exc_info=True,
                )

        logger.info(
            "rabbitmq_sink.batch_written",
            count=written,
            exchange=self._exchange,
            routing_key=self._routing_key,
        )
        return written

    # ------------------------------------------------------------------
    # Schema evolution
    # ------------------------------------------------------------------

    def evolve_schema(self, new_schema: Schema) -> None:
        """No-op — RabbitMQ messages are opaque payloads."""
        self._schema = new_schema
        logger.debug("rabbitmq_sink.evolve_schema_noop", exchange=self._exchange)

    def supports_schema_evolution(self) -> bool:
        return False
