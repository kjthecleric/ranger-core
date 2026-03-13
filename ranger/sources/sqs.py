"""Amazon SQS source connector.

Reads messages from an SQS queue using the boto3 client. Supports long-polling
and optional automatic deletion of consumed messages.
"""

from __future__ import annotations

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
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError as _boto_err:
    raise ImportError(
        "boto3 is required for SQSSource. "
        "Install it with: pip install ranger-core[sqs]"
    ) from _boto_err


class SQSSource(BaseSource):
    """Read messages from an Amazon SQS queue.

    Config keys:
        queue_url: Full URL of the SQS queue.
        region: AWS region (default: ``"us-east-1"``).
        max_messages: Messages per ``receive_message()`` call
            (1–10, default: ``10``).
        wait_time_seconds: Long-poll duration in seconds
            (0–20, default: ``20``).
        visibility_timeout: Seconds a message is hidden from other
            consumers after receipt (default: ``30``).
        delete_after_read: Whether to delete messages once yielded
            (default: ``True``).
        max_batches: Maximum number of receive cycles for bounded
            ``read()`` (default: ``10``).  Set ``0`` for unlimited in
            streaming mode.
        aws_access_key_id: Optional explicit AWS access key.
        aws_secret_access_key: Optional explicit AWS secret key.
        endpoint_url: Optional custom endpoint (for LocalStack, etc.).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._client: Any | None = None  # boto3 SQS client

    # ------------------------------------------------------------------
    # Metadata properties
    # ------------------------------------------------------------------

    @property
    def source_type(self) -> str:  # noqa: D401
        return "sqs"

    @property
    def supports_streaming(self) -> bool:
        return True

    @property
    def supports_incremental(self) -> bool:
        return False

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        try:
            region = self._config.get("region", "us-east-1")
            kwargs: dict[str, Any] = {"region_name": region}

            access_key = self._config.get("aws_access_key_id")
            secret_key = self._config.get("aws_secret_access_key")
            if access_key and secret_key:
                kwargs["aws_access_key_id"] = access_key
                kwargs["aws_secret_access_key"] = secret_key

            endpoint_url = self._config.get("endpoint_url")
            if endpoint_url:
                kwargs["endpoint_url"] = endpoint_url

            self._client = boto3.client("sqs", **kwargs)
            self._connected = True
            logger.info(
                "sqs_source.connected",
                queue_url=self._config.get("queue_url"),
                region=region,
            )
        except Exception as exc:
            self._connected = False
            logger.error("sqs_source.connect_failed", error=str(exc))
            raise ConnectionError(f"Failed to connect to SQS: {exc}") from exc

    def close(self) -> None:
        self._client = None
        self._connected = False
        logger.info("sqs_source.closed")

    def health_check(self) -> HealthStatus:
        """Get queue attributes to verify connectivity."""
        try:
            if self._client is None:
                self.connect()
            assert self._client is not None
            queue_url = self._config.get("queue_url", "")
            resp = self._client.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=["ApproximateNumberOfMessages"],
            )
            approx = resp.get("Attributes", {}).get("ApproximateNumberOfMessages", "0")
            logger.info("sqs_source.health_check_ok", approx_messages=approx)
            return HealthStatus.HEALTHY
        except Exception as exc:
            logger.warning("sqs_source.health_check_failed", error=str(exc))
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _sqs_message_to_record(msg: dict[str, Any]) -> Record:
        """Convert an SQS message dict to a Ranger :class:`Record`."""
        body = msg.get("Body", "")
        try:
            data = json.loads(body)
        except (json.JSONDecodeError, TypeError):
            data = {"raw": body}

        if not isinstance(data, dict):
            data = {"value": data}

        # SQS provides SentTimestamp in epoch-ms
        event_time: datetime | None = None
        attrs = msg.get("Attributes", {})
        sent_ts = attrs.get("SentTimestamp")
        if sent_ts:
            try:
                event_time = datetime.fromtimestamp(int(sent_ts) / 1000.0, tz=timezone.utc)
            except (ValueError, OSError):
                pass

        return Record(
            data=data,
            event_time=event_time,
            source_metadata={
                "source_type": "sqs",
                "message_id": msg.get("MessageId"),
                "receipt_handle": msg.get("ReceiptHandle"),
                "md5_of_body": msg.get("MD5OfBody"),
                "attributes": attrs,
                "message_attributes": msg.get("MessageAttributes", {}),
            },
        )

    def _delete_message(self, receipt_handle: str) -> None:
        """Delete a single message from the queue."""
        assert self._client is not None
        queue_url = self._config["queue_url"]
        try:
            self._client.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle,
            )
        except Exception as exc:
            logger.warning("sqs_source.delete_failed", error=str(exc))

    # ------------------------------------------------------------------
    # Reading
    # ------------------------------------------------------------------

    def read(self) -> Iterator[Record]:
        """Bounded poll — receive messages for up to *max_batches* cycles.

        Yields:
            Record objects, one per SQS message.
        """
        if self._client is None:
            raise RuntimeError("Source not connected — call connect() first")

        queue_url = self._config["queue_url"]
        max_messages: int = min(max(int(self._config.get("max_messages", 10)), 1), 10)
        wait_time: int = int(self._config.get("wait_time_seconds", 20))
        visibility_timeout: int = int(self._config.get("visibility_timeout", 30))
        delete_after: bool = self._config.get("delete_after_read", True)
        max_batches: int = int(self._config.get("max_batches", 10))

        logger.info(
            "sqs_source.read_start",
            queue_url=queue_url,
            max_messages=max_messages,
            max_batches=max_batches,
        )

        total_consumed = 0
        batch_num = 0

        while batch_num < max_batches:
            batch_num += 1
            try:
                resp = self._client.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=max_messages,
                    WaitTimeSeconds=wait_time,
                    VisibilityTimeout=visibility_timeout,
                    AttributeNames=["All"],
                    MessageAttributeNames=["All"],
                )
            except (BotoCoreError, ClientError) as exc:
                logger.error("sqs_source.receive_failed", error=str(exc))
                raise

            messages = resp.get("Messages", [])
            if not messages:
                # Queue is empty — stop bounded read
                break

            for msg in messages:
                record = self._sqs_message_to_record(msg)
                yield record
                total_consumed += 1

                if delete_after:
                    receipt_handle = msg.get("ReceiptHandle")
                    if receipt_handle:
                        self._delete_message(receipt_handle)

        logger.info("sqs_source.read_complete", messages=total_consumed)

    async def read_stream(self, config: StreamConfig | None = None) -> AsyncIterator[Record]:
        """Continuous async polling of the SQS queue.

        Args:
            config: Optional streaming configuration.

        Yields:
            Record objects as they arrive.
        """
        if self._client is None:
            raise RuntimeError("Source not connected — call connect() first")

        queue_url = self._config["queue_url"]
        max_messages: int = min(max(int(self._config.get("max_messages", 10)), 1), 10)
        wait_time: int = int(self._config.get("wait_time_seconds", 20))
        visibility_timeout: int = int(self._config.get("visibility_timeout", 30))
        delete_after: bool = self._config.get("delete_after_read", True)
        idle_timeout: float = config.idle_timeout_seconds if config else 300.0

        logger.info("sqs_source.stream_start")
        last_message_time = time.monotonic()

        while True:
            if time.monotonic() - last_message_time > idle_timeout:
                logger.info("sqs_source.stream_idle_timeout")
                break

            try:
                resp = self._client.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=max_messages,
                    WaitTimeSeconds=wait_time,
                    VisibilityTimeout=visibility_timeout,
                    AttributeNames=["All"],
                    MessageAttributeNames=["All"],
                )
            except (BotoCoreError, ClientError) as exc:
                logger.warning("sqs_source.receive_error", error=str(exc))
                await asyncio.sleep(5)
                continue

            messages = resp.get("Messages", [])
            if not messages:
                await asyncio.sleep(0.1)
                continue

            last_message_time = time.monotonic()

            for msg in messages:
                record = self._sqs_message_to_record(msg)
                yield record

                if delete_after:
                    receipt_handle = msg.get("ReceiptHandle")
                    if receipt_handle:
                        self._delete_message(receipt_handle)

    # ------------------------------------------------------------------
    # Schema operations
    # ------------------------------------------------------------------

    def get_schema(self) -> Schema:
        """Return a generic schema for SQS message payloads."""
        return Schema(
            columns=[
                ColumnDefinition(name="body", type=ColumnType.JSON, nullable=False),
                ColumnDefinition(name="message_id", type=ColumnType.STRING, nullable=False),
                ColumnDefinition(name="receipt_handle", type=ColumnType.STRING, nullable=False),
                ColumnDefinition(name="sent_timestamp", type=ColumnType.TIMESTAMP_TZ, nullable=True),
                ColumnDefinition(name="attributes", type=ColumnType.JSON, nullable=True),
                ColumnDefinition(name="message_attributes", type=ColumnType.JSON, nullable=True),
            ],
        )

    def discover_schema(self) -> DiscoveredSchema:
        """Discover queue info and return schema metadata."""
        schema = self.get_schema()
        queue_url = self._config.get("queue_url", "unknown")

        metadata: dict[str, Any] = {}
        if self._client is not None:
            try:
                resp = self._client.get_queue_attributes(
                    QueueUrl=queue_url,
                    AttributeNames=["All"],
                )
                attrs = resp.get("Attributes", {})
                metadata["approximate_message_count"] = attrs.get("ApproximateNumberOfMessages")
                metadata["approximate_not_visible"] = attrs.get(
                    "ApproximateNumberOfMessagesNotVisible",
                )
                metadata["visibility_timeout"] = attrs.get("VisibilityTimeout")
                metadata["created_timestamp"] = attrs.get("CreatedTimestamp")
            except Exception:
                pass

        return DiscoveredSchema(
            columns=schema.columns,
            primary_key=["message_id"],
            source_name="sqs",
            object_name=queue_url.rsplit("/", 1)[-1] if "/" in queue_url else queue_url,
            object_type="queue",
            source_metadata=metadata,
        )
