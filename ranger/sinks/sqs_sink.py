"""Amazon SQS sink — sends records as messages to an SQS queue.

Uses boto3 SQS client with ``send_message_batch()`` for efficient delivery,
respecting the SQS maximum batch size of 10 messages per API call.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any

import structlog

from ranger.core.models import HealthStatus, Record, Schema
from ranger.sinks.base import BaseSink

try:
    import boto3
    from botocore.exceptions import ClientError

    _HAS_BOTO3 = True
except ImportError:  # pragma: no cover
    _HAS_BOTO3 = False

logger = structlog.get_logger(__name__)

# SQS hard limit for batch operations
_SQS_MAX_BATCH = 10


class SQSSink(BaseSink):
    """Send records as messages to an Amazon SQS queue.

    Config keys:
        queue_url: Full SQS queue URL (required).
        region: AWS region name (optional — uses default if omitted).
        message_group_id: Message group ID for FIFO queues (optional).
        dedup_field: Record field whose value is used as the deduplication ID
            for FIFO queues (optional).
        delay_seconds: Per-message delivery delay in seconds, 0–900
            (default ``0``).
        batch_size: Logical batch size hint — actual SQS batches are capped
            at 10 (default ``10``).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        if not _HAS_BOTO3:
            raise ImportError(
                "boto3 is required for SQSSink. "
                "Install it with: pip install boto3"
            )
        super().__init__(config)

        self._queue_url: str = config["queue_url"]
        self._region: str | None = config.get("region")
        self._message_group_id: str | None = config.get("message_group_id")
        self._dedup_field: str | None = config.get("dedup_field")
        self._delay_seconds: int = int(config.get("delay_seconds", 0))
        self._batch_size: int = min(int(config.get("batch_size", 10)), _SQS_MAX_BATCH)

        self._client: Any = None  # boto3 SQS client
        self._schema: Schema | None = None
        self._is_fifo: bool = self._queue_url.endswith(".fifo")

    @property
    def sink_type(self) -> str:
        return "sqs"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def open(self, schema: Schema) -> None:
        self._schema = schema

        client_kwargs: dict[str, Any] = {}
        if self._region:
            client_kwargs["region_name"] = self._region

        self._client = boto3.client("sqs", **client_kwargs)
        self._opened = True
        logger.info(
            "sqs_sink.opened",
            queue_url=self._queue_url,
            is_fifo=self._is_fifo,
        )

    def close(self) -> None:
        # boto3 clients don't need explicit close, but reset state
        self._client = None
        self._opened = False
        logger.info("sqs_sink.closed", queue_url=self._queue_url)

    def health_check(self) -> HealthStatus:
        try:
            if self._client is None:
                return HealthStatus.UNHEALTHY
            self._client.get_queue_attributes(
                QueueUrl=self._queue_url,
                AttributeNames=["ApproximateNumberOfMessages"],
            )
            return HealthStatus.HEALTHY
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

        # Process in chunks of up to SQS_MAX_BATCH (10)
        for i in range(0, len(records), _SQS_MAX_BATCH):
            chunk = records[i : i + _SQS_MAX_BATCH]
            entries = self._build_batch_entries(chunk)

            try:
                response = self._client.send_message_batch(
                    QueueUrl=self._queue_url,
                    Entries=entries,
                )

                successful = response.get("Successful", [])
                failed = response.get("Failed", [])
                written += len(successful)

                if failed:
                    logger.warning(
                        "sqs_sink.batch_partial_failure",
                        failed_count=len(failed),
                        sample=failed[:3],
                    )
            except ClientError as exc:
                logger.error(
                    "sqs_sink.send_batch_error",
                    error=str(exc),
                    batch_index=i,
                )

        logger.info("sqs_sink.batch_written", count=written, queue_url=self._queue_url)
        return written

    def _build_batch_entries(self, records: list[Record]) -> list[dict[str, Any]]:
        """Build SQS SendMessageBatch entries from records."""
        entries: list[dict[str, Any]] = []
        for idx, rec in enumerate(records):
            body = json.dumps(rec.data, default=str)
            entry: dict[str, Any] = {
                "Id": str(idx),
                "MessageBody": body,
            }

            if self._delay_seconds > 0:
                entry["DelaySeconds"] = self._delay_seconds

            # FIFO-specific attributes
            if self._is_fifo:
                if self._message_group_id:
                    entry["MessageGroupId"] = self._message_group_id
                else:
                    entry["MessageGroupId"] = "default"

                if self._dedup_field and self._dedup_field in rec.data:
                    dedup_value = str(rec.data[self._dedup_field])
                    entry["MessageDeduplicationId"] = hashlib.sha256(
                        dedup_value.encode()
                    ).hexdigest()
                else:
                    # Generate a unique dedup ID from the message body
                    entry["MessageDeduplicationId"] = hashlib.sha256(
                        body.encode()
                    ).hexdigest()

            entries.append(entry)
        return entries

    # ------------------------------------------------------------------
    # Schema evolution
    # ------------------------------------------------------------------

    def evolve_schema(self, new_schema: Schema) -> None:
        """No-op — SQS messages are opaque payloads with no schema enforcement."""
        self._schema = new_schema
        logger.debug("sqs_sink.evolve_schema_noop", queue_url=self._queue_url)

    def supports_schema_evolution(self) -> bool:
        return False
