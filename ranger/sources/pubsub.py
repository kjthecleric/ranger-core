"""Google Cloud Pub/Sub source connector.

Reads messages from a Pub/Sub subscription using synchronous pull and
asynchronous streaming pull modes.
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
    from google.api_core.exceptions import GoogleAPIError, NotFound
    from google.cloud import pubsub_v1
except ImportError as _ps_err:
    raise ImportError(
        "google-cloud-pubsub is required for PubSubSource. "
        "Install it with: pip install ranger-core[pubsub]"
    ) from _ps_err


class PubSubSource(BaseSource):
    """Read messages from a Google Cloud Pub/Sub subscription.

    Config keys:
        project_id: GCP project identifier.
        subscription_id: Pub/Sub subscription name.
        max_messages: Maximum messages per synchronous pull
            (default: ``100``).
        ack_deadline: Ack deadline extension in seconds (default: ``60``).
        timeout: Pull request timeout in seconds (default: ``30``).
        credentials_path: Optional path to a service-account JSON key file.
    """

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._subscriber: pubsub_v1.SubscriberClient | None = None
        self._subscription_path: str = ""

    # ------------------------------------------------------------------
    # Metadata properties
    # ------------------------------------------------------------------

    @property
    def source_type(self) -> str:  # noqa: D401
        return "pubsub"

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
            credentials_path = self._config.get("credentials_path")
            if credentials_path:
                from google.oauth2 import service_account

                credentials = service_account.Credentials.from_service_account_file(
                    credentials_path,
                )
                self._subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
            else:
                self._subscriber = pubsub_v1.SubscriberClient()

            project_id = self._config["project_id"]
            subscription_id = self._config["subscription_id"]
            self._subscription_path = self._subscriber.subscription_path(
                project_id, subscription_id,
            )

            self._connected = True
            logger.info(
                "pubsub_source.connected",
                project=project_id,
                subscription=subscription_id,
            )
        except Exception as exc:
            self._connected = False
            logger.error("pubsub_source.connect_failed", error=str(exc))
            raise ConnectionError(f"Failed to connect to Pub/Sub: {exc}") from exc

    def close(self) -> None:
        if self._subscriber is not None:
            try:
                self._subscriber.close()
            except Exception:
                pass
            self._subscriber = None
        self._connected = False
        logger.info("pubsub_source.closed")

    def health_check(self) -> HealthStatus:
        """Verify the subscription exists and is reachable."""
        try:
            if self._subscriber is None:
                self.connect()
            assert self._subscriber is not None
            self._subscriber.get_subscription(
                request={"subscription": self._subscription_path},
            )
            logger.info("pubsub_source.health_check_ok")
            return HealthStatus.HEALTHY
        except NotFound:
            logger.warning("pubsub_source.subscription_not_found")
            return HealthStatus.UNHEALTHY
        except Exception as exc:
            logger.warning("pubsub_source.health_check_failed", error=str(exc))
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _message_to_record(message: Any) -> Record:
        """Convert a Pub/Sub message to a Ranger :class:`Record`."""
        raw_data = message.data
        if isinstance(raw_data, bytes):
            try:
                data = json.loads(raw_data.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                data = {"raw": raw_data.decode("utf-8", errors="replace")}
        elif isinstance(raw_data, str):
            try:
                data = json.loads(raw_data)
            except json.JSONDecodeError:
                data = {"raw": raw_data}
        else:
            data = {"raw": str(raw_data)}

        if not isinstance(data, dict):
            data = {"value": data}

        event_time: datetime | None = None
        publish_time = getattr(message, "publish_time", None)
        if publish_time is not None:
            if isinstance(publish_time, datetime):
                event_time = publish_time if publish_time.tzinfo else publish_time.replace(tzinfo=timezone.utc)
            elif hasattr(publish_time, "timestamp"):
                event_time = datetime.fromtimestamp(publish_time.timestamp(), tz=timezone.utc)

        attributes = dict(message.attributes) if message.attributes else {}

        return Record(
            data=data,
            event_time=event_time,
            source_metadata={
                "source_type": "pubsub",
                "message_id": message.message_id,
                "attributes": attributes,
                "ordering_key": getattr(message, "ordering_key", None),
            },
        )

    # ------------------------------------------------------------------
    # Reading
    # ------------------------------------------------------------------

    def read(self) -> Iterator[Record]:
        """Synchronous pull — fetch up to *max_messages*, ack, and yield.

        Yields:
            Record objects, one per Pub/Sub message.
        """
        if self._subscriber is None:
            raise RuntimeError("Source not connected — call connect() first")

        max_messages: int = self._config.get("max_messages", 100)
        timeout: float = self._config.get("timeout", 30)

        logger.info(
            "pubsub_source.read_start",
            max_messages=max_messages,
            subscription=self._subscription_path,
        )

        try:
            response = self._subscriber.pull(
                request={
                    "subscription": self._subscription_path,
                    "max_messages": max_messages,
                },
                timeout=timeout,
            )
        except Exception as exc:
            logger.error("pubsub_source.pull_failed", error=str(exc))
            raise

        received = response.received_messages
        ack_ids: list[str] = []
        count = 0

        for received_msg in received:
            record = self._message_to_record(received_msg.message)
            ack_ids.append(received_msg.ack_id)
            yield record
            count += 1

        # Acknowledge all successfully yielded messages
        if ack_ids:
            try:
                self._subscriber.acknowledge(
                    request={
                        "subscription": self._subscription_path,
                        "ack_ids": ack_ids,
                    },
                )
            except Exception as exc:
                logger.warning("pubsub_source.ack_failed", error=str(exc))

        logger.info("pubsub_source.read_complete", messages=count)

    async def read_stream(self, config: StreamConfig | None = None) -> AsyncIterator[Record]:
        """Asynchronous streaming pull using Pub/Sub's callback mechanism.

        Internally runs a streaming-pull future in a background thread and
        feeds records through a :class:`queue.Queue` bridge into the async
        iterator.

        Args:
            config: Optional streaming configuration.

        Yields:
            Record objects as they arrive from Pub/Sub.
        """
        if self._subscriber is None:
            raise RuntimeError("Source not connected — call connect() first")

        idle_timeout: float = config.idle_timeout_seconds if config else 300.0
        ack_deadline: int = self._config.get("ack_deadline", 60)

        record_queue: queue.Queue[Record | None] = queue.Queue(maxsize=1000)
        streaming_future: Any = None

        def _callback(message: Any) -> None:
            """Push records into the bridge queue from the subscriber thread."""
            try:
                record = self._message_to_record(message)
                record_queue.put(record, timeout=5)
                message.ack()
            except Exception as exc:
                logger.warning("pubsub_source.callback_error", error=str(exc))
                message.nack()

        logger.info("pubsub_source.stream_start")

        flow_control = pubsub_v1.types.FlowControl(
            max_messages=self._config.get("max_messages", 100),
        )

        streaming_future = self._subscriber.subscribe(
            self._subscription_path,
            callback=_callback,
            flow_control=flow_control,
            await_callbacks_on_shutdown=False,
        )

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
                        logger.info("pubsub_source.stream_idle_timeout")
                        break
                    await asyncio.sleep(0.01)
        finally:
            if streaming_future is not None:
                streaming_future.cancel()
                try:
                    streaming_future.result(timeout=5)
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Schema operations
    # ------------------------------------------------------------------

    def get_schema(self) -> Schema:
        """Return a generic schema for Pub/Sub message payloads."""
        return Schema(
            columns=[
                ColumnDefinition(name="data", type=ColumnType.JSON, nullable=False),
                ColumnDefinition(name="message_id", type=ColumnType.STRING, nullable=False),
                ColumnDefinition(name="publish_time", type=ColumnType.TIMESTAMP_TZ, nullable=True),
                ColumnDefinition(name="attributes", type=ColumnType.JSON, nullable=True),
                ColumnDefinition(name="ordering_key", type=ColumnType.STRING, nullable=True),
            ],
        )

    def discover_schema(self) -> DiscoveredSchema:
        """Discover subscription info and return schema metadata."""
        schema = self.get_schema()
        subscription_id = self._config.get("subscription_id", "unknown")

        metadata: dict[str, Any] = {}
        if self._subscriber is not None:
            try:
                sub = self._subscriber.get_subscription(
                    request={"subscription": self._subscription_path},
                )
                metadata["topic"] = sub.topic
                metadata["ack_deadline_seconds"] = sub.ack_deadline_seconds
                metadata["retain_acked_messages"] = sub.retain_acked_messages
            except Exception:
                pass

        return DiscoveredSchema(
            columns=schema.columns,
            primary_key=["message_id"],
            source_name="pubsub",
            object_name=subscription_id,
            object_type="subscription",
            source_metadata=metadata,
        )
