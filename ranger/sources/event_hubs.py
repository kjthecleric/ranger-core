"""Azure Event Hubs source connector.

Reads events from an Azure Event Hub using the azure-eventhub SDK.
Supports both bounded (batch) reads and continuous streaming with
checkpointing.
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
    from azure.eventhub import EventHubConsumerClient, EventHubProducerClient
    from azure.eventhub.exceptions import EventHubError
except ImportError as _eh_err:
    raise ImportError(
        "azure-eventhub is required for EventHubsSource. "
        "Install it with: pip install ranger-core[event-hubs]"
    ) from _eh_err


class EventHubsSource(BaseSource):
    """Read events from an Azure Event Hub.

    Config keys:
        connection_string: Full Event Hub connection string including
            the ``EntityPath`` or used in conjunction with *eventhub_name*.
        eventhub_name: Event Hub name (can also be embedded in the
            connection string).
        consumer_group: Consumer group (default: ``"$Default"``).
        starting_position: Where to start reading — ``"earliest"`` or
            ``"latest"`` (default: ``"earliest"``).  Can also be a dict
            mapping partition IDs to positions.
        batch_size: Maximum events per ``receive_batch()`` call
            (default: ``100``).
        max_wait_time: Seconds to wait for a batch before returning
            partial results (default: ``5``).
        partition_id: Optional specific partition to read from.  If
            omitted, all partitions are consumed.
    """

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._consumer: EventHubConsumerClient | None = None

    # ------------------------------------------------------------------
    # Metadata properties
    # ------------------------------------------------------------------

    @property
    def source_type(self) -> str:  # noqa: D401
        return "event_hubs"

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
            conn_str = self._config.get("connection_string", "")
            eventhub_name = self._config.get("eventhub_name")
            consumer_group = self._config.get("consumer_group", "$Default")

            kwargs: dict[str, Any] = {
                "consumer_group": consumer_group,
            }
            if eventhub_name:
                kwargs["eventhub_name"] = eventhub_name

            self._consumer = EventHubConsumerClient.from_connection_string(
                conn_str,
                **kwargs,
            )

            self._connected = True
            logger.info(
                "event_hubs_source.connected",
                eventhub=eventhub_name,
                consumer_group=consumer_group,
            )
        except Exception as exc:
            self._connected = False
            logger.error("event_hubs_source.connect_failed", error=str(exc))
            raise ConnectionError(f"Failed to connect to Event Hubs: {exc}") from exc

    def close(self) -> None:
        if self._consumer is not None:
            try:
                self._consumer.close()
            except Exception:
                pass
            self._consumer = None
        self._connected = False
        logger.info("event_hubs_source.closed")

    def health_check(self) -> HealthStatus:
        """Get Event Hub properties to verify connectivity."""
        try:
            if self._consumer is None:
                self.connect()
            assert self._consumer is not None
            props = self._consumer.get_eventhub_properties()
            logger.info(
                "event_hubs_source.health_check_ok",
                eventhub=props.get("eventhub_name"),
                partition_count=len(props.get("partition_ids", [])),
            )
            return HealthStatus.HEALTHY
        except Exception as exc:
            logger.warning("event_hubs_source.health_check_failed", error=str(exc))
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_starting_position(self) -> str | dict[str, str]:
        """Resolve the starting position from config."""
        pos = self._config.get("starting_position", "earliest")
        if pos == "earliest":
            return "-1"  # Azure SDK convention for earliest
        if pos == "latest":
            return "@latest"
        # Allow pass-through of raw position values
        return str(pos)

    @staticmethod
    def _event_to_record(event: Any, partition_id: str | None = None) -> Record:
        """Convert an Azure EventData object to a Ranger :class:`Record`."""
        raw_body = event.body_as_str()
        try:
            data = json.loads(raw_body)
        except (json.JSONDecodeError, TypeError):
            data = {"raw": raw_body}

        if not isinstance(data, dict):
            data = {"value": data}

        event_time: datetime | None = None
        enqueued = event.enqueued_time
        if enqueued is not None:
            if isinstance(enqueued, datetime):
                event_time = enqueued if enqueued.tzinfo else enqueued.replace(tzinfo=timezone.utc)

        properties = {}
        if event.properties:
            properties = {str(k): str(v) for k, v in event.properties.items()}

        system_props: dict[str, Any] = {}
        if hasattr(event, "system_properties") and event.system_properties:
            for k, v in event.system_properties.items():
                system_props[str(k)] = str(v)

        return Record(
            data=data,
            event_time=event_time,
            source_metadata={
                "source_type": "event_hubs",
                "partition_id": partition_id,
                "offset": event.offset,
                "sequence_number": event.sequence_number,
                "enqueued_time": str(enqueued) if enqueued else None,
                "partition_key": event.partition_key,
                "properties": properties,
                "system_properties": system_props,
            },
        )

    # ------------------------------------------------------------------
    # Reading
    # ------------------------------------------------------------------

    def read(self) -> Iterator[Record]:
        """Bounded read — receive events in batches across partitions.

        Iterates through each partition (or a specific partition if
        configured), calling ``receive_batch()`` to pull events.

        Yields:
            Record objects, one per Event Hub event.
        """
        if self._consumer is None:
            raise RuntimeError("Source not connected — call connect() first")

        batch_size: int = int(self._config.get("batch_size", 100))
        max_wait: float = float(self._config.get("max_wait_time", 5))
        starting_position = self._get_starting_position()
        partition_id = self._config.get("partition_id")

        logger.info(
            "event_hubs_source.read_start",
            batch_size=batch_size,
            starting_position=starting_position,
        )

        total_consumed = 0

        if partition_id is not None:
            # Read from a single partition
            partition_ids = [str(partition_id)]
        else:
            # Read from all partitions
            try:
                props = self._consumer.get_eventhub_properties()
                partition_ids = props["partition_ids"]
            except Exception as exc:
                logger.error("event_hubs_source.get_partitions_failed", error=str(exc))
                raise

        for pid in partition_ids:
            try:
                events = self._consumer.receive_batch(
                    partition_id=pid,
                    max_batch_size=batch_size,
                    max_wait_time=max_wait,
                    starting_position=starting_position,
                )

                for event in events:
                    yield self._event_to_record(event, partition_id=pid)
                    total_consumed += 1

            except EventHubError as exc:
                logger.warning(
                    "event_hubs_source.receive_error",
                    partition=pid,
                    error=str(exc),
                )
                continue

        logger.info("event_hubs_source.read_complete", events=total_consumed)

    async def read_stream(self, config: StreamConfig | None = None) -> AsyncIterator[Record]:
        """Continuous async consumption with per-partition polling.

        Args:
            config: Optional streaming configuration.

        Yields:
            Record objects as they arrive from Event Hubs.
        """
        if self._consumer is None:
            raise RuntimeError("Source not connected — call connect() first")

        batch_size: int = int(self._config.get("batch_size", 100))
        max_wait: float = float(self._config.get("max_wait_time", 5))
        idle_timeout: float = config.idle_timeout_seconds if config else 300.0
        starting_position = self._get_starting_position()
        partition_id = self._config.get("partition_id")

        logger.info("event_hubs_source.stream_start")

        if partition_id is not None:
            partition_ids = [str(partition_id)]
        else:
            try:
                props = self._consumer.get_eventhub_properties()
                partition_ids = props["partition_ids"]
            except Exception as exc:
                logger.error("event_hubs_source.get_partitions_failed", error=str(exc))
                raise

        # Track per-partition positions for incremental advancement
        positions: dict[str, str] = {pid: starting_position for pid in partition_ids}
        last_event_time = time.monotonic()

        while True:
            if time.monotonic() - last_event_time > idle_timeout:
                logger.info("event_hubs_source.stream_idle_timeout")
                break

            received_any = False

            for pid in partition_ids:
                try:
                    events = self._consumer.receive_batch(
                        partition_id=pid,
                        max_batch_size=batch_size,
                        max_wait_time=max_wait,
                        starting_position=positions[pid],
                    )

                    for event in events:
                        received_any = True
                        last_event_time = time.monotonic()
                        # Advance checkpoint for this partition
                        if event.offset is not None:
                            positions[pid] = event.offset
                        yield self._event_to_record(event, partition_id=pid)

                except EventHubError as exc:
                    logger.warning(
                        "event_hubs_source.stream_receive_error",
                        partition=pid,
                        error=str(exc),
                    )

            if not received_any:
                await asyncio.sleep(1.0)

    # ------------------------------------------------------------------
    # Schema operations
    # ------------------------------------------------------------------

    def get_schema(self) -> Schema:
        """Return a generic schema for Event Hub event payloads."""
        return Schema(
            columns=[
                ColumnDefinition(name="body", type=ColumnType.JSON, nullable=False),
                ColumnDefinition(name="partition_id", type=ColumnType.STRING, nullable=False),
                ColumnDefinition(name="offset", type=ColumnType.STRING, nullable=True),
                ColumnDefinition(name="sequence_number", type=ColumnType.INT64, nullable=True),
                ColumnDefinition(name="enqueued_time", type=ColumnType.TIMESTAMP_TZ, nullable=True),
                ColumnDefinition(name="partition_key", type=ColumnType.STRING, nullable=True),
                ColumnDefinition(name="properties", type=ColumnType.JSON, nullable=True),
            ],
        )

    def discover_schema(self) -> DiscoveredSchema:
        """Discover Event Hub info and return schema metadata."""
        schema = self.get_schema()
        eventhub_name = self._config.get("eventhub_name", "unknown")

        metadata: dict[str, Any] = {}
        if self._consumer is not None:
            try:
                props = self._consumer.get_eventhub_properties()
                metadata["eventhub_name"] = props.get("eventhub_name")
                metadata["partition_ids"] = props.get("partition_ids")
                metadata["partition_count"] = len(props.get("partition_ids", []))
                metadata["created_at"] = str(props.get("created_at"))
            except Exception:
                pass

        return DiscoveredSchema(
            columns=schema.columns,
            primary_key=["sequence_number"],
            source_name="event_hubs",
            object_name=eventhub_name,
            object_type="event_hub",
            source_metadata=metadata,
        )
