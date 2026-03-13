"""Amazon Kinesis Data Streams source connector.

Reads records from Kinesis shards using the boto3 client.
Supports both bounded (batch) reads and continuous streaming with checkpointing.
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
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError as _boto_err:
    raise ImportError(
        "boto3 is required for KinesisSource. "
        "Install it with: pip install ranger-core[kinesis]"
    ) from _boto_err


class KinesisSource(BaseSource):
    """Read records from Amazon Kinesis Data Streams.

    Config keys:
        stream_name: Name of the Kinesis stream to consume.
        region: AWS region (default: ``"us-east-1"``).
        shard_iterator_type: Where to start reading —
            ``"TRIM_HORIZON"`` (oldest), ``"LATEST"`` (newest),
            or ``"AT_TIMESTAMP"`` (default: ``"TRIM_HORIZON"``).
        timestamp: ISO-8601 timestamp when *shard_iterator_type* is
            ``"AT_TIMESTAMP"``.
        batch_size: Maximum records per ``get_records()`` call
            (default: ``100``, max ``10_000``).
        poll_interval: Seconds between successive ``get_records()``
            calls to avoid throttling (default: ``1.0``).
        aws_access_key_id: Optional explicit AWS access key.
        aws_secret_access_key: Optional explicit AWS secret key.
        endpoint_url: Optional custom endpoint (for LocalStack, etc.).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._client: Any | None = None  # boto3 kinesis client

    # ------------------------------------------------------------------
    # Metadata properties
    # ------------------------------------------------------------------

    @property
    def source_type(self) -> str:  # noqa: D401
        return "kinesis"

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

            self._client = boto3.client("kinesis", **kwargs)
            self._connected = True
            logger.info(
                "kinesis_source.connected",
                stream=self._config.get("stream_name"),
                region=region,
            )
        except Exception as exc:
            self._connected = False
            logger.error("kinesis_source.connect_failed", error=str(exc))
            raise ConnectionError(f"Failed to connect to Kinesis: {exc}") from exc

    def close(self) -> None:
        # boto3 clients are lightweight — no persistent connections to close
        self._client = None
        self._connected = False
        logger.info("kinesis_source.closed")

    def health_check(self) -> HealthStatus:
        """Describe the stream to verify connectivity."""
        try:
            if self._client is None:
                self.connect()
            assert self._client is not None
            stream_name = self._config.get("stream_name", "")
            resp = self._client.describe_stream_summary(StreamName=stream_name)
            status = resp["StreamDescriptionSummary"]["StreamStatus"]
            logger.info("kinesis_source.health_check_ok", stream_status=status)
            return HealthStatus.HEALTHY if status == "ACTIVE" else HealthStatus.DEGRADED
        except Exception as exc:
            logger.warning("kinesis_source.health_check_failed", error=str(exc))
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_shard_ids(self) -> list[str]:
        """List all shard IDs for the configured stream."""
        assert self._client is not None
        stream_name = self._config["stream_name"]
        shard_ids: list[str] = []
        kwargs: dict[str, Any] = {"StreamName": stream_name}

        while True:
            resp = self._client.list_shards(**kwargs)
            for shard in resp.get("Shards", []):
                shard_ids.append(shard["ShardId"])
            next_token = resp.get("NextToken")
            if not next_token:
                break
            kwargs = {"NextToken": next_token}

        return shard_ids

    def _get_shard_iterator(self, shard_id: str) -> str:
        """Obtain a shard iterator for the given shard."""
        assert self._client is not None
        stream_name = self._config["stream_name"]
        iterator_type = self._config.get("shard_iterator_type", "TRIM_HORIZON")

        params: dict[str, Any] = {
            "StreamName": stream_name,
            "ShardId": shard_id,
            "ShardIteratorType": iterator_type,
        }
        if iterator_type == "AT_TIMESTAMP":
            ts = self._config.get("timestamp")
            if ts:
                params["Timestamp"] = datetime.fromisoformat(ts)

        resp = self._client.get_shard_iterator(**params)
        return resp["ShardIterator"]

    @staticmethod
    def _kinesis_record_to_record(rec: dict[str, Any], shard_id: str) -> Record:
        """Convert a Kinesis record dict to a Ranger :class:`Record`."""
        raw_data = rec["Data"]
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
        ts = rec.get("ApproximateArrivalTimestamp")
        if ts is not None:
            if isinstance(ts, datetime):
                event_time = ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
            elif isinstance(ts, (int, float)):
                event_time = datetime.fromtimestamp(ts, tz=timezone.utc)

        return Record(
            data=data,
            event_time=event_time,
            source_metadata={
                "source_type": "kinesis",
                "shard_id": shard_id,
                "sequence_number": rec.get("SequenceNumber"),
                "partition_key": rec.get("PartitionKey"),
            },
        )

    # ------------------------------------------------------------------
    # Reading
    # ------------------------------------------------------------------

    def read(self) -> Iterator[Record]:
        """Bounded read — iterate through all shards and yield records.

        Stops after reaching *batch_size* total records or exhausting all
        available data in every shard.

        Yields:
            Record objects, one per Kinesis record.
        """
        if self._client is None:
            raise RuntimeError("Source not connected — call connect() first")

        batch_size: int = self._config.get("batch_size", 100)
        poll_interval: float = self._config.get("poll_interval", 1.0)
        stream_name = self._config.get("stream_name")

        logger.info("kinesis_source.read_start", stream=stream_name, batch_size=batch_size)

        shard_ids = self._get_shard_ids()
        total_consumed = 0

        for shard_id in shard_ids:
            if total_consumed >= batch_size:
                break

            shard_iterator = self._get_shard_iterator(shard_id)
            empty_polls = 0
            max_empty_polls = 3

            while shard_iterator and total_consumed < batch_size:
                remaining = batch_size - total_consumed
                resp = self._client.get_records(
                    ShardIterator=shard_iterator,
                    Limit=min(remaining, 10_000),
                )
                records = resp.get("Records", [])

                if not records:
                    empty_polls += 1
                    if empty_polls >= max_empty_polls:
                        break
                    time.sleep(poll_interval)
                else:
                    empty_polls = 0

                for rec in records:
                    if total_consumed >= batch_size:
                        break
                    yield self._kinesis_record_to_record(rec, shard_id)
                    total_consumed += 1

                shard_iterator = resp.get("NextShardIterator")
                if records:
                    time.sleep(poll_interval)

        logger.info("kinesis_source.read_complete", records=total_consumed)

    async def read_stream(self, config: StreamConfig | None = None) -> AsyncIterator[Record]:
        """Continuous async consumption with automatic shard iteration.

        Args:
            config: Optional streaming configuration.

        Yields:
            Record objects as they arrive from Kinesis.
        """
        if self._client is None:
            raise RuntimeError("Source not connected — call connect() first")

        poll_interval: float = self._config.get("poll_interval", 1.0)
        batch_size: int = self._config.get("batch_size", 100)
        idle_timeout: float = config.idle_timeout_seconds if config else 300.0

        logger.info("kinesis_source.stream_start")

        shard_ids = self._get_shard_ids()
        shard_iterators: dict[str, str | None] = {}
        for sid in shard_ids:
            shard_iterators[sid] = self._get_shard_iterator(sid)

        last_record_time = time.monotonic()

        while any(si is not None for si in shard_iterators.values()):
            if time.monotonic() - last_record_time > idle_timeout:
                logger.info("kinesis_source.stream_idle_timeout")
                break

            for shard_id in list(shard_iterators):
                si = shard_iterators[shard_id]
                if si is None:
                    continue

                try:
                    resp = self._client.get_records(
                        ShardIterator=si,
                        Limit=batch_size,
                    )
                except (BotoCoreError, ClientError) as exc:
                    logger.warning(
                        "kinesis_source.get_records_error",
                        shard=shard_id,
                        error=str(exc),
                    )
                    shard_iterators[shard_id] = None
                    continue

                for rec in resp.get("Records", []):
                    last_record_time = time.monotonic()
                    yield self._kinesis_record_to_record(rec, shard_id)

                shard_iterators[shard_id] = resp.get("NextShardIterator")

            await asyncio.sleep(poll_interval)

    # ------------------------------------------------------------------
    # Schema operations
    # ------------------------------------------------------------------

    def get_schema(self) -> Schema:
        """Return a generic schema for Kinesis record payloads."""
        return Schema(
            columns=[
                ColumnDefinition(name="data", type=ColumnType.JSON, nullable=False),
                ColumnDefinition(name="partition_key", type=ColumnType.STRING, nullable=False),
                ColumnDefinition(name="sequence_number", type=ColumnType.STRING, nullable=False),
                ColumnDefinition(name="shard_id", type=ColumnType.STRING, nullable=False),
                ColumnDefinition(
                    name="approximate_arrival_timestamp",
                    type=ColumnType.TIMESTAMP_TZ,
                    nullable=True,
                ),
            ],
        )

    def discover_schema(self) -> DiscoveredSchema:
        """Discover stream info and return schema metadata."""
        schema = self.get_schema()
        stream_name = self._config.get("stream_name", "unknown")

        metadata: dict[str, Any] = {}
        if self._client is not None:
            try:
                resp = self._client.describe_stream_summary(StreamName=stream_name)
                summary = resp["StreamDescriptionSummary"]
                metadata["stream_status"] = summary.get("StreamStatus")
                metadata["shard_count"] = summary.get("OpenShardCount")
                metadata["retention_hours"] = summary.get("RetentionPeriodHours")
            except Exception:
                pass

        return DiscoveredSchema(
            columns=schema.columns,
            primary_key=["sequence_number"],
            source_name="kinesis",
            object_name=stream_name,
            object_type="stream",
            source_metadata=metadata,
        )
