"""Amazon S3 sink — writes records to S3 as Parquet, JSON, or CSV files.

Supports Hive-style partitioning, compression, and configurable file size limits.
"""

from __future__ import annotations

import csv
import io
import json
import uuid
from datetime import datetime, timezone
from typing import Any

import structlog

from ranger.core.models import HealthStatus, Record, Schema
from ranger.sinks.base import BaseSink

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError

    _HAS_BOTO3 = True
except ImportError:  # pragma: no cover
    _HAS_BOTO3 = False

try:
    import pyarrow as pa
    import pyarrow.parquet as pq

    _HAS_ARROW = True
except ImportError:  # pragma: no cover
    _HAS_ARROW = False

logger = structlog.get_logger(__name__)

# Mapping of compression names to Parquet codec identifiers
_PARQUET_COMPRESSION: dict[str, str | None] = {
    "none": None,
    "gzip": "gzip",
    "snappy": "snappy",
    "zstd": "zstd",
}

# File extensions by format + compression
_EXTENSION_MAP: dict[str, str] = {
    "parquet": ".parquet",
    "json": ".json",
    "csv": ".csv",
}

_COMPRESSION_EXT: dict[str, str] = {
    "none": "",
    "gzip": ".gz",
    "snappy": ".snappy",
    "zstd": ".zst",
}


class S3Sink(BaseSink):
    """Write records to Amazon S3 as files.

    Config keys:
        bucket: S3 bucket name (required).
        prefix: Key prefix for uploaded objects (default ``""``).
        region: AWS region (optional; uses default boto3 resolution).
        format: Output format — ``parquet``, ``json``, or ``csv`` (default ``parquet``).
        partition_by: List of column names for Hive-style partitioning.
        compression: Compression codec — ``gzip``, ``snappy``, ``zstd``, ``none`` (default ``none``).
        file_max_records: Max records per file before rolling (default ``100_000``).
        file_max_bytes: Max bytes per file before rolling (default ``256 MB``).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        if not _HAS_BOTO3:
            raise ImportError(
                "boto3 is required for S3Sink. Install it with: pip install boto3"
            )
        super().__init__(config)

        self._bucket: str = config["bucket"]
        self._prefix: str = config.get("prefix", "").strip("/")
        self._region: str | None = config.get("region")
        self._format: str = config.get("format", "parquet").lower()
        self._partition_by: list[str] = config.get("partition_by", [])
        self._compression: str = config.get("compression", "none").lower()
        self._file_max_records: int = config.get("file_max_records", 100_000)
        self._file_max_bytes: int = config.get("file_max_bytes", 256 * 1024 * 1024)

        if self._format not in ("parquet", "json", "csv"):
            raise ValueError(f"Unsupported format: {self._format}")
        if self._compression not in _PARQUET_COMPRESSION:
            raise ValueError(f"Unsupported compression: {self._compression}")
        if self._format == "parquet" and not _HAS_ARROW:
            raise ImportError(
                "pyarrow is required for Parquet output. Install it with: pip install pyarrow"
            )

        self._client: Any = None
        self._schema: Schema | None = None
        self._buffer: list[dict[str, Any]] = []
        self._total_written: int = 0

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def sink_type(self) -> str:
        return "s3"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def open(self, schema: Schema) -> None:
        """Validate the bucket exists and prepare for writing."""
        self._schema = schema
        session_kwargs: dict[str, Any] = {}
        if self._region:
            session_kwargs["region_name"] = self._region
        self._client = boto3.client("s3", **session_kwargs)

        try:
            self._client.head_bucket(Bucket=self._bucket)
        except (ClientError, BotoCoreError) as exc:
            raise RuntimeError(
                f"Cannot access S3 bucket '{self._bucket}': {exc}"
            ) from exc

        self._opened = True
        logger.info(
            "S3 sink opened",
            bucket=self._bucket,
            prefix=self._prefix,
            format=self._format,
        )

    def close(self) -> None:
        """Flush any buffered records and release resources."""
        if self._buffer:
            self._flush_buffer()
        self._client = None
        self._opened = False
        logger.info(
            "S3 sink closed",
            total_written=self._total_written,
            bucket=self._bucket,
        )

    def health_check(self) -> HealthStatus:
        """Verify the S3 bucket is reachable."""
        if self._client is None:
            return HealthStatus.UNHEALTHY
        try:
            self._client.head_bucket(Bucket=self._bucket)
            return HealthStatus.HEALTHY
        except Exception:
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Writing
    # ------------------------------------------------------------------

    def write_batch(self, records: list[Record]) -> int:
        """Buffer records and flush when size limits are reached."""
        if not self._opened:
            raise RuntimeError("Sink not opened. Call open() first.")

        written = 0
        for record in records:
            self._buffer.append(record.data)
            written += 1

            if len(self._buffer) >= self._file_max_records:
                self._flush_buffer()

        return written

    # ------------------------------------------------------------------
    # Schema evolution
    # ------------------------------------------------------------------

    def evolve_schema(self, new_schema: Schema) -> None:
        """No-op — S3 is schema-on-read."""
        self._schema = new_schema
        logger.info("S3 sink schema evolved (schema-on-read; no-op)")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _flush_buffer(self) -> None:
        """Serialize buffered records and upload to S3."""
        if not self._buffer:
            return

        if self._partition_by:
            partitions = self._partition_records(self._buffer)
        else:
            partitions = {(): self._buffer}

        for partition_values, partition_records in partitions.items():
            key = self._build_key(partition_values)
            body = self._serialize(partition_records)
            self._upload(key, body)
            self._total_written += len(partition_records)

        self._buffer = []

    def _partition_records(
        self, records: list[dict[str, Any]]
    ) -> dict[tuple[tuple[str, str], ...], list[dict[str, Any]]]:
        """Group records by partition column values."""
        partitions: dict[tuple[tuple[str, str], ...], list[dict[str, Any]]] = {}
        for rec in records:
            key = tuple(
                (col, str(rec.get(col, "__HIVE_DEFAULT_PARTITION__")))
                for col in self._partition_by
            )
            partitions.setdefault(key, []).append(rec)
        return partitions

    def _build_key(self, partition_values: tuple[tuple[str, str], ...] = ()) -> str:
        """Construct the S3 object key including prefix and partitions."""
        parts: list[str] = []
        if self._prefix:
            parts.append(self._prefix)

        for col, val in partition_values:
            parts.append(f"{col}={val}")

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        unique_id = uuid.uuid4().hex[:8]
        ext = _EXTENSION_MAP[self._format]
        comp_ext = _COMPRESSION_EXT[self._compression]
        filename = f"data_{timestamp}_{unique_id}{ext}{comp_ext}"
        parts.append(filename)

        return "/".join(parts)

    def _serialize(self, records: list[dict[str, Any]]) -> bytes:
        """Serialize records to the configured format and compression."""
        if self._format == "parquet":
            return self._serialize_parquet(records)
        elif self._format == "json":
            return self._serialize_json(records)
        elif self._format == "csv":
            return self._serialize_csv(records)
        raise ValueError(f"Unsupported format: {self._format}")  # pragma: no cover

    def _serialize_parquet(self, records: list[dict[str, Any]]) -> bytes:
        """Serialize records to Parquet bytes."""
        table = pa.Table.from_pylist(records)
        buf = io.BytesIO()
        pq.write_table(
            table,
            buf,
            compression=_PARQUET_COMPRESSION[self._compression],
        )
        return buf.getvalue()

    def _serialize_json(self, records: list[dict[str, Any]]) -> bytes:
        """Serialize records to newline-delimited JSON bytes, optionally compressed."""
        raw = "\n".join(json.dumps(r, default=str) for r in records).encode("utf-8")
        return self._compress(raw)

    def _serialize_csv(self, records: list[dict[str, Any]]) -> bytes:
        """Serialize records to CSV bytes, optionally compressed."""
        if not records:
            return b""
        buf = io.StringIO()
        fieldnames = list(records[0].keys())
        writer = csv.DictWriter(buf, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
        raw = buf.getvalue().encode("utf-8")
        return self._compress(raw)

    def _compress(self, data: bytes) -> bytes:
        """Apply compression for non-Parquet formats."""
        if self._compression == "none":
            return data
        elif self._compression == "gzip":
            import gzip

            return gzip.compress(data)
        elif self._compression == "zstd":
            try:
                import zstandard as zstd
            except ImportError:
                raise ImportError(
                    "zstandard is required for zstd compression. "
                    "Install it with: pip install zstandard"
                )
            cctx = zstd.ZstdCompressor()
            return cctx.compress(data)
        elif self._compression == "snappy":
            try:
                import snappy  # type: ignore[import-untyped]
            except ImportError:
                raise ImportError(
                    "python-snappy is required for snappy compression. "
                    "Install it with: pip install python-snappy"
                )
            return snappy.compress(data)
        return data  # pragma: no cover

    def _upload(self, key: str, body: bytes) -> None:
        """Upload bytes to S3."""
        try:
            self._client.put_object(Bucket=self._bucket, Key=key, Body=body)
            logger.debug("Uploaded to S3", key=key, size_bytes=len(body))
        except (ClientError, BotoCoreError) as exc:
            raise RuntimeError(f"Failed to upload to s3://{self._bucket}/{key}: {exc}") from exc
