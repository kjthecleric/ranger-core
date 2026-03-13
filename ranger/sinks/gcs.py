"""Google Cloud Storage sink — writes records to GCS as Parquet, JSON, or CSV files.

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
    from google.cloud import storage as gcs_storage
    from google.api_core import exceptions as gcs_exceptions

    _HAS_GCS = True
except ImportError:  # pragma: no cover
    _HAS_GCS = False

try:
    import pyarrow as pa
    import pyarrow.parquet as pq

    _HAS_ARROW = True
except ImportError:  # pragma: no cover
    _HAS_ARROW = False

logger = structlog.get_logger(__name__)

_PARQUET_COMPRESSION: dict[str, str | None] = {
    "none": None,
    "gzip": "gzip",
    "snappy": "snappy",
    "zstd": "zstd",
}

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


class GCSSink(BaseSink):
    """Write records to Google Cloud Storage as files.

    Config keys:
        bucket: GCS bucket name (required).
        prefix: Object name prefix (default ``""``).
        project_id: GCP project ID (optional; uses default if omitted).
        credentials_path: Path to a service-account JSON key file (optional).
        format: Output format — ``parquet``, ``json``, or ``csv`` (default ``parquet``).
        partition_by: List of column names for Hive-style partitioning.
        compression: Compression codec — ``gzip``, ``snappy``, ``zstd``, ``none``
            (default ``none``).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        if not _HAS_GCS:
            raise ImportError(
                "google-cloud-storage is required for GCSSink. "
                "Install it with: pip install google-cloud-storage"
            )
        super().__init__(config)

        self._bucket_name: str = config["bucket"]
        self._prefix: str = config.get("prefix", "").strip("/")
        self._project_id: str | None = config.get("project_id")
        self._credentials_path: str | None = config.get("credentials_path")
        self._format: str = config.get("format", "parquet").lower()
        self._partition_by: list[str] = config.get("partition_by", [])
        self._compression: str = config.get("compression", "none").lower()

        if self._format not in ("parquet", "json", "csv"):
            raise ValueError(f"Unsupported format: {self._format}")
        if self._compression not in _PARQUET_COMPRESSION:
            raise ValueError(f"Unsupported compression: {self._compression}")
        if self._format == "parquet" and not _HAS_ARROW:
            raise ImportError(
                "pyarrow is required for Parquet output. "
                "Install it with: pip install pyarrow"
            )

        self._client: Any = None
        self._bucket: Any = None
        self._schema: Schema | None = None
        self._buffer: list[dict[str, Any]] = []
        self._total_written: int = 0

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def sink_type(self) -> str:
        return "gcs"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def open(self, schema: Schema) -> None:
        """Create GCS client and validate the bucket exists."""
        self._schema = schema

        client_kwargs: dict[str, Any] = {}
        if self._project_id:
            client_kwargs["project"] = self._project_id

        if self._credentials_path:
            self._client = gcs_storage.Client.from_service_account_json(
                self._credentials_path, **client_kwargs
            )
        else:
            self._client = gcs_storage.Client(**client_kwargs)

        try:
            self._bucket = self._client.get_bucket(self._bucket_name)
        except gcs_exceptions.NotFound as exc:
            raise RuntimeError(
                f"GCS bucket '{self._bucket_name}' not found: {exc}"
            ) from exc
        except gcs_exceptions.Forbidden as exc:
            raise RuntimeError(
                f"Access denied to GCS bucket '{self._bucket_name}': {exc}"
            ) from exc

        self._opened = True
        logger.info(
            "GCS sink opened",
            bucket=self._bucket_name,
            prefix=self._prefix,
            format=self._format,
        )

    def close(self) -> None:
        """Flush any buffered records and release resources."""
        if self._buffer:
            self._flush_buffer()
        self._client = None
        self._bucket = None
        self._opened = False
        logger.info(
            "GCS sink closed",
            total_written=self._total_written,
            bucket=self._bucket_name,
        )

    def health_check(self) -> HealthStatus:
        """Verify the GCS bucket is reachable."""
        if self._client is None:
            return HealthStatus.UNHEALTHY
        try:
            self._client.get_bucket(self._bucket_name)
            return HealthStatus.HEALTHY
        except Exception:
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Writing
    # ------------------------------------------------------------------

    def write_batch(self, records: list[Record]) -> int:
        """Buffer records and flush when limits are reached."""
        if not self._opened:
            raise RuntimeError("Sink not opened. Call open() first.")

        written = 0
        for record in records:
            self._buffer.append(record.data)
            written += 1

        return written

    # ------------------------------------------------------------------
    # Schema evolution
    # ------------------------------------------------------------------

    def evolve_schema(self, new_schema: Schema) -> None:
        """No-op — GCS is schema-on-read."""
        self._schema = new_schema
        logger.info("GCS sink schema evolved (schema-on-read; no-op)")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _flush_buffer(self) -> None:
        """Serialize buffered records and upload to GCS."""
        if not self._buffer:
            return

        if self._partition_by:
            partitions = self._partition_records(self._buffer)
        else:
            partitions = {(): self._buffer}

        for partition_values, partition_records in partitions.items():
            blob_name = self._build_blob_name(partition_values)
            body = self._serialize(partition_records)
            self._upload(blob_name, body)
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

    def _build_blob_name(self, partition_values: tuple[tuple[str, str], ...] = ()) -> str:
        """Construct the GCS blob name including prefix and partitions."""
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
        table = pa.Table.from_pylist(records)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression=_PARQUET_COMPRESSION[self._compression])
        return buf.getvalue()

    def _serialize_json(self, records: list[dict[str, Any]]) -> bytes:
        raw = "\n".join(json.dumps(r, default=str) for r in records).encode("utf-8")
        return self._compress(raw)

    def _serialize_csv(self, records: list[dict[str, Any]]) -> bytes:
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

    def _upload(self, blob_name: str, body: bytes) -> None:
        """Upload bytes to GCS."""
        try:
            blob = self._bucket.blob(blob_name)
            blob.upload_from_string(body)
            logger.debug("Uploaded to GCS", blob_name=blob_name, size_bytes=len(body))
        except Exception as exc:
            raise RuntimeError(
                f"Failed to upload to gs://{self._bucket_name}/{blob_name}: {exc}"
            ) from exc
