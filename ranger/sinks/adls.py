"""Azure Data Lake Storage Gen2 sink — writes records to ADLS as Parquet, JSON, or CSV.

Supports Hive-style partitioning, compression, and flexible authentication via
connection string or ``DefaultAzureCredential``.
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
    from azure.storage.filedatalake import DataLakeServiceClient
    from azure.core.exceptions import (
        HttpResponseError,
        ResourceNotFoundError,
        ClientAuthenticationError,
    )

    _HAS_ADLS = True
except ImportError:  # pragma: no cover
    _HAS_ADLS = False

try:
    from azure.identity import DefaultAzureCredential

    _HAS_AZURE_IDENTITY = True
except ImportError:  # pragma: no cover
    _HAS_AZURE_IDENTITY = False

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


class ADLSSink(BaseSink):
    """Write records to Azure Data Lake Storage Gen2 as files.

    Config keys:
        account_name: Azure Storage account name (required).
        container: ADLS filesystem / container name (required).
        prefix: Path prefix within the container (default ``""``).
        credential: Either a full connection string or ``"default"`` to use
            ``DefaultAzureCredential`` (default ``"default"``).
        format: Output format — ``parquet``, ``json``, or ``csv``
            (default ``parquet``).
        partition_by: List of column names for Hive-style partitioning.
        compression: Compression codec — ``gzip``, ``snappy``, ``zstd``,
            ``none`` (default ``none``).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        if not _HAS_ADLS:
            raise ImportError(
                "azure-storage-file-datalake is required for ADLSSink. "
                "Install it with: pip install azure-storage-file-datalake"
            )
        super().__init__(config)

        self._account_name: str = config["account_name"]
        self._container_name: str = config["container"]
        self._prefix: str = config.get("prefix", "").strip("/")
        self._credential_cfg: str = config.get("credential", "default")
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

        self._service_client: Any = None
        self._filesystem_client: Any = None
        self._schema: Schema | None = None
        self._buffer: list[dict[str, Any]] = []
        self._total_written: int = 0

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def sink_type(self) -> str:
        return "adls"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def open(self, schema: Schema) -> None:
        """Create the ADLS client and validate the container exists."""
        self._schema = schema

        account_url = f"https://{self._account_name}.dfs.core.windows.net"

        if self._credential_cfg != "default" and "AccountKey=" in self._credential_cfg:
            # Treat as a connection string
            self._service_client = DataLakeServiceClient.from_connection_string(
                self._credential_cfg
            )
        else:
            # Use DefaultAzureCredential
            if not _HAS_AZURE_IDENTITY:
                raise ImportError(
                    "azure-identity is required when using DefaultAzureCredential. "
                    "Install it with: pip install azure-identity"
                )
            credential = DefaultAzureCredential()
            self._service_client = DataLakeServiceClient(
                account_url=account_url, credential=credential
            )

        try:
            self._filesystem_client = self._service_client.get_file_system_client(
                file_system=self._container_name
            )
            # Validate container is accessible
            self._filesystem_client.get_file_system_properties()
        except ResourceNotFoundError as exc:
            raise RuntimeError(
                f"ADLS container '{self._container_name}' not found: {exc}"
            ) from exc
        except ClientAuthenticationError as exc:
            raise RuntimeError(
                f"Authentication failed for ADLS account '{self._account_name}': {exc}"
            ) from exc
        except HttpResponseError as exc:
            raise RuntimeError(
                f"Cannot access ADLS container '{self._container_name}': {exc}"
            ) from exc

        self._opened = True
        logger.info(
            "ADLS sink opened",
            account=self._account_name,
            container=self._container_name,
            prefix=self._prefix,
            format=self._format,
        )

    def close(self) -> None:
        """Flush any buffered records and release resources."""
        if self._buffer:
            self._flush_buffer()
        self._filesystem_client = None
        self._service_client = None
        self._opened = False
        logger.info(
            "ADLS sink closed",
            total_written=self._total_written,
            container=self._container_name,
        )

    def health_check(self) -> HealthStatus:
        """Verify the ADLS container is reachable."""
        if self._filesystem_client is None:
            return HealthStatus.UNHEALTHY
        try:
            self._filesystem_client.get_file_system_properties()
            return HealthStatus.HEALTHY
        except Exception:
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Writing
    # ------------------------------------------------------------------

    def write_batch(self, records: list[Record]) -> int:
        """Buffer records for later flush."""
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
        """No-op — ADLS is schema-on-read."""
        self._schema = new_schema
        logger.info("ADLS sink schema evolved (schema-on-read; no-op)")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _flush_buffer(self) -> None:
        """Serialize buffered records and upload to ADLS."""
        if not self._buffer:
            return

        if self._partition_by:
            partitions = self._partition_records(self._buffer)
        else:
            partitions = {(): self._buffer}

        for partition_values, partition_records in partitions.items():
            file_path = self._build_file_path(partition_values)
            body = self._serialize(partition_records)
            self._upload(file_path, body)
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

    def _build_file_path(self, partition_values: tuple[tuple[str, str], ...] = ()) -> str:
        """Construct the ADLS file path including prefix and partitions."""
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

    def _upload(self, file_path: str, body: bytes) -> None:
        """Upload bytes to ADLS."""
        try:
            file_client = self._filesystem_client.get_file_client(file_path)
            file_client.create_file()
            file_client.append_data(body, offset=0, length=len(body))
            file_client.flush_data(len(body))
            logger.debug(
                "Uploaded to ADLS",
                file_path=file_path,
                size_bytes=len(body),
            )
        except HttpResponseError as exc:
            raise RuntimeError(
                f"Failed to upload to ADLS "
                f"{self._container_name}/{file_path}: {exc}"
            ) from exc
