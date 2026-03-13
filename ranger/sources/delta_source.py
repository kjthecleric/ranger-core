"""Delta Lake source connector via deltalake (delta-rs Python bindings).

Reads Delta tables from local paths or cloud storage (S3, GCS, ADLS) with
support for time-travel, column pruning, partition filtering, and
incremental reads.
"""

from __future__ import annotations

from collections.abc import Iterator
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
)
from ranger.sources.base import BaseSource

logger = structlog.get_logger()

try:
    from deltalake import DeltaTable
except ImportError as _err:
    raise ImportError(
        "deltalake is required for DeltaLakeSource. "
        "Install it with: pip install ranger-core[delta]"
    ) from _err

try:
    import pyarrow as pa
except ImportError as _err:
    raise ImportError(
        "pyarrow is required for DeltaLakeSource. "
        "Install it with: pip install pyarrow"
    ) from _err


# ---------------------------------------------------------------------------
# Arrow type → Ranger ColumnType mapping
# ---------------------------------------------------------------------------

_ARROW_TYPE_MAP: dict[str, ColumnType] = {
    "bool": ColumnType.BOOLEAN,
    "int8": ColumnType.INT8,
    "int16": ColumnType.INT16,
    "int32": ColumnType.INT32,
    "int64": ColumnType.INT64,
    "uint8": ColumnType.INT16,
    "uint16": ColumnType.INT32,
    "uint32": ColumnType.INT64,
    "uint64": ColumnType.INT64,
    "float16": ColumnType.FLOAT32,
    "float32": ColumnType.FLOAT32,
    "float64": ColumnType.FLOAT64,
    "string": ColumnType.STRING,
    "large_string": ColumnType.STRING,
    "utf8": ColumnType.STRING,
    "large_utf8": ColumnType.STRING,
    "binary": ColumnType.BINARY,
    "large_binary": ColumnType.BINARY,
    "date32": ColumnType.DATE,
    "date64": ColumnType.DATE,
    "time32": ColumnType.TIME,
    "time64": ColumnType.TIME,
    "timestamp": ColumnType.TIMESTAMP_TZ,
    "decimal128": ColumnType.DECIMAL,
    "decimal256": ColumnType.DECIMAL,
    "list": ColumnType.ARRAY,
    "large_list": ColumnType.ARRAY,
    "struct": ColumnType.STRUCT,
    "map": ColumnType.MAP,
}


def _arrow_type_to_column_type(arrow_type: pa.DataType) -> ColumnType:
    """Map a PyArrow DataType to a Ranger ColumnType."""
    type_str = str(arrow_type)

    # Handle parameterised types (e.g. "timestamp[ns, tz=UTC]")
    base_type = type_str.split("[")[0].split("(")[0].strip()

    if base_type in _ARROW_TYPE_MAP:
        return _ARROW_TYPE_MAP[base_type]

    # Fallback: check isinstance for common categories
    if pa.types.is_boolean(arrow_type):
        return ColumnType.BOOLEAN
    if pa.types.is_integer(arrow_type):
        return ColumnType.INT64
    if pa.types.is_floating(arrow_type):
        return ColumnType.FLOAT64
    if pa.types.is_decimal(arrow_type):
        return ColumnType.DECIMAL
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return ColumnType.STRING
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return ColumnType.BINARY
    if pa.types.is_date(arrow_type):
        return ColumnType.DATE
    if pa.types.is_time(arrow_type):
        return ColumnType.TIME
    if pa.types.is_timestamp(arrow_type):
        return ColumnType.TIMESTAMP_TZ
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        return ColumnType.ARRAY
    if pa.types.is_struct(arrow_type):
        return ColumnType.STRUCT
    if pa.types.is_map(arrow_type):
        return ColumnType.MAP

    return ColumnType.STRING


class DeltaLakeSource(BaseSource):
    """Read data from a Delta Lake table.

    Config keys
    -----------
    table_path : str
        Path to the Delta table — local filesystem path or cloud URI
        (``s3://bucket/path``, ``gs://bucket/path``, ``abfss://...``).
    version : int | None
        Optional specific table version for time-travel reads.
    timestamp : str | None
        Optional ISO-8601 timestamp for time-travel reads. Mutually
        exclusive with *version*.
    columns : list[str] | None
        Column pruning — only read these columns.
    filter : str | None
        Partition filter expression (e.g. ``"year = 2024"``).
    storage_options : dict | None
        Cloud storage credentials (AWS keys, GCS credentials, ADLS tokens).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._dt: DeltaTable | None = None

        self._table_path: str = config["table_path"]
        self._version: int | None = config.get("version")
        self._timestamp: str | None = config.get("timestamp")
        self._columns: list[str] | None = config.get("columns")
        self._filter: str | None = config.get("filter")
        self._storage_options: dict[str, str] | None = config.get("storage_options")

        if self._version is not None and self._timestamp is not None:
            raise ValueError("Cannot specify both 'version' and 'timestamp' for Delta time-travel.")

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def source_type(self) -> str:
        return "delta_lake"

    @property
    def supports_streaming(self) -> bool:
        return False

    @property
    def supports_incremental(self) -> bool:
        return True

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Open the Delta table."""
        logger.info(
            "delta_lake.connecting",
            table_path=self._table_path,
            version=self._version,
            timestamp=self._timestamp,
        )

        kwargs: dict[str, Any] = {}
        if self._storage_options:
            kwargs["storage_options"] = self._storage_options
        if self._version is not None:
            kwargs["version"] = self._version

        self._dt = DeltaTable(self._table_path, **kwargs)

        # Time-travel by timestamp
        if self._timestamp is not None:
            self._dt.load_as_version(self._timestamp)

        self._connected = True
        logger.info(
            "delta_lake.connected",
            table_path=self._table_path,
            version=self._dt.version(),
        )

    def close(self) -> None:
        """Release the Delta table handle."""
        self._dt = None
        self._connected = False
        logger.debug("delta_lake.closed")

    def health_check(self) -> HealthStatus:
        """Verify access by opening the Delta table and reading its version."""
        try:
            if self._dt is None:
                self.connect()
            assert self._dt is not None
            _ = self._dt.version()
            return HealthStatus.HEALTHY
        except Exception:
            logger.warning("delta_lake.health_check_failed", exc_info=True)
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Reading
    # ------------------------------------------------------------------

    def _to_pyarrow_dataset(self) -> pa.dataset.Dataset | Any:
        """Convert the Delta table to a PyArrow dataset with optional filters."""
        assert self._dt is not None
        return self._dt.to_pyarrow_dataset()

    def read(self) -> Iterator[Record]:
        """Read the Delta table row-by-row as Records.

        Converts the table to PyArrow batches and yields one Record per row.
        Applies column pruning and partition filters when configured.
        """
        if self._dt is None:
            self.connect()
        assert self._dt is not None

        logger.info("delta_lake.read_start", table_path=self._table_path)

        dataset = self._to_pyarrow_dataset()

        # Build scan arguments
        scan_kwargs: dict[str, Any] = {}
        if self._columns:
            scan_kwargs["columns"] = self._columns
        if self._filter:
            scan_kwargs["filter"] = self._filter

        records_yielded = 0
        for batch in dataset.to_batches(**scan_kwargs):
            # Convert RecordBatch to list of dicts
            for row in batch.to_pylist():
                yield Record(
                    data=row,
                    event_time=datetime.now(timezone.utc),
                    source_metadata={
                        "source": "delta_lake",
                        "table_path": self._table_path,
                        "version": self._dt.version(),
                    },
                )
                records_yielded += 1

        logger.info("delta_lake.read_complete", table_path=self._table_path, records=records_yielded)

    def read_batch(self, batch_size: int = 10_000) -> Iterator[list[Record]]:
        """Read the Delta table in batches of Records.

        Uses PyArrow's batch reading for memory-efficient processing.
        """
        if self._dt is None:
            self.connect()
        assert self._dt is not None

        logger.info("delta_lake.read_batch_start", table_path=self._table_path, batch_size=batch_size)

        dataset = self._to_pyarrow_dataset()

        scan_kwargs: dict[str, Any] = {"batch_size": batch_size}
        if self._columns:
            scan_kwargs["columns"] = self._columns
        if self._filter:
            scan_kwargs["filter"] = self._filter

        batch_count = 0
        for arrow_batch in dataset.to_batches(**scan_kwargs):
            records = [
                Record(
                    data=row,
                    event_time=datetime.now(timezone.utc),
                    source_metadata={
                        "source": "delta_lake",
                        "table_path": self._table_path,
                        "version": self._dt.version(),
                    },
                )
                for row in arrow_batch.to_pylist()
            ]
            if records:
                yield records
                batch_count += 1

        logger.info("delta_lake.read_batch_complete", table_path=self._table_path, batches=batch_count)

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def _arrow_schema(self) -> pa.Schema:
        """Get the Arrow schema from the Delta table."""
        if self._dt is None:
            self.connect()
        assert self._dt is not None
        return self._dt.schema().to_pyarrow()

    def get_schema(self) -> Schema:
        """Read the Delta table schema (Arrow schema → Ranger Schema)."""
        arrow_schema = self._arrow_schema()

        columns: list[ColumnDefinition] = []
        for field in arrow_schema:
            col_type = _arrow_type_to_column_type(field.type)
            col_def = ColumnDefinition(
                name=field.name,
                type=col_type,
                nullable=field.nullable,
            )
            # Add precision/scale for decimals
            if pa.types.is_decimal(field.type):
                col_def.precision = field.type.precision
                col_def.scale = field.type.scale

            columns.append(col_def)

        # Detect partition columns
        assert self._dt is not None
        partition_cols: list[str] | None = None
        try:
            metadata = self._dt.metadata()
            if hasattr(metadata, "partition_columns") and metadata.partition_columns:
                partition_cols = list(metadata.partition_columns)
        except Exception:
            pass

        return Schema(
            columns=columns,
            partition_columns=partition_cols,
        )

    def discover_schema(self) -> DiscoveredSchema:
        """Discover schema with Delta table metadata."""
        if self._dt is None:
            self.connect()
        assert self._dt is not None

        base = self.get_schema()

        # Get row count estimate from Delta metadata
        row_count: int | None = None
        try:
            # Use add actions to estimate total row count
            add_actions = self._dt.get_add_actions(flatten=True).to_pandas()
            if "num_records" in add_actions.columns:
                row_count = int(add_actions["num_records"].sum())
        except Exception:
            pass

        version = self._dt.version()

        return DiscoveredSchema(
            columns=base.columns,
            primary_key=base.primary_key,
            partition_columns=base.partition_columns,
            source_name=self.source_type,
            object_name=self._table_path,
            object_type="delta_table",
            row_count_estimate=row_count,
            source_metadata={
                "table_path": self._table_path,
                "version": version,
                "format": "delta",
            },
        )
