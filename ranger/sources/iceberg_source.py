"""Apache Iceberg source connector via pyiceberg.

Reads Iceberg tables from REST, Hive, or Glue catalogs with support for
snapshot-based time-travel, column pruning, row filtering, and
incremental extraction.
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
    from pyiceberg.catalog import load_catalog
    from pyiceberg.table import Table as IcebergTable
    from pyiceberg import types as iceberg_types
except ImportError as _err:
    raise ImportError(
        "pyiceberg is required for IcebergSource. "
        "Install it with: pip install ranger-core[iceberg]"
    ) from _err

try:
    import pyarrow as pa
except ImportError as _err:
    raise ImportError(
        "pyarrow is required for IcebergSource. "
        "Install it with: pip install pyarrow"
    ) from _err


# ---------------------------------------------------------------------------
# Iceberg type → Ranger ColumnType mapping
# ---------------------------------------------------------------------------

def _iceberg_type_to_column_type(iceberg_type: Any) -> ColumnType:
    """Map a PyIceberg type to a Ranger ColumnType."""
    if isinstance(iceberg_type, iceberg_types.BooleanType):
        return ColumnType.BOOLEAN
    if isinstance(iceberg_type, iceberg_types.IntegerType):
        return ColumnType.INT32
    if isinstance(iceberg_type, iceberg_types.LongType):
        return ColumnType.INT64
    if isinstance(iceberg_type, iceberg_types.FloatType):
        return ColumnType.FLOAT32
    if isinstance(iceberg_type, iceberg_types.DoubleType):
        return ColumnType.FLOAT64
    if isinstance(iceberg_type, iceberg_types.DecimalType):
        return ColumnType.DECIMAL
    if isinstance(iceberg_type, iceberg_types.StringType):
        return ColumnType.STRING
    if isinstance(iceberg_type, iceberg_types.BinaryType):
        return ColumnType.BINARY
    if isinstance(iceberg_type, iceberg_types.DateType):
        return ColumnType.DATE
    if isinstance(iceberg_type, iceberg_types.TimeType):
        return ColumnType.TIME
    if isinstance(iceberg_type, iceberg_types.TimestampType):
        return ColumnType.TIMESTAMP
    if isinstance(iceberg_type, iceberg_types.TimestamptzType):
        return ColumnType.TIMESTAMP_TZ
    if isinstance(iceberg_type, iceberg_types.UUIDType):
        return ColumnType.UUID
    if isinstance(iceberg_type, iceberg_types.FixedType):
        return ColumnType.BINARY
    if isinstance(iceberg_type, iceberg_types.ListType):
        return ColumnType.ARRAY
    if isinstance(iceberg_type, iceberg_types.MapType):
        return ColumnType.MAP
    if isinstance(iceberg_type, iceberg_types.StructType):
        return ColumnType.STRUCT
    return ColumnType.STRING


class IcebergSource(BaseSource):
    """Read data from an Apache Iceberg table via pyiceberg.

    Config keys
    -----------
    catalog_type : str
        Catalog type — ``"rest"``, ``"hive"``, ``"glue"``, or any type
        supported by pyiceberg's ``load_catalog``.
    catalog_config : dict
        Catalog connection parameters (``uri``, ``warehouse``, credentials,
        etc.) passed directly to ``load_catalog``.
    namespace : str
        Iceberg namespace / database name.
    table_name : str
        Iceberg table name within the namespace.
    snapshot_id : int | None
        Optional snapshot ID for time-travel reads.
    columns : list[str] | None
        Column pruning — only read these columns.
    row_filter : str | None
        Row filter expression string (e.g. ``"category = 'A' AND amount > 100"``).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._catalog: Any = None
        self._table: IcebergTable | None = None

        self._catalog_type: str = config["catalog_type"]
        self._catalog_config: dict[str, Any] = config.get("catalog_config", {})
        self._namespace: str = config["namespace"]
        self._table_name: str = config["table_name"]
        self._snapshot_id: int | None = config.get("snapshot_id")
        self._columns: list[str] | None = config.get("columns")
        self._row_filter: str | None = config.get("row_filter")

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def source_type(self) -> str:
        return "iceberg"

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
        """Load the Iceberg catalog and table."""
        logger.info(
            "iceberg.connecting",
            catalog_type=self._catalog_type,
            namespace=self._namespace,
            table_name=self._table_name,
        )

        self._catalog = load_catalog(
            name="ranger_catalog",
            **{"type": self._catalog_type, **self._catalog_config},
        )

        full_table_name = f"{self._namespace}.{self._table_name}"
        self._table = self._catalog.load_table(full_table_name)

        self._connected = True
        logger.info(
            "iceberg.connected",
            namespace=self._namespace,
            table_name=self._table_name,
        )

    def close(self) -> None:
        """Release catalog and table references."""
        self._table = None
        self._catalog = None
        self._connected = False
        logger.debug("iceberg.closed")

    def health_check(self) -> HealthStatus:
        """Verify access by loading the table and reading its schema."""
        try:
            if self._table is None:
                self.connect()
            assert self._table is not None
            _ = self._table.schema()
            return HealthStatus.HEALTHY
        except Exception:
            logger.warning("iceberg.health_check_failed", exc_info=True)
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Reading
    # ------------------------------------------------------------------

    def _build_scan(self) -> Any:
        """Build an Iceberg table scan with configured filters and projections."""
        assert self._table is not None

        scan = self._table.scan()

        if self._snapshot_id is not None:
            scan = scan.use_ref(f"snapshot-{self._snapshot_id}")
            # pyiceberg uses snapshot() for specific snapshot reads
            scan = self._table.scan(snapshot_id=self._snapshot_id)

        if self._columns:
            scan = scan.select(*self._columns)

        if self._row_filter:
            scan = scan.filter(self._row_filter)

        return scan

    def read(self) -> Iterator[Record]:
        """Read Iceberg table rows as Records.

        Scans the table to Arrow batches and yields one Record per row.
        """
        if self._table is None:
            self.connect()
        assert self._table is not None

        logger.info(
            "iceberg.read_start",
            namespace=self._namespace,
            table_name=self._table_name,
            snapshot_id=self._snapshot_id,
        )

        scan = self._build_scan()
        records_yielded = 0

        for task in scan.plan_files():
            # Scan to Arrow batches
            arrow_table = scan.to_arrow()
            for batch in arrow_table.to_batches():
                for row in batch.to_pylist():
                    yield Record(
                        data=row,
                        event_time=datetime.now(timezone.utc),
                        source_metadata={
                            "source": "iceberg",
                            "namespace": self._namespace,
                            "table_name": self._table_name,
                            "snapshot_id": self._snapshot_id,
                        },
                    )
                    records_yielded += 1
            # Only process once — to_arrow() reads the full scan
            break

        logger.info(
            "iceberg.read_complete",
            namespace=self._namespace,
            table_name=self._table_name,
            records=records_yielded,
        )

    def read_batch(self, batch_size: int = 10_000) -> Iterator[list[Record]]:
        """Read the Iceberg table in batches of Records."""
        if self._table is None:
            self.connect()
        assert self._table is not None

        logger.info(
            "iceberg.read_batch_start",
            namespace=self._namespace,
            table_name=self._table_name,
            batch_size=batch_size,
        )

        scan = self._build_scan()
        arrow_table = scan.to_arrow()

        batch_count = 0
        current_batch: list[Record] = []

        for arrow_batch in arrow_table.to_batches(max_chunksize=batch_size):
            for row in arrow_batch.to_pylist():
                current_batch.append(
                    Record(
                        data=row,
                        event_time=datetime.now(timezone.utc),
                        source_metadata={
                            "source": "iceberg",
                            "namespace": self._namespace,
                            "table_name": self._table_name,
                            "snapshot_id": self._snapshot_id,
                        },
                    )
                )

                if len(current_batch) >= batch_size:
                    yield current_batch
                    batch_count += 1
                    current_batch = []

        if current_batch:
            yield current_batch
            batch_count += 1

        logger.info(
            "iceberg.read_batch_complete",
            namespace=self._namespace,
            table_name=self._table_name,
            batches=batch_count,
        )

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def get_schema(self) -> Schema:
        """Read the Iceberg table schema."""
        if self._table is None:
            self.connect()
        assert self._table is not None

        iceberg_schema = self._table.schema()
        columns: list[ColumnDefinition] = []

        for field in iceberg_schema.fields:
            col_type = _iceberg_type_to_column_type(field.field_type)
            col_def = ColumnDefinition(
                name=field.name,
                type=col_type,
                nullable=field.optional,
                description=field.doc or None,
            )
            # Add precision/scale for decimals
            if isinstance(field.field_type, iceberg_types.DecimalType):
                col_def.precision = field.field_type.precision
                col_def.scale = field.field_type.scale

            columns.append(col_def)

        # Detect partition columns
        partition_cols: list[str] | None = None
        try:
            partition_spec = self._table.spec()
            if partition_spec and partition_spec.fields:
                partition_cols = [
                    iceberg_schema.find_field(pf.source_id).name
                    for pf in partition_spec.fields
                ]
        except Exception:
            pass

        # Detect primary key from identifier fields
        pk: list[str] | None = None
        try:
            identifier_field_ids = self._table.schema().identifier_field_ids
            if identifier_field_ids:
                pk = [
                    iceberg_schema.find_field(fid).name
                    for fid in identifier_field_ids
                ]
        except Exception:
            pass

        return Schema(
            columns=columns,
            primary_key=pk,
            partition_columns=partition_cols,
        )

    def discover_schema(self) -> DiscoveredSchema:
        """Discover schema with Iceberg table metadata."""
        if self._table is None:
            self.connect()
        assert self._table is not None

        base = self.get_schema()

        # Get row count from snapshots
        row_count: int | None = None
        try:
            current_snapshot = self._table.current_snapshot()
            if current_snapshot and current_snapshot.summary:
                total_records = current_snapshot.summary.get("total-records")
                if total_records is not None:
                    row_count = int(total_records)
        except Exception:
            pass

        # Get snapshot info
        snapshot_info: dict[str, Any] = {}
        try:
            current_snapshot = self._table.current_snapshot()
            if current_snapshot:
                snapshot_info = {
                    "snapshot_id": current_snapshot.snapshot_id,
                    "timestamp_ms": current_snapshot.timestamp_ms,
                }
        except Exception:
            pass

        return DiscoveredSchema(
            columns=base.columns,
            primary_key=base.primary_key,
            partition_columns=base.partition_columns,
            source_name=self.source_type,
            object_name=f"{self._namespace}.{self._table_name}",
            object_type="iceberg_table",
            row_count_estimate=row_count,
            source_metadata={
                "catalog_type": self._catalog_type,
                "namespace": self._namespace,
                "table_name": self._table_name,
                "format": "iceberg",
                **snapshot_info,
            },
        )
