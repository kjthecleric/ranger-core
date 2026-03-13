"""Apache Iceberg sink — writes records to Iceberg tables using PyIceberg.

Supports REST, Hive, and Glue catalogs with append/overwrite modes, auto-create
table, partition specs, and Iceberg-native schema evolution.
"""

from __future__ import annotations

from typing import Any

import structlog

from ranger.core.models import ColumnType, HealthStatus, Record, Schema
from ranger.sinks.base import BaseSink

try:
    import pyarrow as pa

    _HAS_ARROW = True
except ImportError:  # pragma: no cover
    _HAS_ARROW = False

try:
    from pyiceberg.catalog import load_catalog
    from pyiceberg.schema import Schema as IcebergSchema
    from pyiceberg.types import (
        BooleanType,
        DateType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        StringType,
        TimestampType,
        TimestamptzType,
        TimeType,
        BinaryType,
        DecimalType,
        UUIDType,
        NestedField,
    )
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import (
        IdentityTransform,
        YearTransform,
        MonthTransform,
        DayTransform,
        HourTransform,
        BucketTransform,
        TruncateTransform,
    )
    from pyiceberg.exceptions import (
        NoSuchTableError,
        NoSuchNamespaceError,
    )
    from pyiceberg.table import Table as IcebergTable

    _HAS_ICEBERG = True
except ImportError:  # pragma: no cover
    _HAS_ICEBERG = False

logger = structlog.get_logger(__name__)

# Map Ranger column types to PyIceberg types
_ICEBERG_TYPE_MAP: dict[ColumnType, Any] = {}
if _HAS_ICEBERG:
    _ICEBERG_TYPE_MAP = {
        ColumnType.BOOLEAN: BooleanType(),
        ColumnType.INT8: IntegerType(),
        ColumnType.INT16: IntegerType(),
        ColumnType.INT32: IntegerType(),
        ColumnType.INT64: LongType(),
        ColumnType.FLOAT32: FloatType(),
        ColumnType.FLOAT64: DoubleType(),
        ColumnType.DECIMAL: DecimalType(precision=38, scale=18),
        ColumnType.STRING: StringType(),
        ColumnType.BINARY: BinaryType(),
        ColumnType.DATE: DateType(),
        ColumnType.TIME: TimeType(),
        ColumnType.TIMESTAMP: TimestampType(),
        ColumnType.TIMESTAMP_TZ: TimestamptzType(),
        ColumnType.JSON: StringType(),
        ColumnType.ARRAY: StringType(),
        ColumnType.MAP: StringType(),
        ColumnType.STRUCT: StringType(),
        ColumnType.UUID: UUIDType(),
    }

# Map transform names to PyIceberg transform constructors
_TRANSFORM_MAP: dict[str, Any] = {}
if _HAS_ICEBERG:
    _TRANSFORM_MAP = {
        "identity": lambda _cfg: IdentityTransform(),
        "year": lambda _cfg: YearTransform(),
        "month": lambda _cfg: MonthTransform(),
        "day": lambda _cfg: DayTransform(),
        "hour": lambda _cfg: HourTransform(),
        "bucket": lambda cfg: BucketTransform(cfg.get("num_buckets", 16)),
        "truncate": lambda cfg: TruncateTransform(cfg.get("width", 10)),
    }


def _build_iceberg_schema(schema: Schema) -> IcebergSchema:
    """Convert a Ranger Schema to a PyIceberg Schema."""
    fields: list[NestedField] = []
    for idx, col in enumerate(schema.columns, start=1):
        iceberg_type = _ICEBERG_TYPE_MAP.get(col.type, StringType())
        fields.append(
            NestedField(
                field_id=idx,
                name=col.name,
                field_type=iceberg_type,
                required=not col.nullable,
            )
        )
    return IcebergSchema(*fields)


def _build_partition_spec(
    partition_spec_cfg: list[dict[str, Any]],
    iceberg_schema: IcebergSchema,
) -> PartitionSpec:
    """Build a PyIceberg PartitionSpec from user config.

    Each item in *partition_spec_cfg* is a dict with keys:
        - ``source_column``: name of the source column
        - ``transform``: transform name (identity, year, month, day, hour,
          bucket, truncate)
        - ``num_buckets`` / ``width``: extra params for bucket/truncate
    """
    if not partition_spec_cfg:
        return PartitionSpec()

    partition_fields: list[PartitionField] = []
    for idx, spec in enumerate(partition_spec_cfg, start=1000):
        col_name = spec["source_column"]
        transform_name = spec.get("transform", "identity")

        # Resolve the source field ID from the schema
        source_field = iceberg_schema.find_field(col_name)
        transform_factory = _TRANSFORM_MAP.get(transform_name)
        if transform_factory is None:
            raise ValueError(f"Unsupported partition transform: {transform_name}")

        transform = transform_factory(spec)
        partition_fields.append(
            PartitionField(
                source_id=source_field.field_id,
                field_id=idx,
                transform=transform,
                name=f"{col_name}_{transform_name}",
            )
        )

    return PartitionSpec(*partition_fields)


class IcebergSink(BaseSink):
    """Write records to an Apache Iceberg table.

    Config keys:
        catalog_type: Catalog type — ``rest``, ``hive``, or ``glue`` (required).
        catalog_config: Dict of catalog-specific config (required).
        namespace: Iceberg namespace (required).
        table_name: Table name within the namespace (required).
        mode: Write mode — ``append`` or ``overwrite`` (default ``append``).
        auto_create_table: Whether to create the table if it does not exist
            (default ``False``).
        partition_spec: List of partition spec dicts (optional). Each dict has
            ``source_column``, ``transform``, and optionally ``num_buckets``
            or ``width``.
    """

    def __init__(self, config: dict[str, Any]) -> None:
        if not _HAS_ICEBERG:
            raise ImportError(
                "pyiceberg is required for IcebergSink. "
                "Install it with: pip install pyiceberg"
            )
        if not _HAS_ARROW:
            raise ImportError(
                "pyarrow is required for IcebergSink. "
                "Install it with: pip install pyarrow"
            )
        super().__init__(config)

        self._catalog_type: str = config["catalog_type"]
        self._catalog_config: dict[str, Any] = config["catalog_config"]
        self._namespace: str = config["namespace"]
        self._table_name: str = config["table_name"]
        self._mode: str = config.get("mode", "append").lower()
        self._auto_create_table: bool = config.get("auto_create_table", False)
        self._partition_spec_cfg: list[dict[str, Any]] = config.get(
            "partition_spec", []
        )

        if self._mode not in ("append", "overwrite"):
            raise ValueError(f"Unsupported mode: {self._mode}")

        self._catalog: Any = None
        self._table: Any = None
        self._schema_obj: Schema | None = None
        self._total_written: int = 0

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def sink_type(self) -> str:
        return "iceberg"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def open(self, schema: Schema) -> None:
        """Load the Iceberg catalog and table, optionally creating both."""
        self._schema_obj = schema

        catalog_kwargs: dict[str, Any] = {
            "name": self._catalog_type,
            **self._catalog_config,
        }
        try:
            self._catalog = load_catalog(**catalog_kwargs)
        except Exception as exc:
            raise RuntimeError(
                f"Failed to load Iceberg catalog ({self._catalog_type}): {exc}"
            ) from exc

        identifier = f"{self._namespace}.{self._table_name}"

        try:
            self._table = self._catalog.load_table(identifier)
            logger.info(
                "Iceberg sink opened (existing table)",
                identifier=identifier,
            )
        except NoSuchTableError:
            if self._auto_create_table:
                self._table = self._create_table(schema, identifier)
                logger.info(
                    "Iceberg sink opened (table created)",
                    identifier=identifier,
                )
            else:
                raise RuntimeError(
                    f"Iceberg table '{identifier}' not found and "
                    f"auto_create_table is False"
                )
        except NoSuchNamespaceError:
            if self._auto_create_table:
                # Create the namespace, then the table
                try:
                    self._catalog.create_namespace(self._namespace)
                except Exception:
                    pass  # Namespace may already exist in some catalogs
                self._table = self._create_table(schema, identifier)
                logger.info(
                    "Iceberg sink opened (namespace and table created)",
                    identifier=identifier,
                )
            else:
                raise RuntimeError(
                    f"Iceberg namespace '{self._namespace}' not found and "
                    f"auto_create_table is False"
                )

        self._opened = True

    def close(self) -> None:
        """Release resources."""
        self._table = None
        self._catalog = None
        self._opened = False
        logger.info(
            "Iceberg sink closed",
            total_written=self._total_written,
            table=f"{self._namespace}.{self._table_name}",
        )

    def health_check(self) -> HealthStatus:
        """Check whether the Iceberg table is accessible."""
        if self._catalog is None or self._table is None:
            return HealthStatus.UNHEALTHY
        try:
            _ = self._table.current_snapshot()
            return HealthStatus.HEALTHY
        except Exception:
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Writing
    # ------------------------------------------------------------------

    def write_batch(self, records: list[Record]) -> int:
        """Convert records to a PyArrow table and append/overwrite the Iceberg table."""
        if not self._opened:
            raise RuntimeError("Sink not opened. Call open() first.")
        if not records:
            return 0

        rows = [r.data for r in records]
        arrow_table = pa.Table.from_pylist(rows)

        try:
            if self._mode == "overwrite":
                self._table.overwrite(arrow_table)
            else:
                self._table.append(arrow_table)
        except Exception as exc:
            raise RuntimeError(
                f"Iceberg write ({self._mode}) failed for "
                f"'{self._namespace}.{self._table_name}': {exc}"
            ) from exc

        written = len(records)
        self._total_written += written
        return written

    # ------------------------------------------------------------------
    # Schema evolution
    # ------------------------------------------------------------------

    def evolve_schema(self, new_schema: Schema) -> None:
        """Use Iceberg's schema evolution API to add new columns."""
        if self._table is None:
            raise RuntimeError("Sink not opened. Call open() first.")

        existing_names = {field.name for field in self._table.schema().fields}

        with self._table.update_schema() as update:
            for col in new_schema.columns:
                if col.name not in existing_names:
                    iceberg_type = _ICEBERG_TYPE_MAP.get(col.type, StringType())
                    required = not col.nullable
                    update.add_column(
                        path=col.name,
                        field_type=iceberg_type,
                        required=required,
                    )
                    logger.info(
                        "Iceberg schema evolved — added column",
                        column=col.name,
                        iceberg_type=str(iceberg_type),
                    )

        self._schema_obj = new_schema

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _create_table(self, schema: Schema, identifier: str) -> Any:
        """Create a new Iceberg table from a Ranger Schema."""
        iceberg_schema = _build_iceberg_schema(schema)
        partition_spec = _build_partition_spec(
            self._partition_spec_cfg, iceberg_schema
        )

        try:
            table = self._catalog.create_table(
                identifier=identifier,
                schema=iceberg_schema,
                partition_spec=partition_spec,
            )
            return table
        except Exception as exc:
            raise RuntimeError(
                f"Failed to create Iceberg table '{identifier}': {exc}"
            ) from exc
