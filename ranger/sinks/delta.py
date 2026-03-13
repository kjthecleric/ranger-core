"""Delta Lake sink — writes records to Delta tables using delta-rs.

Supports append, overwrite, and merge modes with Hive-style partitioning
and schema evolution.
"""

from __future__ import annotations

from typing import Any

import structlog

from ranger.core.models import HealthStatus, Record, Schema
from ranger.sinks.base import BaseSink

try:
    import pyarrow as pa

    _HAS_ARROW = True
except ImportError:  # pragma: no cover
    _HAS_ARROW = False

try:
    import deltalake
    from deltalake import DeltaTable, write_deltalake

    _HAS_DELTA = True
except ImportError:  # pragma: no cover
    _HAS_DELTA = False

logger = structlog.get_logger(__name__)


class DeltaLakeSink(BaseSink):
    """Write records to a Delta Lake table.

    Config keys:
        table_path: Path to the Delta table — local filesystem path or cloud
            URI such as ``s3://bucket/path`` (required).
        mode: Write mode — ``append``, ``overwrite``, or ``merge``
            (default ``append``).
        partition_by: List of column names for Hive-style partitioning (optional).
        storage_options: Dict of storage options for cloud backends
            (e.g. AWS keys, Azure SAS tokens) (optional).
        merge_predicate: SQL-style predicate for merge operations, e.g.
            ``"source.id = target.id"`` (required when ``mode='merge'``).
        schema_mode: How to handle schema evolution — ``strict``, ``merge``,
            or ``overwrite`` (default ``merge``).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        if not _HAS_DELTA:
            raise ImportError(
                "deltalake (delta-rs) is required for DeltaLakeSink. "
                "Install it with: pip install deltalake"
            )
        if not _HAS_ARROW:
            raise ImportError(
                "pyarrow is required for DeltaLakeSink. "
                "Install it with: pip install pyarrow"
            )
        super().__init__(config)

        self._table_path: str = config["table_path"]
        self._mode: str = config.get("mode", "append").lower()
        self._partition_by: list[str] = config.get("partition_by", [])
        self._storage_options: dict[str, str] = config.get("storage_options", {})
        self._merge_predicate: str | None = config.get("merge_predicate")
        self._schema_mode: str = config.get("schema_mode", "merge").lower()

        if self._mode not in ("append", "overwrite", "merge"):
            raise ValueError(f"Unsupported mode: {self._mode}")
        if self._mode == "merge" and not self._merge_predicate:
            raise ValueError(
                "merge_predicate is required when mode='merge'"
            )
        if self._schema_mode not in ("strict", "merge", "overwrite"):
            raise ValueError(f"Unsupported schema_mode: {self._schema_mode}")

        self._schema_obj: Schema | None = None
        self._dt: Any = None  # DeltaTable instance (may be None for new tables)
        self._total_written: int = 0

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def sink_type(self) -> str:
        return "delta"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def open(self, schema: Schema) -> None:
        """Try to load an existing Delta table, or prepare to create one."""
        self._schema_obj = schema

        try:
            self._dt = DeltaTable(
                self._table_path,
                storage_options=self._storage_options or None,
            )
            logger.info(
                "Delta Lake sink opened (existing table)",
                table_path=self._table_path,
                version=self._dt.version(),
            )
        except Exception:
            # Table does not exist yet — it will be created on first write
            self._dt = None
            logger.info(
                "Delta Lake sink opened (table will be created on first write)",
                table_path=self._table_path,
            )

        self._opened = True

    def close(self) -> None:
        """Release resources."""
        self._dt = None
        self._opened = False
        logger.info(
            "Delta Lake sink closed",
            total_written=self._total_written,
            table_path=self._table_path,
        )

    def health_check(self) -> HealthStatus:
        """Check whether the Delta table is accessible."""
        try:
            dt = DeltaTable(
                self._table_path,
                storage_options=self._storage_options or None,
            )
            _ = dt.version()
            return HealthStatus.HEALTHY
        except Exception:
            # Table might not exist yet — that's acceptable if we'll create it
            return HealthStatus.DEGRADED if self._opened else HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Writing
    # ------------------------------------------------------------------

    def write_batch(self, records: list[Record]) -> int:
        """Convert records to a PyArrow table and write to Delta Lake."""
        if not self._opened:
            raise RuntimeError("Sink not opened. Call open() first.")
        if not records:
            return 0

        rows = [r.data for r in records]
        table = pa.Table.from_pylist(rows)

        if self._mode == "merge":
            written = self._write_merge(table)
        else:
            written = self._write_direct(table)

        self._total_written += written
        return written

    # ------------------------------------------------------------------
    # Schema evolution
    # ------------------------------------------------------------------

    def evolve_schema(self, new_schema: Schema) -> None:
        """Handle schema evolution via the configured schema_mode.

        In ``merge`` mode, new columns are automatically added during writes.
        In ``overwrite`` mode, the entire schema is replaced.
        In ``strict`` mode, schema changes are rejected.
        """
        if self._schema_mode == "strict":
            logger.warning(
                "Schema evolution requested but schema_mode is strict; ignoring",
                table_path=self._table_path,
            )
            return

        # For merge/overwrite, delta-rs handles evolution during write
        # so we just update our internal schema reference
        self._schema_obj = new_schema
        logger.info(
            "Delta Lake schema evolved",
            table_path=self._table_path,
            schema_mode=self._schema_mode,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _write_direct(self, table: pa.Table) -> int:
        """Write a PyArrow table in append or overwrite mode."""
        write_kwargs: dict[str, Any] = {
            "table_or_uri": self._table_path,
            "data": table,
            "mode": self._mode,
            "storage_options": self._storage_options or None,
        }

        if self._partition_by:
            write_kwargs["partition_by"] = self._partition_by

        if self._schema_mode in ("merge", "overwrite"):
            write_kwargs["schema_mode"] = self._schema_mode

        try:
            write_deltalake(**write_kwargs)
        except Exception as exc:
            raise RuntimeError(
                f"Delta Lake write ({self._mode}) failed for "
                f"'{self._table_path}': {exc}"
            ) from exc

        # Refresh internal DeltaTable reference after write
        try:
            self._dt = DeltaTable(
                self._table_path,
                storage_options=self._storage_options or None,
            )
        except Exception:
            pass

        return table.num_rows

    def _write_merge(self, table: pa.Table) -> int:
        """Merge (upsert) a PyArrow table into the existing Delta table."""
        if self._dt is None:
            # Table doesn't exist yet — fall back to a simple write to create it
            logger.info(
                "Delta table does not exist yet; creating with initial write "
                "before merge is possible",
                table_path=self._table_path,
            )
            write_kwargs: dict[str, Any] = {
                "table_or_uri": self._table_path,
                "data": table,
                "mode": "append",
                "storage_options": self._storage_options or None,
            }
            if self._partition_by:
                write_kwargs["partition_by"] = self._partition_by
            write_deltalake(**write_kwargs)
            self._dt = DeltaTable(
                self._table_path,
                storage_options=self._storage_options or None,
            )
            return table.num_rows

        try:
            merger = (
                self._dt.merge(
                    source=table,
                    predicate=self._merge_predicate,
                    source_alias="source",
                    target_alias="target",
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
            )
            merger.execute()
        except Exception as exc:
            raise RuntimeError(
                f"Delta Lake merge failed for '{self._table_path}': {exc}"
            ) from exc

        # Refresh the DeltaTable reference
        try:
            self._dt = DeltaTable(
                self._table_path,
                storage_options=self._storage_options or None,
            )
        except Exception:
            pass

        return table.num_rows
