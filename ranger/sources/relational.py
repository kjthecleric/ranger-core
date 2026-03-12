"""Relational database source — PostgreSQL, MySQL, SQL Server, Oracle.

Uses SQLAlchemy for cross-database connectivity with support for full-table
and incremental reads via a configurable watermark column.
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
    import sqlalchemy as sa
    from sqlalchemy import inspect as sa_inspect, text
    from sqlalchemy.engine import Engine
except ImportError as _sa_err:
    raise ImportError(
        "SQLAlchemy is required for RelationalSource. "
        "Install it with: pip install ranger-core[relational]"
    ) from _sa_err


# ---------------------------------------------------------------------------
# SQLAlchemy type → Ranger ColumnType mapping
# ---------------------------------------------------------------------------

_SA_TYPE_MAP: dict[str, ColumnType] = {
    "BIGINT": ColumnType.INT64,
    "INTEGER": ColumnType.INT32,
    "SMALLINT": ColumnType.INT16,
    "TINYINT": ColumnType.INT8,
    "FLOAT": ColumnType.FLOAT64,
    "REAL": ColumnType.FLOAT32,
    "DOUBLE": ColumnType.FLOAT64,
    "DOUBLE_PRECISION": ColumnType.FLOAT64,
    "NUMERIC": ColumnType.DECIMAL,
    "DECIMAL": ColumnType.DECIMAL,
    "VARCHAR": ColumnType.STRING,
    "NVARCHAR": ColumnType.STRING,
    "CHAR": ColumnType.STRING,
    "TEXT": ColumnType.STRING,
    "CLOB": ColumnType.STRING,
    "BOOLEAN": ColumnType.BOOLEAN,
    "DATE": ColumnType.DATE,
    "TIME": ColumnType.TIME,
    "DATETIME": ColumnType.TIMESTAMP,
    "TIMESTAMP": ColumnType.TIMESTAMP,
    "BLOB": ColumnType.BLOB,
    "BYTEA": ColumnType.BINARY,
    "BINARY": ColumnType.BINARY,
    "VARBINARY": ColumnType.BINARY,
    "JSON": ColumnType.JSON,
    "JSONB": ColumnType.JSON,
    "UUID": ColumnType.UUID,
    "ARRAY": ColumnType.ARRAY,
}


def _map_sa_type(sa_type: Any) -> ColumnType:
    """Map a SQLAlchemy column type to a Ranger :class:`ColumnType`."""
    type_name = type(sa_type).__name__.upper()
    return _SA_TYPE_MAP.get(type_name, ColumnType.STRING)


class RelationalSource(BaseSource):
    """Read data from relational databases via SQLAlchemy.

    Config keys:
        connection_string: SQLAlchemy connection URL
            (e.g. ``postgresql+psycopg2://user:pass@host/db``).
        table:  Table name to read.  Mutually exclusive with *query*.
        query:  Raw SQL query.  Mutually exclusive with *table*.
        schema: Database schema (default: public / dbo depending on dialect).
        incremental_column: Column used for incremental reads.
        last_value: Last-seen value for the incremental column (cursor).
        batch_size: Number of rows fetched per server-side cursor chunk
            (default: ``10_000``).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._engine: Engine | None = None
        self._connection: Any | None = None  # sa.Connection

    # ------------------------------------------------------------------
    # Metadata properties
    # ------------------------------------------------------------------

    @property
    def source_type(self) -> str:
        return "relational"

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
        connection_string = self._config.get("connection_string")
        if not connection_string:
            raise ConnectionError("Missing required config key 'connection_string'")

        try:
            self._engine = sa.create_engine(
                connection_string,
                pool_pre_ping=True,
            )
            # Eagerly verify the connection
            self._connection = self._engine.connect()
            self._connected = True
            logger.info(
                "relational_source.connected",
                dialect=self._engine.dialect.name,
            )
        except Exception as exc:
            self._connected = False
            logger.error("relational_source.connect_failed", error=str(exc))
            raise ConnectionError(f"Failed to connect: {exc}") from exc

    def close(self) -> None:
        if self._connection is not None:
            try:
                self._connection.close()
            except Exception:
                pass
            self._connection = None

        if self._engine is not None:
            self._engine.dispose()
            self._engine = None

        self._connected = False
        logger.info("relational_source.closed")

    def health_check(self) -> HealthStatus:
        """Run a lightweight ``SELECT 1`` probe."""
        try:
            if self._engine is None:
                self.connect()
            assert self._engine is not None
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return HealthStatus.HEALTHY
        except Exception as exc:
            logger.warning("relational_source.health_check_failed", error=str(exc))
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Reading
    # ------------------------------------------------------------------

    def _build_query(self) -> str:
        """Build the SQL query string from config."""
        raw_query: str | None = self._config.get("query")
        if raw_query:
            return raw_query

        table: str | None = self._config.get("table")
        if not table:
            raise ValueError("Config must include either 'table' or 'query'")

        db_schema: str | None = self._config.get("schema")
        qualified = f"{db_schema}.{table}" if db_schema else table
        base_sql = f"SELECT * FROM {qualified}"

        # Incremental predicate
        inc_col: str | None = self._config.get("incremental_column")
        last_value = self._config.get("last_value")
        if inc_col and last_value is not None:
            base_sql += f" WHERE {inc_col} > :last_value"

        return base_sql

    def read(self) -> Iterator[Record]:
        """Yield :class:`Record` objects, one per row."""
        if self._connection is None:
            raise RuntimeError("Source not connected — call connect() first")

        query_str = self._build_query()
        batch_size: int = self._config.get("batch_size", 10_000)

        params: dict[str, Any] = {}
        last_value = self._config.get("last_value")
        if last_value is not None:
            params["last_value"] = last_value

        logger.info(
            "relational_source.read_start",
            query=query_str[:200],
            batch_size=batch_size,
        )

        result = self._connection.execution_options(
            stream_results=True,
            yield_per=batch_size,
        ).execute(text(query_str), params)

        columns = list(result.keys())
        row_count = 0
        for row in result:
            data = dict(zip(columns, row))
            yield Record(
                data=data,
                event_time=datetime.now(timezone.utc),
                source_metadata={
                    "source_type": "relational",
                    "table": self._config.get("table", "custom_query"),
                },
            )
            row_count += 1

        logger.info("relational_source.read_complete", rows=row_count)

    def read_batch(self, batch_size: int = 10_000) -> Iterator[list[Record]]:
        """Yield batches of records from the database."""
        if self._connection is None:
            raise RuntimeError("Source not connected — call connect() first")

        query_str = self._build_query()
        params: dict[str, Any] = {}
        last_value = self._config.get("last_value")
        if last_value is not None:
            params["last_value"] = last_value

        result = self._connection.execution_options(
            stream_results=True,
            yield_per=batch_size,
        ).execute(text(query_str), params)

        columns = list(result.keys())
        batch: list[Record] = []

        for row in result:
            data = dict(zip(columns, row))
            batch.append(
                Record(
                    data=data,
                    event_time=datetime.now(timezone.utc),
                    source_metadata={
                        "source_type": "relational",
                        "table": self._config.get("table", "custom_query"),
                    },
                )
            )
            if len(batch) >= batch_size:
                yield batch
                batch = []

        if batch:
            yield batch

    # ------------------------------------------------------------------
    # Schema operations
    # ------------------------------------------------------------------

    def get_schema(self) -> Schema:
        """Return the schema by inspecting the target table metadata."""
        if self._engine is None:
            raise RuntimeError("Source not connected — call connect() first")

        table = self._config.get("table")
        if not table:
            # For raw queries, fall back to a sample-based inference
            return self._infer_schema_from_sample()

        db_schema: str | None = self._config.get("schema")
        inspector = sa_inspect(self._engine)

        columns: list[ColumnDefinition] = []
        for col_info in inspector.get_columns(table, schema=db_schema):
            col_type = _map_sa_type(col_info["type"])
            columns.append(
                ColumnDefinition(
                    name=col_info["name"],
                    type=col_type,
                    nullable=col_info.get("nullable", True),
                    default_value=str(col_info["default"]) if col_info.get("default") else None,
                )
            )

        # Primary key detection
        pk = inspector.get_pk_constraint(table, schema=db_schema)
        pk_columns: list[str] | None = pk.get("constrained_columns") if pk else None

        return Schema(columns=columns, primary_key=pk_columns or None)

    def _infer_schema_from_sample(self) -> Schema:
        """Infer schema from a small sample when no table name is available."""
        sample_records: list[Record] = []
        for record in self.read():
            sample_records.append(record)
            if len(sample_records) >= 100:
                break

        if not sample_records:
            return Schema(columns=[])

        all_keys: set[str] = set()
        for rec in sample_records:
            all_keys.update(rec.data.keys())

        columns: list[ColumnDefinition] = []
        for key in sorted(all_keys):
            col_type = self._infer_python_type(key, [r.data for r in sample_records])
            columns.append(ColumnDefinition(name=key, type=col_type, nullable=True))

        return Schema(columns=columns)

    @staticmethod
    def _infer_python_type(key: str, sample: list[dict[str, Any]]) -> ColumnType:
        """Infer :class:`ColumnType` from Python values."""
        types_seen: set[str] = set()
        for row in sample:
            val = row.get(key)
            if val is not None:
                types_seen.add(type(val).__name__)

        types_seen.discard("NoneType")
        if not types_seen:
            return ColumnType.STRING
        if types_seen == {"int"}:
            return ColumnType.INT64
        if "float" in types_seen:
            return ColumnType.FLOAT64
        if types_seen == {"bool"}:
            return ColumnType.BOOLEAN
        return ColumnType.STRING

    def discover_schema(self) -> DiscoveredSchema:
        """Discover schema with catalog metadata via SQLAlchemy inspector."""
        schema = self.get_schema()
        table = self._config.get("table", "custom_query")
        db_schema = self._config.get("schema")

        row_count: int | None = None
        if self._engine and self._config.get("table"):
            try:
                qualified = f"{db_schema}.{table}" if db_schema else table
                with self._engine.connect() as conn:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {qualified}"))
                    row_count = result.scalar()
            except Exception:
                pass

        return DiscoveredSchema(
            columns=schema.columns,
            primary_key=schema.primary_key,
            source_name="relational",
            object_name=table,
            object_type="table",
            row_count_estimate=row_count,
            source_metadata={
                "dialect": self._engine.dialect.name if self._engine else "unknown",
                "schema": db_schema,
            },
        )
