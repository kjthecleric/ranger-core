"""Relational database sinks — PostgreSQL and MySQL.

Uses SQLAlchemy for connectivity and provides bulk insert, upsert, and
automatic table creation / schema evolution capabilities.
"""

from __future__ import annotations

import json
from abc import abstractmethod
from typing import Any

import structlog

from ranger.core.models import ColumnType, HealthStatus, Record, Schema
from ranger.sinks.base import BaseSink

try:
    import sqlalchemy as sa
    from sqlalchemy import (
        Column,
        MetaData,
        Table,
        create_engine,
        inspect,
        text,
    )
    from sqlalchemy.engine import Connection, Engine

    _HAS_SQLALCHEMY = True
except ImportError:  # pragma: no cover
    _HAS_SQLALCHEMY = False

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Ranger ColumnType → generic SQLAlchemy type mapping
# ---------------------------------------------------------------------------

_GENERIC_TYPE_MAP: dict[ColumnType, str] = {
    ColumnType.BOOLEAN: "BOOLEAN",
    ColumnType.INT8: "SMALLINT",
    ColumnType.INT16: "SMALLINT",
    ColumnType.INT32: "INTEGER",
    ColumnType.INT64: "BIGINT",
    ColumnType.FLOAT32: "REAL",
    ColumnType.FLOAT64: "DOUBLE PRECISION",
    ColumnType.DECIMAL: "NUMERIC",
    ColumnType.STRING: "TEXT",
    ColumnType.BINARY: "BYTEA",
    ColumnType.DATE: "DATE",
    ColumnType.TIME: "TIME",
    ColumnType.TIMESTAMP: "TIMESTAMP",
    ColumnType.TIMESTAMP_TZ: "TIMESTAMP WITH TIME ZONE",
    ColumnType.JSON: "JSON",
    ColumnType.ARRAY: "JSON",
    ColumnType.MAP: "JSON",
    ColumnType.STRUCT: "JSON",
    ColumnType.UUID: "VARCHAR(36)",
}


# ---------------------------------------------------------------------------
# Base relational sink
# ---------------------------------------------------------------------------


class _RelationalSink(BaseSink):
    """Internal base class shared by PostgreSQL and MySQL sinks.

    Handles SQLAlchemy engine management, table creation, batched writes,
    and schema evolution via ``ALTER TABLE``.
    """

    def __init__(self, config: dict[str, Any]) -> None:
        if not _HAS_SQLALCHEMY:
            raise ImportError(
                "sqlalchemy is required for relational sinks. "
                "Install it with: pip install sqlalchemy"
            )
        super().__init__(config)

        self._connection_string: str = config["connection_string"]
        self._table_name: str = config["table"]
        self._db_schema: str | None = config.get("schema")
        self._if_exists: str = config.get("if_exists", "append")  # append / replace / fail
        self._auto_create_table: bool = config.get("auto_create_table", False)
        self._batch_size: int = config.get("batch_size", 1000)
        self._merge_keys: list[str] = config.get("merge_keys", [])

        self._engine: Engine | None = None
        self._connection: Connection | None = None
        self._schema: Schema | None = None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _sql_type_for(self, col_type: ColumnType) -> str:
        """Return the SQL type string for a given Ranger column type."""
        return _GENERIC_TYPE_MAP.get(col_type, "TEXT")

    def _fully_qualified_table(self) -> str:
        if self._db_schema:
            return f"{self._db_schema}.{self._table_name}"
        return self._table_name

    def _create_table_ddl(self, schema: Schema) -> str:
        """Generate CREATE TABLE IF NOT EXISTS DDL from a Ranger Schema."""
        fq = self._fully_qualified_table()
        col_defs: list[str] = []
        for col in schema.columns:
            sql_type = self._sql_type_for(col.type)
            nullable = "" if col.nullable else " NOT NULL"
            col_defs.append(f"  {col.name} {sql_type}{nullable}")
        cols_sql = ",\n".join(col_defs)
        return f"CREATE TABLE IF NOT EXISTS {fq} (\n{cols_sql}\n)"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def open(self, schema: Schema) -> None:
        self._schema = schema
        self._engine = create_engine(self._connection_string)
        self._connection = self._engine.connect()

        if self._if_exists == "replace":
            fq = self._fully_qualified_table()
            self._connection.execute(text(f"DROP TABLE IF EXISTS {fq}"))
            self._connection.commit()
            logger.info("relational_sink.table_dropped", table=fq)

        if self._auto_create_table or self._if_exists == "replace":
            ddl = self._create_table_ddl(schema)
            self._connection.execute(text(ddl))
            self._connection.commit()
            logger.info("relational_sink.table_ensured", table=self._fully_qualified_table())

        if self._if_exists == "fail":
            insp = inspect(self._engine)
            if insp.has_table(self._table_name, schema=self._db_schema):
                raise RuntimeError(
                    f"Table {self._fully_qualified_table()} already exists and if_exists='fail'"
                )

        self._opened = True
        logger.info(
            "relational_sink.opened",
            sink_type=self.sink_type,
            table=self._fully_qualified_table(),
        )

    def close(self) -> None:
        if self._connection is not None:
            try:
                self._connection.commit()
            except Exception:
                logger.warning("relational_sink.commit_failed_on_close", exc_info=True)
            finally:
                self._connection.close()
                self._connection = None
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
        self._opened = False
        logger.info("relational_sink.closed", sink_type=self.sink_type)

    def health_check(self) -> HealthStatus:
        try:
            if self._connection is None:
                return HealthStatus.UNHEALTHY
            self._connection.execute(text("SELECT 1"))
            return HealthStatus.HEALTHY
        except Exception:
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Writing
    # ------------------------------------------------------------------

    def write_batch(self, records: list[Record]) -> int:
        if not self._opened or self._connection is None:
            raise RuntimeError("Sink not opened. Call open() first.")
        if not records:
            return 0

        if self._schema is None:
            raise RuntimeError("Schema not set. Call open() first.")

        columns = self._schema.column_names()
        written = 0

        for i in range(0, len(records), self._batch_size):
            batch = records[i : i + self._batch_size]
            rows = [{col: rec.data.get(col) for col in columns} for rec in batch]

            if self._merge_keys:
                written += self._upsert_rows(columns, rows)
            else:
                written += self._insert_rows(columns, rows)

        self._connection.commit()
        logger.info("relational_sink.batch_written", count=written, table=self._fully_qualified_table())
        return written

    def _insert_rows(self, columns: list[str], rows: list[dict[str, Any]]) -> int:
        """Plain INSERT via executemany."""
        fq = self._fully_qualified_table()
        placeholders = ", ".join(f":{col}" for col in columns)
        col_list = ", ".join(columns)
        stmt = text(f"INSERT INTO {fq} ({col_list}) VALUES ({placeholders})")
        self._connection.execute(stmt, rows)  # type: ignore[union-attr]
        return len(rows)

    @abstractmethod
    def _upsert_rows(self, columns: list[str], rows: list[dict[str, Any]]) -> int:
        """Database-specific upsert implementation."""
        ...

    # ------------------------------------------------------------------
    # Schema evolution
    # ------------------------------------------------------------------

    def evolve_schema(self, new_schema: Schema) -> None:
        if self._connection is None:
            raise RuntimeError("Sink not opened. Call open() first.")

        existing_names = set()
        if self._schema:
            existing_names = set(self._schema.column_names())

        fq = self._fully_qualified_table()
        for col in new_schema.columns:
            if col.name not in existing_names:
                sql_type = self._sql_type_for(col.type)
                alter = f"ALTER TABLE {fq} ADD COLUMN {col.name} {sql_type}"
                self._connection.execute(text(alter))
                logger.info(
                    "relational_sink.column_added",
                    table=fq,
                    column=col.name,
                    sql_type=sql_type,
                )

        self._connection.commit()
        self._schema = new_schema
        logger.info("relational_sink.schema_evolved", table=fq)


# ---------------------------------------------------------------------------
# PostgreSQL sink
# ---------------------------------------------------------------------------


class PostgreSQLSink(_RelationalSink):
    """Write records to a PostgreSQL table.

    Supports PostgreSQL-native ``INSERT ... ON CONFLICT DO UPDATE`` for upsert
    when ``merge_keys`` are configured.

    Config keys:
        connection_string: SQLAlchemy-compatible PostgreSQL connection URL (required).
        table: Target table name (required).
        schema: Database schema (optional, e.g. ``public``).
        if_exists: ``append`` | ``replace`` | ``fail`` (default ``append``).
        auto_create_table: Auto-create table from Ranger schema (default ``False``).
        batch_size: Number of rows per INSERT (default ``1000``).
        merge_keys: List of columns to use as conflict target for upsert.
    """

    @property
    def sink_type(self) -> str:
        return "postgresql"

    def _upsert_rows(self, columns: list[str], rows: list[dict[str, Any]]) -> int:
        """PostgreSQL UPSERT via INSERT ... ON CONFLICT DO UPDATE."""
        fq = self._fully_qualified_table()
        col_list = ", ".join(columns)
        placeholders = ", ".join(f":{col}" for col in columns)
        conflict_cols = ", ".join(self._merge_keys)
        update_cols = [c for c in columns if c not in self._merge_keys]
        set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

        if update_cols:
            stmt = text(
                f"INSERT INTO {fq} ({col_list}) VALUES ({placeholders}) "
                f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {set_clause}"
            )
        else:
            stmt = text(
                f"INSERT INTO {fq} ({col_list}) VALUES ({placeholders}) "
                f"ON CONFLICT ({conflict_cols}) DO NOTHING"
            )

        self._connection.execute(stmt, rows)  # type: ignore[union-attr]
        return len(rows)


# ---------------------------------------------------------------------------
# MySQL sink
# ---------------------------------------------------------------------------


class MySQLSink(_RelationalSink):
    """Write records to a MySQL table.

    Supports MySQL-native ``INSERT ... ON DUPLICATE KEY UPDATE`` for upsert
    when ``merge_keys`` are configured.

    Config keys:
        connection_string: SQLAlchemy-compatible MySQL connection URL (required).
        table: Target table name (required).
        schema: Database schema (optional).
        if_exists: ``append`` | ``replace`` | ``fail`` (default ``append``).
        auto_create_table: Auto-create table from Ranger schema (default ``False``).
        batch_size: Number of rows per INSERT (default ``1000``).
        merge_keys: List of columns to use for ON DUPLICATE KEY upsert.
    """

    # Override generic types with MySQL-specific types where they differ
    _MYSQL_TYPE_OVERRIDES: dict[ColumnType, str] = {
        ColumnType.BOOLEAN: "TINYINT(1)",
        ColumnType.BINARY: "LONGBLOB",
        ColumnType.FLOAT64: "DOUBLE",
        ColumnType.JSON: "JSON",
        ColumnType.TIMESTAMP_TZ: "TIMESTAMP",
    }

    def _sql_type_for(self, col_type: ColumnType) -> str:
        return self._MYSQL_TYPE_OVERRIDES.get(col_type, _GENERIC_TYPE_MAP.get(col_type, "TEXT"))

    @property
    def sink_type(self) -> str:
        return "mysql"

    def _upsert_rows(self, columns: list[str], rows: list[dict[str, Any]]) -> int:
        """MySQL UPSERT via INSERT ... ON DUPLICATE KEY UPDATE."""
        fq = self._fully_qualified_table()
        col_list = ", ".join(columns)
        placeholders = ", ".join(f":{col}" for col in columns)
        update_cols = [c for c in columns if c not in self._merge_keys]
        set_clause = ", ".join(f"{c} = VALUES({c})" for c in update_cols)

        if update_cols:
            stmt = text(
                f"INSERT INTO {fq} ({col_list}) VALUES ({placeholders}) "
                f"ON DUPLICATE KEY UPDATE {set_clause}"
            )
        else:
            # No columns to update — use INSERT IGNORE
            stmt = text(f"INSERT IGNORE INTO {fq} ({col_list}) VALUES ({placeholders})")

        self._connection.execute(stmt, rows)  # type: ignore[union-attr]
        return len(rows)
