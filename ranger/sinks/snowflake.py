"""Snowflake sink — writes records to Snowflake tables.

Supports INSERT, COPY INTO, and PUT write methods with optional auto-create
table and upsert via MERGE.
"""

from __future__ import annotations

import json
import tempfile
from typing import Any

import structlog

from ranger.core.models import ColumnType, HealthStatus, Record, Schema
from ranger.sinks.base import BaseSink

try:
    import snowflake.connector
    from snowflake.connector import DictCursor, ProgrammingError
    from snowflake.connector.connection import SnowflakeConnection

    _HAS_SNOWFLAKE = True
except ImportError:  # pragma: no cover
    _HAS_SNOWFLAKE = False

logger = structlog.get_logger(__name__)

# Map Ranger column types to Snowflake SQL types
_TYPE_MAP: dict[ColumnType, str] = {
    ColumnType.BOOLEAN: "BOOLEAN",
    ColumnType.INT8: "SMALLINT",
    ColumnType.INT16: "SMALLINT",
    ColumnType.INT32: "INTEGER",
    ColumnType.INT64: "BIGINT",
    ColumnType.FLOAT32: "FLOAT",
    ColumnType.FLOAT64: "DOUBLE",
    ColumnType.DECIMAL: "NUMBER",
    ColumnType.STRING: "VARCHAR",
    ColumnType.BINARY: "BINARY",
    ColumnType.DATE: "DATE",
    ColumnType.TIME: "TIME",
    ColumnType.TIMESTAMP: "TIMESTAMP_NTZ",
    ColumnType.TIMESTAMP_TZ: "TIMESTAMP_TZ",
    ColumnType.JSON: "VARIANT",
    ColumnType.ARRAY: "ARRAY",
    ColumnType.MAP: "OBJECT",
    ColumnType.STRUCT: "OBJECT",
    ColumnType.UUID: "VARCHAR(36)",
}


class SnowflakeSink(BaseSink):
    """Write records to a Snowflake table.

    Config keys:
        account: Snowflake account identifier (required).
        user: Snowflake username (required).
        password: Snowflake password (required).
        database: Target database (required).
        schema: Target schema (required).
        warehouse: Compute warehouse (required).
        table: Target table name (required).
        role: Snowflake role (optional).
        write_method: ``INSERT``, ``COPY``, or ``PUT`` (default ``INSERT``).
        stage_name: Named stage for COPY/PUT methods (default ``~``).
        auto_create_table: Whether to create the table if it does not exist
            (default ``False``).
        merge_keys: List of columns for upsert via MERGE (optional).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        if not _HAS_SNOWFLAKE:
            raise ImportError(
                "snowflake-connector-python is required for SnowflakeSink. "
                "Install it with: pip install snowflake-connector-python"
            )
        super().__init__(config)

        self._account: str = config["account"]
        self._user: str = config["user"]
        self._password: str = config["password"]
        self._database: str = config["database"]
        self._sf_schema: str = config["schema"]
        self._warehouse: str = config["warehouse"]
        self._table: str = config["table"]
        self._role: str | None = config.get("role")
        self._write_method: str = config.get("write_method", "INSERT").upper()
        self._stage_name: str = config.get("stage_name", "~")
        self._auto_create_table: bool = config.get("auto_create_table", False)
        self._merge_keys: list[str] = config.get("merge_keys", [])

        if self._write_method not in ("INSERT", "COPY", "PUT"):
            raise ValueError(f"Unsupported write_method: {self._write_method}")

        self._conn: Any = None
        self._schema_obj: Schema | None = None
        self._total_written: int = 0

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def sink_type(self) -> str:
        return "snowflake"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def open(self, schema: Schema) -> None:
        """Connect to Snowflake and optionally create the target table."""
        self._schema_obj = schema

        connect_kwargs: dict[str, Any] = {
            "account": self._account,
            "user": self._user,
            "password": self._password,
            "database": self._database,
            "schema": self._sf_schema,
            "warehouse": self._warehouse,
        }
        if self._role:
            connect_kwargs["role"] = self._role

        try:
            self._conn = snowflake.connector.connect(**connect_kwargs)
        except Exception as exc:
            raise RuntimeError(f"Failed to connect to Snowflake: {exc}") from exc

        if self._auto_create_table:
            self._create_table_if_not_exists(schema)

        self._opened = True
        logger.info(
            "Snowflake sink opened",
            database=self._database,
            schema=self._sf_schema,
            table=self._table,
            write_method=self._write_method,
        )

    def close(self) -> None:
        """Close the Snowflake connection."""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                logger.warning("Error closing Snowflake connection", exc_info=True)
            self._conn = None
        self._opened = False
        logger.info(
            "Snowflake sink closed",
            total_written=self._total_written,
            table=self._table,
        )

    def health_check(self) -> HealthStatus:
        """Run a lightweight query to verify the connection."""
        if self._conn is None:
            return HealthStatus.UNHEALTHY
        try:
            cur = self._conn.cursor()
            cur.execute("SELECT 1")
            cur.close()
            return HealthStatus.HEALTHY
        except Exception:
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Writing
    # ------------------------------------------------------------------

    def write_batch(self, records: list[Record]) -> int:
        """Write a batch of records using the configured method."""
        if not self._opened:
            raise RuntimeError("Sink not opened. Call open() first.")
        if not records:
            return 0

        if self._merge_keys:
            written = self._write_merge(records)
        elif self._write_method == "INSERT":
            written = self._write_insert(records)
        elif self._write_method in ("COPY", "PUT"):
            written = self._write_copy(records)
        else:
            raise ValueError(f"Unsupported write_method: {self._write_method}")

        self._total_written += written
        return written

    # ------------------------------------------------------------------
    # Schema evolution
    # ------------------------------------------------------------------

    def evolve_schema(self, new_schema: Schema) -> None:
        """ALTER TABLE ADD COLUMN for compatible schema changes."""
        if self._conn is None:
            raise RuntimeError("Sink not opened. Call open() first.")

        existing_columns = self._get_existing_columns()
        cur = self._conn.cursor()

        try:
            for col in new_schema.columns:
                if col.name.upper() not in existing_columns:
                    sf_type = _TYPE_MAP.get(col.type, "VARCHAR")
                    null_clause = "" if col.nullable else " NOT NULL"
                    ddl = (
                        f"ALTER TABLE {self._fqtn()} "
                        f"ADD COLUMN {_quote(col.name)} {sf_type}{null_clause}"
                    )
                    cur.execute(ddl)
                    logger.info(
                        "Snowflake schema evolved — added column",
                        column=col.name,
                        sf_type=sf_type,
                    )
        finally:
            cur.close()

        self._schema_obj = new_schema

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fqtn(self) -> str:
        """Fully-qualified table name."""
        return f"{_quote(self._database)}.{_quote(self._sf_schema)}.{_quote(self._table)}"

    def _create_table_if_not_exists(self, schema: Schema) -> None:
        """Create the target table based on the Ranger schema."""
        col_defs: list[str] = []
        for col in schema.columns:
            sf_type = _TYPE_MAP.get(col.type, "VARCHAR")
            null_clause = "" if col.nullable else " NOT NULL"
            col_defs.append(f"  {_quote(col.name)} {sf_type}{null_clause}")

        columns_sql = ",\n".join(col_defs)
        ddl = f"CREATE TABLE IF NOT EXISTS {self._fqtn()} (\n{columns_sql}\n)"

        cur = self._conn.cursor()
        try:
            cur.execute(ddl)
            logger.info("Ensured table exists", table=self._fqtn())
        finally:
            cur.close()

    def _get_existing_columns(self) -> set[str]:
        """Return the set of column names (upper-cased) for the target table."""
        cur = self._conn.cursor(DictCursor)
        try:
            cur.execute(f"DESCRIBE TABLE {self._fqtn()}")
            return {row["name"].upper() for row in cur}
        finally:
            cur.close()

    def _write_insert(self, records: list[Record]) -> int:
        """Bulk insert via executemany."""
        if not records:
            return 0

        columns = list(records[0].data.keys())
        placeholders = ", ".join(["%s"] * len(columns))
        cols_quoted = ", ".join(_quote(c) for c in columns)
        sql = f"INSERT INTO {self._fqtn()} ({cols_quoted}) VALUES ({placeholders})"

        rows = [tuple(r.data.get(c) for c in columns) for r in records]

        cur = self._conn.cursor()
        try:
            cur.executemany(sql, rows)
            return len(rows)
        except ProgrammingError as exc:
            raise RuntimeError(f"Snowflake INSERT failed: {exc}") from exc
        finally:
            cur.close()

    def _write_copy(self, records: list[Record]) -> int:
        """Write via COPY INTO from a staged NDJSON file."""
        if not records:
            return 0

        ndjson_lines = "\n".join(
            json.dumps(r.data, default=str) for r in records
        )

        with tempfile.NamedTemporaryFile(suffix=".json", mode="w", delete=False) as tmp:
            tmp.write(ndjson_lines)
            tmp_path = tmp.name

        cur = self._conn.cursor()
        try:
            # PUT file to stage
            cur.execute(f"PUT 'file://{tmp_path}' @{self._stage_name} AUTO_COMPRESS=TRUE")

            # COPY INTO table
            import os

            staged_file = os.path.basename(tmp_path)
            copy_sql = (
                f"COPY INTO {self._fqtn()} "
                f"FROM @{self._stage_name}/{staged_file}.gz "
                f"FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE) "
                f"MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE"
            )
            cur.execute(copy_sql)
            return len(records)
        except ProgrammingError as exc:
            raise RuntimeError(f"Snowflake COPY INTO failed: {exc}") from exc
        finally:
            cur.close()
            # Clean up temp file
            try:
                import os

                os.unlink(tmp_path)
            except OSError:
                pass

    def _write_merge(self, records: list[Record]) -> int:
        """Upsert records via MERGE using merge_keys."""
        if not records:
            return 0

        columns = list(records[0].data.keys())
        non_key_columns = [c for c in columns if c not in self._merge_keys]

        # Create a temp table with the same shape
        temp_table = f"__ranger_merge_tmp_{self._table}"
        temp_fqtn = f"{_quote(self._database)}.{_quote(self._sf_schema)}.{_quote(temp_table)}"

        cur = self._conn.cursor()
        try:
            cur.execute(
                f"CREATE TEMPORARY TABLE {temp_fqtn} LIKE {self._fqtn()}"
            )

            # Insert records into temp table
            placeholders = ", ".join(["%s"] * len(columns))
            cols_quoted = ", ".join(_quote(c) for c in columns)
            insert_sql = f"INSERT INTO {temp_fqtn} ({cols_quoted}) VALUES ({placeholders})"
            rows = [tuple(r.data.get(c) for c in columns) for r in records]
            cur.executemany(insert_sql, rows)

            # Build MERGE statement
            join_cond = " AND ".join(
                f"target.{_quote(k)} = src.{_quote(k)}" for k in self._merge_keys
            )
            update_set = ", ".join(
                f"target.{_quote(c)} = src.{_quote(c)}" for c in non_key_columns
            )
            insert_cols = ", ".join(_quote(c) for c in columns)
            insert_vals = ", ".join(f"src.{_quote(c)}" for c in columns)

            merge_sql = (
                f"MERGE INTO {self._fqtn()} AS target "
                f"USING {temp_fqtn} AS src ON {join_cond} "
            )
            if non_key_columns:
                merge_sql += f"WHEN MATCHED THEN UPDATE SET {update_set} "
            merge_sql += (
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
            )

            cur.execute(merge_sql)
            cur.execute(f"DROP TABLE IF EXISTS {temp_fqtn}")
            return len(records)
        except ProgrammingError as exc:
            raise RuntimeError(f"Snowflake MERGE failed: {exc}") from exc
        finally:
            cur.close()


def _quote(identifier: str) -> str:
    """Quote a Snowflake identifier."""
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'
