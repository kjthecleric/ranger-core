"""Google BigQuery sink — writes records to BigQuery tables.

Supports auto-create table, partitioning, clustering, append/truncate
write dispositions, and upsert via MERGE.
"""

from __future__ import annotations

from typing import Any

import structlog

from ranger.core.models import ColumnType, HealthStatus, Record, Schema
from ranger.sinks.base import BaseSink

try:
    from google.cloud import bigquery
    from google.api_core import exceptions as gcp_exceptions

    _HAS_BQ = True
except ImportError:  # pragma: no cover
    _HAS_BQ = False

try:
    import pyarrow as pa

    _HAS_ARROW = True
except ImportError:  # pragma: no cover
    _HAS_ARROW = False

logger = structlog.get_logger(__name__)

# Map Ranger column types to BigQuery types
_TYPE_MAP: dict[ColumnType, str] = {
    ColumnType.BOOLEAN: "BOOL",
    ColumnType.INT8: "INT64",
    ColumnType.INT16: "INT64",
    ColumnType.INT32: "INT64",
    ColumnType.INT64: "INT64",
    ColumnType.FLOAT32: "FLOAT64",
    ColumnType.FLOAT64: "FLOAT64",
    ColumnType.DECIMAL: "NUMERIC",
    ColumnType.STRING: "STRING",
    ColumnType.BINARY: "BYTES",
    ColumnType.DATE: "DATE",
    ColumnType.TIME: "TIME",
    ColumnType.TIMESTAMP: "TIMESTAMP",
    ColumnType.TIMESTAMP_TZ: "TIMESTAMP",
    ColumnType.JSON: "JSON",
    ColumnType.ARRAY: "JSON",
    ColumnType.MAP: "JSON",
    ColumnType.STRUCT: "JSON",
    ColumnType.UUID: "STRING",
}


def _ranger_to_bq_schema(schema: Schema) -> list[Any]:
    """Convert a Ranger Schema to a list of ``bigquery.SchemaField`` objects."""
    fields: list[Any] = []
    for col in schema.columns:
        bq_type = _TYPE_MAP.get(col.type, "STRING")
        mode = "NULLABLE" if col.nullable else "REQUIRED"
        fields.append(bigquery.SchemaField(col.name, bq_type, mode=mode))
    return fields


class BigQuerySink(BaseSink):
    """Write records to a Google BigQuery table.

    Config keys:
        project_id: GCP project ID (required).
        dataset: BigQuery dataset name (required).
        table: BigQuery table name (required).
        credentials_path: Path to a service-account JSON key file (optional).
        write_disposition: ``WRITE_APPEND`` or ``WRITE_TRUNCATE``
            (default ``WRITE_APPEND``).
        auto_create_table: Whether to create the table automatically
            (default ``False``).
        partition_field: Column name used for time partitioning (optional).
        clustering_fields: List of column names for clustering (optional).
        merge_keys: List of columns for upsert via MERGE (optional).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        if not _HAS_BQ:
            raise ImportError(
                "google-cloud-bigquery is required for BigQuerySink. "
                "Install it with: pip install google-cloud-bigquery"
            )
        super().__init__(config)

        self._project_id: str = config["project_id"]
        self._dataset: str = config["dataset"]
        self._table_name: str = config["table"]
        self._credentials_path: str | None = config.get("credentials_path")
        self._write_disposition: str = config.get(
            "write_disposition", "WRITE_APPEND"
        ).upper()
        self._auto_create_table: bool = config.get("auto_create_table", False)
        self._partition_field: str | None = config.get("partition_field")
        self._clustering_fields: list[str] = config.get("clustering_fields", [])
        self._merge_keys: list[str] = config.get("merge_keys", [])

        if self._write_disposition not in ("WRITE_APPEND", "WRITE_TRUNCATE"):
            raise ValueError(
                f"Unsupported write_disposition: {self._write_disposition}"
            )

        self._client: Any = None
        self._table_ref: Any = None
        self._schema_obj: Schema | None = None
        self._total_written: int = 0

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def sink_type(self) -> str:
        return "bigquery"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def open(self, schema: Schema) -> None:
        """Create the BQ client and optionally create the target table."""
        self._schema_obj = schema

        if self._credentials_path:
            self._client = bigquery.Client.from_service_account_json(
                self._credentials_path, project=self._project_id
            )
        else:
            self._client = bigquery.Client(project=self._project_id)

        self._table_ref = f"{self._project_id}.{self._dataset}.{self._table_name}"

        if self._auto_create_table:
            self._create_table_if_not_exists(schema)

        self._opened = True
        logger.info(
            "BigQuery sink opened",
            table_ref=self._table_ref,
            write_disposition=self._write_disposition,
        )

    def close(self) -> None:
        """Release BigQuery client resources."""
        self._client = None
        self._opened = False
        logger.info(
            "BigQuery sink closed",
            total_written=self._total_written,
            table_ref=self._table_ref,
        )

    def health_check(self) -> HealthStatus:
        """Run a lightweight query to verify the client is functional."""
        if self._client is None:
            return HealthStatus.UNHEALTHY
        try:
            list(self._client.query("SELECT 1").result())
            return HealthStatus.HEALTHY
        except Exception:
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Writing
    # ------------------------------------------------------------------

    def write_batch(self, records: list[Record]) -> int:
        """Write a batch of records to BigQuery."""
        if not self._opened:
            raise RuntimeError("Sink not opened. Call open() first.")
        if not records:
            return 0

        if self._merge_keys:
            written = self._write_merge(records)
        else:
            written = self._write_load(records)

        self._total_written += written
        return written

    # ------------------------------------------------------------------
    # Schema evolution
    # ------------------------------------------------------------------

    def evolve_schema(self, new_schema: Schema) -> None:
        """Add new columns to the BigQuery table schema."""
        if self._client is None:
            raise RuntimeError("Sink not opened. Call open() first.")

        try:
            table = self._client.get_table(self._table_ref)
        except gcp_exceptions.NotFound as exc:
            raise RuntimeError(
                f"BigQuery table {self._table_ref} not found: {exc}"
            ) from exc

        existing_names = {field.name for field in table.schema}
        new_fields = list(table.schema)
        added = False

        for col in new_schema.columns:
            if col.name not in existing_names:
                bq_type = _TYPE_MAP.get(col.type, "STRING")
                mode = "NULLABLE" if col.nullable else "REQUIRED"
                new_fields.append(bigquery.SchemaField(col.name, bq_type, mode=mode))
                added = True
                logger.info(
                    "BigQuery schema evolved — adding column",
                    column=col.name,
                    bq_type=bq_type,
                )

        if added:
            table.schema = new_fields
            self._client.update_table(table, ["schema"])

        self._schema_obj = new_schema

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _create_table_if_not_exists(self, schema: Schema) -> None:
        """Create the BigQuery table if it does not yet exist."""
        bq_schema = _ranger_to_bq_schema(schema)
        table = bigquery.Table(self._table_ref, schema=bq_schema)

        if self._partition_field:
            table.time_partitioning = bigquery.TimePartitioning(
                field=self._partition_field
            )
        if self._clustering_fields:
            table.clustering_fields = self._clustering_fields

        try:
            self._client.create_table(table)
            logger.info("BigQuery table created", table_ref=self._table_ref)
        except gcp_exceptions.Conflict:
            # Table already exists — that's fine
            logger.debug("BigQuery table already exists", table_ref=self._table_ref)

    def _write_load(self, records: list[Record]) -> int:
        """Load rows into BigQuery using the JSON load API."""
        rows = [r.data for r in records]

        job_config = bigquery.LoadJobConfig(
            write_disposition=getattr(
                bigquery.WriteDisposition, self._write_disposition
            ),
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )

        if self._schema_obj:
            job_config.schema = _ranger_to_bq_schema(self._schema_obj)

        # Use load_table_from_json for simplicity
        try:
            load_job = self._client.load_table_from_json(
                rows, self._table_ref, job_config=job_config
            )
            load_job.result()  # Wait for completion
            return len(rows)
        except Exception as exc:
            raise RuntimeError(
                f"BigQuery load failed for {self._table_ref}: {exc}"
            ) from exc

    def _write_merge(self, records: list[Record]) -> int:
        """Upsert records into BigQuery via MERGE using a temporary table."""
        if not records:
            return 0

        rows = [r.data for r in records]
        columns = list(rows[0].keys())
        non_key_columns = [c for c in columns if c not in self._merge_keys]

        # Create a temporary staging table
        staging_ref = f"{self._table_ref}__ranger_merge_tmp"

        # Load rows into staging
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
        )

        try:
            load_job = self._client.load_table_from_json(
                rows, staging_ref, job_config=job_config
            )
            load_job.result()

            # Build MERGE DML
            join_cond = " AND ".join(
                f"target.`{k}` = src.`{k}`" for k in self._merge_keys
            )

            merge_sql = f"MERGE `{self._table_ref}` AS target\n"
            merge_sql += f"USING `{staging_ref}` AS src\n"
            merge_sql += f"ON {join_cond}\n"

            if non_key_columns:
                update_set = ", ".join(
                    f"target.`{c}` = src.`{c}`" for c in non_key_columns
                )
                merge_sql += f"WHEN MATCHED THEN UPDATE SET {update_set}\n"

            insert_cols = ", ".join(f"`{c}`" for c in columns)
            insert_vals = ", ".join(f"src.`{c}`" for c in columns)
            merge_sql += (
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
            )

            query_job = self._client.query(merge_sql)
            query_job.result()

            # Clean up staging table
            self._client.delete_table(staging_ref, not_found_ok=True)

            return len(records)
        except Exception as exc:
            # Best-effort cleanup
            try:
                self._client.delete_table(staging_ref, not_found_ok=True)
            except Exception:
                pass
            raise RuntimeError(
                f"BigQuery MERGE failed for {self._table_ref}: {exc}"
            ) from exc
