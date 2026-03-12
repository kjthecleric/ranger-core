"""DuckDB-backed schema registry — the metadata backbone of Ranger.

All discovered schemas, drift events, source catalogs, column lineage, and
pipeline run history are persisted in a single DuckDB file that acts as the
metadata schema management layer.

Usage::

    registry = DuckDBSchemaRegistry("ranger_meta.duckdb")
    version = registry.register_schema("orders_pipeline", "prod_pg", schema)
    history = registry.get_schema_history("orders_pipeline", "prod_pg")
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

import duckdb

from ranger.core.models import (
    ColumnChange,
    ColumnLineageEntry,
    DriftClassification,
    DriftEvent,
    RunResult,
    Schema,
    SchemaDiff,
    SchemaVersion,
    SourceCatalogEntry,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DDL for metadata tables
# ---------------------------------------------------------------------------

_DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS schema_versions (
        schema_id VARCHAR PRIMARY KEY,
        pipeline_name VARCHAR NOT NULL,
        source_name VARCHAR NOT NULL,
        version_number INTEGER NOT NULL,
        schema_definition JSON NOT NULL,
        fingerprint VARCHAR NOT NULL,
        discovered_at TIMESTAMP NOT NULL,
        registered_by VARCHAR,
        is_active BOOLEAN DEFAULT TRUE,
        UNIQUE(pipeline_name, source_name, version_number)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS schema_drift_log (
        drift_id VARCHAR PRIMARY KEY,
        pipeline_name VARCHAR NOT NULL,
        previous_schema_id VARCHAR,
        new_schema_id VARCHAR NOT NULL,
        drift_type VARCHAR NOT NULL,
        changes JSON NOT NULL,
        action_taken VARCHAR NOT NULL,
        detected_at TIMESTAMP NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS source_catalog (
        catalog_id VARCHAR PRIMARY KEY,
        source_name VARCHAR NOT NULL,
        source_type VARCHAR NOT NULL,
        object_name VARCHAR NOT NULL,
        object_type VARCHAR NOT NULL,
        last_discovered_at TIMESTAMP,
        row_count_estimate BIGINT,
        metadata JSON
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS column_lineage (
        lineage_id VARCHAR PRIMARY KEY,
        pipeline_name VARCHAR NOT NULL,
        source_column VARCHAR NOT NULL,
        source_type VARCHAR NOT NULL,
        sink_column VARCHAR NOT NULL,
        sink_type VARCHAR NOT NULL,
        mapping_type VARCHAR NOT NULL,
        created_at TIMESTAMP NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS pipeline_runs (
        run_id VARCHAR PRIMARY KEY,
        pipeline_name VARCHAR NOT NULL,
        status VARCHAR NOT NULL,
        started_at TIMESTAMP NOT NULL,
        completed_at TIMESTAMP,
        records_read BIGINT DEFAULT 0,
        records_written BIGINT DEFAULT 0,
        records_failed BIGINT DEFAULT 0,
        bytes_processed BIGINT DEFAULT 0,
        late_records_count BIGINT DEFAULT 0,
        schema_drift_detected BOOLEAN DEFAULT FALSE,
        error_message TEXT,
        triggered_by VARCHAR,
        config_snapshot JSON,
        duration_seconds DOUBLE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dedup_state (
        record_hash VARCHAR NOT NULL,
        pipeline_name VARCHAR NOT NULL,
        first_seen_at TIMESTAMP NOT NULL,
        last_seen_at TIMESTAMP NOT NULL,
        PRIMARY KEY(record_hash, pipeline_name)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS data_profiles (
        profile_id VARCHAR PRIMARY KEY,
        pipeline_name VARCHAR NOT NULL,
        source_name VARCHAR NOT NULL,
        column_name VARCHAR NOT NULL,
        profiled_at TIMESTAMP NOT NULL,
        record_count BIGINT,
        null_count BIGINT,
        null_percent DOUBLE,
        distinct_count BIGINT,
        min_value VARCHAR,
        max_value VARCHAR,
        mean_value DOUBLE,
        stddev_value DOUBLE,
        top_values JSON,
        histogram JSON
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS validation_results (
        result_id VARCHAR PRIMARY KEY,
        run_id VARCHAR NOT NULL,
        pipeline_name VARCHAR NOT NULL,
        rule_type VARCHAR NOT NULL,
        column_name VARCHAR,
        action VARCHAR NOT NULL,
        records_passed BIGINT DEFAULT 0,
        records_failed BIGINT DEFAULT 0,
        records_warned BIGINT DEFAULT 0,
        validated_at TIMESTAMP NOT NULL
    )
    """,
]


class DuckDBSchemaRegistry:
    """Schema registry backed by a DuckDB file.

    Provides schema versioning, drift logging, source cataloging,
    lineage tracking, and pipeline run history.
    """

    def __init__(self, db_path: str = "ranger_meta.duckdb") -> None:
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        """Create metadata tables if they don't exist."""
        for ddl in _DDL_STATEMENTS:
            self.conn.execute(ddl)
        logger.debug("DuckDB metadata tables ensured at %s", self.db_path)

    # ------------------------------------------------------------------
    # Schema registration
    # ------------------------------------------------------------------

    def register_schema(self, pipeline: str, source: str, schema: Schema) -> SchemaVersion:
        """Register a schema version.  If the schema fingerprint matches the
        current active version, no new version is created.

        Args:
            pipeline: Pipeline name.
            source: Source name.
            schema: The schema to register.

        Returns:
            The active SchemaVersion (existing or newly created).
        """
        fingerprint = schema.fingerprint()

        # Check if this exact schema already exists and is active
        existing = self.conn.execute(
            """
            SELECT schema_id, version_number FROM schema_versions
            WHERE pipeline_name = ? AND source_name = ? AND fingerprint = ? AND is_active = TRUE
            """,
            [pipeline, source, fingerprint],
        ).fetchone()

        if existing:
            logger.debug("Schema unchanged for %s/%s (fingerprint=%s)", pipeline, source, fingerprint)
            return self.get_active_schema_version(pipeline, source)  # type: ignore[return-value]

        # Determine next version number
        max_version = self.conn.execute(
            "SELECT COALESCE(MAX(version_number), 0) FROM schema_versions WHERE pipeline_name = ? AND source_name = ?",
            [pipeline, source],
        ).fetchone()[0]  # type: ignore[index]

        new_version = max_version + 1

        # Deactivate previous active version
        self.conn.execute(
            "UPDATE schema_versions SET is_active = FALSE WHERE pipeline_name = ? AND source_name = ? AND is_active = TRUE",
            [pipeline, source],
        )

        # Insert new version
        schema_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        schema_json = schema.model_dump_json()

        self.conn.execute(
            """
            INSERT INTO schema_versions (schema_id, pipeline_name, source_name, version_number,
                                         schema_definition, fingerprint, discovered_at, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, TRUE)
            """,
            [schema_id, pipeline, source, new_version, schema_json, fingerprint, now],
        )

        logger.info("Registered schema v%d for %s/%s (id=%s)", new_version, pipeline, source, schema_id)

        return SchemaVersion(
            schema_id=schema_id,
            pipeline_name=pipeline,
            source_name=source,
            version_number=new_version,
            schema_definition=schema,
            fingerprint=fingerprint,
            discovered_at=now,
            is_active=True,
        )

    def get_active_schema(self, pipeline: str, source: str) -> Schema | None:
        """Get the currently active schema for a pipeline/source pair."""
        version = self.get_active_schema_version(pipeline, source)
        return version.schema_definition if version else None

    def get_active_schema_version(self, pipeline: str, source: str) -> SchemaVersion | None:
        """Get the active SchemaVersion record."""
        row = self.conn.execute(
            """
            SELECT schema_id, pipeline_name, source_name, version_number,
                   schema_definition, fingerprint, discovered_at, registered_by, is_active
            FROM schema_versions
            WHERE pipeline_name = ? AND source_name = ? AND is_active = TRUE
            """,
            [pipeline, source],
        ).fetchone()

        if not row:
            return None

        return SchemaVersion(
            schema_id=row[0],
            pipeline_name=row[1],
            source_name=row[2],
            version_number=row[3],
            schema_definition=Schema.model_validate_json(row[4]),
            fingerprint=row[5],
            discovered_at=row[6],
            registered_by=row[7],
            is_active=row[8],
        )

    def get_schema_history(self, pipeline: str, source: str) -> list[SchemaVersion]:
        """Get all schema versions for a pipeline/source, ordered by version number."""
        rows = self.conn.execute(
            """
            SELECT schema_id, pipeline_name, source_name, version_number,
                   schema_definition, fingerprint, discovered_at, registered_by, is_active
            FROM schema_versions
            WHERE pipeline_name = ? AND source_name = ?
            ORDER BY version_number ASC
            """,
            [pipeline, source],
        ).fetchall()

        return [
            SchemaVersion(
                schema_id=r[0],
                pipeline_name=r[1],
                source_name=r[2],
                version_number=r[3],
                schema_definition=Schema.model_validate_json(r[4]),
                fingerprint=r[5],
                discovered_at=r[6],
                registered_by=r[7],
                is_active=r[8],
            )
            for r in rows
        ]

    # ------------------------------------------------------------------
    # Schema comparison
    # ------------------------------------------------------------------

    def compare_schemas(self, old_id: str, new_id: str) -> SchemaDiff:
        """Compare two schema versions and return a diff."""
        old_row = self.conn.execute(
            "SELECT schema_definition FROM schema_versions WHERE schema_id = ?", [old_id]
        ).fetchone()
        new_row = self.conn.execute(
            "SELECT schema_definition FROM schema_versions WHERE schema_id = ?", [new_id]
        ).fetchone()

        if not old_row or not new_row:
            raise ValueError(f"Schema ID not found: old={old_id}, new={new_id}")

        old_schema = Schema.model_validate_json(old_row[0])
        new_schema = Schema.model_validate_json(new_row[0])

        return self._diff_schemas(old_id, new_id, old_schema, new_schema)

    def _diff_schemas(self, old_id: str, new_id: str, old: Schema, new: Schema) -> SchemaDiff:
        """Compute column-level diff between two schemas."""
        old_cols = {c.name: c for c in old.columns}
        new_cols = {c.name: c for c in new.columns}
        changes: list[ColumnChange] = []

        # Removed columns (breaking)
        for name in old_cols:
            if name not in new_cols:
                changes.append(
                    ColumnChange(
                        column_name=name,
                        change_type="removed",
                        old_value=old_cols[name].type.value,
                        classification=DriftClassification.BREAKING,
                    )
                )

        # Added columns
        for name in new_cols:
            if name not in old_cols:
                classification = (
                    DriftClassification.COMPATIBLE if new_cols[name].nullable else DriftClassification.BREAKING
                )
                changes.append(
                    ColumnChange(
                        column_name=name,
                        change_type="added",
                        new_value=new_cols[name].type.value,
                        classification=classification,
                    )
                )

        # Type changes
        for name in old_cols:
            if name in new_cols:
                old_col = old_cols[name]
                new_col = new_cols[name]
                if old_col.type != new_col.type:
                    # Determine if widening or narrowing
                    classification = self._classify_type_change(old_col.type.value, new_col.type.value)
                    changes.append(
                        ColumnChange(
                            column_name=name,
                            change_type="type_changed",
                            old_value=old_col.type.value,
                            new_value=new_col.type.value,
                            classification=classification,
                        )
                    )
                if old_col.nullable != new_col.nullable:
                    # nullable → not null = breaking; not null → nullable = compatible
                    classification = (
                        DriftClassification.COMPATIBLE
                        if new_col.nullable
                        else DriftClassification.BREAKING
                    )
                    changes.append(
                        ColumnChange(
                            column_name=name,
                            change_type="nullability_changed",
                            old_value=str(old_col.nullable),
                            new_value=str(new_col.nullable),
                            classification=classification,
                        )
                    )

        overall = (
            DriftClassification.BREAKING
            if any(c.classification == DriftClassification.BREAKING for c in changes)
            else DriftClassification.COMPATIBLE
        )

        return SchemaDiff(
            old_schema_id=old_id,
            new_schema_id=new_id,
            changes=changes,
            overall_classification=overall,
        )

    @staticmethod
    def _classify_type_change(old_type: str, new_type: str) -> DriftClassification:
        """Classify a column type change as compatible (widening) or breaking."""
        # Widening rules: int8→int16→int32→int64, float32→float64, string can widen
        widen_paths: dict[str, set[str]] = {
            "int8": {"int16", "int32", "int64", "float32", "float64", "decimal", "string"},
            "int16": {"int32", "int64", "float32", "float64", "decimal", "string"},
            "int32": {"int64", "float64", "decimal", "string"},
            "int64": {"float64", "decimal", "string"},
            "float32": {"float64", "decimal", "string"},
            "float64": {"decimal", "string"},
            "date": {"timestamp", "timestamp_tz", "string"},
            "timestamp": {"timestamp_tz", "string"},
        }
        if new_type in widen_paths.get(old_type, set()):
            return DriftClassification.COMPATIBLE
        return DriftClassification.BREAKING

    # ------------------------------------------------------------------
    # Drift logging
    # ------------------------------------------------------------------

    def log_drift(self, event: DriftEvent) -> None:
        """Persist a drift event."""
        changes_json = json.dumps([c.model_dump() for c in event.changes])
        self.conn.execute(
            """
            INSERT INTO schema_drift_log (drift_id, pipeline_name, previous_schema_id,
                                          new_schema_id, drift_type, changes, action_taken, detected_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                event.drift_id,
                event.pipeline_name,
                event.previous_schema_id,
                event.new_schema_id,
                event.drift_type.value,
                changes_json,
                event.action_taken.value,
                event.detected_at,
            ],
        )
        logger.info("Drift event logged: %s for %s", event.drift_id, event.pipeline_name)

    def get_drift_history(self, pipeline: str) -> list[DriftEvent]:
        """Get all drift events for a pipeline."""
        rows = self.conn.execute(
            """
            SELECT drift_id, pipeline_name, previous_schema_id, new_schema_id,
                   drift_type, changes, action_taken, detected_at
            FROM schema_drift_log
            WHERE pipeline_name = ?
            ORDER BY detected_at ASC
            """,
            [pipeline],
        ).fetchall()

        results = []
        for r in rows:
            changes_data = json.loads(r[5]) if isinstance(r[5], str) else r[5]
            results.append(
                DriftEvent(
                    drift_id=r[0],
                    pipeline_name=r[1],
                    previous_schema_id=r[2],
                    new_schema_id=r[3],
                    drift_type=DriftClassification(r[4]),
                    changes=[ColumnChange.model_validate(c) for c in changes_data],
                    action_taken=r[6],
                    detected_at=r[7],
                )
            )
        return results

    # ------------------------------------------------------------------
    # Source catalog
    # ------------------------------------------------------------------

    def catalog_source(self, entry: SourceCatalogEntry) -> None:
        """Register or update a source catalog entry."""
        self.conn.execute(
            """
            INSERT OR REPLACE INTO source_catalog (catalog_id, source_name, source_type,
                                                   object_name, object_type, last_discovered_at,
                                                   row_count_estimate, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                entry.catalog_id,
                entry.source_name,
                entry.source_type,
                entry.object_name,
                entry.object_type,
                entry.last_discovered_at,
                entry.row_count_estimate,
                json.dumps(entry.metadata) if entry.metadata else None,
            ],
        )

    # ------------------------------------------------------------------
    # Column lineage
    # ------------------------------------------------------------------

    def log_lineage(self, lineage: ColumnLineageEntry) -> None:
        """Persist a column-level lineage entry."""
        self.conn.execute(
            """
            INSERT INTO column_lineage (lineage_id, pipeline_name, source_column,
                                        source_type, sink_column, sink_type, mapping_type, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                lineage.lineage_id,
                lineage.pipeline_name,
                lineage.source_column,
                lineage.source_type,
                lineage.sink_column,
                lineage.sink_type,
                lineage.mapping_type.value,
                lineage.created_at,
            ],
        )

    # ------------------------------------------------------------------
    # Pipeline runs
    # ------------------------------------------------------------------

    def log_run(self, result: RunResult) -> None:
        """Persist a pipeline run result."""
        self.conn.execute(
            """
            INSERT INTO pipeline_runs (run_id, pipeline_name, status, started_at, completed_at,
                                       records_read, records_written, records_failed, bytes_processed,
                                       late_records_count, schema_drift_detected, error_message,
                                       triggered_by, config_snapshot, duration_seconds)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                result.run_id,
                result.pipeline_name,
                result.status.value,
                result.started_at,
                result.completed_at,
                result.records_read,
                result.records_written,
                result.records_failed,
                result.bytes_processed,
                result.late_records_count,
                result.schema_drift_detected,
                result.error_message,
                result.triggered_by.value,
                json.dumps(result.config_snapshot) if result.config_snapshot else None,
                result.duration_seconds,
            ],
        )

    def get_run_history(self, pipeline: str, limit: int = 50) -> list[dict[str, Any]]:
        """Get recent run history for a pipeline."""
        rows = self.conn.execute(
            """
            SELECT run_id, pipeline_name, status, started_at, completed_at,
                   records_read, records_written, records_failed, bytes_processed,
                   late_records_count, schema_drift_detected, error_message,
                   triggered_by, duration_seconds
            FROM pipeline_runs
            WHERE pipeline_name = ?
            ORDER BY started_at DESC
            LIMIT ?
            """,
            [pipeline, limit],
        ).fetchall()

        columns = [
            "run_id", "pipeline_name", "status", "started_at", "completed_at",
            "records_read", "records_written", "records_failed", "bytes_processed",
            "late_records_count", "schema_drift_detected", "error_message",
            "triggered_by", "duration_seconds",
        ]
        return [dict(zip(columns, row)) for row in rows]

    # ------------------------------------------------------------------
    # Arbitrary queries
    # ------------------------------------------------------------------

    def query(self, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        """Run an arbitrary analytical query against the metadata store.

        Args:
            sql: SQL query string.
            params: Optional query parameters.

        Returns:
            List of result rows as dicts.
        """
        result = self.conn.execute(sql, params or [])
        columns = [desc[0] for desc in result.description]  # type: ignore[union-attr]
        return [dict(zip(columns, row)) for row in result.fetchall()]

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------

    def export_table(self, table: str, path: str, format: str = "parquet") -> None:
        """Export a metadata table to a file.

        Args:
            table: Table name (e.g., 'schema_versions', 'pipeline_runs').
            path: Output file path.
            format: 'parquet', 'csv', or 'json'.
        """
        if format == "parquet":
            self.conn.execute(f"COPY {table} TO '{path}' (FORMAT PARQUET)")
        elif format == "csv":
            self.conn.execute(f"COPY {table} TO '{path}' (FORMAT CSV, HEADER)")
        elif format == "json":
            self.conn.execute(f"COPY {table} TO '{path}' (FORMAT JSON)")
        else:
            raise ValueError(f"Unsupported export format: {format}")

        logger.info("Exported %s to %s (%s)", table, path, format)

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the DuckDB connection."""
        self.conn.close()

    def __enter__(self) -> DuckDBSchemaRegistry:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
