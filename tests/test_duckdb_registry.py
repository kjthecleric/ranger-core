"""Unit tests for the DuckDB schema registry."""

import os
import tempfile

import pytest

from ranger.core.models import (
    ColumnDefinition,
    ColumnType,
    DriftAction,
    DriftClassification,
    DriftEvent,
    RunResult,
    RunStatus,
    Schema,
)
from ranger.schema.duckdb_registry import DuckDBSchemaRegistry


@pytest.fixture
def registry(tmp_path):
    """Create a temporary DuckDB registry for testing."""
    db_path = str(tmp_path / "test_meta.duckdb")
    reg = DuckDBSchemaRegistry(db_path=db_path)
    yield reg
    reg.close()


@pytest.fixture
def sample_schema():
    return Schema(
        columns=[
            ColumnDefinition(name="id", type=ColumnType.INT64, nullable=False),
            ColumnDefinition(name="name", type=ColumnType.STRING, nullable=True),
            ColumnDefinition(name="amount", type=ColumnType.FLOAT64, nullable=True),
        ],
        primary_key=["id"],
    )


class TestSchemaRegistration:
    def test_register_new_schema(self, registry, sample_schema):
        version = registry.register_schema("test_pipeline", "test_source", sample_schema)
        assert version.version_number == 1
        assert version.is_active is True
        assert version.pipeline_name == "test_pipeline"

    def test_register_same_schema_no_new_version(self, registry, sample_schema):
        v1 = registry.register_schema("test_pipeline", "test_source", sample_schema)
        v2 = registry.register_schema("test_pipeline", "test_source", sample_schema)
        assert v1.version_number == v2.version_number

    def test_register_changed_schema_new_version(self, registry, sample_schema):
        v1 = registry.register_schema("test_pipeline", "test_source", sample_schema)

        new_schema = Schema(
            columns=[
                *sample_schema.columns,
                ColumnDefinition(name="email", type=ColumnType.STRING, nullable=True),
            ]
        )
        v2 = registry.register_schema("test_pipeline", "test_source", new_schema)
        assert v2.version_number == 2
        assert v2.is_active is True

    def test_get_active_schema(self, registry, sample_schema):
        registry.register_schema("test_pipeline", "test_source", sample_schema)
        active = registry.get_active_schema("test_pipeline", "test_source")
        assert active is not None
        assert len(active.columns) == 3

    def test_get_schema_history(self, registry, sample_schema):
        registry.register_schema("test_pipeline", "test_source", sample_schema)

        new_schema = Schema(
            columns=[
                *sample_schema.columns,
                ColumnDefinition(name="email", type=ColumnType.STRING, nullable=True),
            ]
        )
        registry.register_schema("test_pipeline", "test_source", new_schema)

        history = registry.get_schema_history("test_pipeline", "test_source")
        assert len(history) == 2
        assert history[0].version_number == 1
        assert history[1].version_number == 2


class TestSchemaComparison:
    def test_compare_added_column(self, registry, sample_schema):
        v1 = registry.register_schema("test_pipeline", "test_source", sample_schema)

        new_schema = Schema(
            columns=[
                *sample_schema.columns,
                ColumnDefinition(name="email", type=ColumnType.STRING, nullable=True),
            ]
        )
        v2 = registry.register_schema("test_pipeline", "test_source", new_schema)

        diff = registry.compare_schemas(v1.schema_id, v2.schema_id)
        assert len(diff.changes) == 1
        assert diff.changes[0].change_type == "added"
        assert diff.changes[0].column_name == "email"
        assert diff.overall_classification == DriftClassification.COMPATIBLE


class TestRunLogging:
    def test_log_and_retrieve_run(self, registry):
        result = RunResult(
            pipeline_name="test_pipeline",
            status=RunStatus.SUCCESS,
            records_read=100,
            records_written=100,
        )
        registry.log_run(result)

        history = registry.get_run_history("test_pipeline")
        assert len(history) == 1
        assert history[0]["status"] == "success"
        assert history[0]["records_read"] == 100
