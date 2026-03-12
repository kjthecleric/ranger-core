"""Unit tests for Ranger core models."""

import pytest

from ranger.core.models import (
    ColumnDefinition,
    ColumnType,
    PipelineConfig,
    PipelineMeta,
    Record,
    Schema,
    SourceConfig,
    SinkConfig,
)


class TestRecord:
    def test_record_creation(self):
        record = Record(data={"id": 1, "name": "Alice"})
        assert record["id"] == 1
        assert record.get("name") == "Alice"
        assert record.get("missing", "default") == "default"
        assert "id" in record

    def test_record_mutation(self):
        record = Record(data={"id": 1})
        record["name"] = "Alice"
        assert record["name"] == "Alice"

    def test_record_arrival_time_set(self):
        record = Record(data={})
        assert record.arrival_time is not None


class TestSchema:
    def test_schema_fingerprint(self):
        schema = Schema(columns=[
            ColumnDefinition(name="id", type=ColumnType.INT64, nullable=False),
            ColumnDefinition(name="name", type=ColumnType.STRING, nullable=True),
        ])
        fp = schema.fingerprint()
        assert isinstance(fp, str)
        assert len(fp) == 16

    def test_schema_fingerprint_deterministic(self):
        cols = [
            ColumnDefinition(name="id", type=ColumnType.INT64, nullable=False),
            ColumnDefinition(name="name", type=ColumnType.STRING, nullable=True),
        ]
        schema1 = Schema(columns=cols)
        schema2 = Schema(columns=list(reversed(cols)))
        # Fingerprint sorts by column name, so order doesn't matter
        assert schema1.fingerprint() == schema2.fingerprint()

    def test_schema_column_names(self):
        schema = Schema(columns=[
            ColumnDefinition(name="a", type=ColumnType.STRING),
            ColumnDefinition(name="b", type=ColumnType.INT32),
        ])
        assert schema.column_names() == ["a", "b"]

    def test_schema_get_column(self):
        schema = Schema(columns=[
            ColumnDefinition(name="id", type=ColumnType.INT64),
        ])
        col = schema.get_column("id")
        assert col is not None
        assert col.type == ColumnType.INT64
        assert schema.get_column("nonexistent") is None


class TestPipelineConfig:
    def test_minimal_config(self):
        config = PipelineConfig(
            pipeline=PipelineMeta(name="test"),
            source=SourceConfig(type="file", config={"path": "data.csv"}),
            sink=SinkConfig(type="local", config={"path": "./output"}),
        )
        assert config.pipeline.name == "test"
        assert config.source.type == "file"
        assert config.sink.type == "local"
        assert config.engine.type == "batch"  # default
