"""Pipeline orchestrator — the main execution entry point.

Wires together source → engine → sink using the registry and config,
then delegates execution to the engine.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import structlog

from ranger.core.config import load_config, load_config_from_dict
from ranger.core.models import PipelineConfig, RunResult, RunStatus, TriggeredBy
from ranger.core.registry import registry
from ranger.schema.duckdb_registry import DuckDBSchemaRegistry

logger = structlog.get_logger(__name__)


class Pipeline:
    """Orchestrates a single pipeline run.

    Usage::

        pipeline = Pipeline.from_yaml("configs/orders.yaml")
        result = pipeline.execute()
    """

    def __init__(self, config: PipelineConfig, metadata_db: str = "ranger_meta.duckdb") -> None:
        self.config = config
        self.schema_registry = DuckDBSchemaRegistry(db_path=metadata_db)

    @classmethod
    def from_yaml(cls, path: str, metadata_db: str = "ranger_meta.duckdb") -> Pipeline:
        """Create a Pipeline from a YAML config file."""
        config = load_config(path)
        return cls(config, metadata_db=metadata_db)

    @classmethod
    def from_dict(cls, data: dict[str, Any], metadata_db: str = "ranger_meta.duckdb") -> Pipeline:
        """Create a Pipeline from a raw config dictionary."""
        config = load_config_from_dict(data)
        return cls(config, metadata_db=metadata_db)

    def execute(self, triggered_by: TriggeredBy = TriggeredBy.MANUAL) -> RunResult:
        """Execute the pipeline: connect source, open sink, run engine.

        Args:
            triggered_by: What triggered this run (manual, scheduler, event, api).

        Returns:
            RunResult with full metrics.
        """
        pipeline_name = self.config.pipeline.name
        log = logger.bind(pipeline=pipeline_name)

        result = RunResult(
            pipeline_name=pipeline_name,
            status=RunStatus.RUNNING,
            triggered_by=triggered_by,
            config_snapshot=self.config.model_dump(by_alias=True),
        )

        log.info("pipeline.starting", source=self.config.source.type, sink=self.config.sink.type)

        try:
            # --- Instantiate components from the registry ---
            source = registry.create_source(self.config.source.type, self.config.source.config)
            engine = registry.create_engine(self.config.engine.type)
            sink = registry.create_sink(self.config.sink.type, self.config.sink.config)

            # --- Connect source ---
            log.info("source.connecting", source_type=source.source_type)
            source.connect()

            # --- Discover and register schema ---
            schema = source.get_schema()
            source.register_schema(self.schema_registry)

            # --- Open sink ---
            log.info("sink.opening", sink_type=sink.sink_type)
            sink.open(schema)

            # --- Run engine ---
            log.info("engine.running", engine_type=self.config.engine.type)
            result = engine.run(source, sink, self.config)
            result.triggered_by = triggered_by

            log.info(
                "pipeline.completed",
                status=result.status.value,
                records_read=result.records_read,
                records_written=result.records_written,
                duration=result.duration_seconds,
            )

        except Exception as exc:
            result.status = RunStatus.FAILED
            result.error_message = str(exc)
            result.completed_at = datetime.now(timezone.utc)
            if result.started_at and result.completed_at:
                result.duration_seconds = (result.completed_at - result.started_at).total_seconds()

            log.error("pipeline.failed", error=str(exc), exc_info=True)

        finally:
            # --- Cleanup ---
            try:
                source.close()  # type: ignore[possibly-undefined]
            except Exception:
                pass
            try:
                sink.close()  # type: ignore[possibly-undefined]
            except Exception:
                pass

            # --- Log run to metadata ---
            try:
                self.schema_registry.log_run(result)
            except Exception:
                log.warning("Failed to log run to metadata DB", exc_info=True)

        return result

    def validate(self) -> list[str]:
        """Dry-run validation: check connectivity, schema, permissions.

        Returns:
            List of validation error messages (empty if valid).
        """
        errors: list[str] = []

        try:
            source = registry.create_source(self.config.source.type, self.config.source.config)
        except KeyError as exc:
            errors.append(f"Source: {exc}")
            return errors

        try:
            engine = registry.create_engine(self.config.engine.type)
        except KeyError as exc:
            errors.append(f"Engine: {exc}")
            return errors

        try:
            sink = registry.create_sink(self.config.sink.type, self.config.sink.config)
        except KeyError as exc:
            errors.append(f"Sink: {exc}")
            return errors

        # Validate source connectivity
        health = source.health_check()
        if health.value != "healthy":
            errors.append(f"Source health check: {health.value}")

        # Validate sink connectivity
        sink_health = sink.health_check()
        if sink_health.value != "healthy":
            errors.append(f"Sink health check: {sink_health.value}")

        # Engine-level validation
        engine_errors = engine.validate(source, sink, self.config)
        errors.extend(engine_errors)

        return errors
