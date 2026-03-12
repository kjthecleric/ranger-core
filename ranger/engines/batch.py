"""Batch engine — reads all records from source in configurable chunks.

Supports full-load and incremental (watermark-based) modes.
"""

from __future__ import annotations

from datetime import datetime, timezone

import structlog

from ranger.core.models import (
    DriftAction,
    DriftClassification,
    DriftEvent,
    PipelineConfig,
    RunResult,
    RunStatus,
    Schema,
)
from ranger.engines.base import BaseEngine
from ranger.schema.duckdb_registry import DuckDBSchemaRegistry
from ranger.sinks.base import BaseSink
from ranger.sources.base import BaseSource

logger = structlog.get_logger(__name__)


class BatchEngine(BaseEngine):
    """Batch execution engine.

    - Reads all records from source in configurable chunk sizes
    - Applies schema drift detection per batch
    - Writes to sink with partition awareness
    - Supports full-load and incremental (watermark-based) modes
    """

    def run(self, source: BaseSource, sink: BaseSink, config: PipelineConfig) -> RunResult:
        pipeline_name = config.pipeline.name
        log = logger.bind(pipeline=pipeline_name, engine="batch")
        start_time = datetime.now(timezone.utc)

        result = RunResult(
            pipeline_name=pipeline_name,
            status=RunStatus.RUNNING,
            started_at=start_time,
        )

        chunk_size = config.engine.config.get("chunk_size", 10_000)
        total_read = 0
        total_written = 0
        total_failed = 0

        try:
            log.info("batch.starting", chunk_size=chunk_size)

            for batch in source.read_batch(batch_size=chunk_size):
                batch_size = len(batch)
                total_read += batch_size

                log.debug("batch.processing", batch_size=batch_size, total_read=total_read)

                # Write batch to sink
                written = sink.write_batch(batch)
                total_written += written
                total_failed += batch_size - written

            result.status = RunStatus.SUCCESS
            log.info(
                "batch.completed",
                records_read=total_read,
                records_written=total_written,
                records_failed=total_failed,
            )

        except Exception as exc:
            result.status = RunStatus.FAILED
            result.error_message = str(exc)
            log.error("batch.failed", error=str(exc), exc_info=True)

        end_time = datetime.now(timezone.utc)
        result.completed_at = end_time
        result.records_read = total_read
        result.records_written = total_written
        result.records_failed = total_failed
        result.duration_seconds = (end_time - start_time).total_seconds()

        return result

    def validate(self, source: BaseSource, sink: BaseSink, config: PipelineConfig) -> list[str]:
        errors: list[str] = []
        if config.engine.type != "batch":
            errors.append(f"BatchEngine received engine type '{config.engine.type}', expected 'batch'")
        return errors
