"""Stream engine — processes data continuously from streaming sources.

Supports Kafka, Kinesis, WebSocket, CDC, and any source that implements
``read_stream()``.  Records are buffered and flushed to the sink in
micro-batches controlled by ``buffer_size`` and ``flush_interval_seconds``.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import structlog

from ranger.core.models import (
    PipelineConfig,
    Record,
    RunResult,
    RunStatus,
    StreamConfig,
)
from ranger.engines.base import BaseEngine
from ranger.engines.late_arrival import LateArrivalHandler, LateArrivalStrategy
from ranger.sinks.base import BaseSink
from ranger.sources.base import BaseSource

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Error strategy
# ---------------------------------------------------------------------------

_VALID_ERROR_STRATEGIES = {"skip", "dead_letter", "fail"}


# ---------------------------------------------------------------------------
# Stream Engine
# ---------------------------------------------------------------------------


class StreamEngine(BaseEngine):
    """Streaming execution engine.

    Continuously consumes records from a streaming source, buffers them,
    and flushes micro-batches to the sink.  Key behaviours:

    * **Buffered writes** – records accumulate until *buffer_size* is
      reached **or** *flush_interval_seconds* has elapsed, whichever
      comes first.
    * **Late-arrival detection** – uses :class:`LateArrivalHandler` to
      track watermarks and apply the configured strategy.
    * **Periodic checkpoints** – logs progress at
      *checkpoint_interval_seconds* intervals.
    * **Bounded runs** – if *max_records* is set the engine stops after
      processing that many records.
    * **Error strategies** – ``skip`` (drop bad records), ``dead_letter``
      (log and continue), ``fail`` (abort the run).
    """

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def validate(
        self,
        source: BaseSource,
        sink: BaseSink,
        config: PipelineConfig,
    ) -> list[str]:
        errors: list[str] = []

        if config.engine.type != "stream":
            errors.append(
                f"StreamEngine received engine type '{config.engine.type}', expected 'stream'"
            )

        if not source.supports_streaming:
            errors.append(
                f"Source '{source.source_type}' does not support streaming reads"
            )

        ecfg = config.engine.config
        strategy = ecfg.get("error_strategy", "skip")
        if strategy not in _VALID_ERROR_STRATEGIES:
            errors.append(
                f"Invalid error_strategy '{strategy}'; must be one of {_VALID_ERROR_STRATEGIES}"
            )

        return errors

    def run(
        self,
        source: BaseSource,
        sink: BaseSink,
        config: PipelineConfig,
    ) -> RunResult:
        """Execute the streaming pipeline.

        Internally delegates to an asyncio event loop so that
        ``source.read_stream()`` (which returns an ``AsyncIterator``) can
        be consumed, while still honouring the synchronous ``run()``
        contract required by :class:`BaseEngine`.
        """
        return asyncio.run(self._run_async(source, sink, config))

    # ------------------------------------------------------------------
    # Async implementation
    # ------------------------------------------------------------------

    async def _run_async(
        self,
        source: BaseSource,
        sink: BaseSink,
        config: PipelineConfig,
    ) -> RunResult:
        pipeline_name = config.pipeline.name
        log = logger.bind(pipeline=pipeline_name, engine="stream")
        start_time = datetime.now(timezone.utc)

        result = RunResult(
            pipeline_name=pipeline_name,
            status=RunStatus.RUNNING,
            started_at=start_time,
        )

        # ---- Extract engine config ---------------------------------
        ecfg: dict[str, Any] = config.engine.config
        buffer_size: int = ecfg.get("buffer_size", 1_000)
        flush_interval: float = ecfg.get("flush_interval_seconds", 5.0)
        checkpoint_interval: float = ecfg.get("checkpoint_interval_seconds", 30.0)
        max_records: int | None = ecfg.get("max_records")
        error_strategy: str = ecfg.get("error_strategy", "skip")

        # ---- Late-arrival handler ----------------------------------
        la_cfg = config.engine.late_arrival
        strategy_map = {
            "watermark": LateArrivalStrategy.REJECT,  # watermark ≈ reject by default
            "append_with_flag": LateArrivalStrategy.APPEND_WITH_FLAG,
            "upsert_merge": LateArrivalStrategy.UPSERT_MERGE,
            "reprocess_partition": LateArrivalStrategy.REPROCESS_PARTITION,
        }
        la_strategy = strategy_map.get(
            la_cfg.strategy.value, LateArrivalStrategy.APPEND_WITH_FLAG
        )
        late_handler = LateArrivalHandler(
            strategy=la_strategy,
            watermark_delay_seconds=la_cfg.watermark_delay_seconds,
        )

        # ---- State -------------------------------------------------
        buffer: list[Record] = []
        total_read = 0
        total_written = 0
        total_failed = 0
        total_late = 0
        last_flush = time.monotonic()
        last_checkpoint = time.monotonic()

        try:
            log.info(
                "stream.starting",
                buffer_size=buffer_size,
                flush_interval=flush_interval,
                max_records=max_records,
                error_strategy=error_strategy,
            )

            # Connect source & open sink
            source.connect()
            sink.open(source.get_schema())

            stream_config = StreamConfig(
                buffer_size=buffer_size,
                flush_interval_seconds=flush_interval,
            )
            stream = source.read_stream(config=stream_config)

            async for record in stream:
                total_read += 1

                # -- Late-arrival check ---------------------------------
                try:
                    late_handler.update_watermark(record.event_time)
                    if late_handler.is_late(record):
                        handled = late_handler.handle_late_record(record)
                        total_late += 1
                        if handled is None:
                            # Record rejected
                            continue
                        record = handled
                except Exception as exc:
                    log.warning("stream.late_check_error", error=str(exc))

                # -- Buffer the record ----------------------------------
                try:
                    buffer.append(record)
                except Exception as exc:
                    total_failed += 1
                    if error_strategy == "fail":
                        raise
                    log.warning("stream.buffer_error", error=str(exc))
                    continue

                # -- Flush conditions -----------------------------------
                now = time.monotonic()
                should_flush = (
                    len(buffer) >= buffer_size
                    or (now - last_flush) >= flush_interval
                )

                if should_flush and buffer:
                    written = self._flush_buffer(buffer, sink, error_strategy, log)
                    total_written += written
                    total_failed += len(buffer) - written
                    buffer = []
                    last_flush = time.monotonic()

                # -- Checkpoint logging ---------------------------------
                now = time.monotonic()
                if (now - last_checkpoint) >= checkpoint_interval:
                    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
                    log.info(
                        "stream.checkpoint",
                        records_read=total_read,
                        records_written=total_written,
                        records_failed=total_failed,
                        late_records=total_late,
                        elapsed_seconds=round(elapsed, 2),
                    )
                    last_checkpoint = time.monotonic()

                # -- Bounded run check ----------------------------------
                if max_records is not None and total_read >= max_records:
                    log.info("stream.max_records_reached", max_records=max_records)
                    break

            # ---- Final flush ------------------------------------------
            if buffer:
                written = self._flush_buffer(buffer, sink, error_strategy, log)
                total_written += written
                total_failed += len(buffer) - written

            result.status = RunStatus.SUCCESS
            log.info(
                "stream.completed",
                records_read=total_read,
                records_written=total_written,
                records_failed=total_failed,
                late_records=total_late,
            )

        except Exception as exc:
            result.status = RunStatus.FAILED
            result.error_message = str(exc)
            log.error("stream.failed", error=str(exc), exc_info=True)

        end_time = datetime.now(timezone.utc)
        result.completed_at = end_time
        result.records_read = total_read
        result.records_written = total_written
        result.records_failed = total_failed
        result.late_records_count = total_late
        result.duration_seconds = (end_time - start_time).total_seconds()

        return result

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _flush_buffer(
        buffer: list[Record],
        sink: BaseSink,
        error_strategy: str,
        log: Any,
    ) -> int:
        """Flush the record buffer to the sink.

        Returns:
            The number of records successfully written.
        """
        try:
            written = sink.write_batch(buffer)
            log.debug("stream.flushed", count=written)
            return written
        except Exception as exc:
            if error_strategy == "fail":
                raise
            log.error(
                "stream.flush_error",
                error=str(exc),
                buffer_size=len(buffer),
            )
            if error_strategy == "skip":
                return 0
            # dead_letter — log details, continue
            log.warning(
                "stream.dead_letter",
                record_count=len(buffer),
                error=str(exc),
            )
            return 0
