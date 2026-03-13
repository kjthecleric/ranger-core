"""Event engine — event-driven, low-latency processing with optional windowing.

Designed for real-time event streams where each event should be processed
individually (or accumulated into time-based windows) with minimal latency.
Supports tumbling, sliding, and session windows.
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
# Constants
# ---------------------------------------------------------------------------

_VALID_ERROR_STRATEGIES = {"skip", "dead_letter", "fail"}
_VALID_WINDOW_TYPES = {"none", "tumbling", "sliding", "session"}


# ---------------------------------------------------------------------------
# Window helpers
# ---------------------------------------------------------------------------


class _TumblingWindow:
    """Fixed-size, non-overlapping time window."""

    def __init__(self, size_seconds: float) -> None:
        self.size_seconds = size_seconds
        self._buffer: list[Record] = []
        self._window_start: float | None = None

    def add(self, record: Record, now: float) -> list[Record] | None:
        """Add a record.  Returns the flushed window if it is complete."""
        if self._window_start is None:
            self._window_start = now

        self._buffer.append(record)

        if (now - self._window_start) >= self.size_seconds:
            return self.flush()
        return None

    def flush(self) -> list[Record]:
        """Force-flush the current window."""
        flushed = self._buffer
        self._buffer = []
        self._window_start = None
        return flushed

    @property
    def pending(self) -> list[Record]:
        return self._buffer


class _SlidingWindow:
    """Overlapping windows of *size_seconds* that advance by *slide_seconds*.

    Events are collected continuously.  Each time the slide interval
    elapses a snapshot of all events within the most recent window is
    emitted.
    """

    def __init__(self, size_seconds: float, slide_seconds: float) -> None:
        self.size_seconds = size_seconds
        self.slide_seconds = slide_seconds
        self._buffer: list[tuple[float, Record]] = []  # (timestamp, record)
        self._last_slide: float | None = None

    def add(self, record: Record, now: float) -> list[Record] | None:
        self._buffer.append((now, record))

        if self._last_slide is None:
            self._last_slide = now

        if (now - self._last_slide) >= self.slide_seconds:
            return self.flush(now)
        return None

    def flush(self, now: float | None = None) -> list[Record]:
        if now is None:
            now = time.monotonic()
        cutoff = now - self.size_seconds
        # Emit all records within the window
        window = [r for ts, r in self._buffer if ts >= cutoff]
        # Evict records older than the window
        self._buffer = [(ts, r) for ts, r in self._buffer if ts >= cutoff]
        self._last_slide = now
        return window

    @property
    def pending(self) -> list[Record]:
        return [r for _, r in self._buffer]


class _SessionWindow:
    """Session window that closes after *gap_seconds* of inactivity."""

    def __init__(self, gap_seconds: float) -> None:
        self.gap_seconds = gap_seconds
        self._buffer: list[Record] = []
        self._last_event: float | None = None

    def add(self, record: Record, now: float) -> list[Record] | None:
        if self._last_event is not None and (now - self._last_event) >= self.gap_seconds:
            flushed = self.flush()
            # Start new session with current record
            self._buffer.append(record)
            self._last_event = now
            return flushed

        self._buffer.append(record)
        self._last_event = now
        return None

    def flush(self) -> list[Record]:
        flushed = self._buffer
        self._buffer = []
        self._last_event = None
        return flushed

    @property
    def pending(self) -> list[Record]:
        return self._buffer


# ---------------------------------------------------------------------------
# Event Engine
# ---------------------------------------------------------------------------


class EventEngine(BaseEngine):
    """Event-driven execution engine with optional windowing.

    Processes events individually or in time-based windows from a
    streaming source.  Features:

    * **Per-event processing** (no windowing) — each record is written
      immediately.
    * **Tumbling windows** — fixed non-overlapping windows of
      *window_size_seconds*.
    * **Sliding windows** — overlapping windows of *window_size_seconds*
      that advance by *window_slide_seconds*.
    * **Session windows** — windows that close after *window_size_seconds*
      of inactivity.
    * **Concurrency control** — limits concurrent event handlers via
      *concurrency*.
    * **Per-event timeout** — *timeout_seconds* for individual event
      processing.
    * **Late-arrival detection** using :class:`LateArrivalHandler`.
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

        if config.engine.type != "event":
            errors.append(
                f"EventEngine received engine type '{config.engine.type}', expected 'event'"
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

        window_type = ecfg.get("window_type", "none")
        if window_type not in _VALID_WINDOW_TYPES:
            errors.append(
                f"Invalid window_type '{window_type}'; must be one of {_VALID_WINDOW_TYPES}"
            )

        if window_type == "sliding" and not ecfg.get("window_slide_seconds"):
            errors.append("Sliding windows require 'window_slide_seconds' to be set")

        return errors

    def run(
        self,
        source: BaseSource,
        sink: BaseSink,
        config: PipelineConfig,
    ) -> RunResult:
        """Execute the event-driven pipeline.

        Internally delegates to an asyncio event loop so that
        ``source.read_stream()`` can be consumed asynchronously.
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
        log = logger.bind(pipeline=pipeline_name, engine="event")
        start_time = datetime.now(timezone.utc)

        result = RunResult(
            pipeline_name=pipeline_name,
            status=RunStatus.RUNNING,
            started_at=start_time,
        )

        # ---- Extract engine config ---------------------------------
        ecfg: dict[str, Any] = config.engine.config
        concurrency: int = ecfg.get("concurrency", 1)
        timeout_seconds: float = ecfg.get("timeout_seconds", 30.0)
        window_type: str = ecfg.get("window_type", "none")
        window_size: float = ecfg.get("window_size_seconds", 60.0)
        window_slide: float = ecfg.get("window_slide_seconds", 10.0)
        error_strategy: str = ecfg.get("error_strategy", "skip")
        max_events: int | None = ecfg.get("max_events")

        # ---- Late-arrival handler ----------------------------------
        la_cfg = config.engine.late_arrival
        strategy_map = {
            "watermark": LateArrivalStrategy.REJECT,
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

        # ---- Window setup ------------------------------------------
        window = self._create_window(window_type, window_size, window_slide)

        # ---- Concurrency semaphore ---------------------------------
        sem = asyncio.Semaphore(concurrency)

        # ---- Metrics -----------------------------------------------
        total_events = 0
        total_written = 0
        total_failed = 0
        total_late = 0
        windows_flushed = 0
        latency_sum = 0.0

        try:
            log.info(
                "event.starting",
                window_type=window_type,
                concurrency=concurrency,
                timeout_seconds=timeout_seconds,
                max_events=max_events,
                error_strategy=error_strategy,
            )

            # Connect source & open sink
            source.connect()
            sink.open(source.get_schema())

            stream_config = StreamConfig(
                buffer_size=ecfg.get("buffer_size", 100),
                flush_interval_seconds=ecfg.get("flush_interval_seconds", 1.0),
            )
            stream = source.read_stream(config=stream_config)

            async for record in stream:
                event_start = time.monotonic()
                total_events += 1

                # -- Late-arrival check --------------------------------
                try:
                    late_handler.update_watermark(record.event_time)
                    if late_handler.is_late(record):
                        handled = late_handler.handle_late_record(record)
                        total_late += 1
                        if handled is None:
                            continue
                        record = handled
                except Exception as exc:
                    log.warning("event.late_check_error", error=str(exc))

                # -- Process with concurrency gate & timeout -----------
                async with sem:
                    try:
                        if window is not None:
                            now = time.monotonic()
                            flushed = window.add(record, now)
                            if flushed:
                                w = await asyncio.wait_for(
                                    self._write_window(
                                        flushed, sink, error_strategy, log
                                    ),
                                    timeout=timeout_seconds,
                                )
                                total_written += w
                                total_failed += len(flushed) - w
                                windows_flushed += 1
                        else:
                            written = await asyncio.wait_for(
                                self._write_event(
                                    record, sink, error_strategy, log
                                ),
                                timeout=timeout_seconds,
                            )
                            if written:
                                total_written += 1
                            else:
                                total_failed += 1

                    except asyncio.TimeoutError:
                        total_failed += 1
                        log.warning(
                            "event.timeout",
                            event_num=total_events,
                            timeout_seconds=timeout_seconds,
                        )
                        if error_strategy == "fail":
                            raise
                    except Exception as exc:
                        total_failed += 1
                        if error_strategy == "fail":
                            raise
                        log.warning("event.process_error", error=str(exc))

                event_elapsed = time.monotonic() - event_start
                latency_sum += event_elapsed

                # -- Bounded run check ---------------------------------
                if max_events is not None and total_events >= max_events:
                    log.info("event.max_events_reached", max_events=max_events)
                    break

            # ---- Flush remaining window records -----------------------
            if window is not None and window.pending:
                remaining = window.flush() if not isinstance(window, _SlidingWindow) else window.flush(time.monotonic())
                if remaining:
                    w = self._flush_buffer_sync(remaining, sink, error_strategy, log)
                    total_written += w
                    total_failed += len(remaining) - w
                    windows_flushed += 1

            result.status = RunStatus.SUCCESS
            avg_latency = (latency_sum / total_events) if total_events > 0 else 0.0
            log.info(
                "event.completed",
                events_processed=total_events,
                events_written=total_written,
                events_failed=total_failed,
                late_records=total_late,
                windows_flushed=windows_flushed,
                avg_latency_ms=round(avg_latency * 1000, 2),
            )

        except Exception as exc:
            result.status = RunStatus.FAILED
            result.error_message = str(exc)
            log.error("event.failed", error=str(exc), exc_info=True)

        end_time = datetime.now(timezone.utc)
        result.completed_at = end_time
        result.records_read = total_events
        result.records_written = total_written
        result.records_failed = total_failed
        result.late_records_count = total_late
        result.duration_seconds = (end_time - start_time).total_seconds()

        return result

    # ------------------------------------------------------------------
    # Window factory
    # ------------------------------------------------------------------

    @staticmethod
    def _create_window(
        window_type: str,
        window_size: float,
        window_slide: float,
    ) -> _TumblingWindow | _SlidingWindow | _SessionWindow | None:
        """Instantiate the appropriate window implementation."""
        if window_type == "tumbling":
            return _TumblingWindow(size_seconds=window_size)
        if window_type == "sliding":
            return _SlidingWindow(size_seconds=window_size, slide_seconds=window_slide)
        if window_type == "session":
            return _SessionWindow(gap_seconds=window_size)
        return None  # "none" — no windowing

    # ------------------------------------------------------------------
    # Write helpers
    # ------------------------------------------------------------------

    @staticmethod
    async def _write_event(
        record: Record,
        sink: BaseSink,
        error_strategy: str,
        log: Any,
    ) -> bool:
        """Write a single event to the sink (runs in executor for I/O)."""
        loop = asyncio.get_running_loop()
        try:
            ok = await loop.run_in_executor(None, sink.write_single, record)
            return ok
        except Exception as exc:
            if error_strategy == "fail":
                raise
            log.warning("event.write_error", error=str(exc))
            if error_strategy == "dead_letter":
                log.warning(
                    "event.dead_letter",
                    record_event_time=str(record.event_time),
                    error=str(exc),
                )
            return False

    @staticmethod
    async def _write_window(
        records: list[Record],
        sink: BaseSink,
        error_strategy: str,
        log: Any,
    ) -> int:
        """Write a flushed window batch to the sink (runs in executor)."""
        loop = asyncio.get_running_loop()
        try:
            written = await loop.run_in_executor(None, sink.write_batch, records)
            log.debug("event.window_flushed", count=written)
            return written
        except Exception as exc:
            if error_strategy == "fail":
                raise
            log.error("event.window_write_error", error=str(exc), window_size=len(records))
            return 0

    @staticmethod
    def _flush_buffer_sync(
        records: list[Record],
        sink: BaseSink,
        error_strategy: str,
        log: Any,
    ) -> int:
        """Synchronous flush for remaining records after the async loop."""
        try:
            written = sink.write_batch(records)
            log.debug("event.final_flush", count=written)
            return written
        except Exception as exc:
            if error_strategy == "fail":
                raise
            log.error("event.final_flush_error", error=str(exc))
            return 0
