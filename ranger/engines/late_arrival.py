"""Late-arrival handler for Ranger engines.

Implements late-arrival detection and handling strategies used by streaming
and event engines.  Records arriving after the watermark (minus the allowed
delay) are classified as *late* and processed according to the configured
strategy.

Strategies:
    REJECT              – silently drop the record.
    APPEND_WITH_FLAG    – keep the record but tag it with ``_is_late``.
    UPSERT_MERGE        – tag with ``_merge_mode = "upsert"`` for downstream merge.
    REPROCESS_PARTITION  – tag with ``_reprocess_partition`` so the partition
                          can be reprocessed.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any

import structlog

from ranger.core.models import Record

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Strategy enum
# ---------------------------------------------------------------------------


class LateArrivalStrategy(str, Enum):
    """Available strategies for handling late-arriving records."""

    REJECT = "reject"
    APPEND_WITH_FLAG = "append_with_flag"
    UPSERT_MERGE = "upsert_merge"
    REPROCESS_PARTITION = "reprocess_partition"


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------


class LateArrivalHandler:
    """Detect and handle late-arriving records based on a watermark.

    The watermark tracks the maximum observed event time.  A record is
    considered *late* when its ``event_time`` is older than
    ``watermark - watermark_delay_seconds``.

    Usage::

        handler = LateArrivalHandler(
            strategy=LateArrivalStrategy.APPEND_WITH_FLAG,
            watermark_delay_seconds=3600,
        )

        for record in records:
            handler.update_watermark(record.event_time)
            if handler.is_late(record):
                result = handler.handle_late_record(record)
                if result is None:
                    continue  # dropped
                record = result
            sink.write_single(record)
    """

    def __init__(
        self,
        strategy: LateArrivalStrategy = LateArrivalStrategy.APPEND_WITH_FLAG,
        watermark_delay_seconds: int = 3600,
        partition_field: str | None = None,
    ) -> None:
        self.strategy = strategy
        self.watermark_delay_seconds = watermark_delay_seconds
        self.partition_field = partition_field

        self._watermark: datetime | None = None
        self._total_late: int = 0
        self._total_processed: int = 0

        self._log = logger.bind(
            component="late_arrival",
            strategy=strategy.value,
            delay_s=watermark_delay_seconds,
        )

    # ------------------------------------------------------------------
    # Watermark management
    # ------------------------------------------------------------------

    def update_watermark(self, event_time: datetime | None) -> None:
        """Advance the watermark to *event_time* if it is newer.

        Args:
            event_time: The event time to consider.  ``None`` is ignored.
        """
        if event_time is None:
            return

        # Ensure timezone-aware comparison
        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=timezone.utc)

        if self._watermark is None or event_time > self._watermark:
            self._watermark = event_time

    # ------------------------------------------------------------------
    # Late detection
    # ------------------------------------------------------------------

    def is_late(self, record: Record) -> bool:
        """Return ``True`` if *record* arrived after the allowed watermark delay.

        A record is late when::

            record.event_time < watermark - watermark_delay_seconds

        Records without an ``event_time`` are never considered late.
        """
        if self._watermark is None or record.event_time is None:
            return False

        event_time = record.event_time
        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=timezone.utc)

        from datetime import timedelta

        cutoff = self._watermark - timedelta(seconds=self.watermark_delay_seconds)
        return event_time < cutoff

    # ------------------------------------------------------------------
    # Handling
    # ------------------------------------------------------------------

    def handle_late_record(self, record: Record) -> Record | None:
        """Apply the configured strategy to a late record.

        Args:
            record: A record that has been identified as late.

        Returns:
            The (possibly modified) record, or ``None`` if the record
            should be dropped.
        """
        self._total_late += 1
        self._total_processed += 1

        if self.strategy == LateArrivalStrategy.REJECT:
            self._log.warning(
                "late_arrival.rejected",
                event_time=str(record.event_time),
                watermark=str(self._watermark),
            )
            return None

        if self.strategy == LateArrivalStrategy.APPEND_WITH_FLAG:
            record._is_late = True
            record.source_metadata["_is_late"] = True
            self._log.debug(
                "late_arrival.flagged",
                event_time=str(record.event_time),
            )
            return record

        if self.strategy == LateArrivalStrategy.UPSERT_MERGE:
            record.source_metadata["_merge_mode"] = "upsert"
            record._is_late = True
            self._log.debug(
                "late_arrival.upsert_merge",
                event_time=str(record.event_time),
            )
            return record

        if self.strategy == LateArrivalStrategy.REPROCESS_PARTITION:
            partition_value: Any = None
            if self.partition_field and self.partition_field in record.data:
                partition_value = record.data[self.partition_field]
            elif self.partition_field and self.partition_field in record.source_metadata:
                partition_value = record.source_metadata[self.partition_field]

            record.source_metadata["_reprocess_partition"] = partition_value
            record._is_late = True
            self._log.debug(
                "late_arrival.reprocess_partition",
                event_time=str(record.event_time),
                partition=partition_value,
            )
            return record

        # Fallback — should not reach here
        self._log.warning("late_arrival.unknown_strategy", strategy=self.strategy.value)
        return record

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def get_stats(self) -> dict[str, Any]:
        """Return summary statistics for the handler.

        Returns:
            Dictionary with ``total_late``, ``total_processed``, and
            ``current_watermark``.
        """
        return {
            "total_late": self._total_late,
            "total_processed": self._total_processed,
            "current_watermark": self._watermark.isoformat() if self._watermark else None,
        }
