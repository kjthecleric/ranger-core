"""Abstract base class for all Ranger sink connectors.

Sinks are responsible for writing records to a destination with schema
awareness and evolution support.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from ranger.core.models import HealthStatus, Record, Schema


class BaseSink(ABC):
    """Base sink for landing data to a destination.

    Lifecycle::

        sink = MySink(config)
        sink.open(schema)
        try:
            sink.write_batch(records)
        finally:
            sink.close()
    """

    def __init__(self, config: dict[str, Any]) -> None:
        self._config = config
        self._opened = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @abstractmethod
    def open(self, schema: Schema) -> None:
        """Prepare the sink for writing with the given schema.

        This may create tables, initialize writers, validate permissions, etc.

        Args:
            schema: The schema of the data about to be written.
        """
        ...

    @abstractmethod
    def close(self) -> None:
        """Flush remaining data and release resources."""
        ...

    def health_check(self) -> HealthStatus:
        """Check connectivity and health of the sink destination."""
        try:
            # Subclasses should override with a lightweight probe
            return HealthStatus.HEALTHY
        except Exception:
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Writing
    # ------------------------------------------------------------------

    @abstractmethod
    def write_batch(self, records: list[Record]) -> int:
        """Write a batch of records to the destination.

        Args:
            records: List of Record objects to write.

        Returns:
            Number of records successfully written.
        """
        ...

    def write_single(self, record: Record) -> bool:
        """Write a single record.  Default wraps ``write_batch``.

        Args:
            record: Single Record to write.

        Returns:
            True if the record was written successfully.
        """
        return self.write_batch([record]) == 1

    # ------------------------------------------------------------------
    # Schema evolution
    # ------------------------------------------------------------------

    @abstractmethod
    def evolve_schema(self, new_schema: Schema) -> None:
        """Apply a compatible schema change to the destination.

        For example: ``ALTER TABLE ADD COLUMN``, Parquet schema merge,
        Delta Lake schema evolution, etc.

        Args:
            new_schema: The new schema to evolve to.
        """
        ...

    def supports_schema_evolution(self) -> bool:
        """Whether this sink supports automatic schema evolution."""
        return True

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    @property
    @abstractmethod
    def sink_type(self) -> str:
        """Return the sink type identifier (e.g., 's3', 'snowflake', 'webhook')."""
        ...

    @property
    def config(self) -> dict[str, Any]:
        """Return a copy of the sink configuration."""
        return dict(self._config)

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> BaseSink:
        return self

    def __exit__(self, exc_type: type | None, exc_val: Exception | None, exc_tb: Any) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} type={self.sink_type}>"
