"""Abstract base class for all Ranger source connectors.

Every source must implement the core read lifecycle. Optionally supports
streaming, API-paginated reading, schema discovery, and DuckDB registration.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Iterator
from typing import TYPE_CHECKING, Any

from ranger.core.models import (
    DiscoveredSchema,
    HealthStatus,
    PaginationConfig,
    Record,
    Schema,
    SchemaVersion,
    StreamConfig,
)

if TYPE_CHECKING:
    from ranger.schema.duckdb_registry import DuckDBSchemaRegistry


class BaseSource(ABC):
    """Enhanced base source with stream, API, and schema capabilities.

    All concrete source connectors (relational, Kafka, REST API, SFTP, etc.)
    inherit from this class and implement the relevant methods.

    Lifecycle::

        source = MySource(config)
        source.connect()
        try:
            for record in source.read():
                ...
        finally:
            source.close()
    """

    def __init__(self, config: dict[str, Any]) -> None:
        self._config = config
        self._connected = False

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the data source.

        Raises:
            ConnectionError: If the source is unreachable.
        """
        ...

    @abstractmethod
    def close(self) -> None:
        """Release all resources and connections."""
        ...

    def health_check(self) -> HealthStatus:
        """Check connectivity and health of the source.

        Default implementation attempts connect/close.  Override for
        lightweight health probes.
        """
        try:
            self.connect()
            self.close()
            return HealthStatus.HEALTHY
        except Exception:
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Reading modes
    # ------------------------------------------------------------------

    @abstractmethod
    def read(self) -> Iterator[Record]:
        """Read all available records from the source.

        This is the standard pull-based reading mode suitable for batch
        and incremental extraction.

        Yields:
            Record objects with populated ``data`` and ``event_time``.
        """
        ...

    def read_stream(self, config: StreamConfig | None = None) -> AsyncIterator[Record]:
        """Read from streaming sources — WebSockets, SSE, gRPC streams, CDC logs.

        Override this method in sources that support continuous streaming.
        The default raises ``NotImplementedError``.

        Args:
            config: Optional streaming configuration (buffer size, flush interval, etc.)

        Yields:
            Record objects as they arrive, asynchronously.

        Raises:
            NotImplementedError: If the source does not support streaming.
        """
        raise NotImplementedError(f"{self.__class__.__name__} does not support streaming reads")

    def read_api(self, pagination: PaginationConfig | None = None) -> Iterator[Record]:
        """Read from API sources with automatic pagination, rate limiting, retry.

        Override this method in API-based sources (REST, GraphQL).
        The default falls back to ``read()``.

        Args:
            pagination: Pagination configuration (strategy, page size, rate limit).

        Yields:
            Record objects from paginated API responses.
        """
        yield from self.read()

    def read_batch(self, batch_size: int = 10_000) -> Iterator[list[Record]]:
        """Read in explicit batches for memory-bounded processing.

        Default implementation collects records from ``read()`` into
        batches of ``batch_size``.

        Args:
            batch_size: Number of records per batch.

        Yields:
            Lists of Record objects, each up to ``batch_size`` length.
        """
        batch: list[Record] = []
        for record in self.read():
            batch.append(record)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    # ------------------------------------------------------------------
    # Schema operations
    # ------------------------------------------------------------------

    @abstractmethod
    def get_schema(self) -> Schema:
        """Return the schema for data this source produces.

        For sources with a fixed schema (relational tables, typed message
        formats), this returns the known schema.  For schema-less sources
        (JSON APIs, MongoDB), this infers the schema from a sample.
        """
        ...

    def discover_schema(self) -> DiscoveredSchema:
        """Actively discover the schema by introspecting the source.

        Goes beyond ``get_schema()`` by querying the source catalog — e.g.,
        ``INFORMATION_SCHEMA`` for databases, introspection for GraphQL,
        API spec for REST.

        Returns:
            DiscoveredSchema with catalog metadata (object name, type, row count estimate).
        """
        base = self.get_schema()
        return DiscoveredSchema(
            columns=base.columns,
            primary_key=base.primary_key,
            partition_columns=base.partition_columns,
            source_name=self.source_type,
            object_name=self._config.get("table", self._config.get("topic", "unknown")),
            object_type="unknown",
        )

    def register_schema(self, registry: DuckDBSchemaRegistry) -> SchemaVersion:
        """Write discovered schema to the DuckDB metadata layer.

        Args:
            registry: The DuckDB schema registry instance.

        Returns:
            The registered SchemaVersion.
        """
        discovered = self.discover_schema()
        pipeline_name = self._config.get("pipeline_name", "unknown")
        return registry.register_schema(
            pipeline=pipeline_name,
            source=discovered.source_name,
            schema=discovered,
        )

    # ------------------------------------------------------------------
    # Metadata properties
    # ------------------------------------------------------------------

    @property
    @abstractmethod
    def source_type(self) -> str:
        """Return the source type identifier (e.g., 'postgresql', 'kafka', 'rest_api')."""
        ...

    @property
    def supports_streaming(self) -> bool:
        """Whether this source supports ``read_stream()``."""
        return False

    @property
    def supports_incremental(self) -> bool:
        """Whether this source supports incremental / watermark-based reads."""
        return False

    @property
    def config(self) -> dict[str, Any]:
        """Return a copy of the source configuration."""
        return dict(self._config)

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> BaseSource:
        self.connect()
        return self

    def __exit__(self, exc_type: type | None, exc_val: Exception | None, exc_tb: Any) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} type={self.source_type}>"
