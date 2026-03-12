"""Plugin registry — auto-discover sources, engines, and sinks via entry points.

Ranger uses Python entry points to register plugins.  Each plugin group
(``ranger.sources``, ``ranger.engines``, ``ranger.sinks``) contains named
entry points that resolve to concrete classes.

Example ``pyproject.toml`` entry::

    [project.entry-points."ranger.sources"]
    postgresql = "ranger.sources.relational:RelationalSource"
"""

from __future__ import annotations

import importlib.metadata
import logging
from typing import Any

from ranger.engines.base import BaseEngine
from ranger.sinks.base import BaseSink
from ranger.sources.base import BaseSource

logger = logging.getLogger(__name__)

# Entry point group names
SOURCE_GROUP = "ranger.sources"
ENGINE_GROUP = "ranger.engines"
SINK_GROUP = "ranger.sinks"


class PluginRegistry:
    """Central registry for all Ranger connector plugins.

    Discovers plugins via Python entry points and caches them
    for instantiation by the pipeline orchestrator.
    """

    def __init__(self) -> None:
        self._sources: dict[str, type[BaseSource]] = {}
        self._engines: dict[str, type[BaseEngine]] = {}
        self._sinks: dict[str, type[BaseSink]] = {}
        self._loaded = False

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    def load_plugins(self) -> None:
        """Discover and load all registered plugins from entry points."""
        if self._loaded:
            return

        self._sources = self._load_group(SOURCE_GROUP)
        self._engines = self._load_group(ENGINE_GROUP)
        self._sinks = self._load_group(SINK_GROUP)
        self._loaded = True

        logger.info(
            "Ranger plugins loaded: %d sources, %d engines, %d sinks",
            len(self._sources),
            len(self._engines),
            len(self._sinks),
        )

    def _load_group(self, group: str) -> dict[str, Any]:
        """Load all entry points from a given group."""
        plugins: dict[str, Any] = {}
        eps = importlib.metadata.entry_points()

        # Python 3.12+ returns a SelectableGroups; 3.9+ returns a dict
        group_eps = eps.select(group=group) if hasattr(eps, "select") else eps.get(group, [])

        for ep in group_eps:
            try:
                cls = ep.load()
                plugins[ep.name] = cls
                logger.debug("Loaded plugin %s.%s → %s", group, ep.name, cls.__name__)
            except Exception:
                logger.warning("Failed to load plugin %s.%s", group, ep.name, exc_info=True)

        return plugins

    # ------------------------------------------------------------------
    # Manual registration (for testing / dynamic registration)
    # ------------------------------------------------------------------

    def register_source(self, name: str, cls: type[BaseSource]) -> None:
        """Manually register a source class."""
        self._sources[name] = cls

    def register_engine(self, name: str, cls: type[BaseEngine]) -> None:
        """Manually register an engine class."""
        self._engines[name] = cls

    def register_sink(self, name: str, cls: type[BaseSink]) -> None:
        """Manually register a sink class."""
        self._sinks[name] = cls

    # ------------------------------------------------------------------
    # Instantiation
    # ------------------------------------------------------------------

    def create_source(self, source_type: str, config: dict[str, Any]) -> BaseSource:
        """Create a source instance from the registry.

        Args:
            source_type: Registered name (e.g., 'postgresql', 'kafka').
            config: Source configuration dict.

        Returns:
            Instantiated BaseSource subclass.

        Raises:
            KeyError: If source_type is not registered.
        """
        self._ensure_loaded()
        if source_type not in self._sources:
            raise KeyError(
                f"Unknown source type '{source_type}'. "
                f"Available: {list(self._sources.keys())}"
            )
        return self._sources[source_type](config)

    def create_engine(self, engine_type: str) -> BaseEngine:
        """Create an engine instance from the registry.

        Args:
            engine_type: Registered name (e.g., 'batch', 'stream', 'event').

        Returns:
            Instantiated BaseEngine subclass.

        Raises:
            KeyError: If engine_type is not registered.
        """
        self._ensure_loaded()
        if engine_type not in self._engines:
            raise KeyError(
                f"Unknown engine type '{engine_type}'. "
                f"Available: {list(self._engines.keys())}"
            )
        return self._engines[engine_type]()

    def create_sink(self, sink_type: str, config: dict[str, Any]) -> BaseSink:
        """Create a sink instance from the registry.

        Args:
            sink_type: Registered name (e.g., 's3', 'webhook', 'kafka_producer').
            config: Sink configuration dict.

        Returns:
            Instantiated BaseSink subclass.

        Raises:
            KeyError: If sink_type is not registered.
        """
        self._ensure_loaded()
        if sink_type not in self._sinks:
            raise KeyError(
                f"Unknown sink type '{sink_type}'. "
                f"Available: {list(self._sinks.keys())}"
            )
        return self._sinks[sink_type](config)

    # ------------------------------------------------------------------
    # Listing
    # ------------------------------------------------------------------

    def list_sources(self) -> list[str]:
        self._ensure_loaded()
        return sorted(self._sources.keys())

    def list_engines(self) -> list[str]:
        self._ensure_loaded()
        return sorted(self._engines.keys())

    def list_sinks(self) -> list[str]:
        self._ensure_loaded()
        return sorted(self._sinks.keys())

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _ensure_loaded(self) -> None:
        if not self._loaded:
            self.load_plugins()


# Singleton instance
registry = PluginRegistry()
