"""Schema drift detector — compares inferred schema against registered schema.

Classifies each change as compatible or breaking and returns a DriftEvent.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from ranger.core.models import (
    DriftAction,
    DriftClassification,
    DriftEvent,
    Schema,
    SchemaDiff,
    SchemaConfig,
)
from ranger.schema.duckdb_registry import DuckDBSchemaRegistry

logger = logging.getLogger(__name__)


class SchemaDriftDetector:
    """Detect schema drift between the current source schema and the registered version.

    Usage::

        detector = SchemaDriftDetector(registry, config)
        drift_event = detector.detect("my_pipeline", "my_source", new_schema)
    """

    def __init__(self, registry: DuckDBSchemaRegistry, config: SchemaConfig) -> None:
        self._registry = registry
        self._config = config

    def detect(self, pipeline: str, source: str, new_schema: Schema) -> DriftEvent | None:
        """Compare the new schema against the registered active schema.

        Args:
            pipeline: Pipeline name.
            source: Source name.
            new_schema: The newly inferred schema from the source.

        Returns:
            A DriftEvent if drift is detected, or None if schemas match.
        """
        active_version = self._registry.get_active_schema_version(pipeline, source)

        if active_version is None:
            # First schema — register it, no drift
            self._registry.register_schema(pipeline, source, new_schema)
            logger.info("Initial schema registered for %s/%s", pipeline, source)
            return None

        # Compare
        diff = self._registry._diff_schemas(
            old_id=active_version.schema_id,
            new_id="pending",
            old=active_version.schema_definition,
            new=new_schema,
        )

        if not diff.changes:
            return None

        # Determine action based on config
        if diff.has_breaking_changes:
            action_str = self._config.drift_handling.get("breaking", "quarantine")
        else:
            action_str = self._config.drift_handling.get("compatible", "auto_evolve")

        action = DriftAction(action_str)

        # Register new schema version
        new_version = self._registry.register_schema(pipeline, source, new_schema)

        # Create drift event
        event = DriftEvent(
            pipeline_name=pipeline,
            previous_schema_id=active_version.schema_id,
            new_schema_id=new_version.schema_id,
            drift_type=diff.overall_classification,
            changes=diff.changes,
            action_taken=action,
        )

        # Log the drift
        self._registry.log_drift(event)

        logger.warning(
            "Schema drift detected for %s/%s: %s (%d changes, action=%s)",
            pipeline,
            source,
            diff.overall_classification.value,
            len(diff.changes),
            action.value,
        )

        return event
