"""Schema auto-evolver — applies compatible changes to sink schemas.

Handles ALTER TABLE, Parquet schema merge, Delta Lake evolution, etc.
"""

from __future__ import annotations

import logging
from typing import Any

from ranger.core.models import ColumnChange, Schema, SchemaDiff
from ranger.sinks.base import BaseSink

logger = logging.getLogger(__name__)


class SchemaEvolver:
    """Apply compatible schema changes to a sink destination.

    Dispatches evolution logic based on the sink type.

    Usage::

        evolver = SchemaEvolver()
        evolver.evolve(sink, old_schema, new_schema, diff)
    """

    def evolve(
        self,
        sink: BaseSink,
        old_schema: Schema,
        new_schema: Schema,
        diff: SchemaDiff,
    ) -> bool:
        """Apply schema evolution to the sink.

        Args:
            sink: The target sink to evolve.
            old_schema: The previous schema.
            new_schema: The new schema to evolve to.
            diff: The computed diff between schemas.

        Returns:
            True if evolution was applied successfully.

        Raises:
            ValueError: If the diff contains breaking changes that cannot be auto-evolved.
        """
        if diff.has_breaking_changes:
            breaking = [c for c in diff.changes if c.classification.value == "breaking"]
            raise ValueError(
                f"Cannot auto-evolve breaking changes: "
                f"{[c.column_name + ': ' + c.change_type for c in breaking]}"
            )

        if not diff.changes:
            logger.debug("No schema changes to apply")
            return True

        # Log what we're evolving
        for change in diff.changes:
            logger.info(
                "Evolving schema: %s column '%s' (type=%s)",
                change.change_type,
                change.column_name,
                change.new_value or change.old_value,
            )

        # Delegate to the sink's schema evolution mechanism
        if sink.supports_schema_evolution():
            sink.evolve_schema(new_schema)
            logger.info(
                "Schema evolution applied to %s: %d changes",
                sink.sink_type,
                len(diff.changes),
            )
            return True
        else:
            logger.warning(
                "Sink %s does not support schema evolution — skipping",
                sink.sink_type,
            )
            return False
