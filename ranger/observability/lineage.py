"""OpenLineage emitter for Ranger pipeline lineage tracking.

Emits START, COMPLETE, and FAIL RunEvents to an OpenLineage-compatible
backend (e.g. Marquez) for data lineage and observability.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# RunResult helper
# ---------------------------------------------------------------------------


class RunResult:
    """Summary of a pipeline run, attached to COMPLETE events."""

    __slots__ = (
        "records_read",
        "records_written",
        "records_rejected",
        "bytes_read",
        "bytes_written",
        "duration_seconds",
        "extra",
    )

    def __init__(
        self,
        records_read: int = 0,
        records_written: int = 0,
        records_rejected: int = 0,
        bytes_read: int = 0,
        bytes_written: int = 0,
        duration_seconds: float = 0.0,
        extra: dict[str, Any] | None = None,
    ) -> None:
        self.records_read = records_read
        self.records_written = records_written
        self.records_rejected = records_rejected
        self.bytes_read = bytes_read
        self.bytes_written = bytes_written
        self.duration_seconds = duration_seconds
        self.extra = extra or {}

    def to_dict(self) -> dict[str, Any]:
        return {
            "records_read": self.records_read,
            "records_written": self.records_written,
            "records_rejected": self.records_rejected,
            "bytes_read": self.bytes_read,
            "bytes_written": self.bytes_written,
            "duration_seconds": self.duration_seconds,
            **self.extra,
        }


# ---------------------------------------------------------------------------
# OpenLineageEmitter
# ---------------------------------------------------------------------------


class OpenLineageEmitter:
    """Emit OpenLineage RunEvents for pipeline lineage tracking.

    Uses the ``openlineage-python`` client library to emit START, COMPLETE,
    and FAIL events to a Marquez or OpenLineage-compatible API.

    Config keys:
        api_url: OpenLineage API endpoint (e.g. ``http://localhost:5000``).
        api_key: Optional API key / bearer token.
        namespace: OpenLineage namespace (default ``"ranger"``).

    Usage::

        emitter = OpenLineageEmitter(
            api_url="http://marquez:5000",
            namespace="production",
        )
        run_id = str(uuid.uuid4())
        emitter.emit_start(run_id, "my_pipeline", "pg_source", "s3_sink")
        # ... run pipeline ...
        emitter.emit_complete(run_id, RunResult(records_read=1000, records_written=998))
    """

    def __init__(
        self,
        api_url: str,
        api_key: str | None = None,
        namespace: str = "ranger",
    ) -> None:
        try:
            from openlineage.client import OpenLineageClient
            from openlineage.client.transport.http import ApiKeyTokenProvider, HttpConfig, HttpTransport
        except ImportError as exc:
            raise ImportError(
                "Install openlineage-python for lineage support: "
                "pip install ranger-core[lineage]"
            ) from exc

        self._namespace = namespace
        self._api_url = api_url

        # Build HTTP transport
        http_config = HttpConfig(
            url=api_url,
            auth=ApiKeyTokenProvider({"apiKey": api_key}) if api_key else None,
        )
        transport = HttpTransport(http_config)
        self._client = OpenLineageClient(transport=transport)

        logger.info("openlineage_init", api_url=api_url, namespace=namespace)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def emit_start(
        self,
        run_id: str,
        pipeline_name: str,
        source_name: str,
        sink_name: str,
        schema: dict[str, Any] | None = None,
    ) -> None:
        """Emit a START RunEvent.

        Args:
            run_id: Unique run identifier.
            pipeline_name: Job / pipeline name.
            source_name: Input dataset name.
            sink_name: Output dataset name.
            schema: Optional schema dict for dataset facets.
        """
        from openlineage.client.run import InputDataset, Job, OutputDataset, Run, RunEvent, RunState

        run = Run(runId=run_id)
        job = Job(namespace=self._namespace, name=pipeline_name)

        inputs = [self.build_input_dataset(source_name, schema)]
        outputs = [self.build_output_dataset(sink_name, schema)]

        event = RunEvent(
            eventType=RunState.START,
            eventTime=self._now_iso(),
            run=run,
            job=job,
            inputs=inputs,
            outputs=outputs,
        )

        self._emit(event, "START", run_id, pipeline_name)

    def emit_complete(
        self,
        run_id: str,
        result: RunResult,
        pipeline_name: str = "",
    ) -> None:
        """Emit a COMPLETE RunEvent with metrics facets.

        Args:
            run_id: Unique run identifier.
            result: Run result metrics.
            pipeline_name: Job / pipeline name (can be empty if already started).
        """
        from openlineage.client.run import Job, Run, RunEvent, RunState

        run = Run(
            runId=run_id,
            facets=self._build_metrics_facets(result),
        )
        job = Job(namespace=self._namespace, name=pipeline_name or "unknown")

        event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=self._now_iso(),
            run=run,
            job=job,
        )

        self._emit(event, "COMPLETE", run_id, pipeline_name)

    def emit_fail(
        self,
        run_id: str,
        error: str | Exception,
        pipeline_name: str = "",
    ) -> None:
        """Emit a FAIL RunEvent.

        Args:
            run_id: Unique run identifier.
            error: Error message or exception.
            pipeline_name: Job / pipeline name.
        """
        from openlineage.client.run import Job, Run, RunEvent, RunState

        error_msg = str(error)
        run = Run(
            runId=run_id,
            facets=self._build_error_facets(error_msg),
        )
        job = Job(namespace=self._namespace, name=pipeline_name or "unknown")

        event = RunEvent(
            eventType=RunState.FAIL,
            eventTime=self._now_iso(),
            run=run,
            job=job,
        )

        self._emit(event, "FAIL", run_id, pipeline_name)

    # ------------------------------------------------------------------
    # Dataset builders
    # ------------------------------------------------------------------

    def build_input_dataset(
        self,
        source_name: str,
        schema: dict[str, Any] | None = None,
    ) -> Any:
        """Build an OpenLineage InputDataset.

        Args:
            source_name: Dataset / source name.
            schema: Optional schema dict with ``columns`` list.

        Returns:
            An ``InputDataset`` instance.
        """
        from openlineage.client.run import InputDataset

        facets = {}
        if schema:
            facets["schema"] = self._build_schema_facet(schema)

        return InputDataset(
            namespace=self._namespace,
            name=source_name,
            facets=facets,
        )

    def build_output_dataset(
        self,
        sink_name: str,
        schema: dict[str, Any] | None = None,
    ) -> Any:
        """Build an OpenLineage OutputDataset.

        Args:
            sink_name: Dataset / sink name.
            schema: Optional schema dict with ``columns`` list.

        Returns:
            An ``OutputDataset`` instance.
        """
        from openlineage.client.run import OutputDataset

        facets = {}
        if schema:
            facets["schema"] = self._build_schema_facet(schema)

        return OutputDataset(
            namespace=self._namespace,
            name=sink_name,
            facets=facets,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _emit(self, event: Any, event_type: str, run_id: str, pipeline: str) -> None:
        """Send a RunEvent to the OpenLineage backend."""
        try:
            self._client.emit(event)
            logger.info(
                "openlineage_event_emitted",
                event_type=event_type,
                run_id=run_id,
                pipeline=pipeline,
            )
        except Exception as exc:
            logger.error(
                "openlineage_emit_failed",
                event_type=event_type,
                run_id=run_id,
                error=str(exc),
            )

    @staticmethod
    def _now_iso() -> str:
        """Return the current UTC time in ISO 8601 format."""
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _build_metrics_facets(result: RunResult) -> dict[str, Any]:
        """Build run facets containing pipeline metrics."""
        return {
            "ranger_metrics": {
                "_producer": "ranger",
                "_schemaURL": "https://ranger.dev/schemas/metrics/v1",
                **result.to_dict(),
            }
        }

    @staticmethod
    def _build_error_facets(error_msg: str) -> dict[str, Any]:
        """Build run facets for an error."""
        return {
            "errorMessage": {
                "_producer": "ranger",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ErrorMessageRunFacet.json",
                "message": error_msg,
                "programmingLanguage": "python",
            }
        }

    @staticmethod
    def _build_schema_facet(schema: dict[str, Any]) -> dict[str, Any]:
        """Build a dataset schema facet from a Ranger schema dict."""
        columns = schema.get("columns", [])
        fields = []
        for col in columns:
            field: dict[str, Any] = {
                "name": col.get("name", ""),
                "type": col.get("type", "string"),
            }
            desc = col.get("description")
            if desc:
                field["description"] = desc
            fields.append(field)

        return {
            "_producer": "ranger",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
            "fields": fields,
        }

    @staticmethod
    def new_run_id() -> str:
        """Generate a new UUID run ID."""
        return str(uuid.uuid4())
