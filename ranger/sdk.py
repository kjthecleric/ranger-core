"""Ranger Python SDK — programmatic interface for external integrations.

Used by scheduler integrations (Airflow, Prefect, Dagster) and the
Ranger API to trigger pipeline runs.

Usage::

    from ranger.sdk import RangerClient

    client = RangerClient()
    result = client.run_pipeline("configs/orders.yaml")
"""

from __future__ import annotations

from typing import Any

from ranger.core.models import RunResult, TriggeredBy
from ranger.core.pipeline import Pipeline


class RangerClient:
    """Client for programmatic pipeline execution.

    Can run locally (in-process) or remotely via the Ranger API.
    """

    def __init__(
        self,
        api_url: str | None = None,
        metadata_db: str = "ranger_meta.duckdb",
    ) -> None:
        self._api_url = api_url
        self._metadata_db = metadata_db

    def run_pipeline(
        self,
        config_path: str | None = None,
        config_dict: dict[str, Any] | None = None,
        triggered_by: TriggeredBy = TriggeredBy.API,
        **overrides: Any,
    ) -> RunResult:
        """Run a pipeline.

        Either ``config_path`` (YAML file) or ``config_dict`` must be provided.

        Args:
            config_path: Path to a YAML pipeline config.
            config_dict: Raw config dictionary.
            triggered_by: What triggered this run.
            **overrides: Key-value overrides to merge into the config.

        Returns:
            RunResult with full metrics.
        """
        if self._api_url:
            return self._run_remote(config_path, config_dict, triggered_by, **overrides)

        return self._run_local(config_path, config_dict, triggered_by, **overrides)

    def _run_local(
        self,
        config_path: str | None,
        config_dict: dict[str, Any] | None,
        triggered_by: TriggeredBy,
        **overrides: Any,
    ) -> RunResult:
        """Run the pipeline in-process."""
        if config_path:
            pipeline = Pipeline.from_yaml(config_path, metadata_db=self._metadata_db)
        elif config_dict:
            if overrides:
                config_dict = {**config_dict, **overrides}
            pipeline = Pipeline.from_dict(config_dict, metadata_db=self._metadata_db)
        else:
            raise ValueError("Either config_path or config_dict must be provided")

        return pipeline.execute(triggered_by=triggered_by)

    def _run_remote(
        self,
        config_path: str | None,
        config_dict: dict[str, Any] | None,
        triggered_by: TriggeredBy,
        **overrides: Any,
    ) -> RunResult:
        """Run the pipeline via the Ranger API."""
        try:
            import httpx
        except ImportError as exc:
            raise ImportError("Install httpx for remote execution: pip install httpx") from exc

        payload: dict[str, Any] = {"triggered_by": triggered_by.value}
        if config_path:
            payload["config_path"] = config_path
        if config_dict:
            payload["config"] = config_dict
        if overrides:
            payload["overrides"] = overrides

        response = httpx.post(
            f"{self._api_url}/api/v1/pipelines/run",
            json=payload,
            timeout=300.0,
        )
        response.raise_for_status()
        return RunResult.model_validate(response.json())

    def validate_pipeline(self, config_path: str) -> list[str]:
        """Validate a pipeline config without running it.

        Returns:
            List of validation error messages.
        """
        pipeline = Pipeline.from_yaml(config_path, metadata_db=self._metadata_db)
        return pipeline.validate()
