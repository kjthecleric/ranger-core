"""Abstract base class for Ranger execution engines.

Engines orchestrate the full read → schema check → late-arrival handling → write
flow.  Concrete implementations include Batch, Stream, and Event engines.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from ranger.core.models import PipelineConfig, RunResult
from ranger.sinks.base import BaseSink
from ranger.sources.base import BaseSource


class BaseEngine(ABC):
    """Base engine that drives a pipeline run.

    The engine is responsible for:
    1. Reading records from the source (in the appropriate mode)
    2. Running schema drift detection
    3. Applying late-arrival handling strategy
    4. Writing records to the sink
    5. Collecting metrics and returning a RunResult
    """

    @abstractmethod
    def run(self, source: BaseSource, sink: BaseSink, config: PipelineConfig) -> RunResult:
        """Execute the pipeline: read from source, process, write to sink.

        Args:
            source: Connected data source.
            sink: Opened data sink.
            config: Full pipeline configuration.

        Returns:
            RunResult with metrics and status.
        """
        ...

    @abstractmethod
    def validate(self, source: BaseSource, sink: BaseSink, config: PipelineConfig) -> list[str]:
        """Dry-run validation without writing data.

        Returns:
            List of validation error messages (empty if valid).
        """
        ...
