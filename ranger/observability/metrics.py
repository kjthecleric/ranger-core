"""Metrics collection and exposition for Ranger pipelines.

Collects counters, gauges, and timings, and can export them
in Prometheus exposition format or push to StatsD.
"""

from __future__ import annotations

import socket
import time
from contextlib import contextmanager
from typing import Any, Iterator

import structlog

logger = structlog.get_logger(__name__)


class MetricsCollector:
    """Collects and exposes pipeline run metrics.

    Supports three metric types:

    * **counter** — monotonically increasing values (records processed, errors).
    * **gauge** — point-in-time values (queue depth, memory usage).
    * **timing** — duration measurements in seconds.

    Usage::

        metrics = MetricsCollector(prefix="ranger_pipeline")
        metrics.increment("records_read", 500)
        metrics.gauge("queue_depth", 42)
        metrics.timing("stage_transform", 1.234)

        # Export
        print(metrics.to_prometheus())
        metrics.to_statsd("localhost", 8125)
    """

    def __init__(self, prefix: str = "ranger") -> None:
        self._prefix = prefix
        self._counters: dict[str, float] = {}
        self._gauges: dict[str, float] = {}
        self._timings: dict[str, list[float]] = {}

    # ------------------------------------------------------------------
    # Recording methods
    # ------------------------------------------------------------------

    def increment(self, name: str, value: float = 1.0) -> None:
        """Increment a counter.

        Args:
            name: Metric name.
            value: Amount to add (default 1).
        """
        self._counters[name] = self._counters.get(name, 0.0) + value

    def record_count(self, name: str, value: float) -> None:
        """Set a counter to an absolute value.

        Args:
            name: Metric name.
            value: Absolute count value.
        """
        self._counters[name] = value

    def gauge(self, name: str, value: float) -> None:
        """Set a gauge value.

        Args:
            name: Metric name.
            value: Current value.
        """
        self._gauges[name] = value

    def timing(self, name: str, seconds: float) -> None:
        """Record a timing measurement.

        Args:
            name: Metric name.
            seconds: Duration in seconds.
        """
        self._timings.setdefault(name, []).append(seconds)

    @contextmanager
    def timer(self, name: str) -> Iterator[None]:
        """Context manager that automatically records elapsed time.

        Usage::

            with metrics.timer("transform_stage"):
                do_work()
        """
        start = time.monotonic()
        try:
            yield
        finally:
            elapsed = time.monotonic() - start
            self.timing(name, elapsed)

    # ------------------------------------------------------------------
    # Retrieval
    # ------------------------------------------------------------------

    def get_all(self) -> dict[str, Any]:
        """Return all metrics as a dict.

        Returns:
            Dict with keys ``counters``, ``gauges``, ``timings``.
            Timings include count, sum, min, max, and avg.
        """
        timing_summaries: dict[str, dict[str, float]] = {}
        for name, values in self._timings.items():
            timing_summaries[name] = {
                "count": len(values),
                "sum": sum(values),
                "min": min(values),
                "max": max(values),
                "avg": sum(values) / len(values) if values else 0.0,
            }

        return {
            "counters": dict(self._counters),
            "gauges": dict(self._gauges),
            "timings": timing_summaries,
        }

    def get_counter(self, name: str) -> float:
        """Get a counter value (0 if not set)."""
        return self._counters.get(name, 0.0)

    def get_gauge(self, name: str) -> float:
        """Get a gauge value (0 if not set)."""
        return self._gauges.get(name, 0.0)

    # ------------------------------------------------------------------
    # Prometheus exposition
    # ------------------------------------------------------------------

    def to_prometheus(self) -> str:
        """Render all metrics in Prometheus exposition format.

        Returns:
            Multi-line string suitable for ``/metrics`` endpoint.
        """
        lines: list[str] = []

        # Counters
        for name, value in sorted(self._counters.items()):
            metric_name = self._prom_name(name)
            lines.append(f"# TYPE {metric_name}_total counter")
            lines.append(f"{metric_name}_total {value}")

        # Gauges
        for name, value in sorted(self._gauges.items()):
            metric_name = self._prom_name(name)
            lines.append(f"# TYPE {metric_name} gauge")
            lines.append(f"{metric_name} {value}")

        # Timings (as summary-style)
        for name, values in sorted(self._timings.items()):
            metric_name = self._prom_name(name)
            count = len(values)
            total = sum(values)
            lines.append(f"# TYPE {metric_name}_seconds summary")
            lines.append(f"{metric_name}_seconds_count {count}")
            lines.append(f"{metric_name}_seconds_sum {total}")

        lines.append("")  # trailing newline
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # StatsD
    # ------------------------------------------------------------------

    def to_statsd(self, host: str = "localhost", port: int = 8125) -> None:
        """Send all metrics to a StatsD server over UDP.

        Args:
            host: StatsD host.
            port: StatsD UDP port (default 8125).
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            for name, value in self._counters.items():
                metric_name = self._statsd_name(name)
                self._send_udp(sock, host, port, f"{metric_name}:{value}|c")

            for name, value in self._gauges.items():
                metric_name = self._statsd_name(name)
                self._send_udp(sock, host, port, f"{metric_name}:{value}|g")

            for name, values in self._timings.items():
                metric_name = self._statsd_name(name)
                for v in values:
                    # StatsD timing is in milliseconds
                    ms = v * 1000
                    self._send_udp(sock, host, port, f"{metric_name}:{ms}|ms")

            logger.info(
                "statsd_metrics_sent",
                host=host,
                port=port,
                counters=len(self._counters),
                gauges=len(self._gauges),
                timings=len(self._timings),
            )
        except Exception as exc:
            logger.error("statsd_send_failed", host=host, port=port, error=str(exc))
        finally:
            sock.close()

    # ------------------------------------------------------------------
    # Reset
    # ------------------------------------------------------------------

    def reset(self) -> None:
        """Clear all collected metrics."""
        self._counters.clear()
        self._gauges.clear()
        self._timings.clear()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _prom_name(self, name: str) -> str:
        """Convert a metric name to a valid Prometheus metric name."""
        safe = name.replace(".", "_").replace("-", "_").replace(" ", "_")
        return f"{self._prefix}_{safe}"

    def _statsd_name(self, name: str) -> str:
        """Convert a metric name to a StatsD-style dotted name."""
        safe = name.replace(" ", "_").replace("-", "_")
        return f"{self._prefix}.{safe}"

    @staticmethod
    def _send_udp(sock: socket.socket, host: str, port: int, data: str) -> None:
        """Send a single UDP datagram."""
        try:
            sock.sendto(data.encode("utf-8"), (host, port))
        except OSError:
            pass  # Best-effort for StatsD
