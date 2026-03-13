"""Webhook sink — delivers records to HTTP endpoints.

Uses httpx for async-capable HTTP delivery with configurable authentication,
retry logic, rate limiting, and batch/single record modes.
"""

from __future__ import annotations

import time
from typing import Any

import structlog

from ranger.core.models import HealthStatus, Record, Schema
from ranger.sinks.base import BaseSink

try:
    import httpx

    _HAS_HTTPX = True
except ImportError:  # pragma: no cover
    _HAS_HTTPX = False

logger = structlog.get_logger(__name__)


class WebhookSink(BaseSink):
    """POST / PUT / PATCH records to an HTTP endpoint.

    Config keys:
        url: Target URL (required).
        method: HTTP method — ``POST``, ``PUT``, or ``PATCH`` (default ``POST``).
        headers: Extra HTTP headers as a dict (optional).
        auth_type: ``none`` | ``bearer`` | ``basic`` | ``api_key`` (default ``none``).
        auth_config: Auth details — ``token`` for bearer, ``username`` / ``password``
            for basic, ``header`` / ``value`` for api_key (optional).
        batch_mode: ``single`` (one request per record) or ``batch`` (all records
            in one request as a JSON array) (default ``batch``).
        timeout: Request timeout in seconds (default ``30``).
        retry_count: Number of retries on failure (default ``3``).
        retry_delay: Base delay between retries in seconds (default ``1.0``).
        rate_limit: Max requests per second — ``0`` for unlimited (default ``0``).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        if not _HAS_HTTPX:
            raise ImportError(
                "httpx is required for WebhookSink. "
                "Install it with: pip install httpx"
            )
        super().__init__(config)

        self._url: str = config["url"]
        self._method: str = config.get("method", "POST").upper()
        self._headers: dict[str, str] = dict(config.get("headers", {}))
        self._auth_type: str = config.get("auth_type", "none")
        self._auth_config: dict[str, str] = config.get("auth_config", {})
        self._batch_mode: str = config.get("batch_mode", "batch")
        self._timeout: float = float(config.get("timeout", 30))
        self._retry_count: int = int(config.get("retry_count", 3))
        self._retry_delay: float = float(config.get("retry_delay", 1.0))
        self._rate_limit: float = float(config.get("rate_limit", 0))

        self._client: httpx.Client | None = None
        self._schema: Schema | None = None
        self._last_request_time: float = 0.0

    @property
    def sink_type(self) -> str:
        return "webhook"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def open(self, schema: Schema) -> None:
        self._schema = schema

        # Build headers with auth
        headers = {"Content-Type": "application/json", **self._headers}
        auth: httpx.BasicAuth | None = None

        if self._auth_type == "bearer":
            token = self._auth_config.get("token", "")
            headers["Authorization"] = f"Bearer {token}"
        elif self._auth_type == "basic":
            auth = httpx.BasicAuth(
                username=self._auth_config.get("username", ""),
                password=self._auth_config.get("password", ""),
            )
        elif self._auth_type == "api_key":
            header_name = self._auth_config.get("header", "X-API-Key")
            header_value = self._auth_config.get("value", "")
            headers[header_name] = header_value

        self._client = httpx.Client(
            headers=headers,
            auth=auth,
            timeout=self._timeout,
        )

        self._opened = True
        logger.info(
            "webhook_sink.opened",
            url=self._url,
            method=self._method,
            batch_mode=self._batch_mode,
        )

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
        self._opened = False
        logger.info("webhook_sink.closed", url=self._url)

    def health_check(self) -> HealthStatus:
        try:
            if self._client is None:
                return HealthStatus.UNHEALTHY
            resp = self._client.request("HEAD", self._url)
            if resp.status_code < 500:
                return HealthStatus.HEALTHY
            return HealthStatus.DEGRADED
        except Exception:
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Writing
    # ------------------------------------------------------------------

    def write_batch(self, records: list[Record]) -> int:
        if not self._opened or self._client is None:
            raise RuntimeError("Sink not opened. Call open() first.")
        if not records:
            return 0

        if self._batch_mode == "single":
            return self._write_single_mode(records)
        else:
            return self._write_batch_mode(records)

    def _write_single_mode(self, records: list[Record]) -> int:
        """Send one HTTP request per record."""
        written = 0
        for rec in records:
            payload = rec.data
            if self._send_with_retry(payload):
                written += 1
        logger.info("webhook_sink.batch_written", count=written, mode="single")
        return written

    def _write_batch_mode(self, records: list[Record]) -> int:
        """Send all records as a JSON array in a single request."""
        payload = [rec.data for rec in records]
        if self._send_with_retry(payload):
            logger.info("webhook_sink.batch_written", count=len(records), mode="batch")
            return len(records)
        logger.warning("webhook_sink.batch_failed", count=len(records))
        return 0

    def _send_with_retry(self, payload: Any) -> bool:
        """Send a request with retry logic and optional rate limiting."""
        for attempt in range(1, self._retry_count + 1):
            try:
                self._apply_rate_limit()
                response = self._client.request(  # type: ignore[union-attr]
                    method=self._method,
                    url=self._url,
                    json=payload,
                )
                response.raise_for_status()
                return True
            except httpx.HTTPStatusError as exc:
                logger.warning(
                    "webhook_sink.http_error",
                    status=exc.response.status_code,
                    attempt=attempt,
                    max_attempts=self._retry_count,
                )
                if attempt < self._retry_count:
                    time.sleep(self._retry_delay * attempt)
            except httpx.RequestError as exc:
                logger.warning(
                    "webhook_sink.request_error",
                    error=str(exc),
                    attempt=attempt,
                    max_attempts=self._retry_count,
                )
                if attempt < self._retry_count:
                    time.sleep(self._retry_delay * attempt)
        return False

    def _apply_rate_limit(self) -> None:
        """Sleep if necessary to respect rate_limit (requests/sec)."""
        if self._rate_limit <= 0:
            return
        min_interval = 1.0 / self._rate_limit
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self._last_request_time = time.monotonic()

    # ------------------------------------------------------------------
    # Schema evolution
    # ------------------------------------------------------------------

    def evolve_schema(self, new_schema: Schema) -> None:
        """No-op — webhook endpoints do not have a managed schema."""
        self._schema = new_schema
        logger.debug("webhook_sink.evolve_schema_noop", index=self._url)

    def supports_schema_evolution(self) -> bool:
        return False
