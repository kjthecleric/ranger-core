"""WebSocket source connector — stream messages via the websockets library.

Connects to a WebSocket endpoint, sends an optional subscribe message on
connect, and yields incoming messages as :class:`Record` objects.  Supports
automatic reconnection with configurable back-off.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Iterator
from datetime import datetime, timezone
from typing import Any

import structlog

from ranger.core.models import (
    ColumnDefinition,
    ColumnType,
    DiscoveredSchema,
    HealthStatus,
    Record,
    Schema,
    StreamConfig,
)
from ranger.sources.base import BaseSource

logger = structlog.get_logger()

try:
    import websockets
    import websockets.client
except ImportError as _err:
    raise ImportError(
        "websockets is required for WebSocketSource. "
        "Install it with: pip install ranger-core[websocket]"
    ) from _err


def _infer_column_type(value: Any) -> ColumnType:
    """Infer a :class:`ColumnType` from a Python value."""
    if isinstance(value, bool):
        return ColumnType.BOOLEAN
    if isinstance(value, int):
        return ColumnType.INT64
    if isinstance(value, float):
        return ColumnType.FLOAT64
    if isinstance(value, list):
        return ColumnType.ARRAY
    if isinstance(value, dict):
        return ColumnType.JSON
    return ColumnType.STRING


class WebSocketSource(BaseSource):
    """Stream data from a WebSocket endpoint.

    Config keys
    -----------
    url : str
        WebSocket URL (``ws://`` or ``wss://``).
    headers : dict
        Extra HTTP headers for the handshake.
    subscribe_message : dict | None
        A JSON-serialisable message sent immediately after connecting
        (e.g. channel subscription payload).
    ping_interval : float
        Interval in seconds between keep-alive pings (default: ``20``).
    reconnect_delay : float
        Initial delay in seconds before reconnecting (default: ``1``).
    max_reconnect_attempts : int
        Maximum consecutive reconnection attempts (default: ``10``).
    message_format : str
        ``json`` (default) or ``text``.
    """

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._ws: Any | None = None  # websockets connection

    # ------------------------------------------------------------------
    # Metadata properties
    # ------------------------------------------------------------------

    @property
    def source_type(self) -> str:
        return "websocket"

    @property
    def supports_streaming(self) -> bool:
        return True

    @property
    def supports_incremental(self) -> bool:
        return False

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Validate configuration — actual WS connection happens in read_stream."""
        url = self._config.get("url")
        if not url:
            raise ConnectionError("Missing required config key 'url'")
        self._connected = True
        logger.info("websocket_source.configured", url=url)

    def close(self) -> None:
        # The async connection is closed inside read_stream;
        # this is a best-effort flag reset.
        self._ws = None
        self._connected = False
        logger.info("websocket_source.closed")

    def health_check(self) -> HealthStatus:
        """Attempt a short-lived connection to verify the endpoint."""
        url = self._config.get("url", "")
        headers = self._config.get("headers")

        async def _probe() -> bool:
            try:
                async with websockets.client.connect(
                    url,
                    additional_headers=headers,
                    open_timeout=5,
                ):
                    return True
            except Exception:
                return False

        try:
            ok = asyncio.get_event_loop().run_until_complete(_probe())
            return HealthStatus.HEALTHY if ok else HealthStatus.UNHEALTHY
        except Exception as exc:
            logger.warning("websocket_source.health_check_failed", error=str(exc))
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Reading
    # ------------------------------------------------------------------

    def read(self) -> Iterator[Record]:
        """Synchronous wrapper — collects all messages until the connection closes."""
        async def _collect() -> list[Record]:
            records: list[Record] = []
            async for record in self._async_read_stream():
                records.append(record)
            return records

        yield from asyncio.get_event_loop().run_until_complete(_collect())

    async def read_stream(self, config: StreamConfig | None = None) -> AsyncIterator[Record]:  # type: ignore[override]
        """Connect to the WebSocket and yield incoming messages as Records.

        Handles automatic reconnection up to *max_reconnect_attempts*.
        """
        async for record in self._async_read_stream(config):
            yield record

    async def _async_read_stream(self, config: StreamConfig | None = None) -> AsyncIterator[Record]:
        """Core async implementation shared by ``read`` and ``read_stream``."""
        url: str = self._config.get("url", "")
        headers = self._config.get("headers")
        subscribe_message: dict[str, Any] | None = self._config.get("subscribe_message")
        ping_interval: float = self._config.get("ping_interval", 20.0)
        reconnect_delay: float = self._config.get("reconnect_delay", 1.0)
        max_reconnect: int = self._config.get("max_reconnect_attempts", 10)
        msg_format: str = self._config.get("message_format", "json")

        # StreamConfig overrides
        if config is not None:
            max_reconnect = config.max_reconnect_attempts

        attempt = 0
        record_count = 0

        while attempt <= max_reconnect:
            try:
                logger.info(
                    "websocket_source.connecting",
                    url=url,
                    attempt=attempt,
                )
                async with websockets.client.connect(
                    url,
                    additional_headers=headers,
                    ping_interval=ping_interval,
                ) as ws:
                    self._ws = ws
                    attempt = 0  # reset on successful connect

                    # Send subscription message
                    if subscribe_message is not None:
                        await ws.send(json.dumps(subscribe_message))
                        logger.info("websocket_source.subscribed")

                    async for raw_message in ws:
                        data: dict[str, Any]
                        if msg_format == "json":
                            try:
                                parsed = json.loads(raw_message)
                                data = parsed if isinstance(parsed, dict) else {"value": parsed}
                            except (json.JSONDecodeError, TypeError):
                                data = {"raw": str(raw_message)}
                        else:
                            data = {"message": str(raw_message)}

                        yield Record(
                            data=data,
                            event_time=datetime.now(timezone.utc),
                            source_metadata={
                                "source_type": "websocket",
                                "url": url,
                                "message_index": record_count,
                            },
                        )
                        record_count += 1

            except websockets.exceptions.ConnectionClosed as exc:
                logger.warning(
                    "websocket_source.connection_closed",
                    code=exc.code,
                    reason=exc.reason,
                    attempt=attempt,
                )
            except Exception as exc:
                logger.error(
                    "websocket_source.error",
                    error=str(exc),
                    attempt=attempt,
                )

            attempt += 1
            if attempt <= max_reconnect:
                delay = reconnect_delay * (2 ** (attempt - 1))
                logger.info("websocket_source.reconnecting", delay=delay)
                await asyncio.sleep(delay)

        logger.info(
            "websocket_source.stream_complete",
            total_records=record_count,
        )

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def get_schema(self) -> Schema:
        """Infer schema by reading a small sample of messages."""
        sample: list[Record] = []
        for record in self.read():
            sample.append(record)
            if len(sample) >= 20:
                break

        if not sample:
            return Schema(columns=[])

        all_keys: set[str] = set()
        for rec in sample:
            all_keys.update(rec.data.keys())

        columns: list[ColumnDefinition] = []
        for key in sorted(all_keys):
            col_type = ColumnType.STRING
            for rec in sample:
                val = rec.data.get(key)
                if val is not None:
                    col_type = _infer_column_type(val)
                    break
            columns.append(ColumnDefinition(name=key, type=col_type, nullable=True))

        return Schema(columns=columns)

    def discover_schema(self) -> DiscoveredSchema:
        schema = self.get_schema()
        url = self._config.get("url", "")
        return DiscoveredSchema(
            columns=schema.columns,
            primary_key=schema.primary_key,
            source_name="websocket",
            object_name=url,
            object_type="websocket_stream",
            source_metadata={"url": url},
        )
