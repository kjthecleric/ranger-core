"""MQTT source connector — subscribe to topics via paho-mqtt.

Connects to an MQTT broker, subscribes to one or more topics, and yields
incoming messages as :class:`Record` objects through the streaming interface.
"""

from __future__ import annotations

import asyncio
import json
import queue
import threading
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
    import paho.mqtt.client as mqtt
except ImportError as _err:
    raise ImportError(
        "paho-mqtt is required for MQTTSource. "
        "Install it with: pip install ranger-core[mqtt]"
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


class MQTTSource(BaseSource):
    """Stream messages from an MQTT broker.

    Config keys
    -----------
    host : str
        MQTT broker hostname.
    port : int
        Broker port (default: ``1883``).
    topics : list[dict]
        List of ``{"topic": str, "qos": int}`` dicts.
    client_id : str
        MQTT client identifier.
    username : str | None
        Broker username.
    password : str | None
        Broker password.
    tls : bool
        Whether to use TLS (default: ``False``).
    tls_ca_path : str | None
        Path to a CA certificate bundle for TLS.
    clean_session : bool
        Start a clean session (default: ``True``).
    qos : int
        Default QoS level for subscriptions — ``0``, ``1``, or ``2``
        (default: ``0``).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._client: mqtt.Client | None = None
        self._message_queue: queue.Queue[mqtt.MQTTMessage] = queue.Queue()
        self._stop_event: threading.Event = threading.Event()

    # ------------------------------------------------------------------
    # Metadata properties
    # ------------------------------------------------------------------

    @property
    def source_type(self) -> str:
        return "mqtt"

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
        host = self._config.get("host", "localhost")
        port = self._config.get("port", 1883)
        client_id = self._config.get("client_id", "")
        clean_session = self._config.get("clean_session", True)
        username = self._config.get("username")
        password = self._config.get("password")
        use_tls = self._config.get("tls", False)
        tls_ca_path = self._config.get("tls_ca_path")

        try:
            self._client = mqtt.Client(
                client_id=client_id,
                clean_session=clean_session,
            )

            if username:
                self._client.username_pw_set(username, password)

            if use_tls:
                self._client.tls_set(ca_certs=tls_ca_path)

            # Wire up callbacks
            self._client.on_connect = self._on_connect
            self._client.on_message = self._on_message
            self._client.on_disconnect = self._on_disconnect

            self._client.connect(host, port, keepalive=60)
            self._client.loop_start()
            self._connected = True
            self._stop_event.clear()
            logger.info("mqtt_source.connected", host=host, port=port)
        except Exception as exc:
            self._connected = False
            logger.error("mqtt_source.connect_failed", error=str(exc))
            raise ConnectionError(f"Failed to connect to MQTT broker {host}:{port}: {exc}") from exc

    def close(self) -> None:
        self._stop_event.set()
        if self._client is not None:
            try:
                self._client.loop_stop()
                self._client.disconnect()
            except Exception:
                pass
            self._client = None
        self._connected = False
        logger.info("mqtt_source.closed")

    def health_check(self) -> HealthStatus:
        try:
            if self._client is None:
                self.connect()
            assert self._client is not None
            if self._client.is_connected():
                return HealthStatus.HEALTHY
            return HealthStatus.UNHEALTHY
        except Exception as exc:
            logger.warning("mqtt_source.health_check_failed", error=str(exc))
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Paho callbacks
    # ------------------------------------------------------------------

    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: dict[str, Any],
        rc: int,
    ) -> None:
        if rc != 0:
            logger.error("mqtt_source.connect_rc", rc=rc)
            return

        topics_cfg: list[dict[str, Any]] = self._config.get("topics", [])
        default_qos: int = self._config.get("qos", 0)

        for item in topics_cfg:
            topic = item if isinstance(item, str) else item.get("topic", "")
            qos = item.get("qos", default_qos) if isinstance(item, dict) else default_qos
            client.subscribe(topic, qos=qos)
            logger.info("mqtt_source.subscribed", topic=topic, qos=qos)

    def _on_message(
        self,
        client: mqtt.Client,
        userdata: Any,
        message: mqtt.MQTTMessage,
    ) -> None:
        self._message_queue.put(message)

    def _on_disconnect(
        self,
        client: mqtt.Client,
        userdata: Any,
        rc: int,
    ) -> None:
        logger.warning("mqtt_source.disconnected", rc=rc)

    # ------------------------------------------------------------------
    # Message parsing
    # ------------------------------------------------------------------

    def _parse_message(self, message: mqtt.MQTTMessage) -> dict[str, Any]:
        """Parse an MQTT message payload into a dict."""
        payload_bytes: bytes = message.payload
        try:
            payload_str = payload_bytes.decode("utf-8")
        except UnicodeDecodeError:
            return {
                "topic": message.topic,
                "payload_hex": payload_bytes.hex(),
                "qos": message.qos,
            }

        # Try JSON
        try:
            parsed = json.loads(payload_str)
            if isinstance(parsed, dict):
                parsed["_topic"] = message.topic
                parsed["_qos"] = message.qos
                return parsed
            return {"topic": message.topic, "payload": parsed, "qos": message.qos}
        except json.JSONDecodeError:
            return {
                "topic": message.topic,
                "payload": payload_str,
                "qos": message.qos,
            }

    # ------------------------------------------------------------------
    # Reading
    # ------------------------------------------------------------------

    def read(self) -> Iterator[Record]:
        """Synchronous read — drain the internal message queue.

        Typically you should prefer :meth:`read_stream` for MQTT.
        This method returns messages already received and does *not* block.
        """
        if self._client is None:
            raise RuntimeError("Source not connected — call connect() first")

        record_count = 0
        while not self._message_queue.empty():
            try:
                msg = self._message_queue.get_nowait()
            except queue.Empty:
                break
            data = self._parse_message(msg)
            yield Record(
                data=data,
                event_time=datetime.now(timezone.utc),
                source_metadata={
                    "source_type": "mqtt",
                    "topic": msg.topic,
                    "qos": msg.qos,
                },
            )
            record_count += 1

        logger.info("mqtt_source.read_complete", records=record_count)

    async def read_stream(self, config: StreamConfig | None = None) -> AsyncIterator[Record]:  # type: ignore[override]
        """Subscribe to MQTT topics and yield messages as they arrive.

        Runs until ``close()`` is called or *max_reconnect_attempts* is
        exceeded.
        """
        if self._client is None:
            raise RuntimeError("Source not connected — call connect() first")

        poll_interval = 0.1  # seconds between queue polls
        record_count = 0

        logger.info("mqtt_source.read_stream_start")

        while not self._stop_event.is_set():
            try:
                msg = self._message_queue.get_nowait()
            except queue.Empty:
                await asyncio.sleep(poll_interval)
                continue

            data = self._parse_message(msg)
            yield Record(
                data=data,
                event_time=datetime.now(timezone.utc),
                source_metadata={
                    "source_type": "mqtt",
                    "topic": msg.topic,
                    "qos": msg.qos,
                    "message_index": record_count,
                },
            )
            record_count += 1

        logger.info("mqtt_source.read_stream_complete", records=record_count)

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def get_schema(self) -> Schema:
        """Infer schema from a small sample of received messages."""
        sample: list[Record] = list(self.read())[:20]

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
        topics_cfg = self._config.get("topics", [])
        topic_names = [
            t if isinstance(t, str) else t.get("topic", "")
            for t in topics_cfg
        ]
        return DiscoveredSchema(
            columns=schema.columns,
            primary_key=schema.primary_key,
            source_name="mqtt",
            object_name=",".join(topic_names),
            object_type="mqtt_topic",
            source_metadata={
                "host": self._config.get("host", ""),
                "port": self._config.get("port", 1883),
                "topics": topic_names,
            },
        )
