"""gRPC source connector — unary and server-streaming RPCs via grpcio.

Connects to a gRPC service and invokes a specified method, yielding each
response message as a :class:`Record`.  Supports TLS and call metadata.
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
    import grpc
    from google.protobuf import descriptor_pool, json_format, symbol_database
    from google.protobuf.descriptor import MethodDescriptor
except ImportError as _err:
    raise ImportError(
        "grpcio and protobuf are required for GRPCSource. "
        "Install them with: pip install ranger-core[grpc]"
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


class GRPCSource(BaseSource):
    """Read data from a gRPC service via unary or server-streaming calls.

    Config keys
    -----------
    host : str
        gRPC server hostname.
    port : int
        gRPC server port (default: ``50051``).
    service_name : str
        Fully-qualified protobuf service name (e.g. ``mypackage.MyService``).
    method_name : str
        RPC method name (e.g. ``ListRecords``).
    request_message : dict
        JSON-serialisable dict representing the request protobuf message.
    tls : bool
        Whether to use a secure channel (default: ``False``).
    tls_cert_path : str | None
        Path to a PEM root certificate for TLS verification.
    metadata : dict
        Extra gRPC call metadata (key → value).
    timeout : float
        Call timeout in seconds (default: ``30``).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._channel: grpc.Channel | None = None

    # ------------------------------------------------------------------
    # Metadata properties
    # ------------------------------------------------------------------

    @property
    def source_type(self) -> str:
        return "grpc"

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
        port = self._config.get("port", 50051)
        use_tls = self._config.get("tls", False)
        target = f"{host}:{port}"

        try:
            if use_tls:
                cert_path = self._config.get("tls_cert_path")
                if cert_path:
                    with open(cert_path, "rb") as f:
                        root_certs = f.read()
                    creds = grpc.ssl_channel_credentials(root_certificates=root_certs)
                else:
                    creds = grpc.ssl_channel_credentials()
                self._channel = grpc.secure_channel(target, creds)
            else:
                self._channel = grpc.insecure_channel(target)

            # Verify connectivity with a short deadline
            grpc.channel_ready_future(self._channel).result(timeout=self._config.get("timeout", 30.0))
            self._connected = True
            logger.info("grpc_source.connected", target=target, tls=use_tls)
        except Exception as exc:
            self._connected = False
            logger.error("grpc_source.connect_failed", error=str(exc))
            raise ConnectionError(f"Failed to connect to gRPC server at {target}: {exc}") from exc

    def close(self) -> None:
        if self._channel is not None:
            try:
                self._channel.close()
            except Exception:
                pass
            self._channel = None
        self._connected = False
        logger.info("grpc_source.closed")

    def health_check(self) -> HealthStatus:
        try:
            if self._channel is None:
                self.connect()
            assert self._channel is not None
            grpc.channel_ready_future(self._channel).result(timeout=5.0)
            return HealthStatus.HEALTHY
        except Exception as exc:
            logger.warning("grpc_source.health_check_failed", error=str(exc))
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Internal: build the generic call
    # ------------------------------------------------------------------

    def _build_metadata(self) -> list[tuple[str, str]]:
        """Convert config metadata dict to gRPC metadata tuples."""
        raw: dict[str, str] = self._config.get("metadata", {})
        return list(raw.items())

    def _make_generic_call(self) -> Any:
        """Perform a generic unary or server-streaming RPC.

        Uses the ``grpc.Channel.unary_unary`` / ``unary_stream``
        generic callable API to avoid requiring generated stubs.
        """
        if self._channel is None:
            raise RuntimeError("Source not connected — call connect() first")

        service_name: str = self._config.get("service_name", "")
        method_name: str = self._config.get("method_name", "")
        full_method = f"/{service_name}/{method_name}"
        timeout = self._config.get("timeout", 30.0)
        metadata = self._build_metadata()
        request_dict: dict[str, Any] = self._config.get("request_message", {})
        request_bytes = json.dumps(request_dict).encode("utf-8")

        # Try server-streaming first; fall back to unary
        try:
            stream_callable = self._channel.unary_stream(
                full_method,
                request_serializer=lambda x: x,
                response_deserializer=lambda x: x,
            )
            responses = stream_callable(
                request_bytes,
                metadata=metadata,
                timeout=timeout,
            )
            # Attempt to peek — if the method is actually unary, gRPC may
            # return an iterator with a single element or raise.
            return responses
        except grpc.RpcError as rpc_err:
            # If the call failed because it's unary, retry as unary
            if rpc_err.code() == grpc.StatusCode.UNIMPLEMENTED:
                unary_callable = self._channel.unary_unary(
                    full_method,
                    request_serializer=lambda x: x,
                    response_deserializer=lambda x: x,
                )
                response = unary_callable(
                    request_bytes,
                    metadata=metadata,
                    timeout=timeout,
                )
                return [response]
            raise

    @staticmethod
    def _decode_response(raw: bytes) -> dict[str, Any]:
        """Decode a raw gRPC response payload into a dict.

        Tries JSON first; falls back to a raw-bytes wrapper.
        """
        try:
            return json.loads(raw)  # type: ignore[no-any-return]
        except (json.JSONDecodeError, UnicodeDecodeError):
            return {"raw": raw.hex()}

    # ------------------------------------------------------------------
    # Reading
    # ------------------------------------------------------------------

    def read(self) -> Iterator[Record]:
        """Make a unary or server-streaming gRPC call and yield Records."""
        responses = self._make_generic_call()
        method_name = self._config.get("method_name", "")
        service_name = self._config.get("service_name", "")
        record_count = 0

        logger.info(
            "grpc_source.read_start",
            service=service_name,
            method=method_name,
        )

        for raw_response in responses:
            data = self._decode_response(raw_response) if isinstance(raw_response, bytes) else raw_response
            if not isinstance(data, dict):
                data = {"value": data}

            yield Record(
                data=data,
                event_time=datetime.now(timezone.utc),
                source_metadata={
                    "source_type": "grpc",
                    "service": service_name,
                    "method": method_name,
                },
            )
            record_count += 1

        logger.info("grpc_source.read_complete", records=record_count)

    async def read_stream(self, config: StreamConfig | None = None) -> AsyncIterator[Record]:  # type: ignore[override]
        """Server-streaming gRPC call as an async iterator.

        Wraps the synchronous gRPC stream in an async generator so it can
        be consumed with ``async for``.
        """
        responses = self._make_generic_call()
        method_name = self._config.get("method_name", "")
        service_name = self._config.get("service_name", "")
        record_count = 0

        logger.info(
            "grpc_source.read_stream_start",
            service=service_name,
            method=method_name,
        )

        loop = asyncio.get_event_loop()
        for raw_response in responses:
            data = await loop.run_in_executor(
                None,
                lambda r=raw_response: self._decode_response(r) if isinstance(r, bytes) else r,
            )
            if not isinstance(data, dict):
                data = {"value": data}

            yield Record(
                data=data,
                event_time=datetime.now(timezone.utc),
                source_metadata={
                    "source_type": "grpc",
                    "service": service_name,
                    "method": method_name,
                    "stream_index": record_count,
                },
            )
            record_count += 1

        logger.info("grpc_source.read_stream_complete", records=record_count)

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def get_schema(self) -> Schema:
        """Infer schema by making one call and inspecting the response."""
        sample_records: list[Record] = []
        for record in self.read():
            sample_records.append(record)
            if len(sample_records) >= 10:
                break

        if not sample_records:
            return Schema(columns=[])

        all_keys: set[str] = set()
        for rec in sample_records:
            all_keys.update(rec.data.keys())

        columns: list[ColumnDefinition] = []
        for key in sorted(all_keys):
            col_type = ColumnType.STRING
            for rec in sample_records:
                val = rec.data.get(key)
                if val is not None:
                    col_type = _infer_column_type(val)
                    break
            columns.append(ColumnDefinition(name=key, type=col_type, nullable=True))

        return Schema(columns=columns)

    def discover_schema(self) -> DiscoveredSchema:
        schema = self.get_schema()
        service = self._config.get("service_name", "")
        method = self._config.get("method_name", "")
        return DiscoveredSchema(
            columns=schema.columns,
            primary_key=schema.primary_key,
            source_name="grpc",
            object_name=f"{service}/{method}",
            object_type="rpc_method",
            source_metadata={
                "service": service,
                "method": method,
                "host": self._config.get("host", ""),
                "port": self._config.get("port", 50051),
            },
        )
