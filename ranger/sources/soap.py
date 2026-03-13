"""SOAP source connector — call SOAP/WSDL services via zeep.

Loads a WSDL definition, invokes a specified operation, and yields the
response data as :class:`Record` objects.

.. note::

    TODO: The entry point for this source was expected to be missing from
    ``pyproject.toml`` but is already registered as::

        soap = "ranger.sources.soap:SOAPSource"

    Verify the entry point matches the class name and module path.
"""

from __future__ import annotations

from collections.abc import Iterator
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
)
from ranger.sources.base import BaseSource

logger = structlog.get_logger()

try:
    import zeep
    from zeep import Client as ZeepClient
    from zeep.transports import Transport
    from zeep.wsse.username import UsernameToken
except ImportError as _err:
    raise ImportError(
        "zeep is required for SOAPSource. "
        "Install it with: pip install ranger-core[soap]"
    ) from _err


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_dot_path(data: Any, path: str) -> Any:
    """Resolve a dot-separated path against a nested structure."""
    parts = path.split(".")
    current = data
    for part in parts:
        if isinstance(current, dict):
            current = current.get(part)
        elif hasattr(current, part):
            current = getattr(current, part)
        else:
            return None
    return current


def _serialize_zeep_object(obj: Any) -> Any:
    """Recursively convert a zeep response object to plain Python types."""
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, (list, tuple)):
        return [_serialize_zeep_object(item) for item in obj]
    if isinstance(obj, dict):
        return {k: _serialize_zeep_object(v) for k, v in obj.items()}
    # zeep objects support dict-like serialization via __json__() or zeep helpers
    try:
        return zeep.helpers.serialize_object(obj, target_cls=dict)
    except Exception:
        pass
    if hasattr(obj, "__dict__"):
        return {k: _serialize_zeep_object(v) for k, v in obj.__dict__.items() if not k.startswith("_")}
    return str(obj)


def _infer_column_type(value: Any) -> ColumnType:
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


class SOAPSource(BaseSource):
    """Read data from a SOAP web service via zeep.

    Config keys
    -----------
    wsdl_url : str
        URL (or local path) to the WSDL document.
    service_name : str | None
        Service name defined in the WSDL (optional if only one service).
    port_name : str | None
        Port name within the service (optional if only one port).
    operation : str
        The SOAP operation to invoke.
    params : dict
        Parameters to pass to the operation.
    headers : dict
        Extra HTTP headers sent with every request.
    auth_type : str
        ``none`` | ``basic`` | ``wsse``.
    auth_config : dict
        ``username``, ``password``, and optionally ``use_digest`` (for WSSE).
    response_path : str
        Dot-notation path to extract the relevant data from the response
        (e.g. ``Body.GetRecordsResult.Records``).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._zeep_client: ZeepClient | None = None

    # ------------------------------------------------------------------
    # Metadata properties
    # ------------------------------------------------------------------

    @property
    def source_type(self) -> str:
        return "soap"

    @property
    def supports_streaming(self) -> bool:
        return False

    @property
    def supports_incremental(self) -> bool:
        return False

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        wsdl_url = self._config.get("wsdl_url")
        if not wsdl_url:
            raise ConnectionError("Missing required config key 'wsdl_url'")

        headers = dict(self._config.get("headers", {}))
        auth_type = self._config.get("auth_type", "none").lower()
        auth_cfg: dict[str, Any] = self._config.get("auth_config", {})

        try:
            # Build transport with optional headers
            session = None
            try:
                import requests

                session = requests.Session()
                session.headers.update(headers)

                if auth_type == "basic":
                    session.auth = (
                        auth_cfg.get("username", ""),
                        auth_cfg.get("password", ""),
                    )
            except ImportError:
                pass

            transport = Transport(session=session) if session else Transport()

            # WSSE authentication
            wsse = None
            if auth_type == "wsse":
                wsse = UsernameToken(
                    username=auth_cfg.get("username", ""),
                    password=auth_cfg.get("password", ""),
                    use_digest=auth_cfg.get("use_digest", False),
                )

            self._zeep_client = ZeepClient(
                wsdl_url,
                transport=transport,
                wsse=wsse,
            )
            self._connected = True
            logger.info("soap_source.connected", wsdl_url=wsdl_url)
        except Exception as exc:
            self._connected = False
            logger.error("soap_source.connect_failed", error=str(exc))
            raise ConnectionError(f"Failed to load WSDL from {wsdl_url}: {exc}") from exc

    def close(self) -> None:
        self._zeep_client = None
        self._connected = False
        logger.info("soap_source.closed")

    def health_check(self) -> HealthStatus:
        try:
            if self._zeep_client is None:
                self.connect()
            assert self._zeep_client is not None
            # Verify the operation exists
            service_name = self._config.get("service_name")
            port_name = self._config.get("port_name")
            operation = self._config.get("operation", "")

            service_proxy = self._zeep_client.service
            if service_name and port_name:
                service_proxy = self._zeep_client.bind(service_name, port_name)

            if not hasattr(service_proxy, operation):
                return HealthStatus.DEGRADED

            return HealthStatus.HEALTHY
        except Exception as exc:
            logger.warning("soap_source.health_check_failed", error=str(exc))
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Reading
    # ------------------------------------------------------------------

    def _call_operation(self) -> Any:
        """Invoke the configured SOAP operation and return the raw response."""
        if self._zeep_client is None:
            raise RuntimeError("Source not connected — call connect() first")

        service_name = self._config.get("service_name")
        port_name = self._config.get("port_name")
        operation = self._config.get("operation", "")
        params: dict[str, Any] = self._config.get("params", {})

        service_proxy = self._zeep_client.service
        if service_name and port_name:
            service_proxy = self._zeep_client.bind(service_name, port_name)

        op_func = getattr(service_proxy, operation)
        return op_func(**params)

    def read(self) -> Iterator[Record]:
        """Call the SOAP operation, parse the response, and yield Records."""
        logger.info(
            "soap_source.read_start",
            operation=self._config.get("operation"),
        )

        raw_response = self._call_operation()
        serialized = _serialize_zeep_object(raw_response)

        # Optionally extract nested data
        response_path = self._config.get("response_path")
        data = serialized
        if response_path and isinstance(serialized, dict):
            data = _resolve_dot_path(serialized, response_path)

        records: list[dict[str, Any]]
        if isinstance(data, list):
            records = [
                _serialize_zeep_object(item) if not isinstance(item, dict) else item
                for item in data
            ]
        elif isinstance(data, dict):
            records = [data]
        elif data is not None:
            records = [{"value": data}]
        else:
            records = []

        record_count = 0
        for item in records:
            if not isinstance(item, dict):
                item = {"value": item}
            yield Record(
                data=item,
                event_time=datetime.now(timezone.utc),
                source_metadata={
                    "source_type": "soap",
                    "operation": self._config.get("operation", ""),
                    "wsdl_url": self._config.get("wsdl_url", ""),
                },
            )
            record_count += 1

        logger.info("soap_source.read_complete", records=record_count)

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def get_schema(self) -> Schema:
        """Infer schema by calling the operation once and inspecting results."""
        sample: list[Record] = []
        for record in self.read():
            sample.append(record)
            if len(sample) >= 50:
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
        """Discover schema by inspecting the WSDL operation output types.

        Falls back to inference from a sample response if WSDL introspection
        is insufficient.
        """
        if self._zeep_client is None:
            raise RuntimeError("Source not connected — call connect() first")

        operation_name = self._config.get("operation", "")
        columns: list[ColumnDefinition] = []

        try:
            # Attempt WSDL introspection
            service_name = self._config.get("service_name")
            port_name = self._config.get("port_name")

            # Access the service definition
            for service in self._zeep_client.wsdl.services.values():
                if service_name and service.name != service_name:
                    continue
                for port in service.ports.values():
                    if port_name and port.name != port_name:
                        continue
                    for op_name, op in port.binding._operations.items():
                        if op_name == operation_name:
                            # Try to extract output message elements
                            output = op.output
                            if output and hasattr(output, "body") and output.body:
                                body_type = output.body.type
                                if hasattr(body_type, "elements"):
                                    for elem_name, element in body_type.elements:
                                        col_type = self._map_xsd_type(element.type)
                                        columns.append(
                                            ColumnDefinition(
                                                name=elem_name,
                                                type=col_type,
                                                nullable=not element.is_required if hasattr(element, "is_required") else True,
                                            )
                                        )
        except Exception as exc:
            logger.debug("soap_source.wsdl_introspection_failed", error=str(exc))

        # If WSDL introspection didn't yield columns, fall back to sample
        if not columns:
            schema = self.get_schema()
            columns = schema.columns

        return DiscoveredSchema(
            columns=columns,
            source_name="soap",
            object_name=operation_name,
            object_type="soap_operation",
            source_metadata={
                "wsdl_url": self._config.get("wsdl_url", ""),
                "service_name": self._config.get("service_name"),
                "port_name": self._config.get("port_name"),
            },
        )

    @staticmethod
    def _map_xsd_type(xsd_type: Any) -> ColumnType:
        """Map an XSD type name to a Ranger :class:`ColumnType`."""
        type_name = getattr(xsd_type, "name", str(xsd_type)).lower() if xsd_type else ""

        mapping: dict[str, ColumnType] = {
            "string": ColumnType.STRING,
            "int": ColumnType.INT32,
            "integer": ColumnType.INT32,
            "long": ColumnType.INT64,
            "short": ColumnType.INT16,
            "byte": ColumnType.INT8,
            "float": ColumnType.FLOAT32,
            "double": ColumnType.FLOAT64,
            "decimal": ColumnType.DECIMAL,
            "boolean": ColumnType.BOOLEAN,
            "date": ColumnType.DATE,
            "time": ColumnType.TIME,
            "datetime": ColumnType.TIMESTAMP,
            "base64binary": ColumnType.BINARY,
            "hexbinary": ColumnType.BINARY,
            "anyuri": ColumnType.STRING,
        }
        return mapping.get(type_name, ColumnType.STRING)
