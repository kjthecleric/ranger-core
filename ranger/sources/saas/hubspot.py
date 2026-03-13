"""HubSpot CRM source connector via hubspot-api-client.

Reads CRM objects (contacts, companies, deals, tickets, products) with
pagination, property selection, and search-API filters.  Supports incremental
extraction via cursor-based ``after`` parameter.
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
    from hubspot import HubSpot
    from hubspot.crm.objects import ApiException as ObjectsApiException
except ImportError as _err:
    raise ImportError(
        "hubspot-api-client is required for HubSpotSource. "
        "Install it with: pip install ranger-core[hubspot]"
    ) from _err


# ---------------------------------------------------------------------------
# HubSpot property type → Ranger ColumnType mapping
# ---------------------------------------------------------------------------

_HS_TYPE_MAP: dict[str, ColumnType] = {
    "string": ColumnType.STRING,
    "number": ColumnType.FLOAT64,
    "date": ColumnType.DATE,
    "datetime": ColumnType.TIMESTAMP_TZ,
    "enumeration": ColumnType.STRING,
    "bool": ColumnType.BOOLEAN,
    "phone_number": ColumnType.STRING,
    "json": ColumnType.JSON,
}


def _hs_type_to_column_type(hs_type: str) -> ColumnType:
    """Map a HubSpot property type to a Ranger ColumnType."""
    return _HS_TYPE_MAP.get(hs_type.lower(), ColumnType.STRING)


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


# ---------------------------------------------------------------------------
# Object type → CRM API accessor mapping
# ---------------------------------------------------------------------------

_OBJECT_TYPES = frozenset({
    "contacts",
    "companies",
    "deals",
    "tickets",
    "products",
    "line_items",
    "quotes",
})


class HubSpotSource(BaseSource):
    """Read CRM objects from HubSpot with pagination and search support.

    Config keys
    -----------
    access_token : str
        HubSpot private app access token.
    object_type : str
        CRM object type — ``contacts``, ``companies``, ``deals``, ``tickets``,
        ``products``, ``line_items``, or ``quotes``.
    properties : list[str] | None
        List of property internal names to fetch. ``None`` fetches default
        properties.
    filters : list[dict] | None
        Filter groups for the HubSpot search API. Each dict should have
        ``propertyName``, ``operator``, and ``value`` keys.
    batch_size : int
        Page size per API call (default ``100``, max ``100``).
    after : str | None
        Paging cursor for incremental extraction — pass the ``after`` value
        from a previous run to resume.
    """

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._client: HubSpot | None = None

        self._access_token: str = config["access_token"]
        self._object_type: str = config["object_type"]
        self._properties: list[str] | None = config.get("properties")
        self._filters: list[dict[str, Any]] | None = config.get("filters")
        self._batch_size: int = min(config.get("batch_size", 100), 100)
        self._after: str | None = config.get("after")

        if self._object_type not in _OBJECT_TYPES:
            raise ValueError(
                f"Unsupported HubSpot object type '{self._object_type}'. "
                f"Supported: {', '.join(sorted(_OBJECT_TYPES))}"
            )

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def source_type(self) -> str:
        return "hubspot"

    @property
    def supports_streaming(self) -> bool:
        return False

    @property
    def supports_incremental(self) -> bool:
        return True

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Create the HubSpot API client."""
        logger.info("hubspot.connecting", object_type=self._object_type)
        self._client = HubSpot(access_token=self._access_token)
        self._connected = True
        logger.info("hubspot.connected", object_type=self._object_type)

    def close(self) -> None:
        """Release the HubSpot client."""
        self._client = None
        self._connected = False
        logger.debug("hubspot.closed")

    def health_check(self) -> HealthStatus:
        """Verify access token by fetching account info."""
        try:
            if self._client is None:
                self.connect()
            assert self._client is not None
            # A lightweight call to verify the token
            self._client.crm.objects.basic_api.get_page(
                object_type=self._object_type,
                limit=1,
            )
            return HealthStatus.HEALTHY
        except Exception:
            logger.warning("hubspot.health_check_failed", exc_info=True)
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Reading
    # ------------------------------------------------------------------

    def _read_via_list(self) -> Iterator[Record]:
        """Paginate through CRM objects using the basic list API."""
        assert self._client is not None
        after = self._after
        records_yielded = 0

        while True:
            kwargs: dict[str, Any] = {
                "object_type": self._object_type,
                "limit": self._batch_size,
            }
            if self._properties:
                kwargs["properties"] = self._properties
            if after:
                kwargs["after"] = after

            page = self._client.crm.objects.basic_api.get_page(**kwargs)

            for result in page.results:
                data = dict(result.properties) if result.properties else {}
                data["hs_object_id"] = result.id

                event_time: datetime | None = None
                if hasattr(result, "updated_at") and result.updated_at:
                    event_time = result.updated_at
                elif hasattr(result, "created_at") and result.created_at:
                    event_time = result.created_at

                yield Record(
                    data=data,
                    event_time=event_time,
                    source_metadata={
                        "source": "hubspot",
                        "object_type": self._object_type,
                        "object_id": result.id,
                    },
                )
                records_yielded += 1

            # Follow paging cursor
            if page.paging and page.paging.next and page.paging.next.after:
                after = page.paging.next.after
            else:
                break

        logger.info("hubspot.read_complete", object_type=self._object_type, records=records_yielded)

    def _read_via_search(self) -> Iterator[Record]:
        """Read CRM objects using the search API with filters."""
        assert self._client is not None
        assert self._filters is not None

        after: str | int = 0
        records_yielded = 0

        while True:
            search_request = {
                "filter_groups": [{"filters": self._filters}],
                "limit": self._batch_size,
                "after": after,
            }
            if self._properties:
                search_request["properties"] = self._properties

            response = self._client.crm.objects.search_api.do_search(
                object_type=self._object_type,
                public_object_search_request=search_request,
            )

            for result in response.results:
                data = dict(result.properties) if result.properties else {}
                data["hs_object_id"] = result.id

                event_time: datetime | None = None
                if hasattr(result, "updated_at") and result.updated_at:
                    event_time = result.updated_at

                yield Record(
                    data=data,
                    event_time=event_time,
                    source_metadata={
                        "source": "hubspot",
                        "object_type": self._object_type,
                        "object_id": result.id,
                    },
                )
                records_yielded += 1

            # Follow paging
            if response.paging and response.paging.next and response.paging.next.after:
                after = response.paging.next.after
            else:
                break

        logger.info(
            "hubspot.search_complete",
            object_type=self._object_type,
            records=records_yielded,
        )

    def read(self) -> Iterator[Record]:
        """Read HubSpot CRM objects — uses search API when filters are set.

        Automatically paginates through all results using cursor-based paging.
        """
        if self._client is None:
            self.connect()

        logger.info(
            "hubspot.read_start",
            object_type=self._object_type,
            has_filters=bool(self._filters),
        )

        if self._filters:
            yield from self._read_via_search()
        else:
            yield from self._read_via_list()

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def get_schema(self) -> Schema:
        """Infer schema by fetching a sample page and examining properties."""
        if self._client is None:
            self.connect()
        assert self._client is not None

        # Fetch one record to get property names and sample values
        page = self._client.crm.objects.basic_api.get_page(
            object_type=self._object_type,
            limit=1,
            properties=self._properties or None,
        )

        columns: list[ColumnDefinition] = [
            ColumnDefinition(
                name="hs_object_id",
                type=ColumnType.STRING,
                nullable=False,
                description="HubSpot object ID",
            )
        ]

        if page.results:
            props = page.results[0].properties or {}
            for key, value in props.items():
                columns.append(
                    ColumnDefinition(
                        name=key,
                        type=_infer_column_type(value),
                        nullable=True,
                    )
                )

        return Schema(columns=columns, primary_key=["hs_object_id"])

    def discover_schema(self) -> DiscoveredSchema:
        """Discover schema using HubSpot CRM properties API.

        Fetches the full property definitions for the object type to get
        accurate type information and descriptions.
        """
        if self._client is None:
            self.connect()
        assert self._client is not None

        columns: list[ColumnDefinition] = [
            ColumnDefinition(
                name="hs_object_id",
                type=ColumnType.STRING,
                nullable=False,
                description="HubSpot object ID",
            )
        ]

        try:
            # Use the properties API for accurate type info
            properties_response = self._client.crm.properties.core_api.get_all(
                object_type=self._object_type,
            )
            for prop in properties_response.results:
                columns.append(
                    ColumnDefinition(
                        name=prop.name,
                        type=_hs_type_to_column_type(prop.type),
                        nullable=True,
                        description=prop.label or prop.name,
                    )
                )
        except Exception:
            logger.warning("hubspot.discover_schema_fallback", exc_info=True)
            base = self.get_schema()
            return DiscoveredSchema(
                columns=base.columns,
                primary_key=base.primary_key,
                source_name=self.source_type,
                object_name=self._object_type,
                object_type="crm_object",
            )

        return DiscoveredSchema(
            columns=columns,
            primary_key=["hs_object_id"],
            partition_columns=None,
            source_name=self.source_type,
            object_name=self._object_type,
            object_type="crm_object",
            source_metadata={
                "object_type": self._object_type,
                "property_count": len(columns) - 1,  # exclude hs_object_id
            },
        )
