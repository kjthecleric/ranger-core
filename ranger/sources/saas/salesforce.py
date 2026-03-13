"""Salesforce CRM source connector via simple-salesforce.

Reads SObject records from Salesforce using SOQL queries or bulk API,
supports incremental extraction, and schema discovery via SObject describe.
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
    from simple_salesforce import Salesforce, SalesforceError
except ImportError as _err:
    raise ImportError(
        "simple-salesforce is required for SalesforceSource. "
        "Install it with: pip install ranger-core[salesforce]"
    ) from _err


# ---------------------------------------------------------------------------
# Salesforce field type → Ranger ColumnType mapping
# ---------------------------------------------------------------------------

_SF_TYPE_MAP: dict[str, ColumnType] = {
    "id": ColumnType.STRING,
    "boolean": ColumnType.BOOLEAN,
    "int": ColumnType.INT32,
    "long": ColumnType.INT64,
    "double": ColumnType.FLOAT64,
    "currency": ColumnType.DECIMAL,
    "percent": ColumnType.FLOAT64,
    "date": ColumnType.DATE,
    "datetime": ColumnType.TIMESTAMP_TZ,
    "time": ColumnType.TIME,
    "string": ColumnType.STRING,
    "textarea": ColumnType.STRING,
    "picklist": ColumnType.STRING,
    "multipicklist": ColumnType.ARRAY,
    "email": ColumnType.STRING,
    "url": ColumnType.STRING,
    "phone": ColumnType.STRING,
    "reference": ColumnType.STRING,
    "base64": ColumnType.BINARY,
    "address": ColumnType.JSON,
    "location": ColumnType.JSON,
    "anyType": ColumnType.JSON,
    "complexvalue": ColumnType.JSON,
    "encryptedstring": ColumnType.STRING,
}


def _sf_type_to_column_type(sf_type: str) -> ColumnType:
    """Map a Salesforce field type string to a Ranger ColumnType."""
    return _SF_TYPE_MAP.get(sf_type.lower(), ColumnType.STRING)


class SalesforceSource(BaseSource):
    """Read records from Salesforce SObjects via SOQL or bulk API.

    Config keys
    -----------
    instance_url : str
        Salesforce instance URL (e.g. ``https://mycompany.salesforce.com``).
    username : str
        Salesforce username.
    password : str
        Salesforce password.
    security_token : str
        Salesforce security token appended to the password.
    domain : str
        Login domain — ``"login"`` (default, production) or ``"test"`` (sandbox).
    sobject : str
        SObject API name (e.g. ``"Account"``, ``"Contact"``).
    soql_query : str | None
        Optional custom SOQL query. When provided, *sobject* is still used for
        schema discovery but the query takes precedence during reads.
    batch_size : int
        Number of records per API page (default ``2000``, max ``2000``).
    include_deleted : bool
        Whether to include soft-deleted records via ``queryAll`` (default ``False``).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._sf: Salesforce | None = None

        # Required config
        self._instance_url: str = config["instance_url"]
        self._username: str = config["username"]
        self._password: str = config["password"]
        self._security_token: str = config.get("security_token", "")
        self._domain: str = config.get("domain", "login")

        # Object / query config
        self._sobject: str = config["sobject"]
        self._soql_query: str | None = config.get("soql_query")
        self._batch_size: int = min(config.get("batch_size", 2000), 2000)
        self._include_deleted: bool = config.get("include_deleted", False)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def source_type(self) -> str:
        return "salesforce"

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
        """Authenticate with Salesforce using username/password + token."""
        logger.info(
            "salesforce.connecting",
            instance_url=self._instance_url,
            username=self._username,
            domain=self._domain,
        )
        self._sf = Salesforce(
            instance_url=self._instance_url,
            username=self._username,
            password=self._password,
            security_token=self._security_token,
            domain=self._domain,
        )
        self._connected = True
        logger.info("salesforce.connected", instance_url=self._instance_url)

    def close(self) -> None:
        """Release the Salesforce session."""
        self._sf = None
        self._connected = False
        logger.debug("salesforce.closed")

    def health_check(self) -> HealthStatus:
        """Verify authentication by querying Salesforce limits."""
        try:
            if self._sf is None:
                self.connect()
            assert self._sf is not None
            self._sf.limits()
            return HealthStatus.HEALTHY
        except Exception:
            logger.warning("salesforce.health_check_failed", exc_info=True)
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Reading
    # ------------------------------------------------------------------

    def _build_soql(self, describe_result: dict[str, Any] | None = None) -> str:
        """Build a SOQL query if no custom query was provided."""
        if self._soql_query:
            return self._soql_query

        # Get field names from describe if not provided
        if describe_result is None:
            assert self._sf is not None
            describe_result = getattr(self._sf, self._sobject).describe()

        field_names = [f["name"] for f in describe_result["fields"]]
        return f"SELECT {', '.join(field_names)} FROM {self._sobject}"

    def read(self) -> Iterator[Record]:
        """Read SObject records using SOQL query (with pagination).

        Uses ``query_all`` when *include_deleted* is True, otherwise ``query``.
        Automatically follows ``nextRecordsUrl`` for pagination.
        """
        if self._sf is None:
            self.connect()
        assert self._sf is not None

        soql = self._build_soql()
        logger.info("salesforce.query_start", sobject=self._sobject, query=soql[:200])

        # Initial query
        if self._include_deleted:
            result = self._sf.query_all(soql, include_deleted=True)
        else:
            result = self._sf.query(soql)

        records_yielded = 0
        while True:
            for raw in result.get("records", []):
                # Remove Salesforce metadata attributes
                data = {k: v for k, v in raw.items() if k != "attributes"}
                yield Record(
                    data=data,
                    event_time=datetime.now(timezone.utc),
                    source_metadata={
                        "source": "salesforce",
                        "sobject": self._sobject,
                        "record_id": data.get("Id", ""),
                    },
                )
                records_yielded += 1

            # Follow pagination
            if result.get("done", True):
                break
            next_url = result.get("nextRecordsUrl")
            if not next_url:
                break
            result = self._sf.query_more(next_url, identifier_is_url=True)

        logger.info("salesforce.query_complete", sobject=self._sobject, records=records_yielded)

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def get_schema(self) -> Schema:
        """Infer schema from the SObject describe metadata."""
        if self._sf is None:
            self.connect()
        assert self._sf is not None

        describe = getattr(self._sf, self._sobject).describe()
        columns: list[ColumnDefinition] = []
        for field in describe["fields"]:
            columns.append(
                ColumnDefinition(
                    name=field["name"],
                    type=_sf_type_to_column_type(field["type"]),
                    nullable=field.get("nillable", True),
                    description=field.get("label", ""),
                    max_length=field.get("length"),
                    precision=field.get("precision"),
                    scale=field.get("scale"),
                )
            )

        # Identify primary key (always "Id" for Salesforce SObjects)
        pk = ["Id"] if any(c.name == "Id" for c in columns) else None

        return Schema(columns=columns, primary_key=pk)

    def discover_schema(self) -> DiscoveredSchema:
        """Discover schema with catalog metadata from Salesforce describe."""
        if self._sf is None:
            self.connect()
        assert self._sf is not None

        describe = getattr(self._sf, self._sobject).describe()
        base = self.get_schema()

        return DiscoveredSchema(
            columns=base.columns,
            primary_key=base.primary_key,
            partition_columns=None,
            source_name=self.source_type,
            object_name=self._sobject,
            object_type="sobject",
            source_metadata={
                "label": describe.get("label", ""),
                "key_prefix": describe.get("keyPrefix", ""),
                "queryable": describe.get("queryable", False),
                "retrieveable": describe.get("retrieveable", False),
                "custom": describe.get("custom", False),
            },
        )
