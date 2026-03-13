"""Stripe source connector via the Stripe Python SDK.

Reads resources (charges, customers, invoices, subscriptions, payment intents)
using Stripe's auto-pagination API with support for incremental extraction
via ``created_after`` filtering.
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
    import stripe as stripe_sdk
except ImportError as _err:
    raise ImportError(
        "stripe is required for StripeSource. "
        "Install it with: pip install ranger-core[stripe]"
    ) from _err


# ---------------------------------------------------------------------------
# Python value → Ranger ColumnType heuristic
# ---------------------------------------------------------------------------

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
# Stripe resource name → SDK class mapping
# ---------------------------------------------------------------------------

_RESOURCE_MAP: dict[str, Any] = {
    "charges": stripe_sdk.Charge,
    "customers": stripe_sdk.Customer,
    "invoices": stripe_sdk.Invoice,
    "subscriptions": stripe_sdk.Subscription,
    "payment_intents": stripe_sdk.PaymentIntent,
    "products": stripe_sdk.Product,
    "prices": stripe_sdk.Price,
    "balance_transactions": stripe_sdk.BalanceTransaction,
    "payouts": stripe_sdk.Payout,
    "refunds": stripe_sdk.Refund,
    "disputes": stripe_sdk.Dispute,
    "events": stripe_sdk.Event,
    "coupons": stripe_sdk.Coupon,
    "plans": stripe_sdk.Plan,
    "setup_intents": stripe_sdk.SetupIntent,
}


class StripeSource(BaseSource):
    """Read resources from Stripe using auto-pagination.

    Config keys
    -----------
    api_key : str
        Stripe secret API key (``sk_live_...`` or ``sk_test_...``).
    resource : str
        Stripe resource name — one of ``charges``, ``customers``, ``invoices``,
        ``subscriptions``, ``payment_intents``, ``products``, ``prices``,
        ``balance_transactions``, ``payouts``, ``refunds``, ``disputes``,
        ``events``, ``coupons``, ``plans``, ``setup_intents``.
    created_after : str | None
        ISO-8601 timestamp for incremental reads — only fetch objects created
        after this time.
    limit : int
        Page size per API call (default ``100``, max ``100``).
    expand : list[str] | None
        List of Stripe object paths to expand (e.g. ``["data.customer"]``).
    filters : dict | None
        Additional query parameters passed to the list endpoint
        (e.g. ``{"status": "active"}``).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)

        self._api_key: str = config["api_key"]
        self._resource_name: str = config["resource"]
        self._created_after: str | None = config.get("created_after")
        self._limit: int = min(config.get("limit", 100), 100)
        self._expand: list[str] | None = config.get("expand")
        self._filters: dict[str, Any] = config.get("filters", {})

        # Resolve the SDK resource class
        if self._resource_name not in _RESOURCE_MAP:
            raise ValueError(
                f"Unsupported Stripe resource '{self._resource_name}'. "
                f"Supported: {', '.join(sorted(_RESOURCE_MAP))}"
            )
        self._resource_cls = _RESOURCE_MAP[self._resource_name]

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def source_type(self) -> str:
        return "stripe"

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
        """Set the global Stripe API key."""
        logger.info("stripe.connecting", resource=self._resource_name)
        stripe_sdk.api_key = self._api_key
        self._connected = True
        logger.info("stripe.connected", resource=self._resource_name)

    def close(self) -> None:
        """No persistent connection to release for Stripe."""
        self._connected = False
        logger.debug("stripe.closed")

    def health_check(self) -> HealthStatus:
        """Verify API key by fetching account info."""
        try:
            if not self._connected:
                self.connect()
            stripe_sdk.Account.retrieve()
            return HealthStatus.HEALTHY
        except Exception:
            logger.warning("stripe.health_check_failed", exc_info=True)
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Reading
    # ------------------------------------------------------------------

    def _build_list_params(self) -> dict[str, Any]:
        """Build the keyword arguments for the Stripe list call."""
        params: dict[str, Any] = {"limit": self._limit}

        # Incremental filter
        if self._created_after:
            ts = datetime.fromisoformat(self._created_after)
            params["created"] = {"gte": int(ts.timestamp())}

        # Expand
        if self._expand:
            params["expand"] = self._expand

        # Additional user filters
        params.update(self._filters)

        return params

    def read(self) -> Iterator[Record]:
        """Read Stripe resources using auto-pagination.

        Yields one :class:`Record` per Stripe object, with ``data`` containing
        the full JSON representation.
        """
        if not self._connected:
            self.connect()

        params = self._build_list_params()
        logger.info("stripe.read_start", resource=self._resource_name, params={k: v for k, v in params.items() if k != "expand"})

        records_yielded = 0
        for obj in self._resource_cls.list(**params).auto_paging_iter():
            data = dict(obj)

            # Extract event time from 'created' (Unix timestamp)
            event_time: datetime | None = None
            created_ts = data.get("created")
            if isinstance(created_ts, (int, float)):
                event_time = datetime.fromtimestamp(created_ts, tz=timezone.utc)

            yield Record(
                data=data,
                event_time=event_time,
                source_metadata={
                    "source": "stripe",
                    "resource": self._resource_name,
                    "object_id": data.get("id", ""),
                },
            )
            records_yielded += 1

        logger.info("stripe.read_complete", resource=self._resource_name, records=records_yielded)

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def get_schema(self) -> Schema:
        """Infer schema by fetching one record and examining its structure."""
        if not self._connected:
            self.connect()

        # Fetch a single record to infer schema
        result = self._resource_cls.list(limit=1)
        items = list(result.get("data", []))

        if not items:
            logger.warning("stripe.schema_empty", resource=self._resource_name)
            return Schema(columns=[], primary_key=["id"])

        sample = dict(items[0])
        columns: list[ColumnDefinition] = []
        for key, value in sample.items():
            columns.append(
                ColumnDefinition(
                    name=key,
                    type=_infer_column_type(value),
                    nullable=True,
                )
            )

        return Schema(columns=columns, primary_key=["id"])

    def discover_schema(self) -> DiscoveredSchema:
        """Discover schema from a sample record."""
        base = self.get_schema()
        return DiscoveredSchema(
            columns=base.columns,
            primary_key=base.primary_key,
            partition_columns=None,
            source_name=self.source_type,
            object_name=self._resource_name,
            object_type="stripe_resource",
            source_metadata={
                "resource": self._resource_name,
                "api_version": stripe_sdk.api_version or "default",
            },
        )
