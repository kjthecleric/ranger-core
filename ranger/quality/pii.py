"""PII detection and masking for Ranger pipelines.

Pattern-based PII detection with no external dependencies for core patterns.
Supports scanning record batches and masking detected PII columns.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any

import structlog

from ranger.core.models import Record

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Built-in PII patterns
# ---------------------------------------------------------------------------

# Each pattern is a tuple of (pii_type_label, compiled_regex).
# Patterns are intentionally broad — they prioritise recall over precision
# so that sensitive data is not missed during scanning.

_PII_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    (
        "email",
        re.compile(
            r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
            re.IGNORECASE,
        ),
    ),
    (
        "phone",
        re.compile(
            r"(?:\+?1[\s\-.]?)?\(?\d{3}\)?[\s\-.]?\d{3}[\s\-.]?\d{4}",
        ),
    ),
    (
        "ssn",
        re.compile(r"\b\d{3}[\s\-]?\d{2}[\s\-]?\d{4}\b"),
    ),
    (
        "credit_card",
        re.compile(
            r"\b(?:4\d{3}|5[1-5]\d{2}|3[47]\d{2}|6(?:011|5\d{2}))"
            r"[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{1,4}\b"
        ),
    ),
    (
        "ip_address",
        re.compile(
            r"\b(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}"
            r"(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\b"
        ),
    ),
]

# Masking strategy constants
MASK_REDACT = "redact"
MASK_HASH = "hash"
MASK_PARTIAL = "partial"

_VALID_STRATEGIES: set[str] = {MASK_REDACT, MASK_HASH, MASK_PARTIAL}


# ---------------------------------------------------------------------------
# PIIDetector
# ---------------------------------------------------------------------------


class PIIDetector:
    """Scan records for PII and optionally mask detected columns.

    Built-in detectable PII types:
        * **email** — standard email addresses
        * **phone** — US phone numbers (with optional country code)
        * **ssn** — US Social Security Numbers
        * **credit_card** — Visa, MasterCard, Amex, Discover
        * **ip_address** — IPv4 addresses

    Custom patterns can be added via :meth:`add_pattern`.

    Usage::

        detector = PIIDetector()
        pii_map = detector.scan_records(records, sample_size=200)
        # pii_map: {"email_col": ["email"], "phone_col": ["phone"]}

        masked = detector.mask_record(record, pii_map, strategy="redact")
    """

    def __init__(
        self,
        extra_patterns: list[tuple[str, str]] | None = None,
    ) -> None:
        self._patterns: list[tuple[str, re.Pattern[str]]] = list(_PII_PATTERNS)

        if extra_patterns:
            for label, regex_str in extra_patterns:
                self._patterns.append((label, re.compile(regex_str)))

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add_pattern(self, label: str, pattern: str) -> None:
        """Register an additional PII pattern.

        Args:
            label: Human-readable PII type name (e.g. ``"passport"``).
            pattern: Regular expression string.
        """
        self._patterns.append((label, re.compile(pattern)))

    def scan_records(
        self,
        records: list[Record],
        sample_size: int = 100,
    ) -> dict[str, list[str]]:
        """Scan a sample of records and return detected PII types per column.

        Args:
            records: Records to scan.
            sample_size: Maximum number of records to sample.

        Returns:
            Dict mapping column names to a list of PII type labels found.
            Only columns where PII was detected are included.
        """
        sample = records[:sample_size]
        if not sample:
            return {}

        # Gather all column names
        all_columns: set[str] = set()
        for rec in sample:
            all_columns.update(rec.data.keys())

        column_pii: dict[str, set[str]] = {col: set() for col in all_columns}

        for rec in sample:
            for col in all_columns:
                value = rec.data.get(col)
                if value is None:
                    continue
                str_value = str(value)
                for label, pattern in self._patterns:
                    if pattern.search(str_value):
                        column_pii[col].add(label)

        # Filter to only columns with detected PII
        result: dict[str, list[str]] = {}
        for col, pii_types in column_pii.items():
            if pii_types:
                result[col] = sorted(pii_types)

        if result:
            logger.warning(
                "pii_detected",
                columns=list(result.keys()),
                details={k: v for k, v in result.items()},
            )
        else:
            logger.info("pii_scan_clean", sample_size=len(sample))

        return result

    def mask_record(
        self,
        record: Record,
        pii_columns: dict[str, list[str]],
        strategy: str = MASK_REDACT,
    ) -> Record:
        """Return a new record with PII columns masked.

        Args:
            record: The original record.
            pii_columns: Mapping of column → PII types (from :meth:`scan_records`).
            strategy: Masking strategy — ``redact``, ``hash``, or ``partial``.

        Returns:
            A new :class:`Record` with masked values for PII columns.
        """
        if strategy not in _VALID_STRATEGIES:
            raise ValueError(
                f"Unknown masking strategy: {strategy!r}. Valid: {_VALID_STRATEGIES}"
            )

        masked_data = dict(record.data)

        for col in pii_columns:
            if col not in masked_data:
                continue
            value = masked_data[col]
            if value is None:
                continue
            masked_data[col] = self._apply_mask(str(value), strategy)

        return Record(
            data=masked_data,
            event_time=record.event_time,
            arrival_time=record.arrival_time,
            source_metadata=record.source_metadata,
        )

    def mask_batch(
        self,
        records: list[Record],
        pii_columns: dict[str, list[str]],
        strategy: str = MASK_REDACT,
    ) -> list[Record]:
        """Mask PII in an entire batch of records.

        Args:
            records: Input records.
            pii_columns: Mapping of column → PII types.
            strategy: Masking strategy.

        Returns:
            List of masked records.
        """
        return [self.mask_record(r, pii_columns, strategy) for r in records]

    # ------------------------------------------------------------------
    # Masking helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _apply_mask(value: str, strategy: str) -> str:
        """Apply a masking strategy to a string value."""
        if strategy == MASK_REDACT:
            return "***REDACTED***"

        if strategy == MASK_HASH:
            return hashlib.sha256(value.encode("utf-8")).hexdigest()

        if strategy == MASK_PARTIAL:
            # Show last 4 characters, mask the rest
            if len(value) <= 4:
                return "****"
            return "*" * (len(value) - 4) + value[-4:]

        return value
