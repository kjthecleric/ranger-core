"""Deduplication engine for Ranger pipelines.

Supports exact, primary-key, and fuzzy deduplication strategies with
configurable keep-policy (first / latest).
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

import structlog

from ranger.core.models import Record

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Strategy constants
# ---------------------------------------------------------------------------

STRATEGY_EXACT = "exact"
STRATEGY_PRIMARY_KEY = "primary_key"
STRATEGY_FUZZY = "fuzzy"

_VALID_STRATEGIES: set[str] = {STRATEGY_EXACT, STRATEGY_PRIMARY_KEY, STRATEGY_FUZZY}

KEEP_FIRST = "first"
KEEP_LATEST = "latest"


# ---------------------------------------------------------------------------
# Deduplicator
# ---------------------------------------------------------------------------


class Deduplicator:
    """Deduplicates a list of records using configurable strategies.

    Strategies:
        ``exact``
            Hash **all** data fields.  Two records are duplicates if every
            field value is identical.

        ``primary_key``
            Hash only the columns specified in *columns*.  Useful when a
            natural or surrogate key exists.

        ``fuzzy``
            Similarity-based matching on the specified *columns*.  Uses
            simple token-overlap (Jaccard) similarity.  Records with
            similarity ≥ *similarity_threshold* are treated as duplicates.

    Config:
        strategy: One of ``exact``, ``primary_key``, ``fuzzy``.
        columns: Column names used for ``primary_key`` and ``fuzzy``.
        keep: Which duplicate to keep — ``first`` or ``latest``.
        similarity_threshold: Float in [0, 1] for ``fuzzy`` (default 0.9).

    Usage::

        dedup = Deduplicator(
            strategy="primary_key",
            columns=["order_id"],
            keep="latest",
        )
        unique_records = dedup.deduplicate(records)
    """

    def __init__(
        self,
        strategy: str = STRATEGY_EXACT,
        columns: list[str] | None = None,
        keep: str = KEEP_FIRST,
        similarity_threshold: float = 0.9,
    ) -> None:
        if strategy not in _VALID_STRATEGIES:
            raise ValueError(
                f"Unknown dedup strategy: {strategy!r}. Valid: {_VALID_STRATEGIES}"
            )
        self._strategy = strategy
        self._columns = columns or []
        self._keep = keep
        self._similarity_threshold = similarity_threshold

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def deduplicate(self, records: list[Record]) -> list[Record]:
        """Remove duplicates and return a deduplicated list.

        Args:
            records: Input records (order is preserved for ``first`` keep).

        Returns:
            Deduplicated list of records.
        """
        if not records:
            return []

        if self._strategy == STRATEGY_EXACT:
            result = self._dedup_exact(records)
        elif self._strategy == STRATEGY_PRIMARY_KEY:
            result = self._dedup_primary_key(records)
        elif self._strategy == STRATEGY_FUZZY:
            result = self._dedup_fuzzy(records)
        else:
            result = records

        removed = len(records) - len(result)
        logger.info(
            "dedup_complete",
            strategy=self._strategy,
            input_count=len(records),
            output_count=len(result),
            removed=removed,
        )
        return result

    # ------------------------------------------------------------------
    # Strategies
    # ------------------------------------------------------------------

    def _dedup_exact(self, records: list[Record]) -> list[Record]:
        """Hash all fields to detect exact duplicates."""
        seen: dict[str, int] = {}  # hash → index of kept record
        result: list[Record] = []

        for idx, record in enumerate(records):
            h = self._hash_record(record.data)
            if h not in seen:
                seen[h] = len(result)
                result.append(record)
            elif self._keep == KEEP_LATEST:
                # Replace the previously-kept record
                result[seen[h]] = record

        return result

    def _dedup_primary_key(self, records: list[Record]) -> list[Record]:
        """Hash only the specified key columns."""
        if not self._columns:
            logger.warning("dedup_primary_key_no_columns; falling back to exact")
            return self._dedup_exact(records)

        seen: dict[str, int] = {}
        result: list[Record] = []

        for record in records:
            key_data = {c: record.data.get(c) for c in self._columns}
            h = self._hash_record(key_data)
            if h not in seen:
                seen[h] = len(result)
                result.append(record)
            elif self._keep == KEEP_LATEST:
                result[seen[h]] = record

        return result

    def _dedup_fuzzy(self, records: list[Record]) -> list[Record]:
        """Similarity-based dedup using Jaccard token overlap."""
        if not self._columns:
            logger.warning("dedup_fuzzy_no_columns; using all columns")

        result: list[Record] = []
        result_tokens: list[set[str]] = []

        for record in records:
            tokens = self._tokenize(record)
            is_dup = False

            for i, existing_tokens in enumerate(result_tokens):
                sim = self._jaccard(tokens, existing_tokens)
                if sim >= self._similarity_threshold:
                    is_dup = True
                    if self._keep == KEEP_LATEST:
                        result[i] = record
                        result_tokens[i] = tokens
                    break

            if not is_dup:
                result.append(record)
                result_tokens.append(tokens)

        return result

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _hash_record(data: dict[str, Any]) -> str:
        """Compute a deterministic SHA-256 hash of a dict."""
        canonical = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def _tokenize(self, record: Record) -> set[str]:
        """Create a set of string tokens from the record for fuzzy matching."""
        columns = self._columns or list(record.data.keys())
        tokens: set[str] = set()
        for col in columns:
            value = record.data.get(col)
            if value is not None:
                # Split on whitespace and lowercase for comparison
                for token in str(value).lower().split():
                    tokens.add(f"{col}:{token}")
        return tokens

    @staticmethod
    def _jaccard(a: set[str], b: set[str]) -> float:
        """Jaccard similarity coefficient."""
        if not a and not b:
            return 1.0
        union = a | b
        if not union:
            return 1.0
        return len(a & b) / len(union)
