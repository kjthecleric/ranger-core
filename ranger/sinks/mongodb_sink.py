"""MongoDB sink — writes records to a MongoDB collection.

Uses pymongo with bulk_write for efficient batch operations, supporting
insert, upsert, and replace write modes.
"""

from __future__ import annotations

from typing import Any

import structlog

from ranger.core.models import HealthStatus, Record, Schema
from ranger.sinks.base import BaseSink

try:
    from pymongo import MongoClient
    from pymongo.collection import Collection
    from pymongo.errors import BulkWriteError
    from pymongo.operations import InsertOne, ReplaceOne, UpdateOne

    _HAS_PYMONGO = True
except ImportError:  # pragma: no cover
    _HAS_PYMONGO = False

logger = structlog.get_logger(__name__)


class MongoDBSink(BaseSink):
    """Write records to a MongoDB collection.

    Config keys:
        connection_string: MongoDB connection URI (required).
        database: Target database name (required).
        collection: Target collection name (required).
        write_mode: ``insert`` | ``upsert`` | ``replace`` (default ``insert``).
        id_field: Record field to match on for upsert/replace operations
            (default ``_id``).
        ordered: Whether bulk writes should be ordered — ``True`` stops on
            first error, ``False`` continues (default ``True``).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        if not _HAS_PYMONGO:
            raise ImportError(
                "pymongo is required for MongoDBSink. "
                "Install it with: pip install pymongo"
            )
        super().__init__(config)

        self._connection_string: str = config["connection_string"]
        self._database_name: str = config["database"]
        self._collection_name: str = config["collection"]
        self._write_mode: str = config.get("write_mode", "insert")
        self._id_field: str = config.get("id_field", "_id")
        self._ordered: bool = config.get("ordered", True)

        self._client: MongoClient | None = None
        self._collection: Collection | None = None
        self._schema: Schema | None = None

    @property
    def sink_type(self) -> str:
        return "mongodb"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def open(self, schema: Schema) -> None:
        self._schema = schema

        self._client = MongoClient(self._connection_string)
        db = self._client[self._database_name]
        self._collection = db[self._collection_name]

        self._opened = True
        logger.info(
            "mongodb_sink.opened",
            database=self._database_name,
            collection=self._collection_name,
            write_mode=self._write_mode,
        )

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
        self._collection = None
        self._opened = False
        logger.info(
            "mongodb_sink.closed",
            database=self._database_name,
            collection=self._collection_name,
        )

    def health_check(self) -> HealthStatus:
        try:
            if self._client is None:
                return HealthStatus.UNHEALTHY
            # Lightweight admin command
            self._client.admin.command("ping")
            return HealthStatus.HEALTHY
        except Exception:
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Writing
    # ------------------------------------------------------------------

    def write_batch(self, records: list[Record]) -> int:
        if not self._opened or self._collection is None:
            raise RuntimeError("Sink not opened. Call open() first.")
        if not records:
            return 0

        operations = self._build_operations(records)
        if not operations:
            return 0

        try:
            result = self._collection.bulk_write(operations, ordered=self._ordered)
            written = (
                result.inserted_count
                + result.upserted_count
                + result.modified_count
            )
            logger.info(
                "mongodb_sink.batch_written",
                inserted=result.inserted_count,
                upserted=result.upserted_count,
                modified=result.modified_count,
                collection=self._collection_name,
            )
            return written
        except BulkWriteError as exc:
            details = exc.details
            n_inserted = details.get("nInserted", 0)
            n_upserted = details.get("nUpserted", 0)
            n_modified = details.get("nModified", 0)
            write_errors = details.get("writeErrors", [])
            logger.warning(
                "mongodb_sink.bulk_write_partial",
                inserted=n_inserted,
                upserted=n_upserted,
                modified=n_modified,
                error_count=len(write_errors),
                sample_errors=write_errors[:3],
            )
            return n_inserted + n_upserted + n_modified

    def _build_operations(self, records: list[Record]) -> list[Any]:
        """Convert records into pymongo bulk write operations."""
        ops: list[Any] = []

        if self._write_mode == "insert":
            for rec in records:
                ops.append(InsertOne(rec.data))

        elif self._write_mode == "upsert":
            for rec in records:
                filter_doc = self._build_filter(rec)
                update_doc = {"$set": rec.data}
                ops.append(UpdateOne(filter_doc, update_doc, upsert=True))

        elif self._write_mode == "replace":
            for rec in records:
                filter_doc = self._build_filter(rec)
                ops.append(ReplaceOne(filter_doc, rec.data, upsert=True))

        else:
            raise ValueError(f"Unsupported write_mode: {self._write_mode}")

        return ops

    def _build_filter(self, record: Record) -> dict[str, Any]:
        """Build a MongoDB filter document for upsert/replace matching."""
        if self._id_field in record.data:
            return {self._id_field: record.data[self._id_field]}
        raise ValueError(
            f"Record is missing the id_field '{self._id_field}' required "
            f"for write_mode='{self._write_mode}'"
        )

    # ------------------------------------------------------------------
    # Schema evolution
    # ------------------------------------------------------------------

    def evolve_schema(self, new_schema: Schema) -> None:
        """No-op — MongoDB is schemaless; documents are self-describing."""
        self._schema = new_schema
        logger.debug(
            "mongodb_sink.evolve_schema_noop",
            collection=self._collection_name,
        )

    def supports_schema_evolution(self) -> bool:
        """MongoDB is schemaless; evolution is inherently supported."""
        return True
