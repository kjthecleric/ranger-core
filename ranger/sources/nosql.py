"""NoSQL database sources — MongoDB, DynamoDB, Couchbase.

Each class wraps the respective client library and exposes records through
the standard :class:`BaseSource` interface.
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


# ---------------------------------------------------------------------------
# Python type → Ranger ColumnType mapping (shared by document stores)
# ---------------------------------------------------------------------------

_PY_TYPE_MAP: dict[str, ColumnType] = {
    "str": ColumnType.STRING,
    "int": ColumnType.INT64,
    "float": ColumnType.FLOAT64,
    "bool": ColumnType.BOOLEAN,
    "list": ColumnType.ARRAY,
    "dict": ColumnType.JSON,
    "NoneType": ColumnType.STRING,
    "ObjectId": ColumnType.STRING,
    "datetime": ColumnType.TIMESTAMP,
    "date": ColumnType.DATE,
    "Decimal128": ColumnType.DECIMAL,
    "Decimal": ColumnType.DECIMAL,
    "bytes": ColumnType.BINARY,
    "Binary": ColumnType.BINARY,
}


def _infer_column_type(key: str, sample: list[dict[str, Any]]) -> ColumnType:
    """Infer a :class:`ColumnType` for *key* by inspecting sample documents."""
    types_seen: set[str] = set()
    for doc in sample:
        val = doc.get(key)
        if val is not None:
            types_seen.add(type(val).__name__)
    types_seen.discard("NoneType")
    if not types_seen:
        return ColumnType.STRING
    # Pick the most common / first mapped type
    for t in types_seen:
        if t in _PY_TYPE_MAP:
            return _PY_TYPE_MAP[t]
    return ColumnType.STRING


# =========================================================================
# MongoDB
# =========================================================================


class MongoDBSource(BaseSource):
    """Read documents from a MongoDB collection via pymongo.

    Config keys:
        connection_string: MongoDB connection URI.
        database: Database name.
        collection: Collection name.
        filter: Optional query filter dict (default: ``{}``).
        projection: Optional projection dict.
        batch_size: Cursor batch size (default: ``5_000``).
        incremental_column: Field used for incremental reads.
        last_value: Last-seen value for the incremental field.
        sample_size: Number of documents to sample when inferring schema
            (default: ``200``).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._client: Any = None  # pymongo.MongoClient
        self._db: Any = None
        self._collection_handle: Any = None

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    @property
    def source_type(self) -> str:
        return "mongodb"

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
        try:
            from pymongo import MongoClient
        except ImportError as exc:
            raise ImportError(
                "pymongo is required for MongoDBSource. "
                "Install it with: pip install ranger-core[nosql]"
            ) from exc

        connection_string = self._config.get("connection_string")
        if not connection_string:
            raise ConnectionError("Missing required config key 'connection_string'")

        database = self._config.get("database")
        collection = self._config.get("collection")
        if not database or not collection:
            raise ConnectionError("Config must include 'database' and 'collection'")

        try:
            self._client = MongoClient(connection_string)
            # Force a connection check
            self._client.admin.command("ping")
            self._db = self._client[database]
            self._collection_handle = self._db[collection]
            self._connected = True
            logger.info(
                "mongodb_source.connected",
                database=database,
                collection=collection,
            )
        except Exception as exc:
            self._connected = False
            logger.error("mongodb_source.connect_failed", error=str(exc))
            raise ConnectionError(f"MongoDB connection failed: {exc}") from exc

    def close(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None
        self._db = None
        self._collection_handle = None
        self._connected = False
        logger.info("mongodb_source.closed")

    def health_check(self) -> HealthStatus:
        try:
            if self._client is None:
                self.connect()
            self._client.admin.command("ping")
            return HealthStatus.HEALTHY
        except Exception as exc:
            logger.warning("mongodb_source.health_check_failed", error=str(exc))
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Reading
    # ------------------------------------------------------------------

    def _build_filter(self) -> dict[str, Any]:
        """Compose the MongoDB query filter, including incremental predicate."""
        base_filter: dict[str, Any] = dict(self._config.get("filter", {}))
        inc_col = self._config.get("incremental_column")
        last_value = self._config.get("last_value")
        if inc_col and last_value is not None:
            base_filter[inc_col] = {"$gt": last_value}
        return base_filter

    def read(self) -> Iterator[Record]:
        if self._collection_handle is None:
            raise RuntimeError("Source not connected — call connect() first")

        query_filter = self._build_filter()
        projection = self._config.get("projection")
        batch_size: int = self._config.get("batch_size", 5_000)

        logger.info(
            "mongodb_source.read_start",
            filter=str(query_filter)[:200],
            batch_size=batch_size,
        )

        cursor = self._collection_handle.find(
            filter=query_filter,
            projection=projection,
            batch_size=batch_size,
        )

        row_count = 0
        for doc in cursor:
            # Convert ObjectId to string for JSON serialisation
            if "_id" in doc:
                doc["_id"] = str(doc["_id"])
            yield Record(
                data=doc,
                event_time=datetime.now(timezone.utc),
                source_metadata={
                    "source_type": "mongodb",
                    "database": self._config.get("database"),
                    "collection": self._config.get("collection"),
                },
            )
            row_count += 1

        logger.info("mongodb_source.read_complete", rows=row_count)

    # ------------------------------------------------------------------
    # Schema operations
    # ------------------------------------------------------------------

    def get_schema(self) -> Schema:
        """Infer schema by sampling *N* documents from the collection."""
        if self._collection_handle is None:
            raise RuntimeError("Source not connected — call connect() first")

        sample_size: int = self._config.get("sample_size", 200)
        sample_docs: list[dict[str, Any]] = list(
            self._collection_handle.find().limit(sample_size)
        )

        if not sample_docs:
            return Schema(columns=[])

        # Normalise _id
        for doc in sample_docs:
            if "_id" in doc:
                doc["_id"] = str(doc["_id"])

        all_keys: set[str] = set()
        for doc in sample_docs:
            all_keys.update(doc.keys())

        columns: list[ColumnDefinition] = []
        for key in sorted(all_keys):
            col_type = _infer_column_type(key, sample_docs)
            columns.append(ColumnDefinition(name=key, type=col_type, nullable=True))

        return Schema(columns=columns, primary_key=["_id"] if "_id" in all_keys else None)

    def discover_schema(self) -> DiscoveredSchema:
        schema = self.get_schema()
        row_count: int | None = None
        if self._collection_handle is not None:
            try:
                row_count = self._collection_handle.estimated_document_count()
            except Exception:
                pass

        return DiscoveredSchema(
            columns=schema.columns,
            primary_key=schema.primary_key,
            source_name="mongodb",
            object_name=self._config.get("collection", "unknown"),
            object_type="collection",
            row_count_estimate=row_count,
            source_metadata={
                "database": self._config.get("database"),
            },
        )


# =========================================================================
# DynamoDB
# =========================================================================


class DynamoDBSource(BaseSource):
    """Read items from an Amazon DynamoDB table via boto3.

    Config keys:
        table_name: DynamoDB table name.
        region: AWS region (default: ``us-east-1``).
        filter_expression: Optional DynamoDB filter expression string.
        expression_attribute_names: Optional attribute name placeholders.
        expression_attribute_values: Optional attribute value placeholders.
        batch_size: Page size for scan/query (default: ``1_000``).
        operation: ``scan`` (default) or ``query``.
        key_condition_expression: Key condition for query operations.
        aws_access_key_id: Optional explicit AWS key.
        aws_secret_access_key: Optional explicit AWS secret.
    """

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._client: Any = None  # boto3 DynamoDB resource Table
        self._table: Any = None

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    @property
    def source_type(self) -> str:
        return "dynamodb"

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
        try:
            import boto3
        except ImportError as exc:
            raise ImportError(
                "boto3 is required for DynamoDBSource. "
                "Install it with: pip install ranger-core[nosql]"
            ) from exc

        table_name = self._config.get("table_name")
        if not table_name:
            raise ConnectionError("Missing required config key 'table_name'")

        region = self._config.get("region", "us-east-1")

        kwargs: dict[str, Any] = {"region_name": region}
        if self._config.get("aws_access_key_id"):
            kwargs["aws_access_key_id"] = self._config["aws_access_key_id"]
            kwargs["aws_secret_access_key"] = self._config.get("aws_secret_access_key", "")

        try:
            self._client = boto3.resource("dynamodb", **kwargs)
            self._table = self._client.Table(table_name)
            # Force a describe to verify the table exists
            self._table.load()
            self._connected = True
            logger.info(
                "dynamodb_source.connected",
                table=table_name,
                region=region,
                item_count=self._table.item_count,
            )
        except Exception as exc:
            self._connected = False
            logger.error("dynamodb_source.connect_failed", error=str(exc))
            raise ConnectionError(f"DynamoDB connection failed: {exc}") from exc

    def close(self) -> None:
        self._client = None
        self._table = None
        self._connected = False
        logger.info("dynamodb_source.closed")

    def health_check(self) -> HealthStatus:
        try:
            if self._table is None:
                self.connect()
            self._table.load()
            return HealthStatus.HEALTHY
        except Exception as exc:
            logger.warning("dynamodb_source.health_check_failed", error=str(exc))
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Reading
    # ------------------------------------------------------------------

    def read(self) -> Iterator[Record]:
        if self._table is None:
            raise RuntimeError("Source not connected — call connect() first")

        operation = self._config.get("operation", "scan")
        batch_size: int = self._config.get("batch_size", 1_000)
        table_name = self._config.get("table_name", "unknown")

        scan_kwargs: dict[str, Any] = {"Limit": batch_size}

        # Optional filter expression
        filter_expr = self._config.get("filter_expression")
        if filter_expr:
            scan_kwargs["FilterExpression"] = filter_expr
        attr_names = self._config.get("expression_attribute_names")
        if attr_names:
            scan_kwargs["ExpressionAttributeNames"] = attr_names
        attr_values = self._config.get("expression_attribute_values")
        if attr_values:
            scan_kwargs["ExpressionAttributeValues"] = attr_values

        # Key condition for query operations
        key_cond = self._config.get("key_condition_expression")
        if operation == "query" and key_cond:
            scan_kwargs["KeyConditionExpression"] = key_cond

        logger.info(
            "dynamodb_source.read_start",
            operation=operation,
            table=table_name,
        )

        row_count = 0
        while True:
            if operation == "query":
                response = self._table.query(**scan_kwargs)
            else:
                response = self._table.scan(**scan_kwargs)

            items = response.get("Items", [])
            for item in items:
                # Convert Decimal to float for JSON compatibility
                data = self._convert_decimals(item)
                yield Record(
                    data=data,
                    event_time=datetime.now(timezone.utc),
                    source_metadata={
                        "source_type": "dynamodb",
                        "table": table_name,
                    },
                )
                row_count += 1

            # Pagination
            last_key = response.get("LastEvaluatedKey")
            if not last_key:
                break
            scan_kwargs["ExclusiveStartKey"] = last_key

        logger.info("dynamodb_source.read_complete", rows=row_count)

    @staticmethod
    def _convert_decimals(obj: Any) -> Any:
        """Recursively convert ``Decimal`` values to ``int`` or ``float``."""
        from decimal import Decimal

        if isinstance(obj, dict):
            return {k: DynamoDBSource._convert_decimals(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [DynamoDBSource._convert_decimals(i) for i in obj]
        if isinstance(obj, Decimal):
            if obj == int(obj):
                return int(obj)
            return float(obj)
        return obj

    # ------------------------------------------------------------------
    # Schema operations
    # ------------------------------------------------------------------

    def get_schema(self) -> Schema:
        """Infer schema from a sample scan of the table."""
        if self._table is None:
            raise RuntimeError("Source not connected — call connect() first")

        response = self._table.scan(Limit=200)
        items = response.get("Items", [])

        if not items:
            return Schema(columns=[])

        items = [self._convert_decimals(item) for item in items]

        all_keys: set[str] = set()
        for item in items:
            all_keys.update(item.keys())

        columns: list[ColumnDefinition] = []
        for key in sorted(all_keys):
            col_type = _infer_column_type(key, items)
            columns.append(ColumnDefinition(name=key, type=col_type, nullable=True))

        # Detect key schema
        key_schema = getattr(self._table, "key_schema", None)
        pk_columns: list[str] = []
        if key_schema:
            for key_def in key_schema:
                pk_columns.append(key_def["AttributeName"])

        return Schema(columns=columns, primary_key=pk_columns or None)

    def discover_schema(self) -> DiscoveredSchema:
        schema = self.get_schema()
        return DiscoveredSchema(
            columns=schema.columns,
            primary_key=schema.primary_key,
            source_name="dynamodb",
            object_name=self._config.get("table_name", "unknown"),
            object_type="table",
            row_count_estimate=self._table.item_count if self._table else None,
            source_metadata={
                "region": self._config.get("region", "us-east-1"),
            },
        )


# =========================================================================
# Couchbase
# NOTE: No entry point declared in pyproject.toml yet.
# To register, add to [project.entry-points."ranger.sources"]:
#   couchbase = "ranger.sources.nosql:CouchbaseSource"
# =========================================================================


class CouchbaseSource(BaseSource):
    """Read documents from a Couchbase bucket via the Couchbase Python SDK.

    Config keys:
        connection_string: Couchbase connection string (e.g. ``couchbase://host``).
        username: Cluster username.
        password: Cluster password.
        bucket: Bucket name.
        scope: Scope name (default: ``_default``).
        collection: Collection name (default: ``_default``).
        query: N1QL query to execute.  If omitted, reads all documents in the
            collection via a ``SELECT * FROM bucket.scope.collection`` query.
        batch_size: Page size for N1QL results (default: ``5_000``).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._cluster: Any = None
        self._bucket_handle: Any = None
        self._scope_handle: Any = None
        self._collection_handle: Any = None

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    @property
    def source_type(self) -> str:
        return "couchbase"

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
        try:
            from couchbase.auth import PasswordAuthenticator
            from couchbase.cluster import Cluster
            from couchbase.options import ClusterOptions
        except ImportError as exc:
            raise ImportError(
                "couchbase SDK is required for CouchbaseSource. "
                "Install it with: pip install ranger-core[nosql]"
            ) from exc

        conn_str = self._config.get("connection_string")
        username = self._config.get("username")
        password = self._config.get("password")
        bucket_name = self._config.get("bucket")

        if not conn_str or not bucket_name:
            raise ConnectionError("Config must include 'connection_string' and 'bucket'")
        if not username or not password:
            raise ConnectionError("Config must include 'username' and 'password'")

        try:
            auth = PasswordAuthenticator(username, password)
            self._cluster = Cluster(conn_str, ClusterOptions(auth))
            # Wait until ready
            self._cluster.wait_until_ready(timeout=datetime.now(timezone.utc).__class__(seconds=10))
            self._bucket_handle = self._cluster.bucket(bucket_name)

            scope_name = self._config.get("scope", "_default")
            collection_name = self._config.get("collection", "_default")
            self._scope_handle = self._bucket_handle.scope(scope_name)
            self._collection_handle = self._scope_handle.collection(collection_name)

            self._connected = True
            logger.info(
                "couchbase_source.connected",
                bucket=bucket_name,
                scope=scope_name,
                collection=collection_name,
            )
        except Exception as exc:
            self._connected = False
            logger.error("couchbase_source.connect_failed", error=str(exc))
            raise ConnectionError(f"Couchbase connection failed: {exc}") from exc

    def close(self) -> None:
        # Couchbase SDK handles cleanup on GC; explicit close not always available
        self._cluster = None
        self._bucket_handle = None
        self._scope_handle = None
        self._collection_handle = None
        self._connected = False
        logger.info("couchbase_source.closed")

    def health_check(self) -> HealthStatus:
        try:
            if self._cluster is None:
                self.connect()
            # Execute a simple diagnostics ping
            self._cluster.ping()
            return HealthStatus.HEALTHY
        except Exception as exc:
            logger.warning("couchbase_source.health_check_failed", error=str(exc))
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Reading
    # ------------------------------------------------------------------

    def _build_query(self) -> str:
        """Build the N1QL query from config."""
        raw_query: str | None = self._config.get("query")
        if raw_query:
            return raw_query

        bucket = self._config.get("bucket", "default")
        scope = self._config.get("scope", "_default")
        collection = self._config.get("collection", "_default")
        return f"SELECT META().id AS _id, * FROM `{bucket}`.`{scope}`.`{collection}`"

    def read(self) -> Iterator[Record]:
        if self._cluster is None:
            raise RuntimeError("Source not connected — call connect() first")

        query = self._build_query()
        logger.info("couchbase_source.read_start", query=query[:200])

        from couchbase.options import QueryOptions

        batch_size: int = self._config.get("batch_size", 5_000)
        result = self._cluster.query(query, QueryOptions(scan_consistency="request_plus"))

        row_count = 0
        for row in result:
            data = dict(row) if not isinstance(row, dict) else row
            yield Record(
                data=data,
                event_time=datetime.now(timezone.utc),
                source_metadata={
                    "source_type": "couchbase",
                    "bucket": self._config.get("bucket"),
                },
            )
            row_count += 1

        logger.info("couchbase_source.read_complete", rows=row_count)

    # ------------------------------------------------------------------
    # Schema operations
    # ------------------------------------------------------------------

    def get_schema(self) -> Schema:
        """Infer schema by sampling documents via N1QL."""
        if self._cluster is None:
            raise RuntimeError("Source not connected — call connect() first")

        query = self._build_query()
        # Limit to sample
        sample_query = f"{query} LIMIT 200"

        from couchbase.options import QueryOptions

        result = self._cluster.query(sample_query, QueryOptions(scan_consistency="request_plus"))
        sample_docs: list[dict[str, Any]] = [dict(row) if not isinstance(row, dict) else row for row in result]

        if not sample_docs:
            return Schema(columns=[])

        all_keys: set[str] = set()
        for doc in sample_docs:
            all_keys.update(doc.keys())

        columns: list[ColumnDefinition] = []
        for key in sorted(all_keys):
            col_type = _infer_column_type(key, sample_docs)
            columns.append(ColumnDefinition(name=key, type=col_type, nullable=True))

        return Schema(columns=columns)

    def discover_schema(self) -> DiscoveredSchema:
        schema = self.get_schema()
        return DiscoveredSchema(
            columns=schema.columns,
            primary_key=schema.primary_key,
            source_name="couchbase",
            object_name=self._config.get("bucket", "unknown"),
            object_type="collection",
            source_metadata={
                "scope": self._config.get("scope", "_default"),
                "collection": self._config.get("collection", "_default"),
            },
        )
