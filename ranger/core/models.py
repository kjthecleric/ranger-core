"""Core data models for Ranger pipelines.

All fundamental types used across sources, engines, sinks, and the schema layer.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class ColumnType(str, Enum):
    """Canonical column types used in the Ranger schema model."""

    BOOLEAN = "boolean"
    INT8 = "int8"
    INT16 = "int16"
    INT32 = "int32"
    INT64 = "int64"
    FLOAT32 = "float32"
    FLOAT64 = "float64"
    DECIMAL = "decimal"
    STRING = "string"
    BINARY = "binary"
    DATE = "date"
    TIME = "time"
    TIMESTAMP = "timestamp"
    TIMESTAMP_TZ = "timestamp_tz"
    JSON = "json"
    ARRAY = "array"
    MAP = "map"
    STRUCT = "struct"
    UUID = "uuid"
    # Geospatial
    POINT = "point"
    LINESTRING = "linestring"
    POLYGON = "polygon"
    GEOMETRY = "geometry"
    # Blob
    BLOB = "blob"


class DriftClassification(str, Enum):
    COMPATIBLE = "compatible"
    BREAKING = "breaking"


class DriftAction(str, Enum):
    AUTO_EVOLVE = "auto_evolve"
    QUARANTINE = "quarantine"
    FAIL = "fail"
    MANUAL = "manual"


class RunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TriggeredBy(str, Enum):
    MANUAL = "manual"
    SCHEDULER = "scheduler"
    EVENT = "event"
    API = "api"


class LateArrivalStrategy(str, Enum):
    WATERMARK = "watermark"
    APPEND_WITH_FLAG = "append_with_flag"
    UPSERT_MERGE = "upsert_merge"
    REPROCESS_PARTITION = "reprocess_partition"


class PaginationStrategy(str, Enum):
    OFFSET = "offset"
    CURSOR = "cursor"
    PAGE_NUMBER = "page_number"
    LINK_HEADER = "link_header"
    TOKEN = "token"


class BackoffStrategy(str, Enum):
    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    FIXED = "fixed"


class BlobHandling(str, Enum):
    PASSTHROUGH = "passthrough"
    REFERENCE = "reference"
    SKIP = "skip"


class NestedDataStrategy(str, Enum):
    FLATTEN_ALL = "flatten_all"
    FLATTEN_DEPTH = "flatten_depth"
    EXTRACT_FIELDS = "extract_fields"
    PRESERVE_JSON = "preserve_json"


class ArrayHandling(str, Enum):
    EXPLODE = "explode"
    JSON_STRING = "json_string"
    FIRST_ELEMENT = "first_element"


class DedupStrategy(str, Enum):
    EXACT_MATCH = "exact_match"
    PRIMARY_KEY = "primary_key"
    HASH_DEDUP = "hash_dedup"
    WINDOW_DEDUP = "window_dedup"


class DedupKeep(str, Enum):
    LATEST = "latest"
    EARLIEST = "earliest"
    FIRST_SEEN = "first_seen"


class ValidationAction(str, Enum):
    WARN = "warn"
    REJECT = "reject"
    FAIL = "fail"


class PIIMaskingStrategy(str, Enum):
    HASH = "hash"
    REDACT = "redact"
    MASK = "mask"
    TOKENIZE = "tokenize"
    ENCRYPT = "encrypt"
    WARN_ONLY = "warn_only"


class WebhookBatchMode(str, Enum):
    SINGLE = "single"
    BATCH = "batch"
    NDJSON_STREAM = "ndjson_stream"


class WebhookAuthType(str, Enum):
    BEARER = "bearer"
    BASIC = "basic"
    API_KEY = "api_key"
    OAUTH2 = "oauth2"
    NONE = "none"


class KafkaSerializationFormat(str, Enum):
    JSON = "json"
    AVRO = "avro"
    PROTOBUF = "protobuf"


class HealthStatus(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


class MappingType(str, Enum):
    DIRECT = "direct"
    RENAMED = "renamed"
    DERIVED = "derived"


# ---------------------------------------------------------------------------
# Record
# ---------------------------------------------------------------------------


class Record(BaseModel):
    """A single data record flowing through the pipeline.

    Records are dict-like with event metadata attached.
    """

    data: dict[str, Any] = Field(default_factory=dict, description="The actual record payload")
    event_time: datetime | None = Field(default=None, description="When the event originally occurred")
    arrival_time: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="When Ranger received this record",
    )
    source_metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Source-specific metadata (partition, offset, file path, etc.)",
    )
    _is_late: bool = False
    _arrival_lag_seconds: float | None = None

    def get(self, key: str, default: Any = None) -> Any:
        """Get a value from the record data."""
        return self.data.get(key, default)

    def __getitem__(self, key: str) -> Any:
        return self.data[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.data[key] = value

    def __contains__(self, key: str) -> bool:
        return key in self.data


class BlobRecord(BaseModel):
    """A record wrapping binary content (images, PDFs, documents)."""

    content: bytes
    mime_type: str = "application/octet-stream"
    filename: str | None = None
    size_bytes: int = 0
    checksum_sha256: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


class ColumnDefinition(BaseModel):
    """Definition of a single column in a schema."""

    name: str
    type: ColumnType
    nullable: bool = True
    description: str | None = None
    default_value: Any = None
    constraints: dict[str, Any] = Field(default_factory=dict)
    # Geospatial
    srid: int | None = None
    # Decimal
    precision: int | None = None
    scale: int | None = None
    # String
    max_length: int | None = None
    # Array / Map / Struct
    element_type: ColumnType | None = None
    key_type: ColumnType | None = None
    value_type: ColumnType | None = None
    fields: list[ColumnDefinition] | None = None


class Schema(BaseModel):
    """Schema definition for a dataset — a list of typed columns."""

    columns: list[ColumnDefinition] = Field(default_factory=list)
    primary_key: list[str] | None = None
    partition_columns: list[str] | None = None

    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def get_column(self, name: str) -> ColumnDefinition | None:
        for col in self.columns:
            if col.name == name:
                return col
        return None

    def fingerprint(self) -> str:
        """Compute a deterministic hash of the schema for comparison."""
        import hashlib

        canonical = "|".join(
            f"{c.name}:{c.type.value}:{c.nullable}" for c in sorted(self.columns, key=lambda c: c.name)
        )
        return hashlib.sha256(canonical.encode()).hexdigest()[:16]


class DiscoveredSchema(Schema):
    """Schema discovered from a source, with extra catalog metadata."""

    source_name: str = ""
    object_name: str = ""
    object_type: str = ""  # table, view, topic, endpoint, collection
    row_count_estimate: int | None = None
    discovered_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source_metadata: dict[str, Any] = Field(default_factory=dict)


class SchemaVersion(BaseModel):
    """An immutable versioned schema stored in the metadata registry."""

    schema_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    pipeline_name: str
    source_name: str
    version_number: int
    schema_definition: Schema
    fingerprint: str
    discovered_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    registered_by: str | None = None
    is_active: bool = True


class ColumnChange(BaseModel):
    """A single column-level change between two schema versions."""

    column_name: str
    change_type: Literal["added", "removed", "type_changed", "nullability_changed", "renamed"]
    old_value: str | None = None
    new_value: str | None = None
    classification: DriftClassification


class SchemaDiff(BaseModel):
    """Diff between two schema versions."""

    old_schema_id: str
    new_schema_id: str
    changes: list[ColumnChange] = Field(default_factory=list)
    overall_classification: DriftClassification = DriftClassification.COMPATIBLE

    @property
    def has_breaking_changes(self) -> bool:
        return any(c.classification == DriftClassification.BREAKING for c in self.changes)


class DriftEvent(BaseModel):
    """A schema drift event to be logged."""

    drift_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    pipeline_name: str
    previous_schema_id: str | None = None
    new_schema_id: str
    drift_type: DriftClassification
    changes: list[ColumnChange] = Field(default_factory=list)
    action_taken: DriftAction
    detected_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# ---------------------------------------------------------------------------
# Source Catalog
# ---------------------------------------------------------------------------


class SourceCatalogEntry(BaseModel):
    """A single discovered object (table, topic, endpoint) in a source."""

    catalog_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source_name: str
    source_type: str
    object_name: str
    object_type: str  # table, view, topic, endpoint, collection
    last_discovered_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    row_count_estimate: int | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ColumnLineageEntry(BaseModel):
    """Column-level lineage mapping from source to sink."""

    lineage_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    pipeline_name: str
    source_column: str
    source_type: str
    sink_column: str
    sink_type: str
    mapping_type: MappingType = MappingType.DIRECT
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# ---------------------------------------------------------------------------
# Configuration Models
# ---------------------------------------------------------------------------


class RetryConfig(BaseModel):
    """Retry configuration for sources and sinks."""

    max_retries: int = 3
    backoff_strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0


class PaginationConfig(BaseModel):
    """Pagination configuration for API sources."""

    strategy: PaginationStrategy = PaginationStrategy.OFFSET
    page_size: int = 100
    max_pages: int | None = None
    rate_limit_rps: float = 10.0
    retry_config: RetryConfig = Field(default_factory=RetryConfig)


class StreamConfig(BaseModel):
    """Configuration for streaming sources."""

    buffer_size: int = 1000
    flush_interval_seconds: float = 5.0
    reconnect_on_failure: bool = True
    max_reconnect_attempts: int = 10
    idle_timeout_seconds: float = 300.0


class NestedDataConfig(BaseModel):
    """Configuration for handling nested / hierarchical data."""

    strategy: NestedDataStrategy = NestedDataStrategy.PRESERVE_JSON
    max_depth: int = 3
    array_handling: ArrayHandling = ArrayHandling.JSON_STRING
    extract_paths: list[str] | None = None


class DedupConfig(BaseModel):
    """Deduplication configuration."""

    strategy: DedupStrategy = DedupStrategy.PRIMARY_KEY
    columns: list[str] = Field(default_factory=list)
    keep: DedupKeep = DedupKeep.LATEST
    window_seconds: int | None = None


class ValidationRule(BaseModel):
    """A single data validation rule."""

    type: str  # not_null, unique, range, pattern, allowed_values, custom_sql, row_count, freshness
    columns: list[str] | None = None
    column: str | None = None
    action: ValidationAction = ValidationAction.WARN
    # type-specific params
    min: float | None = None
    max: float | None = None
    regex: str | None = None
    values: list[Any] | None = None
    expression: str | None = None
    max_age_seconds: int | None = None


class DeadLetterConfig(BaseModel):
    """Dead letter queue / directory configuration."""

    type: str = "local"  # local, s3, gcs
    path: str = "./dead_letter/{pipeline_name}/{run_date}/"
    config: dict[str, Any] = Field(default_factory=dict)


class PIIConfig(BaseModel):
    """PII detection and masking configuration."""

    scan: bool = False
    action: PIIMaskingStrategy = PIIMaskingStrategy.WARN_ONLY
    columns: dict[str, PIIMaskingStrategy] = Field(default_factory=dict)


class QualityConfig(BaseModel):
    """Data quality configuration block."""

    rules: list[ValidationRule] = Field(default_factory=list)
    dead_letter: DeadLetterConfig | None = None
    dedup: DedupConfig | None = None
    pii: PIIConfig | None = None


class EncryptionConfig(BaseModel):
    """Encryption configuration for in-transit and at-rest."""

    in_transit: dict[str, Any] = Field(default_factory=lambda: {"tls": "required", "verify_certs": True})
    at_rest: dict[str, Any] = Field(default_factory=dict)


class LateArrivalConfig(BaseModel):
    """Late-arrival handling configuration."""

    strategy: LateArrivalStrategy = LateArrivalStrategy.APPEND_WITH_FLAG
    watermark_delay_seconds: int = 3600


class EngineConfig(BaseModel):
    """Engine configuration block in a pipeline."""

    type: Literal["batch", "stream", "event"] = "batch"
    config: dict[str, Any] = Field(default_factory=dict)
    late_arrival: LateArrivalConfig = Field(default_factory=LateArrivalConfig)


class SchemaConfig(BaseModel):
    """Schema management configuration block."""

    registry: Literal["duckdb", "confluent"] = "duckdb"
    drift_handling: dict[str, str] = Field(
        default_factory=lambda: {"compatible": "auto_evolve", "breaking": "quarantine"}
    )


class WebhookAuthConfig(BaseModel):
    """Auth config for webhook sinks."""

    type: WebhookAuthType = WebhookAuthType.NONE
    token_env: str | None = None
    username: str | None = None
    password_env: str | None = None
    api_key_header: str | None = None
    api_key_env: str | None = None
    oauth2_token_url: str | None = None
    oauth2_client_id_env: str | None = None
    oauth2_client_secret_env: str | None = None


class ObservabilityConfig(BaseModel):
    """Observability configuration block."""

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    emit_lineage: bool = True
    drift_audit_table: str = "schema_drift_log"
    metrics_endpoint: str | None = None  # Prometheus push URL


class ScheduleConfig(BaseModel):
    """Schedule configuration — informational only, consumed by BYOS integrations."""

    cron: str | None = None
    timezone: str = "UTC"
    scheduler: Literal["airflow", "prefect", "dagster", "cron"] | None = None
    dag_id: str | None = None
    retries: int = 0
    retry_delay_minutes: int = 5
    tags: list[str] = Field(default_factory=list)


class SourceConfig(BaseModel):
    """Source configuration block in a pipeline."""

    type: str
    config: dict[str, Any] = Field(default_factory=dict)
    nested_data: NestedDataConfig | None = None


class SinkConfig(BaseModel):
    """Sink configuration block in a pipeline."""

    type: str
    config: dict[str, Any] = Field(default_factory=dict)


class PipelineConfig(BaseModel):
    """Complete pipeline configuration — the top-level model parsed from YAML."""

    pipeline: PipelineMeta
    source: SourceConfig
    engine: EngineConfig = Field(default_factory=EngineConfig)
    schema_: SchemaConfig = Field(default_factory=SchemaConfig, alias="schema")
    sink: SinkConfig
    quality: QualityConfig = Field(default_factory=QualityConfig)
    encryption: EncryptionConfig | None = None
    observability: ObservabilityConfig = Field(default_factory=ObservabilityConfig)
    schedule: ScheduleConfig | None = None

    model_config = {"populate_by_name": True}


class PipelineMeta(BaseModel):
    """Pipeline identity and version metadata."""

    name: str
    version: str = "1.0"
    description: str | None = None
    owner: str | None = None
    tags: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Run Result
# ---------------------------------------------------------------------------


class RunResult(BaseModel):
    """Result of a single pipeline run."""

    run_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    pipeline_name: str
    status: RunStatus = RunStatus.PENDING
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: datetime | None = None
    records_read: int = 0
    records_written: int = 0
    records_failed: int = 0
    bytes_processed: int = 0
    late_records_count: int = 0
    schema_drift_detected: bool = False
    error_message: str | None = None
    triggered_by: TriggeredBy = TriggeredBy.MANUAL
    config_snapshot: dict[str, Any] = Field(default_factory=dict)
    drift_events: list[DriftEvent] = Field(default_factory=list)
    duration_seconds: float | None = None
