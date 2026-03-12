# 🤠 Ranger — Reliable Data Extraction & Loading Platform

**Ranger** is a pluggable, configuration-driven data extraction and loading framework for Python. Define pipelines in YAML, run them from the CLI or programmatically, and let Ranger handle schema detection, drift management, data quality, and observability.

---

## Features

- **Configuration-driven** — define pipelines in YAML; no code required for common patterns
- **Plugin architecture** — sources, engines, and sinks are auto-discovered via entry points
- **Schema management** — DuckDB-backed schema registry with automatic drift detection and evolution
- **Data quality** — null checks, deduplication, and PII detection rules
- **Secret management** — pluggable providers (env vars, files, Vault, AWS/GCP/Azure)
- **Observability** — structured logging via structlog, OpenLineage integration
- **BYOS (Bring Your Own Scheduler)** — works with Airflow, Prefect, Dagster, or cron
- **Python SDK** — programmatic pipeline execution for custom integrations

## Quick Start

### Prerequisites

- Python 3.11+

### Installation

```bash
# Install with core dependencies only
pip install -e .

# Install with dev tools (pytest, ruff, mypy)
pip install -e ".[dev]"

# Install with specific connector extras
pip install -e ".[rest,snowflake,delta]"

# Install everything
pip install -e ".[all]"
```

### Run the Example Pipeline

1. **Create sample data:**

   ```bash
   mkdir -p data
   printf 'id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,300\n' > data/sample.csv
   ```

2. **Validate the config:**

   ```bash
   ranger validate --config configs/example_pipeline.yaml
   ```

3. **Run the pipeline:**

   ```bash
   ranger run --config configs/example_pipeline.yaml
   ```

   This reads `data/sample.csv`, processes it in batch mode, and writes JSON to `output/example/`.

### Using the Python SDK

```python
from ranger.sdk import RangerClient

client = RangerClient()
result = client.run_pipeline("configs/example_pipeline.yaml")
print(f"Status: {result.status.value}, Records: {result.records_written}")
```

---

## CLI Reference

| Command | Description |
|---------|-------------|
| `ranger run -c pipeline.yaml` | Execute a pipeline |
| `ranger validate -c pipeline.yaml` | Validate config without running |
| `ranger schema show -p <name>` | Show active schema for a pipeline |
| `ranger schema history -p <name>` | Show schema version history |
| `ranger plugins list` | List registered source/engine/sink plugins |
| `ranger export -t <table> -o out.parquet` | Export a metadata table |

---

## Pipeline Configuration

Pipelines are defined in YAML. See [`configs/example_pipeline.yaml`](configs/example_pipeline.yaml) for a complete example.

```yaml
pipeline:
  name: my_pipeline
  version: "1.0"
  description: "Read CSV, write JSON"

source:
  type: file
  config:
    path: "./data/input.csv"
    format: csv

engine:
  type: batch
  config:
    chunk_size: 5000

schema:
  registry: duckdb
  drift_handling:
    compatible: auto_evolve
    breaking: quarantine

sink:
  type: local
  config:
    path: "./output/results"
    format: json

quality:
  rules:
    - type: not_null
      columns: ["id"]
      action: reject
  dedup:
    strategy: primary_key
    columns: ["id"]
    keep: latest
```

---

## Project Structure

```
ranger-core/
├── configs/
│   └── example_pipeline.yaml    # Example pipeline config
├── ranger/
│   ├── __init__.py
│   ├── cli.py                   # Typer CLI application
│   ├── sdk.py                   # Programmatic SDK client
│   ├── core/
│   │   ├── config.py            # YAML config loading & validation
│   │   ├── models.py            # Pydantic domain models
│   │   ├── pipeline.py          # Pipeline orchestration
│   │   └── registry.py          # Plugin registry (entry-point based)
│   ├── sources/
│   │   ├── base.py              # BaseSource abstract class
│   │   ├── file_source.py       # CSV/JSON/Parquet file source
│   │   └── saas/                # SaaS connector stubs
│   ├── engines/
│   │   ├── base.py              # BaseEngine abstract class
│   │   └── batch.py             # Batch processing engine
│   ├── sinks/
│   │   ├── base.py              # BaseSink abstract class
│   │   └── local.py             # Local filesystem sink
│   ├── schema/
│   │   ├── duckdb_registry.py   # DuckDB-backed schema registry
│   │   ├── detector.py          # Schema drift detection
│   │   ├── evolver.py           # Schema evolution logic
│   │   └── models.py            # Schema-specific models
│   ├── secrets/
│   │   └── base.py              # Pluggable secret providers
│   ├── quality/
│   │   └── __init__.py          # Data quality & PII stubs
│   └── observability/
│       └── logger.py            # Structured logging setup
├── tests/
│   ├── test_models.py           # Core model tests
│   └── test_duckdb_registry.py  # Schema registry tests
├── Dockerfile                   # Multi-stage Docker build
├── pyproject.toml               # Project metadata & dependencies
└── .gitignore
```

---

## Connector Support

### Sources (Implemented / Planned)

| Connector | Status | Extra |
|-----------|--------|-------|
| Local files (CSV, JSON, Parquet) | ✅ Implemented | — |
| REST API | 📋 Planned | `rest` |
| Relational DB (Postgres, MySQL) | 📋 Planned | `relational` |
| MongoDB | 📋 Planned | `nosql` |
| Kafka | 📋 Planned | `kafka` |
| SFTP | 📋 Planned | `sftp` |
| Salesforce | 📋 Planned | `salesforce` |
| Elasticsearch | 📋 Planned | `elasticsearch` |

### Sinks

| Connector | Status | Extra |
|-----------|--------|-------|
| Local filesystem | ✅ Implemented | — |
| S3 / GCS / ADLS | 📋 Planned | — |
| Snowflake | 📋 Planned | `snowflake` |
| BigQuery | 📋 Planned | `bigquery` |
| Delta Lake | 📋 Planned | `delta` |
| Iceberg | 📋 Planned | `iceberg` |

---

## Docker

```bash
# Build
docker build -t ranger .

# Run a pipeline
docker run \
  -v $(pwd)/configs:/app/configs \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/output:/app/output \
  ranger run --config configs/example_pipeline.yaml
```

---

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Lint
ruff check ranger/ tests/

# Type check
mypy ranger/
```

---

## License

Apache-2.0
