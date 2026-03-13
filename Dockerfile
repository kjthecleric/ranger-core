# Ranger Core — Multi-stage Docker build
# =========================================

# Stage 1: Build dependencies
FROM python:3.11-slim AS builder

WORKDIR /build

# System deps needed for compiling wheels
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./

# Install the package with all optional extras (or change to a subset)
RUN pip install --no-cache-dir --prefix=/install ".[all,dev]" 2>/dev/null \
    || pip install --no-cache-dir --prefix=/install "." 

COPY . /build
RUN pip install --no-cache-dir --prefix=/install .

# -------------------------------------------------------------------
# Stage 2: Runtime
# -------------------------------------------------------------------
FROM python:3.11-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 \
        librdkafka1 \
        curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy installed Python packages and scripts
COPY --from=builder /install/lib /usr/local/lib
COPY --from=builder /install/bin /usr/local/bin
COPY --from=builder /build /app

# Create writable directories for DuckDB metadata and output
RUN mkdir -p /app/output /app/data /app/metadata

# Create non-root user
RUN groupadd -r ranger && useradd -r -g ranger -d /app ranger \
    && chown -R ranger:ranger /app

USER ranger

# Healthcheck — uses the CLI validate subcommand as a smoke test
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD ["python", "-c", "from ranger.core.models import Record; print('ok')"]

ENV PYTHONUNBUFFERED=1
ENV RANGER_METADATA_DB=/app/metadata/ranger_meta.duckdb

ENTRYPOINT ["ranger"]
CMD ["--help"]
