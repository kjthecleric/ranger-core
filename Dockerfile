# Ranger Core — Multi-stage Docker build
# Stage 1: Build
FROM python:3.11-slim AS builder

WORKDIR /app

COPY pyproject.toml ./
RUN pip install --no-cache-dir build && \
    pip install --no-cache-dir ".[all]"

COPY . .

# Stage 2: Runtime
FROM python:3.11-slim AS runtime

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin/ranger /usr/local/bin/ranger
COPY --from=builder /app .

# Create non-root user
RUN groupadd -r ranger && useradd -r -g ranger ranger
USER ranger

ENTRYPOINT ["ranger"]
CMD ["--help"]
