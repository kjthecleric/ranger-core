"""Local filesystem sink — writes records to local files.

Supports JSON, NDJSON, CSV, and Parquet output formats.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import orjson

from ranger.core.models import Record, Schema
from ranger.sinks.base import BaseSink

logger = logging.getLogger(__name__)


class LocalSink(BaseSink):
    """Write records to the local filesystem.

    Config keys:
        path: Output directory or file path.
        format: Output format — json, ndjson, csv, parquet.
        partition_by: Optional list of columns to partition by (creates subdirectories).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._path = Path(config.get("path", "./output"))
        self._format = config.get("format", "json")
        self._partition_by = config.get("partition_by", [])
        self._schema: Schema | None = None
        self._records: list[dict[str, Any]] = []

    @property
    def sink_type(self) -> str:
        return "local"

    def open(self, schema: Schema) -> None:
        self._schema = schema
        self._path.mkdir(parents=True, exist_ok=True)
        self._opened = True
        logger.info("Local sink opened at %s (format=%s)", self._path, self._format)

    def write_batch(self, records: list[Record]) -> int:
        if not self._opened:
            raise RuntimeError("Sink not opened. Call open() first.")

        written = 0
        for record in records:
            self._records.append(record.data)
            written += 1

        return written

    def evolve_schema(self, new_schema: Schema) -> None:
        self._schema = new_schema
        logger.info("Local sink schema evolved (no-op for file sinks)")

    def close(self) -> None:
        if not self._records:
            return

        if self._format == "json":
            self._write_json()
        elif self._format == "ndjson":
            self._write_ndjson()
        elif self._format == "csv":
            self._write_csv()
        elif self._format == "parquet":
            self._write_parquet()
        else:
            raise ValueError(f"Unsupported output format: {self._format}")

        logger.info("Local sink closed — wrote %d records to %s", len(self._records), self._path)
        self._records = []
        self._opened = False

    def _write_json(self) -> None:
        output_file = self._path / "data.json"
        with open(output_file, "wb") as f:
            f.write(orjson.dumps(self._records, option=orjson.OPT_INDENT_2))

    def _write_ndjson(self) -> None:
        output_file = self._path / "data.ndjson"
        with open(output_file, "wb") as f:
            for record in self._records:
                f.write(orjson.dumps(record))
                f.write(b"\n")

    def _write_csv(self) -> None:
        import csv
        import io

        if not self._records:
            return

        output_file = self._path / "data.csv"
        fieldnames = list(self._records[0].keys())

        with open(output_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self._records)

    def _write_parquet(self) -> None:
        import pyarrow as pa
        import pyarrow.parquet as pq

        if not self._records:
            return

        output_file = self._path / "data.parquet"
        table = pa.Table.from_pylist(self._records)
        pq.write_table(table, str(output_file))
