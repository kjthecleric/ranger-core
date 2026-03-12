"""File source — reads CSV, JSON, NDJSON, Parquet, Avro, XML, Excel, ORC files.

Supports glob patterns, compression auto-detection, and multi-file ingestion.
"""

from __future__ import annotations

import csv
import gzip
import io
import json
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from ranger.core.models import ColumnDefinition, ColumnType, DiscoveredSchema, Record, Schema
from ranger.sources.base import BaseSource

logger = logging.getLogger(__name__)

# Mapping from Python/inferred types to Ranger column types
_TYPE_MAP: dict[str, ColumnType] = {
    "int": ColumnType.INT64,
    "float": ColumnType.FLOAT64,
    "str": ColumnType.STRING,
    "bool": ColumnType.BOOLEAN,
    "NoneType": ColumnType.STRING,
}


class FileSource(BaseSource):
    """Read data from local files.

    Config keys:
        path: File path or glob pattern (e.g., 'data/*.csv').
        format: File format — csv, json, ndjson, parquet, avro, xml, excel, orc.
                Auto-detected from extension if omitted.
        compression: Compression type (auto, gz, bz2, zst, none).
        csv_delimiter: Delimiter for CSV files (default: ',').
        csv_header: Whether CSV has a header row (default: true).
        xml_record_tag: XPath or tag name for record elements in XML.
        excel_sheet: Sheet name or index for Excel files.
    """

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._path_pattern = config.get("path", "")
        self._format = config.get("format", "auto")
        self._compression = config.get("compression", "auto")
        self._files: list[Path] = []

    @property
    def source_type(self) -> str:
        return "file"

    @property
    def supports_incremental(self) -> bool:
        return False

    def connect(self) -> None:
        pattern = self._path_pattern
        base = Path(pattern)

        if "*" in pattern or "?" in pattern:
            # Glob pattern
            parent = base.parent
            glob_pattern = base.name
            self._files = sorted(parent.glob(glob_pattern))
        else:
            if not base.exists():
                raise FileNotFoundError(f"File not found: {pattern}")
            self._files = [base]

        if not self._files:
            raise FileNotFoundError(f"No files matched pattern: {pattern}")

        self._connected = True
        logger.info("FileSource connected: %d files matched", len(self._files))

    def close(self) -> None:
        self._connected = False
        self._files = []

    def read(self) -> Iterator[Record]:
        for file_path in self._files:
            fmt = self._detect_format(file_path)
            logger.debug("Reading %s (format=%s)", file_path, fmt)

            if fmt == "csv":
                yield from self._read_csv(file_path)
            elif fmt == "json":
                yield from self._read_json(file_path)
            elif fmt == "ndjson" or fmt == "jsonl":
                yield from self._read_ndjson(file_path)
            elif fmt == "parquet":
                yield from self._read_parquet(file_path)
            elif fmt == "avro":
                yield from self._read_avro(file_path)
            elif fmt == "xml":
                yield from self._read_xml(file_path)
            elif fmt in ("excel", "xlsx", "xls"):
                yield from self._read_excel(file_path)
            elif fmt == "orc":
                yield from self._read_orc(file_path)
            else:
                raise ValueError(f"Unsupported file format: {fmt}")

    def get_schema(self) -> Schema:
        """Infer schema by reading a sample of records."""
        sample: list[dict[str, Any]] = []
        for record in self.read():
            sample.append(record.data)
            if len(sample) >= 100:
                break

        if not sample:
            return Schema(columns=[])

        # Infer types from sample
        columns: list[ColumnDefinition] = []
        all_keys = set()
        for row in sample:
            all_keys.update(row.keys())

        for key in sorted(all_keys):
            col_type = self._infer_column_type(key, sample)
            columns.append(ColumnDefinition(name=key, type=col_type, nullable=True))

        return Schema(columns=columns)

    def discover_schema(self) -> DiscoveredSchema:
        schema = self.get_schema()
        return DiscoveredSchema(
            columns=schema.columns,
            source_name="file",
            object_name=self._path_pattern,
            object_type="file",
            row_count_estimate=None,
        )

    # ------------------------------------------------------------------
    # Format detection
    # ------------------------------------------------------------------

    def _detect_format(self, path: Path) -> str:
        if self._format != "auto":
            return self._format

        suffix = path.suffix.lower()
        if suffix == ".gz":
            # Look at the stem extension
            suffix = Path(path.stem).suffix.lower()

        format_map = {
            ".csv": "csv",
            ".json": "json",
            ".ndjson": "ndjson",
            ".jsonl": "ndjson",
            ".parquet": "parquet",
            ".avro": "avro",
            ".xml": "xml",
            ".xlsx": "excel",
            ".xls": "excel",
            ".orc": "orc",
        }
        return format_map.get(suffix, "json")

    # ------------------------------------------------------------------
    # Format readers
    # ------------------------------------------------------------------

    def _read_csv(self, path: Path) -> Iterator[Record]:
        opener = self._get_opener(path)
        with opener(path) as f:
            reader = csv.DictReader(
                io.TextIOWrapper(f) if isinstance(f, (gzip.GzipFile, io.BytesIO)) else f,
                delimiter=self._config.get("csv_delimiter", ","),
            )
            for row in reader:
                yield Record(data=dict(row), source_metadata={"file": str(path)})

    def _read_json(self, path: Path) -> Iterator[Record]:
        with open(path, "rb") as f:
            content = f.read()
        data = json.loads(content)
        if isinstance(data, list):
            for item in data:
                yield Record(data=item, source_metadata={"file": str(path)})
        elif isinstance(data, dict):
            yield Record(data=data, source_metadata={"file": str(path)})

    def _read_ndjson(self, path: Path) -> Iterator[Record]:
        import orjson

        with open(path, "rb") as f:
            for line in f:
                line = line.strip()
                if line:
                    data = orjson.loads(line)
                    yield Record(data=data, source_metadata={"file": str(path)})

    def _read_parquet(self, path: Path) -> Iterator[Record]:
        import pyarrow.parquet as pq

        table = pq.read_table(str(path))
        for batch in table.to_batches():
            for row in batch.to_pydict().values():
                pass
            # Convert columnar to row-based
            columns = batch.to_pydict()
            n_rows = batch.num_rows
            for i in range(n_rows):
                data = {col: columns[col][i] for col in columns}
                yield Record(data=data, source_metadata={"file": str(path)})

    def _read_avro(self, path: Path) -> Iterator[Record]:
        try:
            import fastavro
        except ImportError as exc:
            raise ImportError("Install fastavro: pip install ranger-core[avro]") from exc

        with open(path, "rb") as f:
            reader = fastavro.reader(f)
            for record in reader:
                yield Record(data=record, source_metadata={"file": str(path)})

    def _read_xml(self, path: Path) -> Iterator[Record]:
        try:
            from lxml import etree
        except ImportError as exc:
            raise ImportError("Install lxml: pip install ranger-core[xml]") from exc

        record_tag = self._config.get("xml_record_tag", None)
        tree = etree.parse(str(path))

        if record_tag:
            elements = tree.findall(f".//{record_tag}")
        else:
            # Use direct children of root
            elements = list(tree.getroot())

        for elem in elements:
            data = self._xml_element_to_dict(elem)
            yield Record(data=data, source_metadata={"file": str(path)})

    def _read_excel(self, path: Path) -> Iterator[Record]:
        try:
            import openpyxl
        except ImportError as exc:
            raise ImportError("Install openpyxl: pip install ranger-core[excel]") from exc

        sheet_name = self._config.get("excel_sheet", 0)
        wb = openpyxl.load_workbook(str(path), read_only=True, data_only=True)

        if isinstance(sheet_name, int):
            ws = wb.worksheets[sheet_name]
        else:
            ws = wb[sheet_name]

        rows = ws.iter_rows(values_only=True)
        headers = [str(h) if h else f"col_{i}" for i, h in enumerate(next(rows))]

        for row in rows:
            data = dict(zip(headers, row))
            yield Record(data=data, source_metadata={"file": str(path), "sheet": str(sheet_name)})

        wb.close()

    def _read_orc(self, path: Path) -> Iterator[Record]:
        import pyarrow.orc as orc

        table = orc.read_table(str(path))
        columns = table.to_pydict()
        n_rows = table.num_rows
        for i in range(n_rows):
            data = {col: columns[col][i] for col in columns}
            yield Record(data=data, source_metadata={"file": str(path)})

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get_opener(path: Path):
        """Return the appropriate file opener based on extension."""
        if path.suffix.lower() == ".gz":
            return lambda p: gzip.open(p, "rb")
        return lambda p: open(p, "r")

    @staticmethod
    def _infer_column_type(key: str, sample: list[dict[str, Any]]) -> ColumnType:
        """Infer column type from sample values."""
        types_seen: set[str] = set()
        for row in sample:
            val = row.get(key)
            if val is not None:
                types_seen.add(type(val).__name__)

        if not types_seen or types_seen == {"NoneType"}:
            return ColumnType.STRING
        types_seen.discard("NoneType")

        if types_seen == {"int"}:
            return ColumnType.INT64
        if types_seen == {"float"} or types_seen == {"int", "float"}:
            return ColumnType.FLOAT64
        if types_seen == {"bool"}:
            return ColumnType.BOOLEAN
        return ColumnType.STRING

    @staticmethod
    def _xml_element_to_dict(elem) -> dict[str, Any]:
        """Convert an XML element and its children to a dict."""
        result: dict[str, Any] = {}
        # Attributes
        result.update(elem.attrib)
        # Child elements
        for child in elem:
            tag = child.tag
            if len(child):
                result[tag] = FileSource._xml_element_to_dict(child)
            else:
                result[tag] = child.text
        # Text content
        if elem.text and elem.text.strip():
            if not result:
                return {"_text": elem.text.strip()}
            result["_text"] = elem.text.strip()
        return result
