"""File source — reads CSV, JSON, NDJSON, Parquet, Avro, XML, Excel, ORC files.

Supports glob patterns, compression auto-detection (.gz, .bz2, .zst, .zip),
and multi-file ingestion.
"""

from __future__ import annotations

import bz2
import csv
import gzip
import io
import json
import logging
import zipfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from ranger.core.models import ColumnDefinition, ColumnType, DiscoveredSchema, Record, Schema
from ranger.sources.base import BaseSource

logger = logging.getLogger(__name__)

# Extensions that indicate compression (order matters for detection)
_COMPRESSION_EXTENSIONS: set[str] = {".gz", ".bz2", ".zst", ".zip"}

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
        compression: Compression type (auto, gz, bz2, zst, zip, none).
        csv_delimiter: Delimiter for CSV files (default: ',').
        csv_header: Whether CSV has a header row (default: true).
        xml_record_tag: XPath or tag name for record elements in XML.
        excel_sheet: Sheet name or index for Excel files.
        excel_header_row: 1-based row number containing headers (default: 1).
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
            elif fmt in ("ndjson", "jsonl"):
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
        all_keys: set[str] = set()
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
        """Detect file format from extension, stripping compression suffixes."""
        if self._format != "auto":
            return self._format

        suffix = path.suffix.lower()

        # Strip all known compression extensions to find the real format
        stem = path
        while stem.suffix.lower() in _COMPRESSION_EXTENSIONS:
            stem = Path(stem.stem)

        data_suffix = stem.suffix.lower()

        format_map: dict[str, str] = {
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

        # If the outer extension is a compression extension, use the inner one
        if suffix in _COMPRESSION_EXTENSIONS:
            return format_map.get(data_suffix, "json")

        return format_map.get(suffix, "json")

    # ------------------------------------------------------------------
    # Compression
    # ------------------------------------------------------------------

    def _detect_compression(self, path: Path) -> str:
        """Detect compression type from file extension."""
        if self._compression != "auto":
            return self._compression

        suffix = path.suffix.lower()
        compression_map: dict[str, str] = {
            ".gz": "gz",
            ".bz2": "bz2",
            ".zst": "zst",
            ".zip": "zip",
        }
        return compression_map.get(suffix, "none")

    def _open_file(self, path: Path, mode: str = "rb") -> io.IOBase:
        """Open a file, auto-detecting and handling compression.

        Returns a file-like object that yields decompressed bytes.
        """
        compression = self._detect_compression(path)

        if compression == "gz":
            return gzip.open(path, mode)

        if compression == "bz2":
            return bz2.open(path, mode)

        if compression == "zst":
            try:
                import zstandard as zstd
            except ImportError as exc:
                raise ImportError(
                    "Install zstandard for .zst support: pip install ranger-core[zstd]"
                ) from exc
            dctx = zstd.ZstdDecompressor()
            raw = open(path, "rb")
            return dctx.stream_reader(raw)  # type: ignore[return-value]

        if compression == "zip":
            zf = zipfile.ZipFile(path, "r")
            names = zf.namelist()
            if not names:
                raise ValueError(f"ZIP archive is empty: {path}")
            # Read the first file in the archive
            logger.debug("ZIP archive %s: reading entry '%s'", path, names[0])
            return zf.open(names[0])  # type: ignore[return-value]

        # No compression
        return open(path, mode)  # noqa: SIM115

    # ------------------------------------------------------------------
    # Format readers
    # ------------------------------------------------------------------

    def _read_csv(self, path: Path) -> Iterator[Record]:
        with self._open_file(path, "rb") as f:
            text_stream = io.TextIOWrapper(f, encoding="utf-8")
            reader = csv.DictReader(
                text_stream,
                delimiter=self._config.get("csv_delimiter", ","),
            )
            for row in reader:
                yield Record(data=dict(row), source_metadata={"file": str(path)})

    def _read_json(self, path: Path) -> Iterator[Record]:
        with self._open_file(path, "rb") as f:
            content = f.read()
        data = json.loads(content)
        if isinstance(data, list):
            for item in data:
                yield Record(data=item, source_metadata={"file": str(path)})
        elif isinstance(data, dict):
            yield Record(data=data, source_metadata={"file": str(path)})

    def _read_ndjson(self, path: Path) -> Iterator[Record]:
        with self._open_file(path, "rb") as f:
            for raw_line in f:
                line = raw_line.strip() if isinstance(raw_line, bytes) else raw_line.strip().encode()
                if line:
                    data = json.loads(line)
                    yield Record(data=data, source_metadata={"file": str(path)})

    def _read_parquet(self, path: Path) -> Iterator[Record]:
        try:
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise ImportError(
                "Install pyarrow for Parquet support: pip install ranger-core[parquet]"
            ) from exc

        table = pq.read_table(str(path))
        for batch in table.to_batches():
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

        # Read through compression layer if needed
        with self._open_file(path, "rb") as f:
            tree = etree.parse(f)

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
        header_row = self._config.get("excel_header_row", 1)

        wb = openpyxl.load_workbook(str(path), read_only=True, data_only=True)

        if isinstance(sheet_name, int):
            ws = wb.worksheets[sheet_name]
        else:
            ws = wb[sheet_name]

        rows = ws.iter_rows(values_only=True)

        # Skip rows before the header row
        for _ in range(header_row - 1):
            next(rows, None)

        header_values = next(rows, None)
        if header_values is None:
            wb.close()
            return

        headers = [str(h) if h else f"col_{i}" for i, h in enumerate(header_values)]

        for row in rows:
            data = dict(zip(headers, row))
            yield Record(
                data=data,
                source_metadata={"file": str(path), "sheet": str(sheet_name)},
            )

        wb.close()

    def _read_orc(self, path: Path) -> Iterator[Record]:
        try:
            import pyarrow.orc as orc
        except ImportError as exc:
            raise ImportError(
                "Install pyarrow for ORC support: pip install ranger-core[orc]"
            ) from exc

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
    def _xml_element_to_dict(elem: Any) -> dict[str, Any]:
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
