"""Google Sheets source connector via Google Sheets API v4.

Reads spreadsheet data using a service account, treats the first row as
column headers, and yields each subsequent row as a Record.
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

try:
    from google.oauth2 import service_account as google_service_account
    from googleapiclient.discovery import build as google_build
except ImportError as _err:
    raise ImportError(
        "google-api-python-client and google-auth are required for GoogleSheetsSource. "
        "Install them with: pip install ranger-core[google-sheets]"
    ) from _err


# ---------------------------------------------------------------------------
# Value type inference
# ---------------------------------------------------------------------------

def _infer_column_type_from_values(values: list[Any]) -> ColumnType:
    """Infer a :class:`ColumnType` from a sample of cell values.

    Examines non-empty values and picks the most specific type that fits.
    """
    non_empty = [v for v in values if v is not None and v != ""]
    if not non_empty:
        return ColumnType.STRING

    has_bool = False
    has_int = False
    has_float = False

    for v in non_empty:
        if isinstance(v, bool):
            has_bool = True
        elif isinstance(v, int):
            has_int = True
        elif isinstance(v, float):
            has_float = True
        elif isinstance(v, str):
            # Try to detect numeric strings
            try:
                int(v)
                has_int = True
                continue
            except (ValueError, TypeError):
                pass
            try:
                float(v)
                has_float = True
                continue
            except (ValueError, TypeError):
                pass
            # If any value is a non-numeric string, treat column as STRING
            return ColumnType.STRING

    if has_bool and not has_int and not has_float:
        return ColumnType.BOOLEAN
    if has_float:
        return ColumnType.FLOAT64
    if has_int:
        return ColumnType.INT64
    return ColumnType.STRING


_SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


class GoogleSheetsSource(BaseSource):
    """Read data from a Google Sheets spreadsheet.

    Config keys
    -----------
    spreadsheet_id : str
        The Google Sheets spreadsheet ID (from the URL).
    sheet_name : str
        Sheet name or A1 range (e.g. ``"Sheet1"`` or ``"Sheet1!A1:Z"``).
        Defaults to the first sheet.
    credentials_path : str
        Path to the service account JSON key file.
    header_row : int
        1-based row index of the header row (default ``1``).
    value_render_option : str
        How values should be rendered — ``"FORMATTED_VALUE"`` (default)
        or ``"UNFORMATTED_VALUE"``.
    """

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._service: Any = None

        self._spreadsheet_id: str = config["spreadsheet_id"]
        self._sheet_name: str = config.get("sheet_name", "Sheet1")
        self._credentials_path: str = config["credentials_path"]
        self._header_row: int = config.get("header_row", 1)
        self._value_render_option: str = config.get(
            "value_render_option", "FORMATTED_VALUE"
        )

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def source_type(self) -> str:
        return "google_sheets"

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
        """Authenticate with Google using service account credentials."""
        logger.info(
            "google_sheets.connecting",
            spreadsheet_id=self._spreadsheet_id,
            sheet_name=self._sheet_name,
        )
        credentials = google_service_account.Credentials.from_service_account_file(
            self._credentials_path,
            scopes=_SHEETS_SCOPES,
        )
        self._service = google_build("sheets", "v4", credentials=credentials)
        self._connected = True
        logger.info("google_sheets.connected", spreadsheet_id=self._spreadsheet_id)

    def close(self) -> None:
        """Release the Google Sheets service."""
        if self._service is not None:
            self._service.close()
        self._service = None
        self._connected = False
        logger.debug("google_sheets.closed")

    def health_check(self) -> HealthStatus:
        """Verify access by reading spreadsheet metadata."""
        try:
            if self._service is None:
                self.connect()
            assert self._service is not None
            self._service.spreadsheets().get(
                spreadsheetId=self._spreadsheet_id,
            ).execute()
            return HealthStatus.HEALTHY
        except Exception:
            logger.warning("google_sheets.health_check_failed", exc_info=True)
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # Reading
    # ------------------------------------------------------------------

    def _fetch_values(self) -> list[list[Any]]:
        """Fetch all values from the configured sheet/range."""
        assert self._service is not None
        result = (
            self._service.spreadsheets()
            .values()
            .get(
                spreadsheetId=self._spreadsheet_id,
                range=self._sheet_name,
                valueRenderOption=self._value_render_option,
            )
            .execute()
        )
        return result.get("values", [])

    def read(self) -> Iterator[Record]:
        """Read rows from Google Sheets, using the header row as column names.

        Each row after the header becomes a :class:`Record` whose ``data``
        maps column headers to cell values.
        """
        if self._service is None:
            self.connect()

        all_values = self._fetch_values()
        if not all_values:
            logger.warning("google_sheets.empty_sheet", spreadsheet_id=self._spreadsheet_id)
            return

        # Extract headers (1-based → 0-based index)
        header_idx = self._header_row - 1
        if header_idx >= len(all_values):
            logger.warning(
                "google_sheets.header_row_out_of_range",
                header_row=self._header_row,
                total_rows=len(all_values),
            )
            return

        headers = [str(h).strip() for h in all_values[header_idx]]
        data_rows = all_values[header_idx + 1 :]

        logger.info(
            "google_sheets.read_start",
            spreadsheet_id=self._spreadsheet_id,
            sheet_name=self._sheet_name,
            columns=len(headers),
            rows=len(data_rows),
        )

        records_yielded = 0
        for row_idx, row in enumerate(data_rows):
            # Pad short rows with None
            padded = row + [None] * (len(headers) - len(row))
            data = {headers[i]: padded[i] for i in range(len(headers))}

            yield Record(
                data=data,
                event_time=datetime.now(timezone.utc),
                source_metadata={
                    "source": "google_sheets",
                    "spreadsheet_id": self._spreadsheet_id,
                    "sheet_name": self._sheet_name,
                    "row_index": header_idx + 2 + row_idx,  # 1-based row in sheet
                },
            )
            records_yielded += 1

        logger.info(
            "google_sheets.read_complete",
            spreadsheet_id=self._spreadsheet_id,
            records=records_yielded,
        )

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def get_schema(self) -> Schema:
        """Infer schema from the header row and a sample of data rows."""
        if self._service is None:
            self.connect()

        all_values = self._fetch_values()
        if not all_values:
            return Schema(columns=[])

        header_idx = self._header_row - 1
        if header_idx >= len(all_values):
            return Schema(columns=[])

        headers = [str(h).strip() for h in all_values[header_idx]]
        data_rows = all_values[header_idx + 1 :]

        # Sample up to 100 rows for type inference
        sample_rows = data_rows[:100]

        columns: list[ColumnDefinition] = []
        for col_idx, header in enumerate(headers):
            sample_values = [
                row[col_idx] if col_idx < len(row) else None
                for row in sample_rows
            ]
            col_type = _infer_column_type_from_values(sample_values)
            columns.append(
                ColumnDefinition(
                    name=header,
                    type=col_type,
                    nullable=True,
                )
            )

        return Schema(columns=columns)

    def discover_schema(self) -> DiscoveredSchema:
        """Discover schema with spreadsheet metadata."""
        base = self.get_schema()

        # Get row count from a fresh fetch
        row_count: int | None = None
        try:
            all_values = self._fetch_values()
            header_idx = self._header_row - 1
            if all_values and header_idx < len(all_values):
                row_count = len(all_values) - header_idx - 1
        except Exception:
            pass

        return DiscoveredSchema(
            columns=base.columns,
            primary_key=base.primary_key,
            partition_columns=None,
            source_name=self.source_type,
            object_name=f"{self._spreadsheet_id}/{self._sheet_name}",
            object_type="spreadsheet",
            row_count_estimate=row_count,
            source_metadata={
                "spreadsheet_id": self._spreadsheet_id,
                "sheet_name": self._sheet_name,
                "header_row": self._header_row,
                "value_render_option": self._value_render_option,
            },
        )
