"""SFTP source connector — download and parse files via paramiko.

Connects to an SSH/SFTP server, lists matching remote files, downloads them
to a temporary directory, parses them (CSV, JSON, or Parquet), and yields
:class:`Record` objects.  Optionally archives or deletes files after reading.
"""

from __future__ import annotations

import fnmatch
import json
import os
import re
import tempfile
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
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
    import paramiko
except ImportError as _err:
    raise ImportError(
        "paramiko is required for SFTPSource. "
        "Install it with: pip install ranger-core[sftp]"
    ) from _err


# ---------------------------------------------------------------------------
# Column-type inference
# ---------------------------------------------------------------------------

def _infer_column_type(value: Any) -> ColumnType:
    if isinstance(value, bool):
        return ColumnType.BOOLEAN
    if isinstance(value, int):
        return ColumnType.INT64
    if isinstance(value, float):
        return ColumnType.FLOAT64
    if isinstance(value, list):
        return ColumnType.ARRAY
    if isinstance(value, dict):
        return ColumnType.JSON
    return ColumnType.STRING


class SFTPSource(BaseSource):
    """Read files from a remote SFTP server.

    Config keys
    -----------
    host : str
        SFTP server hostname.
    port : int
        SSH port (default: ``22``).
    username : str
        SSH username.
    password : str | None
        SSH password (mutually exclusive with *private_key_path*).
    private_key_path : str | None
        Path to an SSH private key file.
    remote_path : str
        Remote file path or directory.
    file_pattern : str
        Glob or regex pattern to filter files (default: ``*``).
    format : str
        File format — ``csv``, ``json``, or ``parquet`` (default: ``csv``).
    archive_after_read : bool
        Move processed files to *archive_path* (default: ``False``).
    archive_path : str | None
        Remote directory to move files to after reading.
    delete_after_read : bool
        Delete remote files after reading (default: ``False``).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._transport: paramiko.Transport | None = None
        self._sftp: paramiko.SFTPClient | None = None

    # ------------------------------------------------------------------
    # Metadata properties
    # ------------------------------------------------------------------

    @property
    def source_type(self) -> str:
        return "sftp"

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
        host = self._config.get("host", "localhost")
        port = self._config.get("port", 22)
        username = self._config.get("username", "")
        password = self._config.get("password")
        key_path = self._config.get("private_key_path")

        try:
            self._transport = paramiko.Transport((host, port))

            if key_path:
                pkey = paramiko.RSAKey.from_private_key_file(key_path)
                self._transport.connect(username=username, pkey=pkey)
            elif password:
                self._transport.connect(username=username, password=password)
            else:
                raise ConnectionError("Either 'password' or 'private_key_path' must be provided")

            self._sftp = paramiko.SFTPClient.from_transport(self._transport)
            self._connected = True
            logger.info("sftp_source.connected", host=host, port=port)
        except Exception as exc:
            self._connected = False
            logger.error("sftp_source.connect_failed", error=str(exc))
            raise ConnectionError(f"Failed to connect to SFTP server {host}:{port}: {exc}") from exc

    def close(self) -> None:
        if self._sftp is not None:
            try:
                self._sftp.close()
            except Exception:
                pass
            self._sftp = None

        if self._transport is not None:
            try:
                self._transport.close()
            except Exception:
                pass
            self._transport = None

        self._connected = False
        logger.info("sftp_source.closed")

    def health_check(self) -> HealthStatus:
        try:
            if self._sftp is None:
                self.connect()
            assert self._sftp is not None
            self._sftp.listdir(".")
            return HealthStatus.HEALTHY
        except Exception as exc:
            logger.warning("sftp_source.health_check_failed", error=str(exc))
            return HealthStatus.UNHEALTHY

    # ------------------------------------------------------------------
    # File listing
    # ------------------------------------------------------------------

    def _list_matching_files(self) -> list[str]:
        """Return remote file paths matching the configured pattern."""
        if self._sftp is None:
            raise RuntimeError("Source not connected — call connect() first")

        remote_path: str = self._config.get("remote_path", ".")
        file_pattern: str = self._config.get("file_pattern", "*")

        # Determine if remote_path is a file or directory
        try:
            attr = self._sftp.stat(remote_path)
            import stat as stat_mod

            if stat_mod.S_ISREG(attr.st_mode or 0):
                return [remote_path]
        except IOError:
            pass

        # Directory listing
        try:
            entries = self._sftp.listdir(remote_path)
        except IOError as exc:
            logger.error("sftp_source.listdir_failed", path=remote_path, error=str(exc))
            return []

        matched: list[str] = []
        for entry in entries:
            # Try glob match first, then regex
            if fnmatch.fnmatch(entry, file_pattern):
                matched.append(f"{remote_path.rstrip('/')}/{entry}")
            else:
                try:
                    if re.match(file_pattern, entry):
                        matched.append(f"{remote_path.rstrip('/')}/{entry}")
                except re.error:
                    pass

        logger.info(
            "sftp_source.files_matched",
            pattern=file_pattern,
            count=len(matched),
        )
        return sorted(matched)

    # ------------------------------------------------------------------
    # File parsing
    # ------------------------------------------------------------------

    def _parse_file(self, local_path: str, file_format: str) -> list[dict[str, Any]]:
        """Parse a local file into a list of dicts."""
        if file_format == "csv":
            return self._parse_csv(local_path)
        if file_format == "json":
            return self._parse_json(local_path)
        if file_format == "parquet":
            return self._parse_parquet(local_path)
        raise ValueError(f"Unsupported file format: {file_format}")

    @staticmethod
    def _parse_csv(path: str) -> list[dict[str, Any]]:
        import csv

        rows: list[dict[str, Any]] = []
        with open(path, newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                rows.append(dict(row))
        return rows

    @staticmethod
    def _parse_json(path: str) -> list[dict[str, Any]]:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return [data]
        return [{"value": data}]

    @staticmethod
    def _parse_parquet(path: str) -> list[dict[str, Any]]:
        try:
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise ImportError(
                "pyarrow is required for Parquet support. "
                "Install it with: pip install pyarrow"
            ) from exc
        table = pq.read_table(path)
        columns = table.column_names
        rows: list[dict[str, Any]] = []
        for i in range(table.num_rows):
            row = {}
            for col in columns:
                val = table.column(col)[i].as_py()
                row[col] = val
            rows.append(row)
        return rows

    # ------------------------------------------------------------------
    # Post-processing (archive / delete)
    # ------------------------------------------------------------------

    def _post_process_file(self, remote_path: str) -> None:
        """Archive or delete a remote file after successful processing."""
        if self._sftp is None:
            return

        if self._config.get("archive_after_read", False):
            archive_path = self._config.get("archive_path")
            if archive_path:
                filename = os.path.basename(remote_path)
                dest = f"{archive_path.rstrip('/')}/{filename}"
                try:
                    # Ensure archive directory exists
                    try:
                        self._sftp.stat(archive_path)
                    except IOError:
                        self._sftp.mkdir(archive_path)
                    self._sftp.rename(remote_path, dest)
                    logger.info("sftp_source.file_archived", src=remote_path, dest=dest)
                except Exception as exc:
                    logger.warning("sftp_source.archive_failed", file=remote_path, error=str(exc))

        if self._config.get("delete_after_read", False):
            try:
                self._sftp.remove(remote_path)
                logger.info("sftp_source.file_deleted", file=remote_path)
            except Exception as exc:
                logger.warning("sftp_source.delete_failed", file=remote_path, error=str(exc))

    # ------------------------------------------------------------------
    # Reading
    # ------------------------------------------------------------------

    def read(self) -> Iterator[Record]:
        """Download matching remote files, parse them, and yield Records."""
        if self._sftp is None:
            raise RuntimeError("Source not connected — call connect() first")

        file_format: str = self._config.get("format", "csv")
        remote_files = self._list_matching_files()
        total_records = 0

        logger.info(
            "sftp_source.read_start",
            file_count=len(remote_files),
            format=file_format,
        )

        for remote_file in remote_files:
            with tempfile.NamedTemporaryFile(
                suffix=f".{file_format}", delete=False
            ) as tmp:
                local_path = tmp.name

            try:
                self._sftp.get(remote_file, local_path)
                logger.info("sftp_source.file_downloaded", file=remote_file)

                rows = self._parse_file(local_path, file_format)
                for row in rows:
                    yield Record(
                        data=row,
                        event_time=datetime.now(timezone.utc),
                        source_metadata={
                            "source_type": "sftp",
                            "remote_file": remote_file,
                        },
                    )
                    total_records += 1

                self._post_process_file(remote_file)
            except Exception as exc:
                logger.error(
                    "sftp_source.file_read_failed",
                    file=remote_file,
                    error=str(exc),
                )
            finally:
                try:
                    os.unlink(local_path)
                except OSError:
                    pass

        logger.info("sftp_source.read_complete", total_records=total_records)

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def get_schema(self) -> Schema:
        """Infer schema by downloading and parsing the first matching file."""
        if self._sftp is None:
            raise RuntimeError("Source not connected — call connect() first")

        file_format = self._config.get("format", "csv")
        remote_files = self._list_matching_files()

        if not remote_files:
            return Schema(columns=[])

        first_file = remote_files[0]
        with tempfile.NamedTemporaryFile(suffix=f".{file_format}", delete=False) as tmp:
            local_path = tmp.name

        try:
            self._sftp.get(first_file, local_path)
            rows = self._parse_file(local_path, file_format)
        finally:
            try:
                os.unlink(local_path)
            except OSError:
                pass

        if not rows:
            return Schema(columns=[])

        all_keys: set[str] = set()
        for row in rows[:100]:
            if isinstance(row, dict):
                all_keys.update(row.keys())

        columns: list[ColumnDefinition] = []
        for key in sorted(all_keys):
            col_type = ColumnType.STRING
            for row in rows[:100]:
                if isinstance(row, dict) and row.get(key) is not None:
                    col_type = _infer_column_type(row[key])
                    break
            columns.append(ColumnDefinition(name=key, type=col_type, nullable=True))

        return Schema(columns=columns)

    def discover_schema(self) -> DiscoveredSchema:
        """Discover schema by inspecting the first remote file."""
        schema = self.get_schema()
        remote_path = self._config.get("remote_path", "")
        return DiscoveredSchema(
            columns=schema.columns,
            primary_key=schema.primary_key,
            source_name="sftp",
            object_name=remote_path,
            object_type="file",
            source_metadata={
                "host": self._config.get("host", ""),
                "port": self._config.get("port", 22),
                "format": self._config.get("format", "csv"),
                "file_pattern": self._config.get("file_pattern", "*"),
            },
        )
