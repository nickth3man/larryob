"""
Shared base utilities for backfill loaders.

This module provides common patterns, types, and utilities to reduce
code duplication across all backfill loader modules.
"""

import logging
import sqlite3
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import pandas as pd

from src.etl.helpers import _isna
from src.etl.utils import log_load_summary, upsert_rows
from src.etl.validate import validate_rows

logger = logging.getLogger(__name__)

# Default raw data directory
RAW_DIR = Path("raw")

# Type alias for row dictionaries
RowDict = dict[str, Any]


class BackfillError(Exception):
    """Base exception for backfill operations."""

    pass


class FileNotFoundError(BackfillError):
    """Raised when a required CSV file is not found."""

    def __init__(self, path: Path):
        self.path = path
        super().__init__(f"Required file not found: {path}")


class DataValidationError(BackfillError):
    """Raised when data validation fails."""

    def __init__(self, message: str, row_count: int | None = None):
        self.row_count = row_count
        super().__init__(message)


def get_valid_set(
    con: sqlite3.Connection,
    table: str,
    column: str,
) -> set[str]:
    """
    Fetch a set of valid values from a dimension table.

    Args:
        con: SQLite connection
        table: Table name to query
        column: Column name to fetch

    Returns:
        Set of string values from the specified column
    """
    return {r[0] for r in con.execute(f"SELECT {column} FROM {table}")}


def safe_int(value: Any) -> int | None:
    """Safely convert a value to int, returning None for NaN/null."""
    if _isna(value):
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def safe_float(value: Any) -> float | None:
    """Safely convert a value to float, returning None for NaN/null."""
    if _isna(value):
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def safe_str(value: Any, strip: bool = True) -> str | None:
    """Safely convert a value to string, returning None for NaN/null."""
    if _isna(value):
        return None
    s = str(value)
    return s.strip() if strip else s


def csv_path(
    raw_dir: Path,
    filename: str,
    required: bool = False,
) -> Path | None:
    """
    Resolve a CSV path and optionally check existence.

    Args:
        raw_dir: Base directory for raw files
        filename: CSV filename
        required: If True, raises FileNotFoundError when missing

    Returns:
        Path object, or None if not required and doesn't exist
    """
    path = raw_dir / filename
    if not path.exists():
        if required:
            raise FileNotFoundError(path)
        logger.warning("%s not found, skipping", filename)
        return None
    return path


def read_csv_safe(
    path: Path,
    low_memory: bool = False,
    chunksize: int | None = None,
    usecols: list[str] | None = None,
) -> Any:
    """
    Safely read a CSV file with consistent error handling.

    Args:
        path: Path to CSV file
        low_memory: Pandas low_memory flag
        chunksize: If set, returns TextFileReader for iteration
        usecols: Columns to read (None = all)

    Returns:
        DataFrame or TextFileReader if chunksize is set
    """
    return pd.read_csv(
        path,
        low_memory=low_memory,
        chunksize=chunksize,
        usecols=usecols,
    )


class BaseBackfillLoader(ABC):
    """
    Abstract base class for backfill loaders.

    Provides a consistent interface and common functionality for all
    backfill operations. Subclasses implement the transform_row method
    and optionally override process_batch for custom batching logic.
    """

    # Subclasses should define these
    table_name: str = ""
    csv_filename: str = ""
    requires_validation: bool = True

    def __init__(self, raw_dir: Path = RAW_DIR):
        self.raw_dir = raw_dir
        self.skipped = 0
        self.processed = 0

    @abstractmethod
    def transform_row(self, row: dict[str, Any], context: dict[str, Any]) -> RowDict | None:
        """
        Transform a CSV row into a database row dict.

        Args:
            row: Raw row from CSV
            context: Shared context (e.g., valid season IDs, lookups)

        Returns:
            Transformed row dict, or None to skip
        """
        ...

    def get_context(self, con: sqlite3.Connection) -> dict[str, Any]:
        """
        Build shared context for row transformations.

        Override to provide dimension lookups, valid ID sets, etc.
        """
        return {}

    def process_batch(
        self,
        con: sqlite3.Connection,
        rows: list[RowDict],
        context: dict[str, Any],
    ) -> int:
        """
        Process a batch of transformed rows.

        Default implementation validates and upserts. Override for
        custom logic (e.g., UPDATE instead of INSERT).
        """
        if not rows:
            return 0

        if self.requires_validation:
            rows = validate_rows(self.table_name, rows)

        return upsert_rows(con, self.table_name, rows)

    def load(self, con: sqlite3.Connection) -> int:
        """
        Execute the full load operation.

        Returns:
            Number of rows inserted/updated
        """
        path = csv_path(self.raw_dir, self.csv_filename)
        if path is None:
            return 0

        df = read_csv_safe(path, low_memory=False)
        context = self.get_context(con)

        rows: list[RowDict] = []
        for raw_row in df.to_dict("records"):
            transformed = self.transform_row(raw_row, context)
            if transformed is None:
                self.skipped += 1
            else:
                rows.append(transformed)
                self.processed += 1

        inserted = self.process_batch(con, rows, context)

        logger.info(
            "%s: %d inserted/ignored, %d skipped",
            self.table_name,
            inserted,
            self.skipped,
        )

        try:
            log_load_summary(con, self.table_name)
        except Exception:
            pass  # Table may not support load summary

        return inserted


class ChunkedBackfillLoader(BaseBackfillLoader):
    """
    Base class for loaders that need to process large files in chunks.

    Use when reading CSVs that don't fit in memory or when you want
    to commit in batches.
    """

    chunk_size: int = 50_000

    def load(self, con: sqlite3.Connection) -> int:
        """Execute the load with chunked processing."""
        path = csv_path(self.raw_dir, self.csv_filename)
        if path is None:
            return 0

        context = self.get_context(con)
        total_inserted = 0

        reader = read_csv_safe(
            path,
            low_memory=False,
            chunksize=self.chunk_size,
        )

        for chunk in reader:
            rows: list[RowDict] = []
            for raw_row in chunk.to_dict("records"):
                transformed = self.transform_row(raw_row, context)
                if transformed is None:
                    self.skipped += 1
                else:
                    rows.append(transformed)
                    self.processed += 1

            inserted = self.process_batch(con, rows, context)
            total_inserted += inserted
            logger.debug("%s chunk: +%d rows", self.table_name, inserted)

        con.commit()

        logger.info(
            "%s: %d inserted/ignored, %d skipped",
            self.table_name,
            total_inserted,
            self.skipped,
        )

        try:
            log_load_summary(con, self.table_name)
        except Exception:
            pass

        return total_inserted
