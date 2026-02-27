"""Shared helper infrastructure for CSV-backed seasonal stat loaders."""

import sqlite3
from collections.abc import Callable
from pathlib import Path
from typing import Any

from src.etl.backfill._base import read_csv_safe


def _load_rows(
    con: sqlite3.Connection,
    path: Path,
    transform_row: Callable[[dict[str, Any], set[str]], dict[str, Any] | None],
    valid_seasons: set[str],
) -> tuple[list[dict], int]:
    """Read CSV rows and apply a season-aware row transformer."""
    _ = con  # Signature keeps parity with loader call sites.
    df = read_csv_safe(path, low_memory=False)

    rows: list[dict] = []
    skipped = 0
    for row in df.to_dict("records"):
        transformed = transform_row(row, valid_seasons)
        if transformed is None:
            skipped += 1
        else:
            rows.append(transformed)

    return rows, skipped
