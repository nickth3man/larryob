"""Helper functions shared by bulk play-by-play backfill loaders."""

import logging
import sqlite3
from pathlib import Path

import pandas as pd

from src.db.operations import upsert_rows
from src.etl.backfill._base import read_csv_safe
from src.etl.play_by_play import _transform_pbp
from src.etl.validation import validate_rows

logger = logging.getLogger(__name__)

_TABLE = "fact_play_by_play"
_PBP_SUBDIR = "pbp"


def _pbp_dir(raw_dir: Path) -> Path | None:
    """
    Return the ``raw/pbp/`` subdirectory if it exists, else ``None``.

    Parameters
    ----------
    raw_dir:
        Root of the raw-data directory tree.
    """
    path = raw_dir / _PBP_SUBDIR
    if not path.is_dir():
        logger.debug("PBP subdirectory not found at %s — skipping bulk load.", path)
        return None
    return path


def _list_csv_files(pbp_dir: Path) -> list[Path]:
    """
    Return a sorted list of ``*.csv`` files inside *pbp_dir*.

    Parameters
    ----------
    pbp_dir:
        Directory to scan.
    """
    files = sorted(pbp_dir.glob("*.csv"))
    if not files:
        logger.info("No CSV files found in %s.", pbp_dir)
    return files


def _read_csv_file(path: Path) -> pd.DataFrame | None:
    """
    Read a single CSV file; return ``None`` and log a warning on error.

    Parameters
    ----------
    path:
        Path to the CSV file to read.
    """
    try:
        return read_csv_safe(path, low_memory=False)
    except (pd.errors.EmptyDataError, pd.errors.ParserError, OSError) as exc:
        logger.warning("Failed to read %s: %s", path.name, exc)
        return None


def _transform_and_filter(
    df: pd.DataFrame,
    game_id_filter: set[str] | None = None,
) -> list[dict]:
    """
    Apply ``_transform_pbp`` and optional game-ID filtering, then validate.

    Parameters
    ----------
    df:
        Raw DataFrame from a bulk CSV file (may have uppercase column names).
    game_id_filter:
        When supplied, only rows whose ``game_id`` is in this set are kept.
        Pass ``None`` to include every row.

    Returns
    -------
    list[dict]
        Validated rows ready for insertion.
    """
    if df.empty:
        return []

    rows = _transform_pbp(df)

    if game_id_filter is not None:
        rows = [r for r in rows if r.get("game_id") in game_id_filter]

    # validate_rows passes through unregistered tables; still called for
    # consistency and forward compatibility with future PBP schema models.
    return validate_rows(_TABLE, rows)


def _insert_rows(con: sqlite3.Connection, rows: list[dict]) -> int:
    """
    Upsert *rows* into ``fact_play_by_play`` with ``INSERT OR IGNORE``.

    Parameters
    ----------
    con:
        Database connection.
    rows:
        Transformed, validated row dicts.

    Returns
    -------
    int
        Number of rows inserted (already-present rows are silently ignored).
    """
    if not rows:
        return 0
    return upsert_rows(con, _TABLE, rows, conflict="IGNORE")
