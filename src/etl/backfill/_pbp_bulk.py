"""
Backfill loader for play-by-play data from bulk CSV files.

Strategy
--------
* Scan ``raw/pbp/`` for ``*.csv`` files — one or many, any naming convention.
* Normalise each file via :func:`~src.etl.transform.play_by_play._transform_pbp`, which
  handles both uppercase NBA-API column names (``GAME_ID``, ``EVENTNUM``, …)
  and already-lowercased variants.
* Insert into ``fact_play_by_play`` with ``INSERT OR IGNORE`` on ``event_id``
  (the primary key), so the loader is safe to re-run.
* Gracefully return 0 if ``raw/pbp/`` is missing or empty.

Public API
----------
* :func:`load_bulk_pbp`         — loads all CSV files under ``raw/pbp/``.
* :func:`load_bulk_pbp_season`  — loads only rows whose ``game_id`` belongs to
  the requested season (resolved via ``fact_game``); uses
  ``already_loaded`` / ``record_run`` for idempotency.
"""

import logging
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from src.db.operations import upsert_rows
from src.db.tracking import already_loaded, log_load_summary, record_run
from src.etl.backfill._base import RAW_DIR, read_csv_safe
from src.etl.transform.play_by_play import _transform_pbp
from src.etl.validation import validate_rows

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------ #
# Constants                                                           #
# ------------------------------------------------------------------ #

_TABLE = "fact_play_by_play"
_LOADER_ID = "backfill.pbp_bulk"
_PBP_SUBDIR = "pbp"


# ------------------------------------------------------------------ #
# Internal helpers                                                    #
# ------------------------------------------------------------------ #


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
        df = read_csv_safe(path, low_memory=False)
        return df
    except Exception as exc:  # noqa: BLE001
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


def _fetch_season_game_ids(con: sqlite3.Connection, season: str) -> set[str]:
    """
    Return the set of ``game_id`` values in ``fact_game`` for *season*.

    Parameters
    ----------
    con:
        Database connection.
    season:
        Season identifier (e.g. ``"2024-25"``).
    """
    cursor = con.execute(
        "SELECT game_id FROM fact_game WHERE season_id = ?",
        (season,),
    )
    return {row[0] for row in cursor.fetchall()}


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


# ------------------------------------------------------------------ #
# Public API                                                          #
# ------------------------------------------------------------------ #


def load_bulk_pbp(
    con: sqlite3.Connection,
    raw_dir: Path = RAW_DIR,
) -> int:
    """
    Load all play-by-play CSV files from ``raw/pbp/`` into ``fact_play_by_play``.

    Scans every ``*.csv`` file inside ``<raw_dir>/pbp/``, normalises rows via
    :func:`~src.etl.transform.play_by_play._transform_pbp` (handles uppercase NBA-API
    column names), and inserts with ``INSERT OR IGNORE`` so re-runs are safe.

    Gracefully returns ``0`` when ``raw/pbp/`` is missing or contains no CSV
    files.

    Parameters
    ----------
    con:
        SQLite database connection.
    raw_dir:
        Root of the raw-data directory tree (default: ``raw/``).

    Returns
    -------
    int
        Total rows inserted; rows that were already present count as ``0`` due
        to the ``IGNORE`` conflict clause.
    """
    pbp_dir = _pbp_dir(raw_dir)
    if pbp_dir is None:
        return 0

    csv_files = _list_csv_files(pbp_dir)
    if not csv_files:
        return 0

    total = 0
    for csv_file in csv_files:
        df = _read_csv_file(csv_file)
        if df is None or df.empty:
            logger.warning("Skipping empty or unreadable file: %s", csv_file.name)
            continue

        rows = _transform_and_filter(df)
        if not rows:
            logger.info("%s: no valid rows after transform.", csv_file.name)
            continue

        n = _insert_rows(con, rows)
        total += n
        logger.info(
            "%s: %d row(s) inserted/ignored into %s.",
            csv_file.name,
            n,
            _TABLE,
        )

    logger.info(
        "%s (bulk): %d total row(s) inserted/ignored across %d file(s).",
        _TABLE,
        total,
        len(csv_files),
    )
    return total


def load_bulk_pbp_season(
    con: sqlite3.Connection,
    season: str,
    raw_dir: Path = RAW_DIR,
) -> int:
    """
    Load bulk play-by-play data for a single season from ``raw/pbp/``.

    Resolves all ``game_id`` values for *season* from ``fact_game``, then
    filters CSV rows to those games before inserting.  Idempotency is enforced
    via :func:`~src.db.tracking.already_loaded` and
    :func:`~src.db.tracking.record_run`.

    Parameters
    ----------
    con:
        SQLite database connection.
    season:
        Season identifier, e.g. ``"2024-25"``.
    raw_dir:
        Root of the raw-data directory tree (default: ``raw/``).

    Returns
    -------
    int
        Rows inserted; ``0`` if the season was already loaded, the PBP
        directory is missing, no CSV files exist, or no matching rows were
        found.
    """
    if already_loaded(con, _TABLE, season, _LOADER_ID):
        logger.info("Skipping bulk PBP load for %s (already loaded).", season)
        return 0

    pbp_dir = _pbp_dir(raw_dir)
    if pbp_dir is None:
        return 0

    csv_files = _list_csv_files(pbp_dir)
    if not csv_files:
        return 0

    season_game_ids = _fetch_season_game_ids(con, season)
    if not season_game_ids:
        logger.warning(
            "No games found in fact_game for season %s — cannot filter PBP rows.",
            season,
        )
        return 0

    logger.info(
        "Loading bulk PBP for season %s: %d game(s), %d CSV file(s).",
        season,
        len(season_game_ids),
        len(csv_files),
    )

    started_at = datetime.now(UTC).isoformat()
    total = 0

    for csv_file in csv_files:
        df = _read_csv_file(csv_file)
        if df is None or df.empty:
            logger.warning("Skipping empty or unreadable file: %s", csv_file.name)
            continue

        rows = _transform_and_filter(df, game_id_filter=season_game_ids)
        if not rows:
            logger.debug("%s: no rows match season %s.", csv_file.name, season)
            continue

        n = _insert_rows(con, rows)
        total += n
        logger.info(
            "%s: %d row(s) inserted/ignored for season %s.",
            csv_file.name,
            n,
            season,
        )

    logger.info(
        "%s (bulk, %s): %d total row(s) inserted/ignored.",
        _TABLE,
        season,
        total,
    )

    status = "ok" if total > 0 else "empty"
    record_run(con, _TABLE, season, _LOADER_ID, total, status, started_at)

    try:
        log_load_summary(con, _TABLE, season_id=season)
    except Exception:  # noqa: BLE001
        pass  # Non-fatal: summary logging is informational only

    return total


if __name__ == "__main__":  # pragma: no cover
    from src.db.schema import init_db

    logging.basicConfig(level=logging.INFO)
    _con = init_db()
    load_bulk_pbp(_con)
    _con.close()
