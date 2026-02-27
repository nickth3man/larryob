"""
Backfill loader for play-by-play data from bulk CSV files.

Strategy
--------
* Scan ``raw/pbp/`` for ``*.csv`` files — one or many, any naming convention.
* Normalise each file via :func:`~src.etl.play_by_play._transform_pbp`, which
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

from src.db.tracking import already_loaded, log_load_summary, record_run
from src.etl.backfill._base import RAW_DIR
from src.etl.backfill._pbp_bulk_helpers import (
    _TABLE as _PBP_TABLE,
)
from src.etl.backfill._pbp_bulk_helpers import (
    _insert_rows,
    _list_csv_files,
    _pbp_dir,
    _read_csv_file,
    _transform_and_filter,
)

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------ #
# Constants                                                           #
# ------------------------------------------------------------------ #

_LOADER_ID = "backfill.pbp_bulk"


# ------------------------------------------------------------------ #
# Internal helpers                                                    #
# ------------------------------------------------------------------ #


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
    :func:`~src.etl.play_by_play._transform_pbp` (handles uppercase NBA-API
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
            _PBP_TABLE,
        )

    logger.info(
        "%s (bulk): %d total row(s) inserted/ignored across %d file(s).",
        _PBP_TABLE,
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
    if already_loaded(con, _PBP_TABLE, season, _LOADER_ID):
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
        _PBP_TABLE,
        season,
        total,
    )

    status = "ok" if total > 0 else "empty"
    record_run(con, _PBP_TABLE, season, _LOADER_ID, total, status, started_at)

    try:
        log_load_summary(con, _PBP_TABLE, season_id=season)
    except (sqlite3.DatabaseError, ValueError):
        pass  # Non-fatal: summary logging is informational only

    return total


if __name__ == "__main__":  # pragma: no cover
    from src.db.schema import init_db

    logging.basicConfig(level=logging.INFO)
    _con = init_db()
    load_bulk_pbp(_con)
    _con.close()
