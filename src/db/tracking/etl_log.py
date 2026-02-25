"""ETL run tracking and load summary logging."""

import logging
import re
import sqlite3
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger(__name__)


def _validate_identifier(name: str) -> None:
    """Validate SQL identifier to prevent injection."""
    if not re.fullmatch(r"^[a-zA-Z0-9_]+$", name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")


def already_loaded(
    con: sqlite3.Connection,
    table: str,
    season_id: str | None,
    loader: str,
) -> bool:
    """
    Check if an ETL run completed successfully for this table/season/loader combination.

    Parameters
    ----------
    con : sqlite3.Connection
        Database connection
    table : str
        Target table name
    season_id : str | None
        Season identifier
    loader : str
        Loader name (e.g., 'nba_api', 'basketball_reference')

    Returns
    -------
    bool
        True if data already loaded, False otherwise
    """
    try:
        sql = "SELECT 1 FROM etl_run_log WHERE table_name = ? AND loader = ? AND status = 'ok'"
        params: list[Any] = [table, loader]
        if season_id:
            sql += " AND season_id = ?"
            params.append(season_id)
        else:
            sql += " AND season_id IS NULL"

        cur = con.execute(sql, params)
        return cur.fetchone() is not None
    except sqlite3.OperationalError as e:
        if "no such table" in str(e).lower():
            return False
        logger.debug("OperationalError in already_loaded: %s", e)
        return False


def record_run(
    con: sqlite3.Connection,
    table: str,
    season_id: str | None,
    loader: str,
    row_count: int | None,
    status: str,
    started_at: str | None = None,
) -> None:
    """
    Log an ETL run in the etl_run_log table.

    Parameters
    ----------
    con : sqlite3.Connection
        Database connection
    table : str
        Target table name
    season_id : str | None
        Season identifier
    loader : str
        Loader name
    row_count : int | None
        Number of rows loaded
    status : str
        Load status ('ok', 'failed', etc.)
    started_at : str | None
        ISO timestamp of when the run started
    """
    try:
        now = datetime.now(UTC).isoformat()
        start = started_at or now
        con.execute(
            """
            INSERT INTO etl_run_log (
                table_name, season_id, loader, started_at, finished_at, row_count, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (table, season_id, loader, start, now, row_count, status),
        )
        con.commit()
    except sqlite3.OperationalError as e:
        if "no such table" in str(e).lower():
            return
        logger.debug("OperationalError in record_run: %s", e)


def log_load_summary(
    con: sqlite3.Connection,
    table: str,
    season_id: str | None = None,
    min_rows: int = 0,
) -> int:
    """
    Log actual row count for table (optionally filtered by season_id).

    Parameters
    ----------
    con : sqlite3.Connection
        Database connection
    table : str
        Target table name
    season_id : str | None
        Season identifier to filter by
    min_rows : int
        Minimum expected rows (will warn if below this threshold)

    Returns
    -------
    int
        Number of rows loaded
    """
    _validate_identifier(table)
    sql = f"SELECT COUNT(*) FROM {table}"
    params: list[Any] = []
    if season_id:
        cols = {row[1] for row in con.execute(f"PRAGMA table_info({table})").fetchall()}
        if "season_id" in cols:
            sql += " WHERE season_id = ?"
            params.append(season_id)
        elif "game_id" in cols:
            sql = (
                f"SELECT COUNT(*) FROM {table} t"
                f" JOIN fact_game g ON g.game_id = t.game_id"
                f" WHERE g.season_id = ?"
            )
            params.append(season_id)

    count = con.execute(sql, params).fetchone()[0]

    msg = f"Table {table}"
    if season_id:
        msg += f" (season {season_id})"
    msg += f" loaded {count:,} rows"

    if count < min_rows:
        logger.warning("%s (Expected minimum %d rows!)", msg, min_rows)
    else:
        logger.info(msg)

    return count
