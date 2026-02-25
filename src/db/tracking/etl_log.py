"""ETL run tracking and load summary logging."""

import logging
import re
import sqlite3
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger(__name__)


def _validate_identifier(name: str) -> None:
    """
    Validate that the given name is a safe SQL identifier containing only ASCII letters, digits, or underscores.
    
    Raises:
        ValueError: If name contains any characters other than A–Z, a–z, 0–9, or underscore.
    """
    if not re.fullmatch(r"^[a-zA-Z0-9_]+$", name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")


def already_loaded(
    con: sqlite3.Connection,
    table: str,
    season_id: str | None,
    loader: str,
) -> bool:
    """
    Determine whether a prior ETL run for the specified table, season, and loader completed with status 'ok'.
    
    If the etl_run_log table does not exist or an OperationalError occurs, the function returns False.
    
    Parameters:
        con (sqlite3.Connection): Database connection.
        table (str): Target table name as recorded in etl_run_log.table_name.
        season_id (str | None): Season identifier, or None to match a NULL season_id in the log.
        loader (str): Loader name recorded in etl_run_log.loader.
    
    Returns:
        bool: True if an 'ok' run exists for the specified table/season/loader, False otherwise.
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
    Record an ETL run entry in the etl_run_log table.
    
    Inserts a row with table_name, season_id, loader, started_at (uses provided value or current UTC ISO timestamp), finished_at (current UTC ISO timestamp), row_count, and status, then commits the transaction. If the etl_run_log table does not exist the function returns silently; other sqlite3.OperationalError cases are logged at debug level.
    
    Parameters:
        table (str): Target table name.
        season_id (str | None): Season identifier or None.
        loader (str): Name of the loader that performed the run.
        row_count (int | None): Number of rows loaded, or None if unknown.
        status (str): Load status (for example 'ok' or 'failed').
        started_at (str | None): ISO timestamp to record as the run start; if None, the current UTC ISO timestamp is used.
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
    Log the number of rows in a table and warn if the count is below a threshold.
    
    Counts rows in the specified table, optionally filtering by the provided season_id when the table contains a season_id column or by joining fact_game when only a game_id column is present. Logs a warning if the resulting count is less than min_rows; otherwise logs an info message.
    
    Parameters:
        con (sqlite3.Connection): Database connection.
        table (str): Target table name (must be a valid SQL identifier).
        season_id (str | None): Optional season identifier to filter rows.
        min_rows (int): Minimum expected rows; a warning is emitted if the count is less than this.
    
    Returns:
        int: Number of rows counted.
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
