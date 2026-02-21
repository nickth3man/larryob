"""
Shared ETL utilities: caching, upsert helpers, logging, and ETL run tracking.
"""

import json
import logging
import re
import sqlite3
import sys
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .config import CacheConfig

logger = logging.getLogger(__name__)

LOG_FORMAT = "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s"
LOG_DATE_FORMAT = "%H:%M:%S"


def setup_logging(
    level: str = "INFO",
    log_file: Path | None = None,
) -> None:
    """
    Configure the root logger once.
    Call early (e.g. in main()) before any other logging.
    """
    root = logging.getLogger()
    root.setLevel(level.upper())
    for h in root.handlers[:]:
        root.removeHandler(h)
    fmt = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    root.addHandler(sh)
    if log_file:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        root.addHandler(fh)


CACHE_DIR = CacheConfig.cache_dir()
CACHE_DIR.mkdir(exist_ok=True)

CACHE_VERSION = CacheConfig.CACHE_VERSION


def cache_path(key: str) -> Path:
    return CACHE_DIR / f"{key}.json"


def load_cache(key: str, ttl_days: float | None = None) -> Any | None:
    p = cache_path(key)
    if p.exists():
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(data, dict) and "v" in data and "ts" in data and "data" in data:
                if data["v"] != CACHE_VERSION:
                    return None
                if ttl_days is not None:
                    age_seconds = time.time() - data["ts"]
                    if age_seconds > ttl_days * 86400:
                        return None
                return data["data"]
            return None
        except json.JSONDecodeError:
            return None
    return None


def save_cache(key: str, data: Any) -> None:
    payload = {
        "v": CACHE_VERSION,
        "ts": time.time(),
        "data": data,
    }
    cache_path(key).write_text(json.dumps(payload), encoding="utf-8")


_VALID_CONFLICT = frozenset({"IGNORE", "REPLACE", "ABORT", "ROLLBACK", "FAIL"})


def _validate_identifier(name: str) -> None:
    if not re.fullmatch(r"^[a-zA-Z0-9_]+$", name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")


def upsert_rows(
    con: sqlite3.Connection,
    table: str,
    rows: list[dict],
    conflict: str = "IGNORE",
    autocommit: bool = True,
) -> int:
    """
    INSERT OR <conflict> a list of dicts into *table*.
    Returns the number of rows inserted.
    """
    if not rows:
        return 0
    if conflict and conflict.upper() not in _VALID_CONFLICT:
        raise ValueError(f"Invalid conflict clause: {conflict!r}")
    _validate_identifier(table)
    for c in rows[0].keys():
        _validate_identifier(c)

    columns = list(rows[0].keys())
    placeholders = ", ".join("?" * len(columns))
    col_list = ", ".join(columns)
    or_clause = f" OR {conflict}" if conflict else ""
    sql = f"INSERT{or_clause} INTO {table} ({col_list}) VALUES ({placeholders})"
    data = [tuple(r[c] for c in columns) for r in rows]

    try:
        cur = con.executemany(sql, data)
        if autocommit:
            con.commit()
        return cur.rowcount
    except sqlite3.OperationalError as e:
        if "no such table" in str(e).lower():
            logger.warning("Skipping upsert into missing table '%s': %s", table, e)
            return 0
        logger.error("OperationalError in upsert_rows for table '%s': %s", table, e)
        raise


def already_loaded(
    con: sqlite3.Connection,
    table: str,
    season_id: str | None,
    loader: str,
) -> bool:
    """Check if an ETL run completed successfully for this table/season/loader combination."""
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
    """Log an ETL run in the etl_run_log table."""
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
    """Log actual row count for table (optionally filtered by season_id)."""
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


@contextmanager
def transaction(con: sqlite3.Connection):
    """
    Context manager for explicit SQLite transactions.
    Yields the connection. Commits on success, rolls back on exception.
    """
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
