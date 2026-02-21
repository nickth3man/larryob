"""
Shared ETL utilities: rate-limit handling, JSON caching, logging.
"""

import json
import logging
import sqlite3
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

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

CACHE_DIR = Path(__file__).parent.parent.parent / ".cache"
CACHE_DIR.mkdir(exist_ok=True)


# --------------------------------------------------------------------------- #
# Rate-limit safe caller with exponential back-off                            #
# --------------------------------------------------------------------------- #

def call_with_backoff(
    fn: Callable[[], Any],
    *,
    base_sleep: float = 3.0,
    max_retries: int = 5,
    label: str = "",
) -> Any:
    """
    Call *fn* (a zero-arg callable) retrying on any exception with
    exponential back-off.  Suitable for nba_api rate-limit errors.
    """
    for attempt in range(1, max_retries + 1):
        try:
            result = fn()
            time.sleep(base_sleep)
            return result
        except Exception as exc:
            wait = base_sleep * (2 ** attempt)
            logger.warning(
                "Attempt %d/%d failed for %r: %s — retrying in %.0fs",
                attempt, max_retries, label, exc, wait,
            )
            if attempt == max_retries:
                raise
            time.sleep(wait)


# --------------------------------------------------------------------------- #
# Simple JSON file cache keyed by an arbitrary string                         #
# --------------------------------------------------------------------------- #

CACHE_VERSION = 2  # bump when ETL output shape changes

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
            # Fallback for old cache format (v1)
            if CACHE_VERSION == 1:
                return data
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


# --------------------------------------------------------------------------- #
# Generic bulk-insert helper for SQLite                                       #
# --------------------------------------------------------------------------- #

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
    columns = list(rows[0].keys())
    placeholders = ", ".join("?" * len(columns))
    col_list = ", ".join(columns)
    sql = f"INSERT OR {conflict} INTO {table} ({col_list}) VALUES ({placeholders})"
    data = [tuple(r[c] for c in columns) for r in rows]
    cur = con.executemany(sql, data)
    if autocommit:
        con.commit()
    return cur.rowcount


# --------------------------------------------------------------------------- #
# ETL Run Log & Auditing Helpers                                              #
# --------------------------------------------------------------------------- #

from datetime import datetime, timezone

def already_loaded(
    con: sqlite3.Connection,
    table: str,
    season_id: str | None,
    loader: str
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
    except sqlite3.OperationalError:
        # Table might not exist yet during first-time bootstrap
        return False

def record_run(
    con: sqlite3.Connection,
    table: str,
    season_id: str | None,
    loader: str,
    row_count: int | None,
    status: str,
    started_at: str | None = None
) -> None:
    """Log an ETL run in the etl_run_log table."""
    try:
        now = datetime.now(timezone.utc).isoformat()
        start = started_at or now
        con.execute(
            """
            INSERT INTO etl_run_log (
                table_name, season_id, loader, started_at, finished_at, row_count, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (table, season_id, loader, start, now, row_count, status)
        )
        con.commit()
    except sqlite3.OperationalError:
        pass


def log_load_summary(
    con: sqlite3.Connection,
    table: str,
    season_id: str | None = None,
    min_rows: int = 0
) -> int:
    """Log actual row count for table (optionally filtered by season_id)."""
    sql = f"SELECT COUNT(*) FROM {table}"
    params = []
    if season_id:
        sql += " WHERE season_id = ?"
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


# --------------------------------------------------------------------------- #
# Transaction Context Manager                                                 #
# --------------------------------------------------------------------------- #

from contextlib import contextmanager

@contextmanager
def transaction(con: sqlite3.Connection):
    """
    Context manager for explicit SQLite transactions.
    Yields the connection. Commits on success, rolls back on exception.
    """
    try:
        # SQLite python module manages transactions automatically, but we can explicitly begin
        con.execute("BEGIN;")
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
