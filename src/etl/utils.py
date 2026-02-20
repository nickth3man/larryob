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

def cache_path(key: str) -> Path:
    return CACHE_DIR / f"{key}.json"


def load_cache(key: str) -> Any | None:
    p = cache_path(key)
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    return None


def save_cache(key: str, data: Any) -> None:
    cache_path(key).write_text(json.dumps(data), encoding="utf-8")


# --------------------------------------------------------------------------- #
# Generic bulk-insert helper for SQLite                                       #
# --------------------------------------------------------------------------- #

def upsert_rows(
    con: sqlite3.Connection,
    table: str,
    rows: list[dict],
    conflict: str = "IGNORE",
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
    con.commit()
    return cur.rowcount
