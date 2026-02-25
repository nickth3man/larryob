"""Database upsert operations and transaction management."""

import logging
import re
import sqlite3
from collections.abc import Iterable
from contextlib import contextmanager
from itertools import islice

logger = logging.getLogger(__name__)

_VALID_CONFLICT = frozenset({"IGNORE", "REPLACE", "ABORT", "ROLLBACK", "FAIL"})


def _validate_identifier(name: str) -> None:
    """Validate SQL identifier to prevent injection."""
    if not re.fullmatch(r"^[a-zA-Z0-9_]+$", name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")


def _chunked(iterable: Iterable, n: int):
    """Yield successive n-sized chunks from iterable."""
    it = iter(iterable)
    while batch := list(islice(it, n)):
        yield batch


def upsert_rows(
    con: sqlite3.Connection,
    table: str,
    rows: list[dict],
    conflict: str = "IGNORE",
    autocommit: bool = True,
) -> int:
    """
    INSERT OR <conflict> a list of dicts into *table* with batching.
    Returns the number of rows inserted.

    Parameters
    ----------
    con : sqlite3.Connection
        Database connection
    table : str
        Target table name
    rows : list[dict]
        Rows to upsert
    conflict : str
        SQLite conflict clause (IGNORE, REPLACE, ABORT, ROLLBACK, FAIL)
    autocommit : bool
        Whether to commit after insertion

    Returns
    -------
    int
        Number of rows inserted
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

    # SQLite maximum host parameters safeguard (safe default: 999 max vars per batch)
    chunk_size = max(1, 900 // len(columns))

    total_inserted = 0
    try:
        for chunk in _chunked(rows, chunk_size):
            data = [tuple(r[c] for c in columns) for r in chunk]
            cur = con.executemany(sql, data)
            total_inserted += cur.rowcount

        if autocommit:
            con.commit()
        return total_inserted
    except sqlite3.OperationalError as e:
        if "no such table" in str(e).lower():
            logger.warning("Skipping upsert into missing table '%s': %s", table, e)
            return 0
        logger.error("OperationalError in upsert_rows for table '%s': %s", table, e)
        raise


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
