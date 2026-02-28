"""Source fingerprint tracking for ETL loaders.

Tracks a hash of raw source data per (table_name, season_id, loader) in the
etl_source_fingerprint table. When a loader is about to run, it computes a hash
of the source data (e.g., an MD5/SHA256 of the raw file bytes or API response)
and passes it to should_run_loader. If the hash matches the stored value the
loader can be skipped; if it differs (or no record exists) the loader must run.

This adds a second layer of idempotency on top of already_loaded():

    1. already_loaded() — coarse guard: has this season ever been loaded?
    2. should_run_loader() — fine guard: has the *source data* changed since
       the last successful load?

Usage pattern:
    source_hash = compute_hash(raw_bytes)  # caller's responsibility
    if not already_loaded(con, table, season, loader):
        load(...)
    elif should_run_loader(con, table, season, loader, source_hash):
        load(...)
        save_loader_fingerprint(con, table, season, loader, source_hash)
"""

import logging
import sqlite3
from datetime import UTC, datetime

logger = logging.getLogger(__name__)


def should_run_loader(
    con: sqlite3.Connection,
    table_name: str,
    season_id: str,
    loader: str,
    source_hash: str,
) -> bool:
    """Return True if the loader should run based on source fingerprint comparison.

    Returns True when:
    - No fingerprint record exists for (table_name, season_id, loader), or
    - The stored hash differs from source_hash.

    Returns False when the stored hash matches source_hash exactly, indicating
    the source data is unchanged since the last successful load.

    If the etl_source_fingerprint table does not exist, returns True so that the
    loader runs and can create its fingerprint on first use.

    Parameters:
        con: SQLite database connection.
        table_name: Target table name (e.g. 'player_game_log').
        season_id: Season identifier (e.g. '2023-24').
        loader: Loader name (e.g. 'game_logs.load_season').
        source_hash: Hash string computed from the current source data.

    Returns:
        bool: True if the loader should run, False if it can be skipped.
    """
    try:
        row = con.execute(
            "SELECT source_hash FROM etl_source_fingerprint"
            " WHERE table_name = ? AND season_id = ? AND loader = ?",
            (table_name, season_id, loader),
        ).fetchone()
    except sqlite3.OperationalError as e:
        if "no such table" in str(e).lower():
            return True
        logger.debug("OperationalError in should_run_loader: %s", e)
        return True

    if row is None:
        return True
    return row[0] != source_hash


def save_loader_fingerprint(
    con: sqlite3.Connection,
    table_name: str,
    season_id: str,
    loader: str,
    source_hash: str,
) -> None:
    """Upsert the source fingerprint for a (table_name, season_id, loader) key.

    Inserts a new record or replaces an existing one with the provided
    source_hash and the current UTC timestamp as updated_at.

    Parameters:
        con: SQLite database connection.
        table_name: Target table name.
        season_id: Season identifier.
        loader: Loader name.
        source_hash: Hash string computed from the current source data.
    """
    now = datetime.now(UTC).isoformat()
    con.execute(
        """
        INSERT INTO etl_source_fingerprint
            (table_name, season_id, loader, source_hash, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (table_name, season_id, loader)
        DO UPDATE SET source_hash = excluded.source_hash,
                      updated_at  = excluded.updated_at
        """,
        (table_name, season_id, loader, source_hash, now),
    )
    con.commit()
