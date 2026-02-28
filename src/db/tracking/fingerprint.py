"""
Source fingerprint tracking for ETL load decisions.

This module provides functions to track content hashes of source data,
enabling intelligent decisions about when to re-run loaders. Instead of
just checking if a loader has run (coarse skip logic), we can detect when
source data has actually changed.

Design Decisions
----------------
- Uses etl_source_fingerprint table to store (table, season, loader) -> hash
- should_run_loader returns True when hash is missing or changed
- Enables re-processing when source data is updated

Usage
-----
    from src.db.tracking.fingerprint import should_run_loader, record_source_fingerprint

    if should_run_loader(con, "player_game_log", "2023-24", "loader", source_hash):
        # Load data
        record_source_fingerprint(con, "player_game_log", "2023-24", "loader", source_hash)
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import UTC, datetime

logger = logging.getLogger(__name__)


def get_source_fingerprint(
    con: sqlite3.Connection,
    table_name: str,
    season_id: str | None,
    loader: str,
) -> str | None:
    """
    Get the stored source hash for a table/season/loader combination.

    Args:
        con: SQLite database connection
        table_name: Target table name
        season_id: Season identifier (or None for non-seasonal data)
        loader: Loader name

    Returns:
        The stored source hash, or None if no fingerprint exists
    """
    row = con.execute(
        "SELECT source_hash FROM etl_source_fingerprint "
        "WHERE table_name = ? AND season_id IS ? AND loader = ?",
        (table_name, season_id, loader),
    ).fetchone()

    return row[0] if row else None


def record_source_fingerprint(
    con: sqlite3.Connection,
    table_name: str,
    season_id: str | None,
    loader: str,
    source_hash: str,
) -> None:
    """
    Record a source fingerprint for a table/season/loader combination.

    Uses INSERT OR REPLACE to update existing fingerprints.

    Args:
        con: SQLite database connection
        table_name: Target table name
        season_id: Season identifier (or None for non-seasonal data)
        loader: Loader name
        source_hash: Hash of the source data
    """
    now = datetime.now(tz=UTC).isoformat()
    con.execute(
        "INSERT OR REPLACE INTO etl_source_fingerprint "
        "(table_name, season_id, loader, source_hash, updated_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (table_name, season_id, loader, source_hash, now),
    )
    con.commit()
    logger.debug(
        "Recorded fingerprint for %s/%s/%s: %s",
        table_name,
        season_id or "null",
        loader,
        source_hash[:8] + "...",
    )


def should_run_loader(
    con: sqlite3.Connection,
    table_name: str,
    season_id: str | None,
    loader: str,
    current_source_hash: str,
) -> bool:
    """
    Determine if a loader should run based on source fingerprint.

    Returns True when:
    - No fingerprint exists (first run)
    - The stored hash differs from current_source_hash (source changed)

    Returns False when:
    - The stored hash matches current_source_hash (no change)

    Args:
        con: SQLite database connection
        table_name: Target table name
        season_id: Season identifier (or None for non-seasonal data)
        loader: Loader name
        current_source_hash: Hash of current source data

    Returns:
        True if the loader should run, False if it can be skipped
    """
    stored_hash = get_source_fingerprint(con, table_name, season_id, loader)

    if stored_hash is None:
        logger.debug(
            "No fingerprint for %s/%s/%s - should run",
            table_name,
            season_id or "null",
            loader,
        )
        return True

    if stored_hash != current_source_hash:
        logger.debug(
            "Hash changed for %s/%s/%s: %s -> %s - should run",
            table_name,
            season_id or "null",
            loader,
            stored_hash[:8] + "...",
            current_source_hash[:8] + "...",
        )
        return True

    logger.debug(
        "Hash unchanged for %s/%s/%s - can skip",
        table_name,
        season_id or "null",
        loader,
    )
    return False
