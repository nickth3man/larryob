"""
Checkpoint logging: row-count snapshots and etl_run_log tailing between pipeline stages.

These functions are called after each stage completes to record progress and
surface any anomalies before the next stage begins.

Design Decisions
----------------
- Uses Sequence[str] for tables parameter to accept both list and tuple
- Validates table names before querying to prevent SQL injection
- Gracefully handles missing tables (returns None for counts)

Usage
-----
    state = CheckpointState()
    log_checkpoint(con, Stage.DIMENSIONS, DIMENSIONS_TABLES, state, runlog_tail=12)
"""

from __future__ import annotations

import logging
import sqlite3
import time
from collections.abc import Sequence

from src.pipeline.constants import _VALID_IDENTIFIER
from src.pipeline.models import CheckpointState, Stage

logger = logging.getLogger(__name__)


def _safe_table_count(con: sqlite3.Connection, table_name: str) -> int | None:
    """Safely get row count for a table.

    Args:
        con: SQLite connection.
        table_name: Table name to count.

    Returns:
        Row count or None if table doesn't exist or name is invalid.
    """
    from src.etl.utils import _validate_identifier as _validate_sql_identifier

    if not _VALID_IDENTIFIER.fullmatch(table_name):
        return None
    try:
        _validate_sql_identifier(table_name)
        result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        return int(result[0]) if result else 0
    except sqlite3.OperationalError:
        return None
    except ValueError:
        return None


def _get_runlog_status_map(con: sqlite3.Connection) -> dict[str, int]:
    """Get status counts from etl_run_log.

    Args:
        con: SQLite connection.

    Returns:
        Dictionary mapping status to count.
    """
    try:
        rows = con.execute("SELECT status, COUNT(*) FROM etl_run_log GROUP BY status").fetchall()
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc).lower():
            return {}
        raise
    return {status: int(count) for status, count in rows}


def _compute_delta(previous: dict[str, int], current: dict[str, int]) -> dict[str, int]:
    """Compute the delta between two status maps.

    Args:
        previous: Previous status map.
        current: Current status map.

    Returns:
        Dictionary of changed keys with their delta values.
    """
    if previous is current:
        return {}

    all_keys = set(previous) | set(current)
    return {
        key: current.get(key, 0) - previous.get(key, 0)
        for key in sorted(all_keys)
        if current.get(key, 0) != previous.get(key, 0)
    }


def _log_runlog_tail(con: sqlite3.Connection, checkpoint: str, limit: int) -> None:
    """Log the most recent entries from etl_run_log.

    Args:
        con: SQLite connection.
        checkpoint: Checkpoint name for logging context.
        limit: Maximum number of rows to log.
    """
    try:
        rows = con.execute(
            """
            SELECT
                id, table_name, COALESCE(season_id, '-'), loader, status,
                COALESCE(row_count, -1), started_at, COALESCE(finished_at, '-')
            FROM etl_run_log
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc).lower():
            logger.debug(
                "Skipping etl_run_log tail at checkpoint=%s: etl_run_log missing", checkpoint
            )
            return
        raise

    logger.info("Checkpoint %s: etl_run_log tail (latest %d rows)", checkpoint, limit)
    for row in rows:
        row_count_display = row[5] if row[5] >= 0 else None
        logger.info(
            "Checkpoint %s: runlog id=%s table=%s season=%s loader=%s status=%s "
            "row_count=%s started=%s finished=%s",
            checkpoint,
            row[0],
            row[1],
            row[2],
            row[3],
            row[4],
            row_count_display,
            row[6],
            row[7],
        )


def log_checkpoint(
    con: sqlite3.Connection,
    stage: Stage,
    tables: Sequence[str],
    state: CheckpointState,
    runlog_tail: int,
) -> None:
    """Log checkpoint status including runlog counts and table row counts.

    Args:
        con: SQLite connection.
        stage: Pipeline stage identifier.
        tables: Tables to include in row count (accepts list or tuple).
        state: Mutable checkpoint state to update.
        runlog_tail: Number of runlog rows to display.
    """
    status_map = _get_runlog_status_map(con)
    status_delta = _compute_delta(state.status_map, status_map)
    now = time.perf_counter()
    elapsed = (now - state.last_timestamp) if state.last_timestamp is not None else None

    logger.info(
        "Checkpoint %s: etl_run_log status counts=%s delta=%s elapsed_since_previous=%s",
        stage.value,
        status_map,
        status_delta or {},
        f"{elapsed:.2f}s" if elapsed is not None else "n/a",
    )

    new_table_counts: dict[str, int | None] = dict(state.table_counts)
    for table in tables:
        row_count = _safe_table_count(con, table)
        previous_count = state.table_counts.get(table)
        delta = (
            row_count - previous_count
            if row_count is not None and previous_count is not None
            else None
        )
        logger.info(
            "Checkpoint %s: table=%s row_count=%s delta=%s previous=%s",
            stage.value,
            table,
            row_count if row_count is not None else "n/a",
            delta if delta is not None else "n/a",
            previous_count if previous_count is not None else "n/a",
        )
        new_table_counts[table] = row_count

    _log_runlog_tail(con, stage.value, limit=runlog_tail)
    state.update(status_map, new_table_counts, now)
