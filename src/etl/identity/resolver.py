"""
Deterministic player/team identifier resolution.

When a backfill loader encounters a source ID (e.g. a Basketball-Reference
bref_id) that cannot be matched to an existing dim_player row, this module
creates a placeholder dim_player entry and records the mapping in
dim_player_identifier so that the historical record is preserved rather than
silently dropped.
"""

import sqlite3


def resolve_or_create_player(
    con: sqlite3.Connection,
    source_system: str,
    source_id: str,
    full_name: str,
) -> str:
    """
    Return the canonical player_id for a given external identifier.

    Resolution order:
      1. Check dim_player_identifier for an existing mapping — return it if found.
      2. If no mapping exists, create a placeholder dim_player row with a
         deterministic synthetic player_id of the form
         ``placeholder_{source_system}_{source_id}``.
      3. Insert a row in dim_player_identifier linking source_system+source_id
         to the new placeholder player_id with match_confidence=0.0.
      4. Return the placeholder player_id.

    This function is idempotent: calling it twice with the same
    source_system+source_id returns the same player_id without creating
    duplicate rows.

    Args:
        con: SQLite database connection.
        source_system: Identifier namespace (e.g. ``"bref"``).
        source_id: The external ID within that namespace (e.g. ``"ackerdo01"``).
        full_name: Human-readable full name used to populate the placeholder row.

    Returns:
        Canonical player_id string.
    """
    existing = con.execute(
        "SELECT player_id FROM dim_player_identifier WHERE source_system = ? AND source_id = ?",
        (source_system, source_id),
    ).fetchone()
    if existing is not None:
        return existing[0]

    placeholder_id = f"placeholder_{source_system}_{source_id}"

    # Split full_name into first/last best-effort (last word = last name).
    parts = full_name.strip().rsplit(" ", 1)
    if len(parts) == 2:
        first_name, last_name = parts
    else:
        first_name = ""
        last_name = full_name.strip()

    con.execute(
        """
        INSERT OR IGNORE INTO dim_player
            (player_id, first_name, last_name, full_name, is_active)
        VALUES (?, ?, ?, ?, 0)
        """,
        (placeholder_id, first_name, last_name, full_name),
    )
    con.execute(
        """
        INSERT OR IGNORE INTO dim_player_identifier
            (source_system, source_id, player_id, match_confidence)
        VALUES (?, ?, ?, 0.0)
        """,
        (source_system, source_id, placeholder_id),
    )
    con.commit()

    return placeholder_id
