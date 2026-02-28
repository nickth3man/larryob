"""
Deterministic identifier resolution for no-drop ingestion.

This module provides functions to resolve external identifiers (e.g., bref_id)
to internal IDs (player_id, team_id). When a mapping doesn't exist, placeholder
entries are created in the dimension tables to ensure no records are dropped.

Design Decisions
----------------
- Uses dim_player_identifier and dim_team_identifier crosswalk tables
- Creates placeholder entries with synthetic UUID-based IDs when needed
- All placeholder players have is_active=0 (historical/inactive)
- Idempotent: calling twice with same params returns same ID

Usage
-----
    from src.etl.identity.resolver import resolve_or_create_player

    player_id = resolve_or_create_player(con, "bref", "jamesle01", "LeBron James")
"""

from __future__ import annotations

import logging
import sqlite3
import uuid

logger = logging.getLogger(__name__)


def _generate_synthetic_id() -> str:
    """Generate a synthetic ID for placeholder entities."""
    return f"syn_{uuid.uuid4().hex[:12]}"


def resolve_or_create_player(
    con: sqlite3.Connection,
    source_system: str,
    source_id: str,
    full_name: str,
) -> str:
    """
    Resolve an external player identifier to an internal player_id.

    If the (source_system, source_id) pair already exists in dim_player_identifier,
    returns the associated player_id. Otherwise:
    1. Creates a placeholder player in dim_player with a synthetic ID
    2. Creates the identifier mapping in dim_player_identifier
    3. Returns the new player_id

    Args:
        con: SQLite database connection
        source_system: Source system name (e.g., 'bref', 'nba_api')
        source_id: Identifier in the source system
        full_name: Player's full name (used for placeholder creation)

    Returns:
        The resolved or newly created player_id
    """
    # 1) Check if identifier already exists
    row = con.execute(
        "SELECT player_id FROM dim_player_identifier WHERE source_system = ? AND source_id = ?",
        (source_system, source_id),
    ).fetchone()

    if row is not None:
        return row[0]

    # 2) Create placeholder player with synthetic ID
    player_id = _generate_synthetic_id()

    # Parse name into first/last (best effort)
    name_parts = full_name.strip().split(None, 1)
    first_name = name_parts[0] if name_parts else ""
    last_name = name_parts[1] if len(name_parts) > 1 else ""

    con.execute(
        "INSERT INTO dim_player (player_id, first_name, last_name, full_name, is_active) "
        "VALUES (?, ?, ?, ?, 0)",
        (player_id, first_name, last_name, full_name),
    )

    # 3) Create identifier mapping
    con.execute(
        "INSERT INTO dim_player_identifier (source_system, source_id, player_id, match_confidence) "
        "VALUES (?, ?, ?, 1.0)",
        (source_system, source_id, player_id),
    )

    con.commit()
    logger.debug(
        "Created placeholder player %s for %s:%s",
        player_id,
        source_system,
        source_id,
    )

    return player_id


def resolve_or_create_team(
    con: sqlite3.Connection,
    source_system: str,
    source_id: str,
    full_name: str,
) -> str:
    """
    Resolve an external team identifier to an internal team_id.

    If the (source_system, source_id) pair already exists in dim_team_identifier,
    returns the associated team_id. Otherwise:
    1. Creates a placeholder team in dim_team with a synthetic ID
    2. Creates the identifier mapping in dim_team_identifier
    3. Returns the new team_id

    Args:
        con: SQLite database connection
        source_system: Source system name (e.g., 'bref', 'nba_api')
        source_id: Identifier in the source system (e.g., abbreviation)
        full_name: Team's full name (used for placeholder creation)

    Returns:
        The resolved or newly created team_id
    """
    # 1) Check if identifier already exists
    row = con.execute(
        "SELECT team_id FROM dim_team_identifier WHERE source_system = ? AND source_id = ?",
        (source_system, source_id),
    ).fetchone()

    if row is not None:
        return row[0]

    # 2) Create placeholder team with synthetic ID
    team_id = _generate_synthetic_id()

    # Parse name into city/nickname (best effort)
    name_parts = full_name.strip().rsplit(None, 1)
    city = name_parts[0] if name_parts else ""
    nickname = name_parts[1] if len(name_parts) > 1 else full_name

    # Use source_id as abbreviation (it's often the abbreviation)
    abbrev = source_id[:5] if len(source_id) <= 5 else source_id[:3].upper()

    con.execute(
        "INSERT INTO dim_team (team_id, abbreviation, full_name, city, nickname) "
        "VALUES (?, ?, ?, ?, ?)",
        (team_id, abbrev, full_name, city, nickname),
    )

    # 3) Create identifier mapping
    con.execute(
        "INSERT INTO dim_team_identifier (source_system, source_id, team_id) VALUES (?, ?, ?)",
        (source_system, source_id, team_id),
    )

    con.commit()
    logger.debug(
        "Created placeholder team %s for %s:%s",
        team_id,
        source_system,
        source_id,
    )

    return team_id
