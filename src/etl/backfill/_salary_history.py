"""
Backfill loader for historical salary data from open-source CSV files.

Loads salary data from a CSV into fact_salary, normalising season IDs,
matching players via dim_player, and resolving teams via dim_team.

Expected CSV columns (flexible — column aliases are handled):

    player_name | player       — player name string
    season_id   | season       — YYYY-YY or integer end-year (e.g. 2024 → 2023-24)
    team_abbrev | team         — NBA or bref team abbreviation
    salary                    — integer USD or formatted string like "$12,345,678"

Rows are inserted idempotently by (player_id, team_id, season_id) using
INSERT OR REPLACE so that re-running is safe.
"""

import logging
import re
import sqlite3
from pathlib import Path
from typing import Any

from src.db.operations import upsert_rows
from src.etl._salaries_fetch import _parse_salary
from src.etl.backfill._base import read_csv_safe, safe_str
from src.etl.helpers import int_season_to_id
from src.etl.salaries import _normalize_name

logger = logging.getLogger(__name__)

# Default filename looked up inside raw_dir when open_file is None.
_DEFAULT_FILENAME = "open_salaries.csv"

# Valid YYYY-YY pattern (e.g. "2023-24").
_SEASON_RE = re.compile(r"^\d{4}-\d{2}$")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _col(row: dict[str, Any], *candidates: str) -> Any:
    """Return the first candidate key found in *row*, or ``None``."""
    for key in candidates:
        if key in row:
            return row[key]
    return None


def _parse_season_id(raw: Any) -> str | None:
    """
    Normalise a raw season value to YYYY-YY format.

    Accepts:
        - "2023-24"  (already valid)  → returned unchanged
        - 2024 / "2024" / 2024.0     (integer end-year) → "2023-24"

    Returns:
        Normalised season_id string, or ``None`` if the value cannot be parsed.
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if _SEASON_RE.match(s):
        return s
    # Treat as a numeric end-year (e.g. Basketball-Reference convention).
    try:
        end_year = int(float(s))
        if 1946 <= end_year <= 2100:
            return int_season_to_id(end_year)
    except (ValueError, TypeError):
        pass
    return None


def _build_player_lookup(con: sqlite3.Connection) -> dict[str, str]:
    """
    Return a dict mapping normalised player name → player_id.

    Uses the same normalisation as the live salary scraper so matching
    behaviour is consistent across sources.
    """
    rows = con.execute("SELECT player_id, full_name FROM dim_player").fetchall()
    return {_normalize_name(full_name): player_id for player_id, full_name in rows}


def _build_team_lookup(con: sqlite3.Connection) -> dict[str, str]:
    """
    Return a dict mapping any known team abbreviation (uppercase) → team_id.

    Covers both the NBA ``abbreviation`` column and the Basketball-Reference
    ``bref_abbrev`` column so open-source files using either convention resolve
    correctly.  In the event of a collision the NBA abbreviation wins (last
    write wins for bref if it duplicates an NBA key).
    """
    lookup: dict[str, str] = {}
    rows = con.execute("SELECT team_id, abbreviation, bref_abbrev FROM dim_team").fetchall()
    for team_id, nba_abbr, bref_abbr in rows:
        team_id_str = str(team_id).strip()
        # Register bref first so NBA key can overwrite on collision.
        if bref_abbr:
            lookup[str(bref_abbr).strip().upper()] = team_id_str
        if nba_abbr:
            lookup[str(nba_abbr).strip().upper()] = team_id_str
    return lookup


# ---------------------------------------------------------------------------
# Public loader
# ---------------------------------------------------------------------------


def load_salary_history(
    con: sqlite3.Connection,
    open_file: Path | None = None,
    raw_dir: Path = Path("raw"),
) -> int:
    """Load salaries from open-source CSV into fact_salary. Returns rows inserted.

    Parameters
    ----------
    con:
        Open SQLite connection.
    open_file:
        Explicit path to the CSV.  When ``None`` the loader looks for
        ``raw_dir / "open_salaries.csv"``.
    raw_dir:
        Base directory searched for the default CSV filename.

    Returns
    -------
    int
        Number of rows inserted (or replaced) into ``fact_salary``.
        Returns 0 if the file is missing or no valid rows are found.
    """
    # ------------------------------------------------------------------ #
    # 1. Resolve and validate file path                                   #
    # ------------------------------------------------------------------ #
    path: Path = open_file if open_file is not None else raw_dir / _DEFAULT_FILENAME
    if not path.exists():
        logger.warning("load_salary_history: file not found — skipping: %s", path)
        return 0

    df = read_csv_safe(path, low_memory=False)
    logger.info(
        "load_salary_history: %d rows read from %s (columns: %s)",
        len(df),
        path,
        sorted(df.columns),
    )

    # ------------------------------------------------------------------ #
    # 2. Build dimension lookups                                          #
    # ------------------------------------------------------------------ #
    player_lookup = _build_player_lookup(con)
    team_lookup = _build_team_lookup(con)

    # ------------------------------------------------------------------ #
    # 3. Transform rows                                                   #
    # ------------------------------------------------------------------ #
    rows_to_insert: list[dict] = []
    skipped_season: int = 0
    skipped_player: int = 0
    skipped_team: int = 0
    skipped_salary: int = 0

    for raw_row in df.to_dict("records"):
        # -- Season --------------------------------------------------------
        raw_season = _col(raw_row, "season_id", "season")
        season_id = _parse_season_id(raw_season)
        if season_id is None:
            logger.debug("load_salary_history: invalid season %r — row skipped", raw_season)
            skipped_season += 1
            continue

        # -- Player --------------------------------------------------------
        raw_name = safe_str(_col(raw_row, "player_name", "player"))
        if raw_name is None:
            logger.debug(
                "load_salary_history: missing player name for season %s — row skipped",
                season_id,
            )
            skipped_player += 1
            continue
        player_id = player_lookup.get(_normalize_name(raw_name))
        if player_id is None:
            logger.debug(
                "load_salary_history: unresolved player %r (season=%s) — row skipped",
                raw_name,
                season_id,
            )
            skipped_player += 1
            continue

        # -- Team ----------------------------------------------------------
        raw_team = safe_str(_col(raw_row, "team_abbrev", "team"))
        if raw_team is None:
            logger.debug(
                "load_salary_history: missing team for %r (season=%s) — row skipped",
                raw_name,
                season_id,
            )
            skipped_team += 1
            continue
        team_id = team_lookup.get(raw_team.strip().upper())
        if team_id is None:
            logger.debug(
                "load_salary_history: unresolved team %r for %r (season=%s) — row skipped",
                raw_team,
                raw_name,
                season_id,
            )
            skipped_team += 1
            continue

        # -- Salary --------------------------------------------------------
        raw_salary = _col(raw_row, "salary")
        if isinstance(raw_salary, str):
            salary = _parse_salary(raw_salary)
        else:
            try:
                salary = int(raw_salary) if raw_salary is not None else None
            except (ValueError, TypeError):
                salary = None
        if not salary:  # None or 0
            logger.debug(
                "load_salary_history: invalid/zero salary %r for %r (season=%s) — row skipped",
                raw_salary,
                raw_name,
                season_id,
            )
            skipped_salary += 1
            continue

        rows_to_insert.append(
            {
                "player_id": player_id,
                "team_id": team_id,
                "season_id": season_id,
                "salary": salary,
            }
        )

    # ------------------------------------------------------------------ #
    # 4. Diagnostics                                                      #
    # ------------------------------------------------------------------ #
    logger.info(
        "load_salary_history: %d valid rows; skipped — season=%d player=%d team=%d salary=%d",
        len(rows_to_insert),
        skipped_season,
        skipped_player,
        skipped_team,
        skipped_salary,
    )

    if not rows_to_insert:
        logger.warning("load_salary_history: no valid rows to insert.")
        return 0

    # ------------------------------------------------------------------ #
    # 5. Upsert                                                           #
    # ------------------------------------------------------------------ #
    inserted = upsert_rows(con, "fact_salary", rows_to_insert, conflict="REPLACE")
    logger.info("load_salary_history: %d rows inserted/replaced in fact_salary.", inserted)
    return inserted
