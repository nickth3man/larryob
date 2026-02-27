"""Salary extraction helpers."""

import logging
import sqlite3

from ..config import nba_abbr_to_bref

logger = logging.getLogger(__name__)


def _abbr_to_bref(abbr: str) -> str:
    """Map NBA abbreviation to Basketball-Reference abbreviation."""
    result = nba_abbr_to_bref(abbr)
    return result if result is not None else abbr


def _season_team_map(
    con: sqlite3.Connection,
    season_id: str,
) -> tuple[dict[str, str], str]:
    """Map season-specific Basketball-Reference abbreviations to team IDs."""
    start_year = int(season_id.split("-")[0])

    rows = con.execute(
        """
        SELECT DISTINCT fts.bref_abbrev, dth.team_id
        FROM fact_team_season AS fts
        JOIN dim_team_history AS dth
          ON dth.team_abbrev = fts.bref_abbrev
         AND dth.season_founded <= ?
         AND dth.season_active_till >= ?
        WHERE fts.season_id = ?
          AND fts.bref_abbrev IS NOT NULL
        """,
        (start_year, start_year, season_id),
    ).fetchall()
    if rows:
        bref_to_team: dict[str, str] = {}
        for bref_abbr, team_id in rows:
            b = str(bref_abbr).strip().upper()
            t = str(team_id).strip()
            prev = bref_to_team.get(b)
            if prev and prev != t:
                logger.warning(
                    "fact_team_season duplicate bref_abbrev mapping season=%s bref_abbrev=%s team_ids=%s/%s; using first",
                    season_id,
                    b,
                    prev,
                    t,
                )
                continue
            bref_to_team[b] = t
        return bref_to_team, "fact_team_season"

    cur = con.execute("SELECT team_id, abbreviation FROM dim_team")
    bref_to_team = {}
    for team_id, nba_abbr in cur.fetchall():
        b = _abbr_to_bref(str(nba_abbr).strip().upper())
        bref_to_team[b] = str(team_id).strip()
    return bref_to_team, "dim_team"
