"""
Centralized business-rule validation for ETL output rows.
"""

import logging
import sqlite3

from pydantic import ValidationError

from .models import (
    FactGameRow,
    FactPlayerAdvancedSeasonRow,
    FactPlayerSeasonStatsRow,
    FactPlayerShootingSeasonRow,
    FactSalaryRow,
    PlayerGameLogRow,
    TeamGameLogRow,
)

logger = logging.getLogger(__name__)

_ROW_MODELS = {
    "player_game_log": PlayerGameLogRow,
    "team_game_log": TeamGameLogRow,
    "fact_game": FactGameRow,
    "fact_salary": FactSalaryRow,
    "fact_player_season_stats": FactPlayerSeasonStatsRow,
    "fact_player_advanced_season": FactPlayerAdvancedSeasonRow,
    "fact_player_shooting_season": FactPlayerShootingSeasonRow,
}


def _row_ident(row: dict) -> dict:
    return {
        k: row[k]
        for k in ["game_id", "player_id", "team_id", "season_id", "bref_player_id"]
        if k in row
    }


def validate_rows(table: str, rows: list[dict]) -> list[dict]:
    """
    Validates rows against their Pydantic schema.
    Drops invalid rows and logs a warning.
    """
    model_cls = _ROW_MODELS.get(table)
    if not model_cls:
        # Pass through if no model defined (or handle custom logic here)
        return rows

    valid_rows = []
    for row in rows:
        try:
            # model_dump ensures we get typed/parsed values back (e.g., date strings -> objects)
            validated = model_cls.model_validate(row)
            # Update the row with validated data while keeping any unmapped extra fields
            row.update(validated.model_dump(exclude_unset=True))
            valid_rows.append(row)
        except ValidationError as exc:
            errors = exc.errors()
            msg = errors[0].get("msg", str(exc)) if errors else str(exc)
            logger.warning(
                "Validation failed for table '%s', rule: %s (ident=%r)",
                table,
                msg,
                _row_ident(row),
            )

    return valid_rows


def check_game_stat_consistency(con: sqlite3.Connection, game_id: str) -> list[str]:
    """
    Return list of warning strings if player stats don't reconcile to team stats for the given game.
    Checks: SUM(player pts) == team pts, SUM(player reb) == team reb, etc.
    """
    warnings = []

    # Compare team_game_log aggregates vs player_game_log aggregates
    sql = """
    WITH p_agg AS (
        SELECT
            team_id,
            SUM(pts) as p_pts,
            SUM(reb) as p_reb,
            SUM(ast) as p_ast
        FROM player_game_log
        WHERE game_id = ?
        GROUP BY team_id
    )
    SELECT
        t.team_id,
        t.pts as t_pts, p.p_pts,
        t.reb as t_reb, p.p_reb,
        t.ast as t_ast, p.p_ast
    FROM team_game_log t
    LEFT JOIN p_agg p ON t.team_id = p.team_id
    WHERE t.game_id = ?
    """

    rows = con.execute(sql, (game_id, game_id)).fetchall()
    for r in rows:
        team_id, t_pts, p_pts, t_reb, p_reb, t_ast, p_ast = r

        if p_pts is not None and t_pts != p_pts:
            warnings.append(
                f"PTS mismatch for team {team_id} in game {game_id}: Team={t_pts}, Players={p_pts}"
            )
        if p_reb is not None and t_reb != p_reb:
            warnings.append(
                f"REB mismatch for team {team_id} in game {game_id}: Team={t_reb}, Players={p_reb}"
            )
        if p_ast is not None and t_ast != p_ast:
            warnings.append(
                f"AST mismatch for team {team_id} in game {game_id}: Team={t_ast}, Players={p_ast}"
            )

    return warnings


def run_consistency_checks(con: sqlite3.Connection, season_id: str) -> int:
    """Run reconciliation checks for all games in season_id using a set-based query."""
    game_count = con.execute(
        "SELECT COUNT(*) FROM fact_game WHERE season_id = ?",
        (season_id,),
    ).fetchone()[0]

    sql = """
    WITH p_agg AS (
        SELECT
            p.game_id,
            p.team_id,
            SUM(p.pts) AS p_pts,
            SUM(p.reb) AS p_reb,
            SUM(p.ast) AS p_ast
        FROM player_game_log p
        JOIN fact_game g ON g.game_id = p.game_id
        WHERE g.season_id = ?
        GROUP BY p.game_id, p.team_id
    )
    SELECT
        t.game_id,
        t.team_id,
        t.pts, p.p_pts,
        t.reb, p.p_reb,
        t.ast, p.p_ast
    FROM team_game_log t
    JOIN fact_game g ON g.game_id = t.game_id
    LEFT JOIN p_agg p ON p.game_id = t.game_id AND p.team_id = t.team_id
    WHERE g.season_id = ?
      AND (
        (p.p_pts IS NOT NULL AND t.pts != p.p_pts)
        OR (p.p_reb IS NOT NULL AND t.reb != p.p_reb)
        OR (p.p_ast IS NOT NULL AND t.ast != p.p_ast)
      )
    ORDER BY t.game_id, t.team_id
    """
    mismatches = con.execute(sql, (season_id, season_id)).fetchall()

    total_warnings = 0
    for game_id, team_id, t_pts, p_pts, t_reb, p_reb, t_ast, p_ast in mismatches:
        if p_pts is not None and t_pts != p_pts:
            logger.warning(
                "PTS mismatch for team %s in game %s: Team=%s, Players=%s",
                team_id,
                game_id,
                t_pts,
                p_pts,
            )
            total_warnings += 1
        if p_reb is not None and t_reb != p_reb:
            logger.warning(
                "REB mismatch for team %s in game %s: Team=%s, Players=%s",
                team_id,
                game_id,
                t_reb,
                p_reb,
            )
            total_warnings += 1
        if p_ast is not None and t_ast != p_ast:
            logger.warning(
                "AST mismatch for team %s in game %s: Team=%s, Players=%s",
                team_id,
                game_id,
                t_ast,
                p_ast,
            )
            total_warnings += 1

    if total_warnings == 0:
        logger.info("Consistency check passed for season %s (%d games)", season_id, game_count)
    else:
        logger.warning(
            "Consistency check found %d discrepancies in season %s", total_warnings, season_id
        )

    return total_warnings
