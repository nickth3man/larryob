"""
Centralized business-rule validation for ETL output rows.
"""

import logging
import sqlite3
from collections.abc import Callable

from pydantic import BaseModel, ValidationError

from .models import PlayerGameLogRow, TeamGameLogRow

logger = logging.getLogger(__name__)

# Basic predicates
def _lte(col1: str, col2: str) -> Callable[[dict], bool]:
    def fn(row: dict) -> bool:
        v1, v2 = row.get(col1), row.get(col2)
        if v1 is None or v2 is None:
            return True
        return v1 <= v2
    return fn

def _gte_zero(col: str) -> Callable[[dict], bool]:
    def fn(row: dict) -> bool:
        v = row.get(col)
        if v is None:
            return True
        return v >= 0
    return fn

def _pct_bounds(col: str, upper: float = 1.0) -> Callable[[dict], bool]:
    def fn(row: dict) -> bool:
        v = row.get(col)
        if v is None:
            return True
        return 0.0 <= v <= upper
    return fn

def _sum_equals(cols: list[str], target_col: str) -> Callable[[dict], bool]:
    def fn(row: dict) -> bool:
        target = row.get(target_col)
        vals = [row.get(c) for c in cols]
        if target is None or any(v is None for v in vals):
            return True
        return sum(vals) == target
    return fn

def _date_format(col: str) -> Callable[[dict], bool]:
    from datetime import datetime
    def fn(row: dict) -> bool:
        v = row.get(col)
        if v is None:
            return True
        try:
            datetime.strptime(str(v), "%Y-%m-%d")
            return True
        except ValueError:
            return False
    return fn


RULES: dict[str, list[tuple[str, Callable[[dict], bool], str]]] = {
    "player_game_log": [
        ("fgm_fga", _lte("fgm", "fga"), "fgm > fga"),
        ("fg3m_fg3a", _lte("fg3m", "fg3a"), "fg3m > fg3a"),
        ("ftm_fta", _lte("ftm", "fta"), "ftm > fta"),
        ("pts", _gte_zero("pts"), "pts < 0"),
        ("minutes", _gte_zero("minutes_played"), "minutes < 0"),
        ("reb_sum", _sum_equals(["oreb", "dreb"], "reb"), "oreb + dreb != reb"),
    ],
    "team_game_log": [
        ("fgm_fga", _lte("fgm", "fga"), "fgm > fga"),
        ("fg3m_fg3a", _lte("fg3m", "fg3a"), "fg3m > fg3a"),
        ("ftm_fta", _lte("ftm", "fta"), "ftm > fta"),
        ("pts", _gte_zero("pts"), "pts < 0"),
        ("reb_sum", _sum_equals(["oreb", "dreb"], "reb"), "oreb + dreb != reb"),
    ],
    "fact_game": [
        ("home_score", _gte_zero("home_score"), "score < 0"),
        ("away_score", _gte_zero("away_score"), "score < 0"),
        ("game_date", _date_format("game_date"), "invalid date format"),
    ],
    "fact_salary": [
        ("salary", _gte_zero("salary"), "salary < 0"), # actually using >=0 to be safe with missing data/0
    ],
    "fact_player_season_stats": [
        ("fgm_fga", _lte("fg", "fga"), "fg > fga"),
        ("fg3m_fg3a", _lte("x3p", "x3pa"), "x3p > x3pa"),
        ("ftm_fta", _lte("ft", "fta"), "ft > fta"),
        ("pts", _gte_zero("pts"), "pts < 0"),
    ],
    "fact_player_advanced_season": [
        ("ts_pct", _pct_bounds("ts_pct", upper=1.5), "ts_pct out of bounds [0,1.5]"),
        ("orb_pct", _pct_bounds("orb_pct"), "orb_pct out of bounds [0,1]"),
        ("drb_pct", _pct_bounds("drb_pct"), "drb_pct out of bounds [0,1]"),
        ("usg_pct", _pct_bounds("usg_pct"), "usg_pct out of bounds [0,1]"),
    ],
    "fact_player_shooting_season": [
        # zone percentage columns sum ≈ 1.0 (within 0.05 tolerance) handled customly
    ],
}

_ROW_MODELS: dict[str, type[BaseModel]] = {
    "player_game_log": PlayerGameLogRow,
    "team_game_log": TeamGameLogRow,
}

_ROW_MODEL_REQUIRED_KEYS: dict[str, set[str]] = {
    "player_game_log": {"game_id", "player_id", "team_id"},
    "team_game_log": {"game_id", "team_id"},
}


def _row_ident(row: dict) -> dict:
    return {
        k: row[k]
        for k in ["game_id", "player_id", "team_id", "season_id", "bref_player_id"]
        if k in row
    }


def validate_rows(table: str, rows: list[dict]) -> list[dict]:
    """
    Drop invalid rows based on business rules for `table` and log warnings.
    Returns only valid rows.
    """
    rules = RULES.get(table, [])
    if not rules and table != "fact_player_shooting_season":
        return rows

    valid_rows = []
    model_cls = _ROW_MODELS.get(table)
    model_required_keys = _ROW_MODEL_REQUIRED_KEYS.get(table, set())
    # Optimized list comprehension where possible, though iteration remains for complex predicate logic
    for row in rows:
        if model_cls is not None and model_required_keys.issubset(row):
            try:
                validated = model_cls.model_validate(row)
                # Keep any loader-supplied extras while normalizing typed fields from the model.
                row = {**row, **validated.model_dump()}
            except ValidationError as exc:
                errors = exc.errors()
                first = errors[0] if errors else {}
                msg = first.get("msg", str(exc)) if isinstance(first, dict) else str(exc)
                logger.warning(
                    "Validation failed for table '%s', model rule: %s (ident=%r)",
                    table, msg, _row_ident(row),
                )
                continue

        is_valid = True

        # Standard rules
        for field_name, predicate, msg in rules:
            if not predicate(row):
                logger.warning(
                    "Validation failed for table '%s', rule '%s': %s (ident=%r)",
                    table, field_name, msg, _row_ident(row)
                )
                is_valid = False
                break

        # Custom rule for fact_player_shooting_season
        if is_valid and table == "fact_player_shooting_season":
            zones = [
                row.get("pct_fga_0_3"), row.get("pct_fga_3_10"), row.get("pct_fga_10_16"),
                row.get("pct_fga_16_3p"), row.get("pct_fga_3p")
            ]
            if not any(z is None for z in zones):
                zone_sum = sum(zones)
                if abs(zone_sum - 1.0) > 0.05:
                    logger.warning(
                        "Validation failed for table '%s', rule 'zone_sum': sum is %.3f, expected ~1.0 (ident=%r)",
                        table, zone_sum, _row_ident(row)
                    )
                    is_valid = False

        if is_valid:
            valid_rows.append(row)

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
            warnings.append(f"PTS mismatch for team {team_id} in game {game_id}: Team={t_pts}, Players={p_pts}")
        if p_reb is not None and t_reb != p_reb:
            warnings.append(f"REB mismatch for team {team_id} in game {game_id}: Team={t_reb}, Players={p_reb}")
        if p_ast is not None and t_ast != p_ast:
            warnings.append(f"AST mismatch for team {team_id} in game {game_id}: Team={t_ast}, Players={p_ast}")

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
                team_id, game_id, t_pts, p_pts,
            )
            total_warnings += 1
        if p_reb is not None and t_reb != p_reb:
            logger.warning(
                "REB mismatch for team %s in game %s: Team=%s, Players=%s",
                team_id, game_id, t_reb, p_reb,
            )
            total_warnings += 1
        if p_ast is not None and t_ast != p_ast:
            logger.warning(
                "AST mismatch for team %s in game %s: Team=%s, Players=%s",
                team_id, game_id, t_ast, p_ast,
            )
            total_warnings += 1

    if total_warnings == 0:
        logger.info("Consistency check passed for season %s (%d games)", season_id, game_count)
    else:
        logger.warning("Consistency check found %d discrepancies in season %s", total_warnings, season_id)

    return total_warnings
