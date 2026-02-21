"""
Centralized business-rule validation for ETL output rows.
"""

import logging
import sqlite3
import re
from typing import Any, Callable

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

def _pct_bounds(col: str) -> Callable[[dict], bool]:
    def fn(row: dict) -> bool:
        v = row.get(col)
        if v is None:
            return True
        return 0.0 <= v <= 1.0
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
    pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    def fn(row: dict) -> bool:
        v = row.get(col)
        if v is None:
            return True
        return bool(pattern.match(str(v)))
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
        ("salary", _gte_zero("salary"), "salary <= 0"), # actually using >=0 to be safe with missing data/0
    ],
    "fact_player_season_stats": [
        ("fgm_fga", _lte("fg", "fga"), "fg > fga"),
        ("fg3m_fg3a", _lte("x3p", "x3pa"), "x3p > x3pa"),
        ("ftm_fta", _lte("ft", "fta"), "ft > fta"),
        ("pts", _gte_zero("pts"), "pts < 0"),
    ],
    "fact_player_advanced_season": [
        ("ts_pct", _pct_bounds("ts_pct"), "ts_pct out of bounds [0,1]"),
        ("orb_pct", _pct_bounds("orb_pct"), "orb_pct out of bounds [0,1]"),
        ("drb_pct", _pct_bounds("drb_pct"), "drb_pct out of bounds [0,1]"),
        ("usg_pct", _pct_bounds("usg_pct"), "usg_pct out of bounds [0,1]"),
    ],
    "fact_player_shooting_season": [
        # zone percentage columns sum ≈ 1.0 (within 0.05 tolerance) handled customly
    ],
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
    for row in rows:
        is_valid = True
        
        # Standard rules
        for field_name, predicate, msg in rules:
            if not predicate(row):
                logger.warning(
                    "Validation failed for table '%s', rule '%s': %s (row=%r)",
                    table, field_name, msg, row
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
                zone_sum = sum(z for z in zones if z is not None)
                if abs(zone_sum - 1.0) > 0.05:
                    logger.warning(
                        "Validation failed for table '%s', rule 'zone_sum': sum is %.3f, expected ~1.0 (row=%r)",
                        table, zone_sum, row
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


def run_consistency_checks(con: sqlite3.Connection, season_id: str) -> None:
    """Run check_game_stat_consistency for all games in season_id, log warnings."""
    games = con.execute("SELECT game_id FROM fact_game WHERE season_id = ?", (season_id,)).fetchall()
    
    total_warnings = 0
    for (game_id,) in games:
        warnings = check_game_stat_consistency(con, game_id)
        for w in warnings:
            logger.warning(w)
            total_warnings += 1
            
    if total_warnings == 0:
        logger.info("Consistency check passed for season %s (%d games)", season_id, len(games))
    else:
        logger.warning("Consistency check found %d discrepancies in season %s", total_warnings, season_id)
