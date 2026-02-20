"""
DuckDB analytics layer.

Provides a single `get_duck_con()` factory that:
  1. Connects to an in-memory (or persistent) DuckDB instance.
  2. Installs & loads the native sqlite_scanner extension.
  3. Attaches the SQLite database as the 'nba' schema.
  4. Creates all analytical VIEWs on top of the attached tables.

Usage
-----
    from src.db.analytics import get_duck_con

    con = get_duck_con()
    df = con.execute("SELECT * FROM vw_player_season_totals LIMIT 10").df()
"""

import logging
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

SQLITE_DB = Path(__file__).parent.parent.parent / "nba_raw_data.db"

# All VIEWs are defined here as (name, SQL) pairs.
# SQL references the SQLite tables via the 'nba.' prefix after attachment.
_VIEWS: list[tuple[str, str]] = [

    # ------------------------------------------------------------------ #
    # Shooting efficiency                                                  #
    # ------------------------------------------------------------------ #
    (
        "vw_player_shooting",
        """
        SELECT
            p.player_id,
            p.full_name,
            g.season_id,
            COUNT(*)                                                AS games,
            SUM(l.minutes_played)                                   AS total_minutes,
            SUM(l.fgm)  AS fgm,  SUM(l.fga)  AS fga,
            SUM(l.fg3m) AS fg3m, SUM(l.fg3a) AS fg3a,
            SUM(l.ftm)  AS ftm,  SUM(l.fta)  AS fta,
            SUM(l.pts)  AS pts,
            -- eFG% = (FGM + 0.5 * FG3M) / FGA
            ROUND(
                (SUM(l.fgm) + 0.5 * COALESCE(SUM(l.fg3m), 0))
                / NULLIF(SUM(l.fga), 0),
            3) AS efg_pct,
            -- TS% = PTS / (2 * (FGA + 0.44 * FTA))
            ROUND(
                SUM(l.pts)
                / NULLIF(2.0 * (SUM(l.fga) + 0.44 * COALESCE(SUM(l.fta), 0)), 0),
            3) AS ts_pct
        FROM nba.player_game_log l
        JOIN nba.dim_player p USING (player_id)
        JOIN nba.fact_game g USING (game_id)
        GROUP BY p.player_id, p.full_name, g.season_id
        """,
    ),

    # ------------------------------------------------------------------ #
    # Per-game averages (season totals / games played)                    #
    # ------------------------------------------------------------------ #
    (
        "vw_player_season_totals",
        """
        SELECT
            p.player_id,
            p.full_name,
            g.season_id,
            COUNT(*)                                       AS gp,
            ROUND(AVG(l.minutes_played), 1)                AS mpg,
            ROUND(AVG(l.pts),   1)                         AS ppg,
            ROUND(AVG(l.reb),   1)                         AS rpg,
            ROUND(AVG(l.ast),   1)                         AS apg,
            ROUND(AVG(l.stl),   1)                         AS spg,
            ROUND(AVG(l.blk),   1)                         AS bpg,
            ROUND(AVG(l.tov),   1)                         AS topg,
            ROUND(AVG(l.plus_minus), 1)                    AS net_rtg,
            SUM(l.pts)                                     AS total_pts,
            SUM(l.reb)                                     AS total_reb,
            SUM(l.ast)                                     AS total_ast
        FROM nba.player_game_log l
        JOIN nba.dim_player p USING (player_id)
        JOIN nba.fact_game g USING (game_id)
        GROUP BY p.player_id, p.full_name, g.season_id
        """,
    ),

    # ------------------------------------------------------------------ #
    # Last N games rolling window (default 10)                            #
    # ------------------------------------------------------------------ #
    (
        "vw_player_last10",
        """
        WITH ranked AS (
            SELECT
                l.*,
                g.game_date,
                ROW_NUMBER() OVER (
                    PARTITION BY l.player_id
                    ORDER BY g.game_date DESC
                ) AS rn
            FROM nba.player_game_log l
            JOIN nba.fact_game g USING (game_id)
        )
        SELECT
            r.player_id,
            p.full_name,
            COUNT(*)                       AS games,
            ROUND(AVG(r.pts),  1)          AS ppg,
            ROUND(AVG(r.reb),  1)          AS rpg,
            ROUND(AVG(r.ast),  1)          AS apg,
            ROUND(AVG(r.plus_minus), 1)    AS net_rtg
        FROM ranked r
        JOIN nba.dim_player p USING (player_id)
        WHERE r.rn <= 10
        GROUP BY r.player_id, p.full_name
        """,
    ),

    # ------------------------------------------------------------------ #
    # Team season standings                                               #
    # ------------------------------------------------------------------ #
    (
        "vw_team_standings",
        """
        SELECT
            t.abbreviation,
            t.full_name,
            g.season_id,
            COUNT(*)                                        AS gp,
            SUM(CASE WHEN l.pts > opp.pts THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN l.pts < opp.pts THEN 1 ELSE 0 END) AS losses,
            ROUND(
                SUM(CASE WHEN l.pts > opp.pts THEN 1.0 ELSE 0.0 END)
                / NULLIF(COUNT(*), 0),
            3) AS win_pct
        FROM nba.team_game_log l
        JOIN nba.team_game_log opp
            ON opp.game_id = l.game_id AND opp.team_id != l.team_id
        JOIN nba.dim_team t ON t.team_id = l.team_id
        JOIN nba.fact_game g ON g.game_id = l.game_id
        GROUP BY t.abbreviation, t.full_name, g.season_id
        ORDER BY g.season_id DESC, win_pct DESC
        """,
    ),

    # ------------------------------------------------------------------ #
    # Pace estimate (possessions per 48 min)                             #
    # Requires modern era data (1973-74 onwards where TOV/OReb tracked) #
    # ------------------------------------------------------------------ #
    (
        "vw_team_pace",
        """
        SELECT
            t.abbreviation,
            t.full_name,
            g.season_id,
            SUM(l.fga)                                     AS total_fga,
            SUM(COALESCE(l.fta, 0))                        AS total_fta,
            SUM(COALESCE(l.oreb, 0))                       AS total_oreb,
            SUM(COALESCE(l.tov, 0))                        AS total_tov,
            SUM(COALESCE(l.fgm, 0))                        AS total_fgm,
            -- Possession estimate:
            -- Poss ≈ FGA + 0.4*FTA - 1.07*(ORB/(ORB+Opp_DRB))*(FGA-FGM) + TOV
            ROUND(
                0.5 * (
                    SUM(l.fga)
                    + 0.4 * COALESCE(SUM(l.fta), 0)
                    - 1.07 * (
                        COALESCE(SUM(l.oreb), 0)
                        / NULLIF(COALESCE(SUM(l.oreb), 0) + 1, 0)
                    ) * (SUM(l.fga) - COALESCE(SUM(l.fgm), 0))
                    + COALESCE(SUM(l.tov), 0)
                ),
            1) AS poss_estimate
        FROM nba.team_game_log l
        JOIN nba.dim_team t ON t.team_id = l.team_id
        JOIN nba.fact_game g USING (game_id)
        WHERE l.oreb IS NOT NULL AND l.tov IS NOT NULL
        GROUP BY t.abbreviation, t.full_name, g.season_id
        """,
    ),

    # ------------------------------------------------------------------ #
    # Play-by-play: shot distribution by event type                      #
    # ------------------------------------------------------------------ #
    (
        "vw_pbp_shot_summary",
        """
        SELECT
            p.full_name                        AS player_name,
            pbp.game_id,
            g.game_date,
            g.season_id,
            SUM(CASE WHEN pbp.eventmsgtype = 1 THEN 1 ELSE 0 END) AS made_shots,
            SUM(CASE WHEN pbp.eventmsgtype = 2 THEN 1 ELSE 0 END) AS missed_shots,
            SUM(CASE WHEN pbp.eventmsgtype = 3 THEN 1 ELSE 0 END) AS free_throws,
            SUM(CASE WHEN pbp.eventmsgtype = 5 THEN 1 ELSE 0 END) AS turnovers
        FROM nba.fact_play_by_play pbp
        JOIN nba.dim_player p ON p.player_id = pbp.player1_id
        JOIN nba.fact_game g ON g.game_id = pbp.game_id
        GROUP BY p.full_name, pbp.game_id, g.game_date, g.season_id
        """,
    ),

    # ------------------------------------------------------------------ #
    # Awards: career trophy case per player                               #
    # ------------------------------------------------------------------ #
    (
        "vw_player_awards",
        """
        SELECT
            p.full_name,
            a.award_name,
            COUNT(*) AS times_won,
            MIN(a.season_id) AS first_won,
            MAX(a.season_id) AS last_won
        FROM nba.fact_player_award a
        JOIN nba.dim_player p USING (player_id)
        GROUP BY p.full_name, a.award_name
        ORDER BY p.full_name, times_won DESC
        """,
    ),

    # ------------------------------------------------------------------ #
    # Salary: contract as % of cap (era-agnostic comparison)             #
    # ------------------------------------------------------------------ #
    (
        "vw_salary_cap_pct",
        """
        SELECT
            p.full_name,
            t.abbreviation AS team,
            s.season_id,
            s.salary,
            cap.cap_amount,
            ROUND(100.0 * s.salary / NULLIF(cap.cap_amount, 0), 2) AS cap_pct
        FROM nba.fact_salary s
        JOIN nba.dim_player p USING (player_id)
        JOIN nba.dim_team t USING (team_id)
        JOIN nba.dim_salary_cap cap USING (season_id)
        ORDER BY s.season_id DESC, cap_pct DESC
        """,
    ),
]


def get_duck_con(
    sqlite_path: Path = SQLITE_DB,
    duck_db_path: str = ":memory:",
) -> duckdb.DuckDBPyConnection:
    """
    Return an open DuckDB connection with the SQLite database attached
    as schema 'nba' and all analytical views installed.

    Parameters
    ----------
    sqlite_path : Path
        Path to the SQLite `nba_raw_data.db` file.
    duck_db_path : str
        ':memory:' for ephemeral analytics, or a file path to persist
        DuckDB's own native columnar store alongside SQLite.
    """
    con = duckdb.connect(duck_db_path)

    # Install & load the sqlite extension (bundled with DuckDB ≥ 0.8)
    con.execute("INSTALL sqlite;")
    con.execute("LOAD sqlite;")

    # Attach the SQLite database — its tables become accessible via 'nba.'
    con.execute(f"ATTACH '{sqlite_path}' AS nba (TYPE sqlite, READ_ONLY);")
    logger.info("Attached SQLite db: %s", sqlite_path)

    # Create all analytical views
    for name, sql in _VIEWS:
        con.execute(f"CREATE OR REPLACE VIEW {name} AS {sql}")
        logger.debug("View created: %s", name)

    logger.info("DuckDB analytics layer ready (%d views).", len(_VIEWS))
    return con


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    duck = get_duck_con()
    print("Available views:")
    views = duck.execute("SHOW TABLES").fetchall()
    for v in views:
        print(" -", v[0])
    duck.close()
