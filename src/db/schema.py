"""
SQLite schema definitions.

All fact tables use NULL (not 0) for stats that were not officially tracked
in early NBA eras (e.g., blocks/steals pre-1973, 3-pointers pre-1979).
Running this module is idempotent — it is safe to call on an existing db.
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent.parent / "nba_raw_data.db"

DDL_STATEMENTS = [
    # ------------------------------------------------------------------ #
    # Dimension: seasons                                                   #
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS dim_season (
        season_id   TEXT PRIMARY KEY,   -- e.g. '2023-24'
        start_year  INTEGER NOT NULL,
        end_year    INTEGER NOT NULL,
        season_type TEXT NOT NULL        -- 'Regular Season' | 'Playoffs' | 'Play-In' | 'Preseason'
    ) STRICT;
    """,

    # ------------------------------------------------------------------ #
    # Dimension: teams                                                     #
    # A franchise keeps the same team_id even across relocations/rebranDs  #
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS dim_team (
        team_id      TEXT PRIMARY KEY,   -- NBA numeric id as text, e.g. '1610612747'
        abbreviation TEXT NOT NULL,      -- e.g. 'LAL'
        full_name    TEXT NOT NULL,      -- e.g. 'Los Angeles Lakers'
        city         TEXT NOT NULL,
        nickname     TEXT NOT NULL,      -- e.g. 'Lakers'
        conference   TEXT,               -- 'East' | 'West'
        division     TEXT,
        color_primary   TEXT,            -- hex e.g. '#552583'
        color_secondary TEXT,
        arena_name   TEXT,
        founded_year INTEGER
    ) STRICT;
    """,

    # ------------------------------------------------------------------ #
    # Dimension: players                                                   #
    # Surrogate key is the NBA numeric id (stored as TEXT to be safe)      #
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS dim_player (
        player_id    TEXT PRIMARY KEY,   -- NBA numeric id as text, e.g. '2544'
        first_name   TEXT NOT NULL,
        last_name    TEXT NOT NULL,
        full_name    TEXT NOT NULL,
        birth_date   TEXT,               -- ISO-8601 'YYYY-MM-DD', NULL if unknown
        birth_city   TEXT,
        birth_country TEXT,
        height_cm    REAL,
        weight_kg    REAL,
        position     TEXT,               -- 'PG' | 'SG' | 'SF' | 'PF' | 'C'
        draft_year   INTEGER,
        draft_round  INTEGER,
        draft_number INTEGER,
        is_active    INTEGER NOT NULL DEFAULT 1  -- 0 | 1
    ) STRICT;
    """,

    # ------------------------------------------------------------------ #
    # Temporal: roster stints                                             #
    # Tracks every player-team employment period to support mid-season   #
    # trade queries (e.g., "which team was X on 2024-02-01?")             #
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS fact_roster (
        roster_id  INTEGER PRIMARY KEY,
        player_id  TEXT NOT NULL REFERENCES dim_player(player_id),
        team_id    TEXT NOT NULL REFERENCES dim_team(team_id),
        season_id  TEXT NOT NULL REFERENCES dim_season(season_id),
        start_date TEXT NOT NULL,        -- ISO-8601
        end_date   TEXT,                 -- NULL = currently active
        CHECK (end_date IS NULL OR end_date > start_date)
    );
    """,

    # ------------------------------------------------------------------ #
    # Fact: games                                                         #
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS fact_game (
        game_id      TEXT PRIMARY KEY,   -- NBA 10-digit game id, e.g. '0022301001'
        season_id    TEXT NOT NULL REFERENCES dim_season(season_id),
        game_date    TEXT NOT NULL,      -- ISO-8601
        home_team_id TEXT NOT NULL REFERENCES dim_team(team_id),
        away_team_id TEXT NOT NULL REFERENCES dim_team(team_id),
        home_score   INTEGER,
        away_score   INTEGER,
        season_type  TEXT NOT NULL,      -- 'Regular Season' | 'Playoffs' | 'Play-In'
        status       TEXT NOT NULL DEFAULT 'Final',  -- 'Scheduled' | 'In-Progress' | 'Final'
        arena        TEXT,
        attendance   INTEGER,
        UNIQUE (home_team_id, away_team_id, game_date),
        CHECK (home_score IS NULL OR home_score >= 0),
        CHECK (away_score IS NULL OR away_score >= 0)
    );
    """,

    # ------------------------------------------------------------------ #
    # Fact: team box-score aggregates per game                            #
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS team_game_log (
        game_id  TEXT NOT NULL REFERENCES fact_game(game_id),
        team_id  TEXT NOT NULL REFERENCES dim_team(team_id),
        fgm      INTEGER, fga  INTEGER,
        fg3m     INTEGER, fg3a INTEGER,   -- NULL pre-1979 (no 3-point line)
        ftm      INTEGER, fta  INTEGER,
        oreb     INTEGER,                  -- NULL pre-1973-74
        dreb     INTEGER,                  -- NULL pre-1973-74
        reb      INTEGER,
        ast      INTEGER,
        stl      INTEGER,                  -- NULL pre-1973-74
        blk      INTEGER,                  -- NULL pre-1973-74
        tov      INTEGER,                  -- NULL pre-1973-74
        pf       INTEGER,
        pts      INTEGER,
        plus_minus INTEGER,
        PRIMARY KEY (game_id, team_id)
    );
    """,

    # ------------------------------------------------------------------ #
    # Fact: individual player box scores (one row per player per game)    #
    # This is the largest table and the heart of the analytics layer.     #
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS player_game_log (
        game_id        TEXT NOT NULL REFERENCES fact_game(game_id),
        player_id      TEXT NOT NULL REFERENCES dim_player(player_id),
        team_id        TEXT NOT NULL REFERENCES dim_team(team_id),  -- tracks traded players
        minutes_played REAL,
        fgm  INTEGER, fga  INTEGER,
        fg3m INTEGER, fg3a INTEGER,   -- NULL pre-1979
        ftm  INTEGER, fta  INTEGER,
        oreb INTEGER,                  -- NULL pre-1973-74
        dreb INTEGER,                  -- NULL pre-1973-74
        reb  INTEGER,
        ast  INTEGER,
        stl  INTEGER,                  -- NULL pre-1973-74
        blk  INTEGER,                  -- NULL pre-1973-74
        tov  INTEGER,                  -- NULL pre-1973-74
        pf   INTEGER,
        pts  INTEGER,
        plus_minus INTEGER,
        starter INTEGER,               -- 0 | 1 | NULL if unknown
        PRIMARY KEY (game_id, player_id)
    );
    """,

    # ------------------------------------------------------------------ #
    # Fact: play-by-play events                                           #
    # EVENTMSGTYPE codes (official NBA):                                  #
    #   1=Make 2=Miss 3=FreeThrow 4=Rebound 5=Turnover 6=Foul            #
    #   7=Violation 8=Substitution 9=Timeout 10=JumpBall 11=Ejection     #
    #   12=StartPeriod 13=EndPeriod                                       #
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS fact_play_by_play (
        event_id            TEXT PRIMARY KEY,   -- game_id + '_' + eventnum
        game_id             TEXT NOT NULL REFERENCES fact_game(game_id),
        period              INTEGER NOT NULL,
        pc_time_string      TEXT,               -- clock remaining e.g. '10:46'
        wc_time_string      TEXT,               -- wall-clock time
        eventmsgtype        INTEGER NOT NULL,
        eventmsgactiontype  INTEGER,
        player1_id          TEXT REFERENCES dim_player(player_id),
        player2_id          TEXT REFERENCES dim_player(player_id),
        player3_id          TEXT REFERENCES dim_player(player_id),
        team1_id            TEXT REFERENCES dim_team(team_id),
        team2_id            TEXT REFERENCES dim_team(team_id),
        home_description    TEXT,
        visitor_description TEXT,
        neutral_description TEXT,
        score               TEXT,               -- e.g. '14 - 10'
        score_margin        INTEGER
    );
    """,

    # ------------------------------------------------------------------ #
    # Fact: awards / accolades                                            #
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS fact_player_award (
        award_id   INTEGER PRIMARY KEY,
        player_id  TEXT NOT NULL REFERENCES dim_player(player_id),
        season_id  TEXT NOT NULL REFERENCES dim_season(season_id),
        award_name TEXT NOT NULL,   -- 'MVP' | 'DPOY' | 'ROY' | 'All-NBA 1st' ...
        award_type TEXT NOT NULL,   -- 'individual' | 'weekly' | 'team_inclusion'
        trophy_name TEXT,           -- historical trophy name e.g. 'Maurice Podoloff Trophy'
        votes_received INTEGER,
        votes_possible INTEGER
    );
    """,

    # ------------------------------------------------------------------ #
    # Dimension: salary cap by season                                     #
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS dim_salary_cap (
        season_id TEXT PRIMARY KEY REFERENCES dim_season(season_id),
        cap_amount INTEGER NOT NULL             -- in USD
    ) STRICT;
    """,

    # ------------------------------------------------------------------ #
    # Fact: player salaries                                               #
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS fact_salary (
        salary_id  INTEGER PRIMARY KEY,
        player_id  TEXT NOT NULL REFERENCES dim_player(player_id),
        team_id    TEXT NOT NULL REFERENCES dim_team(team_id),
        season_id  TEXT NOT NULL REFERENCES dim_season(season_id),
        salary     INTEGER NOT NULL,    -- USD
        UNIQUE (player_id, team_id, season_id)
    );
    """,

    # ------------------------------------------------------------------ #
    # Indexes for common query patterns                                   #
    # ------------------------------------------------------------------ #
    "CREATE INDEX IF NOT EXISTS idx_pgl_player ON player_game_log(player_id);",
    "CREATE INDEX IF NOT EXISTS idx_pgl_team   ON player_game_log(team_id);",
    "CREATE INDEX IF NOT EXISTS idx_pgl_game   ON player_game_log(game_id);",
    "CREATE INDEX IF NOT EXISTS idx_game_date  ON fact_game(game_date);",
    "CREATE INDEX IF NOT EXISTS idx_game_season ON fact_game(season_id);",
    "CREATE INDEX IF NOT EXISTS idx_pbp_game   ON fact_play_by_play(game_id);",
    "CREATE INDEX IF NOT EXISTS idx_pbp_player1 ON fact_play_by_play(player1_id);",
    "CREATE INDEX IF NOT EXISTS idx_roster_player ON fact_roster(player_id);",
    "CREATE INDEX IF NOT EXISTS idx_pgl_player_season ON player_game_log(player_id, game_id);",
]


def init_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Create all tables and indexes; returns an open connection."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA foreign_keys=ON;")
    for ddl in DDL_STATEMENTS:
        con.execute(ddl)
    con.commit()
    return con


if __name__ == "__main__":
    con = init_db()
    tables = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
    ).fetchall()
    print("Initialized database at:", DB_PATH)
    print("Tables:", [t[0] for t in tables])
    con.close()
