"""
SQLite schema definitions.

All fact tables use NULL (not 0) for stats that were not officially tracked
in early NBA eras (e.g., blocks/steals pre-1973-74, 3-pointers pre-1979-80).
Running this module is idempotent — it is safe to call on an existing db.
"""

import logging
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent.parent / "nba_raw_data.db"

# Migrations applied with try/except so re-running is safe.
# SQLite does not support ALTER TABLE ... ADD COLUMN IF NOT EXISTS.
ALTER_STATEMENTS = [
    # dim_player additions for Basketball-Reference cross-reference.
    # SQLite's ALTER TABLE ADD COLUMN does not support UNIQUE; uniqueness is
    # enforced by the index created below.
    "ALTER TABLE dim_player ADD COLUMN bref_id TEXT",
    "ALTER TABLE dim_player ADD COLUMN college TEXT",
    "ALTER TABLE dim_player ADD COLUMN hof INTEGER DEFAULT 0",
    # dim_team: bref uses different abbreviations (BRK vs BKN, CHO vs CHA, etc.)
    "ALTER TABLE dim_team ADD COLUMN bref_abbrev TEXT",
    # Indexes on the new columns — must run AFTER the ALTER TABLE statements.
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_player_bref ON dim_player(bref_id)",
]

ROLLBACK_STATEMENTS = [
    "DROP INDEX IF EXISTS idx_player_bref",
    "ALTER TABLE dim_player DROP COLUMN bref_id",
    "ALTER TABLE dim_player DROP COLUMN college",
    "ALTER TABLE dim_player DROP COLUMN hof",
    "ALTER TABLE dim_team DROP COLUMN bref_abbrev",
]

DDL_STATEMENTS = [
    # ------------------------------------------------------------------ #
    # Dimension: seasons                                                   #
    # season_type lives on fact_game, not here — a season row covers all  #
    # game types (Regular Season, Playoffs, Play-In) for that year.        #
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS dim_season (
        season_id   TEXT PRIMARY KEY,   -- e.g. '2023-24'
        start_year  INTEGER NOT NULL,
        end_year    INTEGER NOT NULL
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
        position     TEXT CHECK (position IN ('PG','SG','SF','PF','C','G','F','G-F','F-G','F-C','C-F') OR position IS NULL),
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
        CHECK (end_date IS NULL OR end_date > start_date),
        UNIQUE (player_id, team_id, season_id)
    ) STRICT;
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
    ) STRICT;
    """,
    # ------------------------------------------------------------------ #
    # Fact: team box-score aggregates per game                            #
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS team_game_log (
        game_id  TEXT NOT NULL REFERENCES fact_game(game_id),
        team_id  TEXT NOT NULL REFERENCES dim_team(team_id),
        fgm      INTEGER, fga  INTEGER,
        fg3m     INTEGER, fg3a INTEGER,   -- NULL pre-1979-80 (no 3-point line)
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
    ) STRICT;
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
        minutes_played REAL,               -- decimal minutes (not MM:SS); convert upstream
        fgm  INTEGER, fga  INTEGER,
        fg3m INTEGER, fg3a INTEGER,   -- NULL pre-1979-80
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
    ) STRICT;
    """,
    # ------------------------------------------------------------------ #
    # Fact: play-by-play events                                           #
    # EVENTMSGTYPE codes (official NBA):                                  #
    #   1=Make 2=Miss 3=FreeThrow 4=Rebound 5=Turnover 6=Foul            #
    #   7=Violation 8=Substitution 9=Timeout 10=JumpBall 11=Ejection     #
    #   12=StartPeriod 13=EndPeriod                                       #
    # person*type: 0=None 1=Player 2=Team 3=Official                     #
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS fact_play_by_play (
        event_id            TEXT PRIMARY KEY,   -- game_id + '_' + zero-padded eventnum (6 digits)
        game_id             TEXT NOT NULL REFERENCES fact_game(game_id),
        period              INTEGER NOT NULL,
        pc_time_string      TEXT,               -- clock remaining e.g. '10:46'
        wc_time_string      TEXT,               -- wall-clock time
        eventmsgtype        INTEGER NOT NULL,
        eventmsgactiontype  INTEGER,
        player1_id          TEXT REFERENCES dim_player(player_id),
        player2_id          TEXT REFERENCES dim_player(player_id),
        player3_id          TEXT REFERENCES dim_player(player_id),
        person1type         INTEGER,
        person2type         INTEGER,
        person3type         INTEGER,
        team1_id            TEXT REFERENCES dim_team(team_id),
        team2_id            TEXT REFERENCES dim_team(team_id),
        home_description    TEXT,
        visitor_description TEXT,
        neutral_description TEXT,
        score               TEXT,               -- e.g. '14 - 10'
        score_margin        TEXT                -- '+5', '-3', 'TIE', or NULL; cast to INTEGER at query time
    ) STRICT;
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
        award_type TEXT NOT NULL,   -- 'individual' | 'weekly' | 'monthly' | 'team_inclusion'
        trophy_name TEXT,           -- historical trophy name e.g. 'Maurice Podoloff Trophy'
        votes_received INTEGER,
        votes_possible INTEGER,
        UNIQUE (player_id, season_id, award_name)
    ) STRICT;
    """,
    # ------------------------------------------------------------------ #
    # Fact: all-star selections                                           #
    # Source: Basketball-Reference All-Star Selections                    #
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS fact_all_star (
        all_star_id      INTEGER PRIMARY KEY,
        player_id        TEXT NOT NULL REFERENCES dim_player(player_id),
        season_id        TEXT NOT NULL REFERENCES dim_season(season_id),
        team_id          TEXT REFERENCES dim_team(team_id),
        selection_team   TEXT,   -- raw label from source ('East', 'West', 'Team LeBron', etc.)
        is_starter       INTEGER CHECK (is_starter IN (0, 1) OR is_starter IS NULL),
        is_replacement   INTEGER NOT NULL DEFAULT 0 CHECK (is_replacement IN (0, 1)),
        UNIQUE (player_id, season_id)
    ) STRICT;
    """,
    # ------------------------------------------------------------------ #
    # Fact: end-of-season team selections                                #
    # Source: Basketball-Reference End of Season Teams                    #
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS fact_all_nba (
        selection_id   INTEGER PRIMARY KEY,
        player_id      TEXT NOT NULL REFERENCES dim_player(player_id),
        season_id      TEXT NOT NULL REFERENCES dim_season(season_id),
        team_type      TEXT NOT NULL,  -- All-NBA | All-Defense | All-Rookie | All-ABA | All-BAA
        team_number    INTEGER CHECK (team_number IN (1, 2, 3)),
        position       TEXT,
        UNIQUE (player_id, season_id, team_type)
    ) STRICT;
    """,
    # ------------------------------------------------------------------ #
    # Fact: end-of-season team voting detail                             #
    # Source: Basketball-Reference End of Season Teams (Voting)           #
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS fact_all_nba_vote (
        vote_id              INTEGER PRIMARY KEY,
        player_id            TEXT NOT NULL REFERENCES dim_player(player_id),
        season_id            TEXT NOT NULL REFERENCES dim_season(season_id),
        team_type            TEXT NOT NULL,
        team_number          INTEGER CHECK (team_number IN (1, 2, 3) OR team_number IS NULL),
        position             TEXT,
        pts_won              INTEGER,
        pts_max              INTEGER,
        share                REAL,
        first_team_votes     INTEGER,
        second_team_votes    INTEGER,
        third_team_votes     INTEGER,
        UNIQUE (player_id, season_id, team_type)
    ) STRICT;
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
    ) STRICT;
    """,
    # ------------------------------------------------------------------ #
    # Indexes for common query patterns                                   #
    # ------------------------------------------------------------------ #
    "CREATE INDEX IF NOT EXISTS idx_pgl_player ON player_game_log(player_id);",
    "CREATE INDEX IF NOT EXISTS idx_pgl_team   ON player_game_log(team_id);",
    "CREATE INDEX IF NOT EXISTS idx_pgl_game   ON player_game_log(game_id);",
    "CREATE INDEX IF NOT EXISTS idx_pgl_player_game ON player_game_log(player_id, game_id);",
    "CREATE INDEX IF NOT EXISTS idx_game_date  ON fact_game(game_date);",
    "CREATE INDEX IF NOT EXISTS idx_game_season ON fact_game(season_id);",
    "CREATE INDEX IF NOT EXISTS idx_game_home  ON fact_game(home_team_id);",
    "CREATE INDEX IF NOT EXISTS idx_game_away  ON fact_game(away_team_id);",
    "CREATE INDEX IF NOT EXISTS idx_pbp_game   ON fact_play_by_play(game_id);",
    "CREATE INDEX IF NOT EXISTS idx_pbp_game_period ON fact_play_by_play(game_id, period);",
    "CREATE INDEX IF NOT EXISTS idx_pbp_player1 ON fact_play_by_play(player1_id);",
    "CREATE INDEX IF NOT EXISTS idx_roster_player ON fact_roster(player_id);",
    "CREATE INDEX IF NOT EXISTS idx_roster_player_dates ON fact_roster(player_id, start_date, end_date);",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_roster_unique ON fact_roster(player_id, team_id, season_id);",
    "CREATE INDEX IF NOT EXISTS idx_tgl_team   ON team_game_log(team_id);",
    "CREATE INDEX IF NOT EXISTS idx_allstar_player ON fact_all_star(player_id);",
    "CREATE INDEX IF NOT EXISTS idx_allstar_season ON fact_all_star(season_id);",
    "CREATE INDEX IF NOT EXISTS idx_allnba_player ON fact_all_nba(player_id);",
    "CREATE INDEX IF NOT EXISTS idx_allnba_season ON fact_all_nba(season_id);",
    "CREATE INDEX IF NOT EXISTS idx_allnba_vote_player ON fact_all_nba_vote(player_id);",
    "CREATE INDEX IF NOT EXISTS idx_allnba_vote_season ON fact_all_nba_vote(season_id);",
    # ------------------------------------------------------------------ #
    # Dimension: franchise/team history (SuperSonics→Thunder, etc.)       #
    # One row per city/name era for each franchise.                        #
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS dim_team_history (
        id                  INTEGER PRIMARY KEY,
        team_id             TEXT NOT NULL REFERENCES dim_team(team_id),
        team_city           TEXT NOT NULL,
        team_name           TEXT NOT NULL,
        team_abbrev         TEXT NOT NULL,
        season_founded      INTEGER NOT NULL,
        season_active_till  INTEGER NOT NULL,
        league              TEXT NOT NULL,  -- 'NBA' | 'BAA' | 'ABA'
        UNIQUE (team_id, season_founded)
    ) STRICT;
    """,
    # ------------------------------------------------------------------ #
    # Fact: team season summaries (pace, ratings, four factors, etc.)     #
    # Source: Basketball-Reference Team Summaries                          #
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS fact_team_season (
        id           INTEGER PRIMARY KEY,
        season_id    TEXT NOT NULL REFERENCES dim_season(season_id),
        bref_abbrev  TEXT NOT NULL,
        lg           TEXT,
        playoffs     INTEGER NOT NULL DEFAULT 0,
        w INTEGER, l INTEGER,
        pw REAL, pl REAL,
        mov REAL,
        sos REAL, srs REAL,
        o_rtg REAL, d_rtg REAL, n_rtg REAL,
        pace REAL,
        ts_pct REAL, e_fg_pct REAL,
        tov_pct REAL, orb_pct REAL, ft_fga REAL,
        opp_e_fg_pct REAL, opp_tov_pct REAL,
        drb_pct REAL, opp_ft_fga REAL,
        arena TEXT,
        attend INTEGER, attend_g INTEGER,
        UNIQUE (season_id, bref_abbrev)
    ) STRICT;
    """,
    # ------------------------------------------------------------------ #
    # Dimension: league-wide averages per season                          #
    # Required for PER, Win Shares, BPM, VORP formulas.                  #
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS dim_league_season (
        season_id    TEXT PRIMARY KEY REFERENCES dim_season(season_id),
        num_teams    INTEGER,
        avg_pace     REAL,
        avg_ortg     REAL,
        avg_pts      REAL,
        avg_fga      REAL,
        avg_fta      REAL,
        avg_trb      REAL,
        avg_ast      REAL,
        avg_stl      REAL,
        avg_blk      REAL,
        avg_tov      REAL
    ) STRICT;
    """,
    # ------------------------------------------------------------------ #
    # Fact: NBA draft pick history                                         #
    # Source: Basketball-Reference Draft Pick History                      #
    # Uses bref player/team identifiers (no FK to dim tables).            #
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS fact_draft (
        id                  INTEGER PRIMARY KEY,
        season_id           TEXT NOT NULL REFERENCES dim_season(season_id),
        draft_round         INTEGER,
        overall_pick        INTEGER,
        bref_team_abbrev    TEXT,
        bref_player_id      TEXT,
        player_name         TEXT,
        college             TEXT,
        lg                  TEXT,
        UNIQUE (season_id, overall_pick)
    ) STRICT;
    """,
    # ------------------------------------------------------------------ #
    # Fact: player season totals (1947–present)                           #
    # Source: Basketball-Reference Player Totals                           #
    # Uses bref_player_id — no FK to dim_player (covers ABA/BAA eras).   #
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS fact_player_season_stats (
        id              INTEGER PRIMARY KEY,
        bref_player_id  TEXT NOT NULL,
        season_id       TEXT NOT NULL REFERENCES dim_season(season_id),
        lg              TEXT,
        team_abbrev     TEXT,
        pos             TEXT,
        age             INTEGER,
        g INTEGER, gs INTEGER, mp INTEGER,
        fg INTEGER, fga INTEGER,
        x3p INTEGER, x3pa INTEGER,
        ft INTEGER, fta INTEGER,
        orb INTEGER, drb INTEGER, reb INTEGER,
        ast INTEGER, stl INTEGER, blk INTEGER,
        tov INTEGER, pf INTEGER, pts INTEGER,
        UNIQUE (bref_player_id, season_id, team_abbrev)
    ) STRICT;
    """,
    # ------------------------------------------------------------------ #
    # Fact: player advanced season stats (1947–present)                   #
    # PER, WS, BPM, VORP are precomputed by Basketball-Reference.         #
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS fact_player_advanced_season (
        id              INTEGER PRIMARY KEY,
        bref_player_id  TEXT NOT NULL,
        season_id       TEXT NOT NULL REFERENCES dim_season(season_id),
        team_abbrev     TEXT,
        pos TEXT, age INTEGER, g INTEGER, gs INTEGER, mp INTEGER,
        per REAL, ts_pct REAL, x3p_ar REAL, f_tr REAL,
        orb_pct REAL, drb_pct REAL, trb_pct REAL,
        ast_pct REAL, stl_pct REAL, blk_pct REAL,
        tov_pct REAL, usg_pct REAL,
        ows REAL, dws REAL, ws REAL, ws_48 REAL,
        obpm REAL, dbpm REAL, bpm REAL, vorp REAL,
        UNIQUE (bref_player_id, season_id, team_abbrev)
    ) STRICT;
    """,
    # ------------------------------------------------------------------ #
    # Fact: player shooting zone breakdown (1997–present)                 #
    # Source: Basketball-Reference Player Shooting                         #
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS fact_player_shooting_season (
        id              INTEGER PRIMARY KEY,
        bref_player_id  TEXT NOT NULL,
        season_id       TEXT NOT NULL REFERENCES dim_season(season_id),
        team_abbrev     TEXT,
        g INTEGER, mp INTEGER,
        avg_dist_fga    REAL,
        pct_fga_2p      REAL, pct_fga_0_3   REAL, pct_fga_3_10  REAL,
        pct_fga_10_16   REAL, pct_fga_16_3p REAL, pct_fga_3p    REAL,
        fg_pct_2p       REAL, fg_pct_0_3    REAL, fg_pct_3_10   REAL,
        fg_pct_10_16    REAL, fg_pct_16_3p  REAL, fg_pct_3p     REAL,
        pct_ast_2p      REAL, pct_ast_3p    REAL,
        pct_dunks_fga   REAL, num_dunks     INTEGER,
        pct_corner3_3pa REAL, corner3_pct   REAL,
        UNIQUE (bref_player_id, season_id, team_abbrev)
    ) STRICT;
    """,
    # ------------------------------------------------------------------ #
    # Fact: player play-by-play season aggregates (1997–present)          #
    # Source: Basketball-Reference Player Play By Play                     #
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS fact_player_pbp_season (
        id              INTEGER PRIMARY KEY,
        bref_player_id  TEXT NOT NULL,
        season_id       TEXT NOT NULL REFERENCES dim_season(season_id),
        team_abbrev     TEXT,
        g INTEGER, mp INTEGER,
        pg_pct REAL, sg_pct REAL, sf_pct REAL, pf_pct REAL, c_pct REAL,
        on_court_pm_per100  REAL,
        net_pm_per100       REAL,
        bad_pass_tov        INTEGER,
        lost_ball_tov       INTEGER,
        shoot_foul_committed INTEGER,
        off_foul_committed  INTEGER,
        shoot_foul_drawn    INTEGER,
        off_foul_drawn      INTEGER,
        pts_gen_by_ast      INTEGER,
        and1                INTEGER,
        fga_blocked         INTEGER,
        UNIQUE (bref_player_id, season_id, team_abbrev)
    ) STRICT;
    """,
    # ------------------------------------------------------------------ #
    # Indexes for new tables                                              #
    # ------------------------------------------------------------------ #
    "CREATE INDEX IF NOT EXISTS idx_team_hist_team  ON dim_team_history(team_id);",
    "CREATE INDEX IF NOT EXISTS idx_fts_season      ON fact_team_season(season_id);",
    "CREATE INDEX IF NOT EXISTS idx_fts_abbrev      ON fact_team_season(bref_abbrev);",
    "CREATE INDEX IF NOT EXISTS idx_draft_season    ON fact_draft(season_id);",
    "CREATE INDEX IF NOT EXISTS idx_draft_player    ON fact_draft(bref_player_id);",
    "CREATE INDEX IF NOT EXISTS idx_pss_player      ON fact_player_season_stats(bref_player_id);",
    "CREATE INDEX IF NOT EXISTS idx_pss_season      ON fact_player_season_stats(season_id);",
    "CREATE INDEX IF NOT EXISTS idx_pas_player      ON fact_player_advanced_season(bref_player_id);",
    "CREATE INDEX IF NOT EXISTS idx_pas_season      ON fact_player_advanced_season(season_id);",
    "CREATE INDEX IF NOT EXISTS idx_pshoot_player   ON fact_player_shooting_season(bref_player_id);",
    "CREATE INDEX IF NOT EXISTS idx_ppbp_player     ON fact_player_pbp_season(bref_player_id);",
    # ------------------------------------------------------------------ #
    # Internal: ETL Run Log                                               #
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS etl_run_log (
        id          INTEGER PRIMARY KEY,
        table_name  TEXT NOT NULL,
        season_id   TEXT,
        loader      TEXT NOT NULL,       -- e.g. 'game_logs.load_season'
        started_at  TEXT NOT NULL,       -- ISO-8601 UTC
        finished_at TEXT,
        row_count   INTEGER,
        status      TEXT NOT NULL        -- 'ok' | 'error'
    ) STRICT;
    """,
    "CREATE INDEX IF NOT EXISTS idx_runlog_table_season ON etl_run_log(table_name, season_id);",
]


def get_db_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Return a configured SQLite connection (WAL, FKs enabled)."""
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA foreign_keys=ON;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con


def init_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Create all tables and indexes; returns an open connection."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = get_db_connection(db_path)
    # One-time cleanup: remove misnamed index from earlier schema versions
    con.execute("DROP INDEX IF EXISTS idx_pgl_player_season;")
    for ddl in DDL_STATEMENTS:
        con.execute(ddl)
    # ALTER TABLE statements are not idempotent in SQLite; swallow the
    # OperationalError that fires when the column already exists.
    for alter in ALTER_STATEMENTS:
        try:
            con.execute(alter)
        except sqlite3.OperationalError as e:
            if "duplicate column name" in str(e).lower():
                logger.debug("ALTER skipped (already applied?): %s", alter)
            else:
                raise
    con.commit()
    return con


def rollback_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Execute rollback statements in reverse order."""
    con = sqlite3.connect(db_path)
    for sql in ROLLBACK_STATEMENTS:
        try:
            con.execute(sql)
        except sqlite3.OperationalError as e:
            logger.debug("Rollback statement failed: %s", e)
    con.commit()
    return con


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    con = init_db()
    tables = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
    ).fetchall()
    logger.info("Initialized database at: %s", DB_PATH)
    logger.info("Tables: %s", [t[0] for t in tables])
    con.close()
