"""Tests: backfill bulk play-by-play loader."""

import pandas as pd

from src.etl.backfill._pbp_bulk import load_bulk_pbp, load_bulk_pbp_season

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Game IDs with no leading zeros so they survive the CSV integer round-trip.
# pandas infers all-digit CSV values as integers; str(int("0022300001")) == "22300001"
# which breaks the FK to fact_game. IDs starting with "9" have no leading zeros to lose.
_GAME_A = "9022300001"
_GAME_B = "9022300002"
_GAME_OTHER = "9019000001"  # belongs to a different (unseeded) season


def _make_pbp_csv(tmp_path, game_id=_GAME_A):
    """Write a minimal PBP CSV and return the pbp/ dir path.

    Player and team IDs are intentionally 0 so they transform to None and
    avoid foreign-key constraints on dim_player / dim_team.
    The caller is responsible for seeding fact_game for *game_id*.
    """
    pbp_dir = tmp_path / "pbp"
    pbp_dir.mkdir(parents=True, exist_ok=True)
    csv_path = pbp_dir / f"{game_id}.csv"
    pd.DataFrame(
        {
            "GAME_ID": [game_id, game_id],
            "EVENTNUM": [1, 2],
            "PERIOD": [1, 1],
            "PCTIMESTRING": ["11:48", "11:32"],
            "WCTIMESTRING": ["8:04 PM", "8:05 PM"],
            "EVENTMSGTYPE": [1, 2],
            "EVENTMSGACTIONTYPE": [1, 5],
            "PLAYER1_ID": [0, 0],
            "PLAYER2_ID": [0, 0],
            "PLAYER3_ID": [0, 0],
            "PLAYER1_TEAM_ID": [0, 0],
            "PLAYER2_TEAM_ID": [0, 0],
            "HOMEDESCRIPTION": ["event 1", None],
            "VISITORDESCRIPTION": [None, "event 2"],
            "NEUTRALDESCRIPTION": [None, None],
            "SCORE": ["2 - 0", "2 - 0"],
            "SCOREMARGIN": [2, 2],
        }
    ).to_csv(csv_path, index=False)
    return pbp_dir


def _seed_fact_game(con, season, game_id, game_date="2023-10-24"):
    """Insert dim_season, dim_team, and fact_game rows for FK compliance.

    fact_game has UNIQUE(home_team_id, away_team_id, game_date) — callers
    must pass distinct game_date values when seeding multiple games with the
    same teams to avoid silent INSERT OR IGNORE drops.
    """
    con.execute(
        "INSERT OR IGNORE INTO dim_season (season_id, start_year, end_year) VALUES (?,?,?)",
        (season, int(season[:4]), int(season[:4]) + 1),
    )
    con.execute(
        """INSERT OR IGNORE INTO dim_team
           (team_id, abbreviation, full_name, city, nickname)
           VALUES ('1610612747','LAL','Los Angeles Lakers','Los Angeles','Lakers')""",
    )
    con.execute(
        """INSERT OR IGNORE INTO dim_team
           (team_id, abbreviation, full_name, city, nickname)
           VALUES ('1610612744','GSW','Golden State Warriors','San Francisco','Warriors')""",
    )
    con.execute(
        """INSERT OR IGNORE INTO fact_game
           (game_id, season_id, game_date, home_team_id, away_team_id,
            home_score, away_score, season_type, status)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (
            game_id,
            season,
            game_date,
            "1610612747",
            "1610612744",
            120,
            110,
            "Regular Season",
            "Final",
        ),
    )
    con.commit()


# ---------------------------------------------------------------------------
# load_bulk_pbp tests
# ---------------------------------------------------------------------------


def test_load_bulk_pbp_missing_raw_dir_returns_zero(sqlite_con, tmp_path):
    """Passing a raw_dir that does not exist returns 0 gracefully."""
    missing = tmp_path / "does_not_exist"
    assert load_bulk_pbp(sqlite_con, raw_dir=missing) == 0


def test_load_bulk_pbp_empty_pbp_dir_returns_zero(sqlite_con, tmp_path):
    """An existing but empty raw/pbp/ dir returns 0."""
    (tmp_path / "pbp").mkdir(parents=True)
    assert load_bulk_pbp(sqlite_con, raw_dir=tmp_path) == 0


def test_load_bulk_pbp_missing_pbp_subdir_returns_zero(sqlite_con, tmp_path):
    """raw_dir exists but has no pbp/ subdir returns 0."""
    assert load_bulk_pbp(sqlite_con, raw_dir=tmp_path) == 0


def test_load_bulk_pbp_inserts_rows(sqlite_con, tmp_path):
    """Valid PBP CSV -> rows inserted into fact_play_by_play."""
    _seed_fact_game(sqlite_con, "2023-24", _GAME_A)
    _make_pbp_csv(tmp_path, game_id=_GAME_A)
    result = load_bulk_pbp(sqlite_con, raw_dir=tmp_path)
    assert result == 2
    count = sqlite_con.execute("SELECT COUNT(*) FROM fact_play_by_play").fetchone()[0]
    assert count == 2


def test_load_bulk_pbp_idempotent(sqlite_con, tmp_path):
    """Running load_bulk_pbp twice does not duplicate rows (INSERT OR IGNORE)."""
    _seed_fact_game(sqlite_con, "2023-24", _GAME_A)
    _make_pbp_csv(tmp_path, game_id=_GAME_A)
    first = load_bulk_pbp(sqlite_con, raw_dir=tmp_path)
    second = load_bulk_pbp(sqlite_con, raw_dir=tmp_path)
    assert first == 2
    assert second == 0
    assert sqlite_con.execute("SELECT COUNT(*) FROM fact_play_by_play").fetchone()[0] == 2


def test_load_bulk_pbp_multiple_files(sqlite_con, tmp_path):
    """Multiple CSV files in pbp/ are all loaded."""
    _seed_fact_game(sqlite_con, "2023-24", _GAME_A, game_date="2023-10-24")
    _seed_fact_game(sqlite_con, "2023-24", _GAME_B, game_date="2023-10-25")  # distinct date
    _make_pbp_csv(tmp_path, game_id=_GAME_A)
    _make_pbp_csv(tmp_path, game_id=_GAME_B)
    result = load_bulk_pbp(sqlite_con, raw_dir=tmp_path)
    assert result == 4
    assert sqlite_con.execute("SELECT COUNT(*) FROM fact_play_by_play").fetchone()[0] == 4


# ---------------------------------------------------------------------------
# load_bulk_pbp_season tests
# ---------------------------------------------------------------------------


def test_load_bulk_pbp_season_already_loaded_skips(sqlite_con, tmp_path):
    """Season already recorded as ok -> loader returns 0 immediately."""
    season = "2023-24"
    _make_pbp_csv(tmp_path, game_id=_GAME_A)
    _seed_fact_game(sqlite_con, season, _GAME_A)
    sqlite_con.execute(
        """INSERT INTO etl_run_log
           (table_name, season_id, loader, started_at, finished_at, row_count, status)
           VALUES ('fact_play_by_play', ?, 'backfill.pbp_bulk',
                   '2024-01-01T00:00:00', '2024-01-01T00:01:00', 2, 'ok')""",
        (season,),
    )
    sqlite_con.commit()
    assert load_bulk_pbp_season(sqlite_con, season, raw_dir=tmp_path) == 0


def test_load_bulk_pbp_season_filters_to_season_game_ids(sqlite_con, tmp_path):
    """Only rows whose game_id belongs to the requested season are inserted."""
    season = "2023-24"
    _make_pbp_csv(tmp_path, game_id=_GAME_A)
    _make_pbp_csv(tmp_path, game_id=_GAME_OTHER)
    _seed_fact_game(sqlite_con, season, _GAME_A)  # only _GAME_A is in this season

    result = load_bulk_pbp_season(sqlite_con, season, raw_dir=tmp_path)
    assert result == 2
    rows = sqlite_con.execute("SELECT DISTINCT game_id FROM fact_play_by_play").fetchall()
    assert rows == [(_GAME_A,)]


def test_load_bulk_pbp_season_no_games_returns_zero(sqlite_con, tmp_path):
    """Season with no games in fact_game -> 0 rows inserted."""
    _make_pbp_csv(tmp_path)
    assert load_bulk_pbp_season(sqlite_con, "1999-00", raw_dir=tmp_path) == 0


def test_load_bulk_pbp_season_missing_dir_returns_zero(sqlite_con, tmp_path):
    """Missing raw_dir -> 0 even when season has games."""
    season = "2023-24"
    _seed_fact_game(sqlite_con, season, _GAME_A)
    assert load_bulk_pbp_season(sqlite_con, season, raw_dir=tmp_path / "nope") == 0
