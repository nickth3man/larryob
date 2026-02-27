"""Tests: Pydantic validation models (PlayerGameLogRow, TeamGameLogRow)."""

import pytest
from pydantic import ValidationError

from src.etl.schemas import PlayerGameLogRow, TeamGameLogRow

# ------------------------------------------------------------------ #
# PlayerGameLogRow — happy path                                       #
# ------------------------------------------------------------------ #


def test_player_game_log_row_valid_full_row() -> None:
    row = PlayerGameLogRow(
        game_id="0022300001",
        player_id="2544",
        team_id="1610612747",
        minutes_played=35.0,
        fgm=10,
        fga=20,
        fg3m=2,
        fg3a=5,
        ftm=3,
        fta=4,
        oreb=1,
        dreb=6,
        reb=7,
        ast=8,
        stl=1,
        blk=0,
        tov=3,
        pf=1,
        pts=25,
        plus_minus=10,
        starter=1,
    )
    assert row.game_id == "0022300001"
    assert row.pts == 25


def test_player_game_log_row_all_optional_fields_none() -> None:
    row = PlayerGameLogRow(
        game_id="001",
        player_id="111",
        team_id="222",
    )
    assert row.fgm is None
    assert row.pts is None
    assert row.starter is None


# ------------------------------------------------------------------ #
# PlayerGameLogRow — branch coverage: each validation rule           #
# ------------------------------------------------------------------ #


def test_player_game_log_row_raises_when_fgm_exceeds_fga() -> None:
    with pytest.raises(ValidationError, match="FGM cannot be greater than FGA"):
        PlayerGameLogRow(
            game_id="001",
            player_id="1",
            team_id="2",
            fgm=15,
            fga=10,
        )


def test_player_game_log_row_raises_when_fg3m_exceeds_fg3a() -> None:
    with pytest.raises(ValidationError, match="FG3M cannot be greater than FG3A"):
        PlayerGameLogRow(
            game_id="001",
            player_id="1",
            team_id="2",
            fg3m=5,
            fg3a=3,
        )


def test_player_game_log_row_raises_when_ftm_exceeds_fta() -> None:
    with pytest.raises(ValidationError, match="FTM cannot be greater than FTA"):
        PlayerGameLogRow(
            game_id="001",
            player_id="1",
            team_id="2",
            ftm=7,
            fta=5,
        )


def test_player_game_log_row_raises_when_pts_negative() -> None:
    with pytest.raises(ValidationError, match="greater than or equal to 0"):
        PlayerGameLogRow(
            game_id="001",
            player_id="1",
            team_id="2",
            pts=-1,
        )


def test_player_game_log_row_raises_when_reb_sum_mismatch() -> None:
    with pytest.raises(ValidationError, match="OREB \\+ DREB must equal REB"):
        PlayerGameLogRow(
            game_id="001",
            player_id="1",
            team_id="2",
            oreb=2,
            dreb=3,
            reb=10,
        )


def test_player_game_log_row_raises_when_minutes_negative() -> None:
    with pytest.raises(ValidationError, match="minutes_played"):
        PlayerGameLogRow(
            game_id="001",
            player_id="1",
            team_id="2",
            minutes_played=-1.0,
        )


def test_player_game_log_row_passes_when_all_nullable_fields_are_none() -> None:
    """Null fields must bypass all comparisons — no ValidationError."""
    row = PlayerGameLogRow(
        game_id="001",
        player_id="1",
        team_id="2",
        fgm=None,
        fga=None,
        fg3m=None,
        fg3a=None,
        ftm=None,
        fta=None,
        oreb=None,
        dreb=None,
        reb=None,
        pts=None,
        minutes_played=None,
    )
    assert row.fgm is None


def test_player_game_log_row_passes_when_fgm_equals_fga() -> None:
    row = PlayerGameLogRow(
        game_id="001",
        player_id="1",
        team_id="2",
        fgm=10,
        fga=10,
    )
    assert row.fgm == row.fga


# ------------------------------------------------------------------ #
# TeamGameLogRow — happy path                                        #
# ------------------------------------------------------------------ #


def test_team_game_log_row_valid_full_row() -> None:
    row = TeamGameLogRow(
        game_id="0022300001",
        team_id="1610612747",
        fgm=42,
        fga=85,
        fg3m=12,
        fg3a=30,
        ftm=20,
        fta=25,
        oreb=8,
        dreb=35,
        reb=43,
        ast=24,
        stl=7,
        blk=4,
        tov=13,
        pf=18,
        pts=116,
        plus_minus=10,
    )
    assert row.team_id == "1610612747"
    assert row.pts == 116


# ------------------------------------------------------------------ #
# TeamGameLogRow — validation rules                                  #
# ------------------------------------------------------------------ #


def test_team_game_log_row_raises_when_fgm_exceeds_fga() -> None:
    with pytest.raises(ValidationError, match="FGM cannot be greater than FGA"):
        TeamGameLogRow(game_id="001", team_id="1", fgm=50, fga=30)


def test_team_game_log_row_raises_when_fg3m_exceeds_fg3a() -> None:
    with pytest.raises(ValidationError, match="FG3M cannot be greater than FG3A"):
        TeamGameLogRow(game_id="001", team_id="1", fg3m=20, fg3a=10)


def test_team_game_log_row_raises_when_ftm_exceeds_fta() -> None:
    with pytest.raises(ValidationError, match="FTM cannot be greater than FTA"):
        TeamGameLogRow(game_id="001", team_id="1", ftm=30, fta=20)


def test_team_game_log_row_raises_when_pts_negative() -> None:
    with pytest.raises(ValidationError, match="greater than or equal to 0"):
        TeamGameLogRow(game_id="001", team_id="1", pts=-5)


def test_team_game_log_row_raises_when_reb_sum_mismatch() -> None:
    with pytest.raises(ValidationError, match="OREB \\+ DREB must equal REB"):
        TeamGameLogRow(game_id="001", team_id="1", oreb=5, dreb=10, reb=100)


def test_team_game_log_row_passes_when_pts_is_zero() -> None:
    row = TeamGameLogRow(game_id="001", team_id="1", pts=0)
    assert row.pts == 0
