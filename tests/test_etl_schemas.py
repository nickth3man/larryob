"""Tests for src.etl.schemas — Pydantic row models and validation rules."""

import pytest
from pydantic import ValidationError

from src.etl.schemas import (
    BaseGameLogRow,
    FactGameRow,
    FactPlayerAdvancedSeasonRow,
    FactPlayerSeasonStatsRow,
    FactPlayerShootingSeasonRow,
    FactSalaryRow,
    PlayerGameLogRow,
    TeamGameLogRow,
)

# ------------------------------------------------------------------ #
# BaseGameLogRow / shooting invariants                                #
# ------------------------------------------------------------------ #


def test_base_game_log_valid_row():
    row = BaseGameLogRow(fgm=8, fga=15, fg3m=2, fg3a=5, ftm=3, fta=4, oreb=2, dreb=4, reb=6, pts=21)
    assert row.pts == 21


def test_base_game_log_fgm_greater_than_fga_fails():
    with pytest.raises(ValidationError, match="FGM cannot be greater than FGA"):
        BaseGameLogRow(fgm=10, fga=8)


def test_base_game_log_fg3m_greater_than_fg3a_fails():
    with pytest.raises(ValidationError, match="FG3M cannot be greater than FG3A"):
        BaseGameLogRow(fg3m=5, fg3a=3)


def test_base_game_log_ftm_greater_than_fta_fails():
    with pytest.raises(ValidationError, match="FTM cannot be greater than FTA"):
        BaseGameLogRow(ftm=4, fta=2)


def test_base_game_log_reb_mismatch_fails():
    with pytest.raises(ValidationError, match="OREB \\+ DREB must equal REB"):
        BaseGameLogRow(oreb=2, dreb=4, reb=10)


def test_base_game_log_reb_consistent_passes():
    row = BaseGameLogRow(oreb=2, dreb=4, reb=6)
    assert row.reb == 6


def test_base_game_log_none_stats_are_allowed():
    # Historical rows may have all None
    row = BaseGameLogRow(fgm=None, fga=None, pts=None)
    assert row.fgm is None


def test_base_game_log_negative_stat_fails():
    with pytest.raises(ValidationError):
        BaseGameLogRow(pts=-1)


# ------------------------------------------------------------------ #
# PlayerGameLogRow                                                    #
# ------------------------------------------------------------------ #


def _player_row(**kwargs):
    defaults = dict(game_id="G1", player_id="P1", team_id="T1")
    defaults.update(kwargs)
    return PlayerGameLogRow(**defaults)


def test_player_game_log_minimal():
    row = _player_row()
    assert row.game_id == "G1"


def test_player_game_log_valid_stats():
    row = _player_row(pts=25, reb=7, ast=3, fgm=9, fga=18)
    assert row.pts == 25


def test_player_game_log_minutes_must_be_non_negative():
    with pytest.raises(ValidationError):
        _player_row(minutes_played=-1.0)


def test_player_game_log_minutes_zero_is_valid():
    row = _player_row(minutes_played=0.0)
    assert row.minutes_played == 0.0


def test_player_game_log_extra_fields_ignored():
    # model_config extra="ignore" should swallow unknown keys
    row = _player_row(unknown_field="x")
    assert not hasattr(row, "unknown_field")


# ------------------------------------------------------------------ #
# TeamGameLogRow                                                      #
# ------------------------------------------------------------------ #


def test_team_game_log_minimal():
    row = TeamGameLogRow(game_id="G1", team_id="T1")
    assert row.team_id == "T1"


def test_team_game_log_valid_stats():
    row = TeamGameLogRow(game_id="G1", team_id="T1", pts=110, fgm=42, fga=90)
    assert row.pts == 110


def test_team_game_log_fgm_exceeds_fga_fails():
    with pytest.raises(ValidationError, match="FGM cannot be greater than FGA"):
        TeamGameLogRow(game_id="G1", team_id="T1", fgm=50, fga=40)


# ------------------------------------------------------------------ #
# FactGameRow                                                         #
# ------------------------------------------------------------------ #


def test_fact_game_row_valid():
    row = FactGameRow(game_id="G1", home_score=110, away_score=105, game_date="2023-10-24")
    assert row.home_score == 110


def test_fact_game_row_negative_score_fails():
    with pytest.raises(ValidationError):
        FactGameRow(game_id="G1", home_score=-5)


def test_fact_game_row_zero_score_is_valid():
    row = FactGameRow(game_id="G1", home_score=0, away_score=0)
    assert row.home_score == 0


def test_fact_game_row_none_scores_allowed():
    row = FactGameRow(game_id="G1", home_score=None, away_score=None)
    assert row.home_score is None


def test_fact_game_row_date_parsing():
    row = FactGameRow(game_id="G1", game_date="2024-01-15")
    from datetime import date

    assert row.game_date == date(2024, 1, 15)


# ------------------------------------------------------------------ #
# FactSalaryRow                                                       #
# ------------------------------------------------------------------ #


def test_fact_salary_row_valid():
    row = FactSalaryRow(player_id="P1", team_id="T1", season_id="2023-24", salary=5_000_000)
    assert row.salary == 5_000_000


def test_fact_salary_row_zero_salary_allowed():
    row = FactSalaryRow(player_id="P1", team_id="T1", season_id="2023-24", salary=0)
    assert row.salary == 0


def test_fact_salary_row_negative_salary_fails():
    with pytest.raises(ValidationError):
        FactSalaryRow(player_id="P1", team_id="T1", season_id="2023-24", salary=-100)


def test_fact_salary_row_none_salary_allowed():
    row = FactSalaryRow(player_id="P1", team_id="T1", season_id="2023-24", salary=None)
    assert row.salary is None


# ------------------------------------------------------------------ #
# FactPlayerSeasonStatsRow                                            #
# ------------------------------------------------------------------ #


def test_fact_player_season_stats_valid():
    row = FactPlayerSeasonStatsRow(fg=400, fga=900, pts=1200)
    assert row.pts == 1200


def test_fact_player_season_stats_fg_exceeds_fga_fails():
    with pytest.raises(ValidationError, match="fg > fga"):
        FactPlayerSeasonStatsRow(fg=500, fga=400)


def test_fact_player_season_stats_3p_exceeds_3pa_fails():
    with pytest.raises(ValidationError, match="x3p > x3pa"):
        FactPlayerSeasonStatsRow(x3p=200, x3pa=100)


def test_fact_player_season_stats_ft_exceeds_fta_fails():
    with pytest.raises(ValidationError, match="ft > fta"):
        FactPlayerSeasonStatsRow(ft=300, fta=200)


def test_fact_player_season_stats_none_values_pass():
    row = FactPlayerSeasonStatsRow(fg=None, fga=None, pts=None)
    assert row.fg is None


# ------------------------------------------------------------------ #
# FactPlayerAdvancedSeasonRow                                         #
# ------------------------------------------------------------------ #


def test_fact_player_advanced_valid():
    row = FactPlayerAdvancedSeasonRow(ts_pct=0.58, usg_pct=0.25)
    assert row.ts_pct == pytest.approx(0.58)


def test_fact_player_advanced_ts_pct_above_bound_fails():
    with pytest.raises(ValidationError):
        FactPlayerAdvancedSeasonRow(ts_pct=2.0)  # max is 1.5


def test_fact_player_advanced_negative_pct_fails():
    with pytest.raises(ValidationError):
        FactPlayerAdvancedSeasonRow(orb_pct=-0.1)


def test_fact_player_advanced_none_values_pass():
    row = FactPlayerAdvancedSeasonRow(ts_pct=None, usg_pct=None)
    assert row.ts_pct is None


# ------------------------------------------------------------------ #
# FactPlayerShootingSeasonRow                                         #
# ------------------------------------------------------------------ #


def test_fact_player_shooting_valid_zones():
    row = FactPlayerShootingSeasonRow(
        pct_fga_0_3=0.30,
        pct_fga_3_10=0.15,
        pct_fga_10_16=0.10,
        pct_fga_16_3p=0.15,
        pct_fga_3p=0.30,
    )
    assert row.pct_fga_0_3 == pytest.approx(0.30)


def test_fact_player_shooting_zones_sum_near_one_passes():
    # 0.30 + 0.15 + 0.10 + 0.15 + 0.30 = 1.00
    FactPlayerShootingSeasonRow(
        pct_fga_0_3=0.30,
        pct_fga_3_10=0.15,
        pct_fga_10_16=0.10,
        pct_fga_16_3p=0.15,
        pct_fga_3p=0.30,
    )


def test_fact_player_shooting_zones_sum_far_from_one_fails():
    with pytest.raises(ValidationError, match="zone_sum"):
        FactPlayerShootingSeasonRow(
            pct_fga_0_3=0.10,
            pct_fga_3_10=0.10,
            pct_fga_10_16=0.10,
            pct_fga_16_3p=0.10,
            pct_fga_3p=0.10,  # sum = 0.50, far from 1.0
        )


def test_fact_player_shooting_partial_zones_skips_validation():
    # Validation only runs when ALL zones are set
    row = FactPlayerShootingSeasonRow(pct_fga_0_3=0.99)
    assert row.pct_fga_0_3 == pytest.approx(0.99)
