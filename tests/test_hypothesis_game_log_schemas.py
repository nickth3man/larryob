"""
Property-based tests for game log schema validators.

Tests BaseGameLogRow, PlayerGameLogRow, and TeamGameLogRow, covering
shooting and rebound validation and edge cases.
"""

import pytest
from hypothesis import assume, given
from hypothesis import strategies as st
from pydantic import ValidationError

from src.etl.schemas import (
    BaseGameLogRow,
    PlayerGameLogRow,
    TeamGameLogRow,
)
from tests.hypothesis_strategies import (
    invalid_rebound_stats,
    invalid_shooting_stats,
    rebound_stats,
    shooting_stats,
)

# =============================================================================
# BaseGameLogRow Validator Tests
# =============================================================================


class TestBaseGameLogRowValidators:
    """Tests for BaseGameLogRow shooting and rebound validators."""

    @given(shooting_stats())
    def test_valid_shooting_stats_pass(self, stats):
        """
        Property: Valid shooting stats (made <= attempted) should always pass validation.
        """
        row = BaseGameLogRow(**stats)
        assert row.fgm <= row.fga
        assert row.fg3m <= row.fg3a
        assert row.ftm <= row.fta

    @given(invalid_shooting_stats())
    def test_invalid_fgm_fga_raises(self, stats):
        """
        Property: When FGM > FGA, validation should raise ValueError.
        """
        with pytest.raises(ValidationError) as exc_info:
            BaseGameLogRow(**stats)
        assert "FGM cannot be greater than FGA" in str(exc_info.value)

    @given(rebound_stats())
    def test_valid_rebound_stats_pass(self, stats):
        """
        Property: Valid rebound stats (oreb + dreb == reb) should always pass.
        """
        row = BaseGameLogRow(**stats)
        if row.oreb is not None and row.dreb is not None and row.reb is not None:
            assert row.oreb + row.dreb == row.reb

    @given(invalid_rebound_stats())
    def test_invalid_rebounds_raises(self, stats):
        """
        Property: When oreb + dreb != reb, validation should raise ValueError.
        """
        with pytest.raises(ValidationError) as exc_info:
            BaseGameLogRow(**stats)
        assert "OREB + DREB must equal REB" in str(exc_info.value)

    @given(
        st.integers(min_value=0, max_value=30),
        st.integers(min_value=0, max_value=20),
        st.integers(min_value=0, max_value=20),
    )
    def test_three_point_constraint(self, fga, fg3a, fgm):
        """
        Property: FG3A cannot exceed FGA (3-pointers are a subset of field goals).
        """
        assume(fg3a <= fga)
        assume(fgm <= fga)

        # This should not raise when fg3m <= fg3a and fgm <= fga
        row = BaseGameLogRow(fga=fga, fg3a=fg3a, fgm=fgm, fg3m=0, ftm=0, fta=0)
        assert row.fg3a <= row.fga


# =============================================================================
# PlayerGameLogRow Tests
# =============================================================================


class TestPlayerGameLogRow:
    """Tests for PlayerGameLogRow model."""

    @given(
        st.text(min_size=1, max_size=20),  # game_id
        st.text(min_size=1, max_size=20),  # player_id
        st.text(min_size=1, max_size=20),  # team_id
        shooting_stats(),
    )
    def test_player_game_log_with_valid_shooting(self, game_id, player_id, team_id, stats):
        """
        Property: Player game logs with valid shooting stats should validate.
        """
        data = {
            "game_id": game_id,
            "player_id": player_id,
            "team_id": team_id,
            **stats,
        }
        row = PlayerGameLogRow(**data)
        assert row.fgm <= row.fga

    @given(
        st.text(min_size=1, max_size=20),
        st.text(min_size=1, max_size=20),
        st.text(min_size=1, max_size=20),
        st.integers(min_value=0, max_value=100),
        st.integers(min_value=0, max_value=100),
    )
    def test_minutes_played_non_negative(self, game_id, player_id, team_id, minutes, pts):
        """
        Property: Minutes played should always be non-negative.
        """
        row = PlayerGameLogRow(
            game_id=game_id,
            player_id=player_id,
            team_id=team_id,
            minutes_played=float(minutes),
            pts=pts,
        )
        assert row.minutes_played >= 0


# =============================================================================
# TeamGameLogRow Tests
# =============================================================================


class TestTeamGameLogRow:
    """Tests for TeamGameLogRow model."""

    @given(
        st.text(min_size=1, max_size=20),
        st.text(min_size=1, max_size=20),
        shooting_stats(),
        rebound_stats(),
    )
    def test_team_game_log_valid_stats(self, game_id, team_id, shooting, rebounds):
        """
        Property: Team game logs with valid aggregated stats should validate.
        """
        data = {
            "game_id": game_id,
            "team_id": team_id,
            **shooting,
            **rebounds,
        }
        row = TeamGameLogRow(**data)
        assert row.fgm <= row.fga
        if row.oreb is not None and row.dreb is not None and row.reb is not None:
            assert row.oreb + row.dreb == row.reb


# =============================================================================
# Edge Case Tests
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    @given(st.just(0))
    def test_zero_values_accepted(self, zero):
        """
        Property: Zero values should be accepted for all stats.
        """
        row = BaseGameLogRow(
            fgm=zero,
            fga=zero,
            fg3m=zero,
            fg3a=zero,
            ftm=zero,
            fta=zero,
            oreb=zero,
            dreb=zero,
            reb=zero,
        )
        assert row.fgm == 0
        assert row.fga == 0

    @given(st.none())
    def test_none_values_accepted(self, none_val):
        """
        Property: None values should be accepted for optional fields.
        """
        row = PlayerGameLogRow(
            game_id="0022300001",
            player_id="2544",
            team_id="1610612747",
            minutes_played=none_val,
            starter=none_val,
            fgm=none_val,
            fga=none_val,
        )
        assert row.minutes_played is None
        assert row.starter is None

    @given(st.integers(min_value=0, max_value=10000))
    def test_large_stat_values(self, large_value):
        """
        Property: Large but valid stat values should be accepted.
        """
        row = BaseGameLogRow(
            pts=large_value,
            reb=large_value,
            ast=large_value,
        )
        assert row.pts == large_value
        assert row.reb == large_value
        assert row.ast == large_value
