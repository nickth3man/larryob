"""
Property-based tests for Pydantic schema validators.

These tests verify that the schema validators correctly enforce business rules
across a wide range of generated inputs, catching edge cases that might be
missed with example-based testing.
"""

import pytest
from hypothesis import assume, given
from hypothesis import strategies as st
from pydantic import ValidationError

from src.etl.schemas import (
    BaseGameLogRow,
    FactAllNbaRow,
    FactAllNbaVoteRow,
    FactAllStarRow,
    FactDraftRow,
    FactPlayerAwardRow,
    FactPlayerSeasonStatsRow,
    FactPlayerShootingSeasonRow,
    FactRosterRow,
    PlayerGameLogRow,
    TeamGameLogRow,
)
from tests.hypothesis_strategies import (
    all_nba_teams,
    all_nba_votes,
    award_votes,
    draft_picks,
    game_log_dicts,
    invalid_award_votes,
    invalid_rebound_stats,
    invalid_shooting_stats,
    rebound_stats,
    roster_entries,
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

    @given(game_log_dicts())
    def test_valid_game_log_dicts(self, data):
        """
        Property: Valid game log dictionaries should always create valid rows.
        """
        row = PlayerGameLogRow(**data)
        assert row.game_id == data["game_id"]
        assert row.player_id == data["player_id"]
        assert row.team_id == data["team_id"]

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
# FactPlayerSeasonStatsRow Tests
# =============================================================================


class TestFactPlayerSeasonStatsRow:
    """Tests for FactPlayerSeasonStatsRow shooting validators."""

    @given(
        st.tuples(
            st.integers(min_value=0, max_value=2000), st.integers(min_value=0, max_value=2000)
        ).map(lambda x: (min(x), max(x))),
        st.tuples(
            st.integers(min_value=0, max_value=1000), st.integers(min_value=0, max_value=1000)
        ).map(lambda x: (min(x), max(x))),
        st.tuples(
            st.integers(min_value=0, max_value=1000), st.integers(min_value=0, max_value=1000)
        ).map(lambda x: (min(x), max(x))),
    )
    def test_season_stats_valid_shooting(self, fg_data, x3p_data, ft_data):
        """
        Property: Season stats with valid shooting percentages should validate.
        """
        fg, fga = fg_data
        x3p, x3pa = x3p_data
        ft, fta = ft_data

        row = FactPlayerSeasonStatsRow(fg=fg, fga=fga, x3p=x3p, x3pa=x3pa, ft=ft, fta=fta)
        assert row.fg <= row.fga
        assert row.x3p <= row.x3pa
        assert row.ft <= row.fta

    @given(
        st.integers(min_value=101, max_value=200),
        st.integers(min_value=0, max_value=100),
    )
    def test_season_stats_invalid_fg_raises(self, fg, fga):
        """
        Property: When fg > fga, validation should raise ValueError.
        """
        with pytest.raises(ValidationError) as exc_info:
            FactPlayerSeasonStatsRow(fg=fg, fga=fga)
        assert "fg > fga" in str(exc_info.value)


# =============================================================================
# FactPlayerShootingSeasonRow Tests
# =============================================================================


class TestFactPlayerShootingSeasonRow:
    """Tests for FactPlayerShootingSeasonRow zone validators."""

    @given(
        st.floats(min_value=0.0, max_value=0.5),
        st.floats(min_value=0.0, max_value=0.3),
        st.floats(min_value=0.0, max_value=0.2),
        st.floats(min_value=0.0, max_value=0.3),
        st.floats(min_value=0.0, max_value=0.5),
    )
    def test_zone_percentages_sum_to_one(self, pct_0_3, pct_3_10, pct_10_16, pct_16_3p, pct_3p):
        """
        Property: When all zone percentages are provided and sum to ~1.0, validation passes.
        """
        # Normalize to sum to 1.0
        total = pct_0_3 + pct_3_10 + pct_10_16 + pct_16_3p + pct_3p
        assume(total > 0)  # Avoid division by zero

        row = FactPlayerShootingSeasonRow(
            pct_fga_0_3=pct_0_3 / total,
            pct_fga_3_10=pct_3_10 / total,
            pct_fga_10_16=pct_10_16 / total,
            pct_fga_16_3p=pct_16_3p / total,
            pct_fga_3p=pct_3p / total,
        )
        # Sum should be within 0.05 of 1.0
        zone_sum = sum(
            [
                row.pct_fga_0_3 or 0,
                row.pct_fga_3_10 or 0,
                row.pct_fga_10_16 or 0,
                row.pct_fga_16_3p or 0,
                row.pct_fga_3p or 0,
            ]
        )
        assert abs(zone_sum - 1.0) <= 0.05

    @given(
        st.floats(min_value=0.1, max_value=0.5),
        st.floats(min_value=0.1, max_value=0.5),
        st.floats(min_value=0.1, max_value=0.5),
        st.floats(min_value=0.1, max_value=0.5),
        st.floats(min_value=0.1, max_value=0.5),
    )
    def test_zone_percentages_too_high_raises(self, a, b, c, d, e):
        """
        Property: When zone percentages sum to more than 1.05, validation raises.
        """
        assume(a + b + c + d + e > 1.05)

        with pytest.raises(ValidationError) as exc_info:
            FactPlayerShootingSeasonRow(
                pct_fga_0_3=a,
                pct_fga_3_10=b,
                pct_fga_10_16=c,
                pct_fga_16_3p=d,
                pct_fga_3p=e,
            )
        assert "zone_sum" in str(exc_info.value)


# =============================================================================
# FactPlayerAwardRow Tests
# =============================================================================


class TestFactPlayerAwardRow:
    """Tests for FactPlayerAwardRow vote validators."""

    @given(award_votes())
    def test_valid_award_votes_pass(self, votes):
        """
        Property: Valid award votes (received <= possible) should always pass.
        """
        received, possible = votes
        row = FactPlayerAwardRow(
            player_id="123",
            season_id="2023-24",
            award_name="MVP",
            award_type="individual",
            votes_received=received,
            votes_possible=possible,
        )
        assert row.votes_received <= row.votes_possible

    @given(invalid_award_votes())
    def test_invalid_award_votes_raises(self, votes):
        """
        Property: When votes_received > votes_possible, validation should raise.
        """
        received, possible = votes
        with pytest.raises(ValidationError) as exc_info:
            FactPlayerAwardRow(
                player_id="123",
                season_id="2023-24",
                award_name="MVP",
                award_type="individual",
                votes_received=received,
                votes_possible=possible,
            )
        assert "votes_received cannot exceed votes_possible" in str(exc_info.value)


# =============================================================================
# FactAllStarRow Tests
# =============================================================================


class TestFactAllStarRow:
    """Tests for FactAllStarRow flag validators."""

    @given(st.sampled_from([0, 1, None]))
    def test_valid_is_starter_values(self, is_starter):
        """
        Property: is_starter should only accept 0, 1, or None.
        """
        row = FactAllStarRow(
            player_id="123",
            season_id="2023-24",
            is_starter=is_starter,
        )
        assert row.is_starter in (0, 1, None)

    @given(st.integers().filter(lambda x: x not in (0, 1, None)))
    def test_invalid_is_starter_raises(self, is_starter):
        """
        Property: is_starter values other than 0, 1, None should raise.
        """
        with pytest.raises(ValidationError) as exc_info:
            FactAllStarRow(
                player_id="123",
                season_id="2023-24",
                is_starter=is_starter,
            )
        assert "is_starter must be 0, 1, or None" in str(exc_info.value)

    @given(st.sampled_from([0, 1]))
    def test_valid_is_replacement_values(self, is_replacement):
        """
        Property: is_replacement should only accept 0 or 1.
        """
        row = FactAllStarRow(
            player_id="123",
            season_id="2023-24",
            is_replacement=is_replacement,
        )
        assert row.is_replacement in (0, 1)


# =============================================================================
# FactAllNbaRow Tests
# =============================================================================


class TestFactAllNbaRow:
    """Tests for FactAllNbaRow validators."""

    @given(all_nba_teams())
    def test_valid_all_nba_teams(self, data):
        """
        Property: Valid All-NBA team data should always pass.
        """
        row = FactAllNbaRow(**data)
        assert row.team_type.strip() != ""
        if row.team_number is not None:
            assert row.team_number in (1, 2, 3)

    @given(st.text(max_size=0))
    def test_empty_team_type_raises(self, empty_team_type):
        """
        Property: Empty team_type should raise validation error.
        """
        with pytest.raises(ValidationError) as exc_info:
            FactAllNbaRow(
                player_id="123",
                season_id="2023-24",
                team_type=empty_team_type,
            )
        assert "team_type must not be empty" in str(exc_info.value)


# =============================================================================
# FactAllNbaVoteRow Tests
# =============================================================================


class TestFactAllNbaVoteRow:
    """Tests for FactAllNbaVoteRow validators."""

    @given(all_nba_votes())
    def test_valid_all_nba_votes(self, data):
        """
        Property: Valid All-NBA vote data should always pass.
        """
        row = FactAllNbaVoteRow(**data)
        if row.pts_won is not None and row.pts_max is not None:
            assert row.pts_won <= row.pts_max

    @given(
        st.integers(min_value=101, max_value=200),
        st.integers(min_value=0, max_value=100),
    )
    def test_pts_won_exceeds_max_raises(self, pts_won, pts_max):
        """
        Property: When pts_won > pts_max, validation should raise.
        """
        with pytest.raises(ValidationError) as exc_info:
            FactAllNbaVoteRow(
                player_id="123",
                season_id="2023-24",
                team_type="First Team",
                pts_won=pts_won,
                pts_max=pts_max,
            )
        assert "pts_won cannot exceed pts_max" in str(exc_info.value)


# =============================================================================
# FactDraftRow Tests
# =============================================================================


class TestFactDraftRow:
    """Tests for FactDraftRow validators."""

    @given(draft_picks())
    def test_valid_draft_picks(self, data):
        """
        Property: Valid draft pick data should always pass.
        """
        row = FactDraftRow(**data)
        if row.draft_round is not None:
            assert row.draft_round >= 1
        if row.overall_pick is not None:
            assert row.overall_pick >= 1

    @given(st.integers(min_value=-100, max_value=0))
    def test_invalid_draft_round_raises(self, invalid_round):
        """
        Property: Draft rounds less than 1 should raise.
        """
        assume(invalid_round < 1)
        with pytest.raises(ValidationError) as exc_info:
            FactDraftRow(
                season_id="2023-24",
                draft_round=invalid_round,
            )
        assert "draft_round must be >= 1" in str(exc_info.value)


# =============================================================================
# FactRosterRow Tests
# =============================================================================


class TestFactRosterRow:
    """Tests for FactRosterRow date validators."""

    @given(roster_entries())
    def test_valid_roster_entries(self, data):
        """
        Property: Valid roster entries with correct date ranges should pass.
        """
        row = FactRosterRow(**data)
        if row.end_date is not None:
            assert row.end_date > row.start_date

    @given(
        st.just("2024-01-01"),
        st.just("2023-12-01"),  # end_date before start_date
    )
    def test_end_date_before_start_raises(self, start_date, end_date):
        """
        Property: When end_date <= start_date, validation should raise.
        """
        with pytest.raises(ValidationError) as exc_info:
            FactRosterRow(
                player_id="123",
                team_id="1610612747",
                season_id="2023-24",
                start_date=start_date,
                end_date=end_date,
            )
        assert "end_date must be after start_date" in str(exc_info.value)


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
