"""
Property-based tests for award, All-NBA, All-Star, draft, and roster schema validators.

Tests FactPlayerAwardRow, FactAllStarRow, FactAllNbaRow, FactAllNbaVoteRow,
FactDraftRow, and FactRosterRow validators.
"""

import pytest
from hypothesis import assume, given
from hypothesis import strategies as st
from pydantic import ValidationError

from src.etl.schemas import (
    FactAllNbaRow,
    FactAllNbaVoteRow,
    FactAllStarRow,
    FactDraftRow,
    FactPlayerAwardRow,
    FactRosterRow,
)
from tests.hypothesis_strategies import (
    all_nba_teams,
    all_nba_votes,
    award_votes,
    draft_picks,
    invalid_award_votes,
    roster_entries,
)

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
