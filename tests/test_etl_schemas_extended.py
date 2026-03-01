# tests/test_etl_schemas_extended.py
"""Tests for the 6 new Pydantic row models added to src.etl.schemas."""

import pytest
from pydantic import ValidationError

from src.etl.schemas import (
    FactAllNbaRow,
    FactAllNbaVoteRow,
    FactAllStarRow,
    FactDraftRow,
    FactPlayerAwardRow,
    FactRosterRow,
)

# ------------------------------------------------------------------ #
# FactPlayerAwardRow                                                  #
# ------------------------------------------------------------------ #


def test_fact_player_award_valid():
    row = FactPlayerAwardRow(
        player_id="2544",
        season_id="2023-24",
        award_name="MVP",
        award_type="individual",
    )
    assert row.award_name == "MVP"


def test_fact_player_award_missing_required_field_fails():
    with pytest.raises(ValidationError):
        FactPlayerAwardRow(player_id="2544", season_id="2023-24", award_name="MVP")  # ty: ignore[missing-argument]
        # missing award_type


def test_fact_player_award_invalid_award_type_fails():
    with pytest.raises(ValidationError):
        FactPlayerAwardRow(
            player_id="2544",
            season_id="2023-24",
            award_name="MVP",
            award_type="bogus",  # ty: ignore[invalid-argument-type]
        )


def test_fact_player_award_all_valid_award_types():
    for t in ("individual", "weekly", "monthly", "team_inclusion"):
        row = FactPlayerAwardRow(player_id="P1", season_id="2023-24", award_name="X", award_type=t)
        assert row.award_type == t


def test_fact_player_award_votes_valid():
    row = FactPlayerAwardRow(
        player_id="P1",
        season_id="2023-24",
        award_name="MVP",
        award_type="individual",
        votes_received=80,
        votes_possible=100,
    )
    assert row.votes_received == 80


def test_fact_player_award_votes_received_exceeds_possible_fails():
    with pytest.raises(ValidationError, match="votes_received cannot exceed votes_possible"):
        FactPlayerAwardRow(
            player_id="P1",
            season_id="2023-24",
            award_name="MVP",
            award_type="individual",
            votes_received=101,
            votes_possible=100,
        )


def test_fact_player_award_votes_received_equals_possible_passes():
    row = FactPlayerAwardRow(
        player_id="P1",
        season_id="2023-24",
        award_name="MVP",
        award_type="individual",
        votes_received=100,
        votes_possible=100,
    )
    assert row.votes_received == 100


def test_fact_player_award_negative_votes_fails():
    with pytest.raises(ValidationError):
        FactPlayerAwardRow(
            player_id="P1",
            season_id="2023-24",
            award_name="MVP",
            award_type="individual",
            votes_received=-1,
        )


def test_fact_player_award_extra_fields_ignored():
    row = FactPlayerAwardRow(
        player_id="P1",
        season_id="2023-24",
        award_name="MVP",
        award_type="individual",
        unknown_field="ignored",  # ty: ignore[unknown-argument]
    )
    assert not hasattr(row, "unknown_field")


# ------------------------------------------------------------------ #
# FactAllStarRow                                                      #
# ------------------------------------------------------------------ #


def test_fact_all_star_valid_minimal():
    row = FactAllStarRow(player_id="2544", season_id="2023-24")
    assert row.player_id == "2544"
    assert row.is_replacement == 0


def test_fact_all_star_missing_player_id_fails():
    with pytest.raises(ValidationError):
        FactAllStarRow(season_id="2023-24")  # ty: ignore[missing-argument]


def test_fact_all_star_is_starter_valid_values():
    for v in (0, 1, None):
        row = FactAllStarRow(player_id="P1", season_id="2023-24", is_starter=v)
        assert row.is_starter == v


def test_fact_all_star_is_starter_invalid_value_fails():
    with pytest.raises(ValidationError, match="is_starter must be 0, 1, or None"):
        FactAllStarRow(player_id="P1", season_id="2023-24", is_starter=2)


def test_fact_all_star_is_replacement_invalid_value_fails():
    with pytest.raises(ValidationError, match="is_replacement must be 0 or 1"):
        FactAllStarRow(player_id="P1", season_id="2023-24", is_replacement=5)


def test_fact_all_star_with_team_fields():
    row = FactAllStarRow(
        player_id="P1",
        season_id="2023-24",
        team_id="1610612747",
        selection_team="Team LeBron",
        is_starter=1,
        is_replacement=0,
    )
    assert row.selection_team == "Team LeBron"


# ------------------------------------------------------------------ #
# FactAllNbaRow                                                       #
# ------------------------------------------------------------------ #


def test_fact_all_nba_valid():
    row = FactAllNbaRow(player_id="2544", season_id="2023-24", team_type="All-NBA", team_number=1)
    assert row.team_number == 1


def test_fact_all_nba_missing_team_type_fails():
    with pytest.raises(ValidationError):
        FactAllNbaRow(player_id="P1", season_id="2023-24")  # ty: ignore[missing-argument]


def test_fact_all_nba_empty_team_type_fails():
    with pytest.raises(ValidationError, match="team_type must not be empty"):
        FactAllNbaRow(player_id="P1", season_id="2023-24", team_type="   ")


def test_fact_all_nba_valid_team_numbers():
    for n in (1, 2, 3, None):
        row = FactAllNbaRow(player_id="P1", season_id="2023-24", team_type="All-NBA", team_number=n)
        assert row.team_number == n


def test_fact_all_nba_invalid_team_number_fails():
    with pytest.raises(ValidationError, match="team_number must be 1, 2, or 3"):
        FactAllNbaRow(player_id="P1", season_id="2023-24", team_type="All-NBA", team_number=4)


def test_fact_all_nba_position_optional():
    row = FactAllNbaRow(player_id="P1", season_id="2023-24", team_type="All-Defense", position="SF")
    assert row.position == "SF"


# ------------------------------------------------------------------ #
# FactAllNbaVoteRow                                                   #
# ------------------------------------------------------------------ #


def test_fact_all_nba_vote_valid():
    row = FactAllNbaVoteRow(
        player_id="2544",
        season_id="2023-24",
        team_type="All-NBA",
        pts_won=900,
        pts_max=1000,
        share=0.9,
    )
    assert row.share == pytest.approx(0.9)


def test_fact_all_nba_vote_pts_won_exceeds_max_fails():
    with pytest.raises(ValidationError, match="pts_won cannot exceed pts_max"):
        FactAllNbaVoteRow(
            player_id="P1",
            season_id="2023-24",
            team_type="All-NBA",
            pts_won=1001,
            pts_max=1000,
        )


def test_fact_all_nba_vote_pts_won_equals_max_passes():
    row = FactAllNbaVoteRow(
        player_id="P1",
        season_id="2023-24",
        team_type="All-NBA",
        pts_won=1000,
        pts_max=1000,
    )
    assert row.pts_won == 1000


def test_fact_all_nba_vote_share_out_of_range_fails():
    with pytest.raises(ValidationError):
        FactAllNbaVoteRow(player_id="P1", season_id="2023-24", team_type="All-NBA", share=1.5)


def test_fact_all_nba_vote_negative_pts_won_fails():
    with pytest.raises(ValidationError):
        FactAllNbaVoteRow(player_id="P1", season_id="2023-24", team_type="All-NBA", pts_won=-1)


def test_fact_all_nba_vote_vote_counts_valid():
    row = FactAllNbaVoteRow(
        player_id="P1",
        season_id="2023-24",
        team_type="All-NBA",
        first_team_votes=50,
        second_team_votes=30,
        third_team_votes=20,
    )
    assert row.first_team_votes == 50


# ------------------------------------------------------------------ #
# FactDraftRow                                                        #
# ------------------------------------------------------------------ #


def test_fact_draft_valid_minimal():
    row = FactDraftRow(season_id="2023-24")
    assert row.season_id == "2023-24"


def test_fact_draft_missing_season_id_fails():
    with pytest.raises(ValidationError):
        FactDraftRow(draft_round=1, overall_pick=1)  # ty: ignore[missing-argument]


def test_fact_draft_valid_round_1_and_2():
    for r in (1, 2, None):
        row = FactDraftRow(season_id="2023-24", draft_round=r)
        assert row.draft_round == r


def test_fact_draft_invalid_round_fails():
    with pytest.raises(ValidationError, match="draft_round must be 1 or 2"):
        FactDraftRow(season_id="2023-24", draft_round=3)


def test_fact_draft_overall_pick_must_be_positive():
    with pytest.raises(ValidationError):
        FactDraftRow(season_id="2023-24", overall_pick=0)


def test_fact_draft_overall_pick_one_passes():
    row = FactDraftRow(season_id="2023-24", overall_pick=1)
    assert row.overall_pick == 1


def test_fact_draft_full_row():
    row = FactDraftRow(
        season_id="2023-24",
        draft_round=1,
        overall_pick=1,
        bref_team_abbrev="SAS",
        bref_player_id="wembavi01",
        player_name="Victor Wembanyama",
        college=None,
        lg="NBA",
    )
    assert row.player_name == "Victor Wembanyama"


# ------------------------------------------------------------------ #
# FactRosterRow                                                       #
# ------------------------------------------------------------------ #


def test_fact_roster_valid():
    row = FactRosterRow(
        player_id="2544",
        team_id="1610612747",
        season_id="2023-24",
        start_date="2023-10-01",
    )
    assert row.start_date == "2023-10-01"
    assert row.end_date is None


def test_fact_roster_missing_start_date_fails():
    with pytest.raises(ValidationError):
        FactRosterRow(player_id="P1", team_id="T1", season_id="2023-24")  # ty: ignore[missing-argument]


def test_fact_roster_end_date_after_start_passes():
    row = FactRosterRow(
        player_id="P1",
        team_id="T1",
        season_id="2023-24",
        start_date="2023-10-01",
        end_date="2024-04-14",
    )
    assert row.end_date == "2024-04-14"


def test_fact_roster_end_date_before_start_fails():
    with pytest.raises(ValidationError, match="end_date must be after start_date"):
        FactRosterRow(
            player_id="P1",
            team_id="T1",
            season_id="2023-24",
            start_date="2023-10-01",
            end_date="2023-09-30",
        )


def test_fact_roster_end_date_equal_to_start_fails():
    with pytest.raises(ValidationError, match="end_date must be after start_date"):
        FactRosterRow(
            player_id="P1",
            team_id="T1",
            season_id="2023-24",
            start_date="2023-10-01",
            end_date="2023-10-01",
        )
