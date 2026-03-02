"""
Property-based tests for build_game_rows and build_team_rows.

These tests verify that game- and team-level transformation functions maintain
expected properties across a wide range of inputs.
"""

import pandas as pd
from hypothesis import given
from hypothesis import strategies as st

from src.etl.transform._game_logs import (
    build_game_rows,
    build_player_rows,
    build_team_rows,
)
from tests.hypothesis_strategies import (
    game_ids,
    player_ids,
    positive_ints,
    season_ids,
    team_ids,
)

# =============================================================================
# build_game_rows Tests
# =============================================================================

# Strategy to generate a pair of records for a single game with distinct home/away teams.
# Each pair has: one row with "TEAM_A vs. TEAM_B" (home) and one with "TEAM_B @ TEAM_A" (away).
_distinct_team_pairs = st.tuples(team_ids, team_ids).filter(lambda t: t[0] != t[1])

_game_record_pair = st.tuples(game_ids, _distinct_team_pairs).map(
    lambda x: (
        x[0],  # game_id
        [
            {
                "GAME_ID": x[0],
                "TEAM_ID": x[1][0],
                "MATCHUP": f"{x[1][0]} vs. {x[1][1]}",
                "GAME_DATE": "2023-10-24",
            },
            {
                "GAME_ID": x[0],
                "TEAM_ID": x[1][1],
                "MATCHUP": f"{x[1][1]} @ {x[1][0]}",
                "GAME_DATE": "2023-10-24",
            },
        ],
    )
)


class TestBuildGameRows:
    """Property-based tests for build_game_rows function."""

    @given(
        season_ids,
        st.sampled_from(["Regular Season", "Playoffs", "Pre Season"]),
        st.lists(_game_record_pair, min_size=1, max_size=5, unique_by=lambda x: x[0]),
    )
    def test_build_game_rows_creates_valid_games(self, season_id, season_type, game_pairs):
        """
        Property: Generated game rows should have valid structure.
        """
        records = [row for _, pair_rows in game_pairs for row in pair_rows]

        df = pd.DataFrame(records)
        rows = build_game_rows(df, season_id, season_type)

        assert len(rows) > 0

        for row in rows:
            assert row["game_id"] is not None
            assert row["season_id"] == season_id
            assert row["season_type"] == season_type
            assert row["home_team_id"] is not None
            assert row["away_team_id"] is not None
            assert row["home_team_id"] != row["away_team_id"]

    @given(
        st.just("2023-24"),
        st.just("Regular Season"),
        st.lists(
            _game_record_pair,
            min_size=1,
            max_size=4,
            unique_by=lambda x: x[0],
        ),
    )
    def test_build_game_rows_vs_matchup_identifies_home_team(
        self, season_id, season_type, game_pairs
    ):
        """
        Property: "vs." matchup should identify first team as home team.
        """
        records = [row for _, pair_rows in game_pairs for row in pair_rows]

        df = pd.DataFrame(records)
        rows = build_game_rows(df, season_id, season_type)

        assert len(rows) > 0

        for row in rows:
            assert row["home_team_id"] is not None

    @given(
        st.just("2023-24"),
        st.just("Regular Season"),
        st.lists(
            _game_record_pair,
            min_size=1,
            max_size=4,
            unique_by=lambda x: x[0],
        ),
    )
    def test_build_game_rows_at_matchup_identifies_away_team(
        self, season_id, season_type, game_pairs
    ):
        """
        Property: "@" matchup should identify first team as away team.
        """
        records = [row for _, pair_rows in game_pairs for row in pair_rows]

        df = pd.DataFrame(records)
        rows = build_game_rows(df, season_id, season_type)

        assert len(rows) > 0

        for row in rows:
            assert row["away_team_id"] is not None


# =============================================================================
# build_team_rows Tests
# =============================================================================

_team_stats_dict = st.fixed_dictionaries(
    {
        "GAME_ID": game_ids,
        "PLAYER_ID": player_ids,
        "TEAM_ID": team_ids,
        "PTS": positive_ints,
        "REB": positive_ints,
        "AST": positive_ints,
        "FGM": positive_ints,
        "FGA": positive_ints,
        "FG3M": positive_ints,
        "FG3A": positive_ints,
        "FTM": positive_ints,
        "FTA": positive_ints,
        "OREB": positive_ints,
        "DREB": positive_ints,
        "STL": positive_ints,
        "BLK": positive_ints,
        "TOV": positive_ints,
        "PF": positive_ints,
    }
)


class TestBuildTeamRows:
    @given(st.lists(_team_stats_dict, min_size=1, max_size=20))
    def test_build_team_rows_aggregates_stats(self, records):
        """
        Property: Team rows should aggregate player stats correctly.
        """
        df = pd.DataFrame(records)
        rows = build_team_rows(df)

        # Each unique game/team combo should produce one row
        expected_count = df.groupby(["GAME_ID", "TEAM_ID"]).ngroups
        assert len(rows) == expected_count

    @given(st.lists(_team_stats_dict, min_size=1, max_size=10))
    def test_build_team_rows_string_ids(self, records):
        """
        Property: game_id and team_id should be strings in output.
        """
        df = pd.DataFrame(records)
        rows = build_team_rows(df)

        for row in rows:
            assert isinstance(row["game_id"], str)
            assert isinstance(row["team_id"], str)

    @given(
        st.lists(
            st.fixed_dictionaries(
                {
                    "GAME_ID": game_ids,
                    "PLAYER_ID": player_ids,
                    "TEAM_ID": team_ids,
                    "PTS": st.integers(min_value=10, max_value=30),
                    "REB": positive_ints,
                    "AST": positive_ints,
                    "FGM": positive_ints,
                    "FGA": positive_ints,
                    "FG3M": positive_ints,
                    "FG3A": positive_ints,
                    "FTM": positive_ints,
                    "FTA": positive_ints,
                    "OREB": positive_ints,
                    "DREB": positive_ints,
                    "STL": positive_ints,
                    "BLK": positive_ints,
                    "TOV": positive_ints,
                    "PF": positive_ints,
                }
            ),
            min_size=5,
            max_size=15,
        )
    )
    def test_build_team_rows_sums_correctly(self, records):
        """
        Property: Sum of team stats should equal sum of player stats for each team.
        """
        df = pd.DataFrame(records)

        # Calculate expected sums — cast keys to str to match build_team_rows output
        expected_sums = {
            (str(game_id), str(team_id)): pts
            for (game_id, team_id), pts in df.groupby(["GAME_ID", "TEAM_ID"])["PTS"]
            .sum()
            .to_dict()
            .items()
        }

        rows = build_team_rows(df)

        # Verify each team's points sum matches
        for row in rows:
            key = (row["game_id"], row["team_id"])
            assert key in expected_sums
            assert row["pts"] == expected_sums[key]


# =============================================================================
# Edge Case Tests
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_build_player_rows_empty_dataframe(self):
        """
        Property: Empty DataFrame should return empty list.
        """
        df = pd.DataFrame()
        rows = build_player_rows(df)
        assert rows == []

    def test_build_game_rows_missing_columns(self):
        """
        Property: DataFrame without required columns should return empty list.
        """
        df = pd.DataFrame({"OTHER_COLUMN": [1, 2, 3]})
        rows = build_game_rows(df, "2023-24", "Regular Season")
        assert rows == []

    def test_build_team_rows_empty_dataframe(self):
        """
        Property: Empty DataFrame should return empty list.
        """
        df = pd.DataFrame()
        rows = build_team_rows(df)
        assert rows == []

    @given(
        st.fixed_dictionaries(
            {
                "GAME_ID": game_ids,
                "PLAYER_ID": player_ids,
                "TEAM_ID": team_ids,
                "PTS": st.just(None),
                "REB": st.just(None),
            }
        )
    )
    def test_build_player_rows_handles_nan(self, record):
        """
        Property: NaN values should be converted to None.
        """
        df = pd.DataFrame([record])
        rows = build_player_rows(df)

        assert len(rows) == 1
        assert rows[0]["pts"] is None
        assert rows[0]["reb"] is None
