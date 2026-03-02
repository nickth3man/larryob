"""
Property-based tests for data transformation functions.

These tests verify that transformation functions maintain expected properties
and handle edge cases correctly across a wide range of inputs.
"""

import pandas as pd
from hypothesis import assume, given
from hypothesis import strategies as st

from src.etl.transform._game_logs import (
    _normalize_early_era_rebounds,
    build_game_rows,
    build_player_rows,
    build_team_rows,
    parse_matchup,
)
from tests.hypothesis_strategies import (
    early_era_rebound_stats,
    game_ids,
    matchups,
    player_ids,
    positive_ints,
    rebound_stats,
    season_ids,
    team_ids,
)

# =============================================================================
# parse_matchup Tests
# =============================================================================


class TestParseMatchup:
    @given(
        st.text(alphabet=st.characters(whitelist_categories=("Lu", "Ll")), min_size=2, max_size=5),
        st.text(alphabet=st.characters(whitelist_categories=("Lu", "Ll")), min_size=2, max_size=5),
    )
    def test_parse_matchup_vs_format(self, team1, team2):
        """
        Property: "TEAM1 vs. TEAM2" should parse with is_home=True.
        """
        matchup = f"{team1} vs. {team2}"
        first, second, is_home = parse_matchup(matchup)

        assert first == team1
        assert second == team2

    @given(
        st.text(alphabet=st.characters(whitelist_categories=("Lu", "Ll")), min_size=2, max_size=5),
        st.text(alphabet=st.characters(whitelist_categories=("Lu", "Ll")), min_size=2, max_size=5),
    )
    def test_parse_matchup_at_format(self, team1, team2):
        """
        Property: "TEAM1 @ TEAM2" should parse with is_home=False.
        """
        matchup = f"{team1} @ {team2}"
        first, second, is_home = parse_matchup(matchup)

        assert first == team1
        assert second == team2
        assert is_home is False

    @given(st.text())
    def test_parse_matchup_malformed_returns_none(self, malformed):
        """
        Property: Malformed matchup strings should return (None, None, False).
        """
        assume(" vs. " not in malformed)
        assume(" @ " not in malformed)

        first, second, is_home = parse_matchup(malformed)

        assert first is None
        assert second is None
        assert is_home is False

    @given(
        st.text(min_size=1, max_size=10),
        st.text(min_size=1, max_size=10),
        st.text(min_size=1, max_size=10),
    )
    def test_parse_matchup_with_extra_whitespace(self, prefix, team, suffix):
        """
        Property: Matchup strings with extra whitespace should be handled.
        """
        assume(" vs. " not in prefix and " @ " not in prefix)
        assume(" vs. " not in suffix and " @ " not in suffix)

        matchup = f"  {team} vs. {team}  "
        first, second, is_home = parse_matchup(matchup)

        # Should handle leading/trailing whitespace
        assert first is not None
        assert second is not None
        assert is_home is True

    @given(st.text(alphabet=st.characters(whitelist_categories=("Lu", "Ll"))))
    def test_parse_matchup_roundtrip(self, team_abbr):
        """
        Property: Parsing a generated matchup preserves team abbreviations.
        """
        assume(len(team_abbr) >= 2)
        assume(" vs. " not in team_abbr)
        assume(" @ " not in team_abbr)

        matchup = f"{team_abbr} vs. {team_abbr}"
        first, second, is_home = parse_matchup(matchup)

        assert first == team_abbr
        assert second == team_abbr


# =============================================================================
# _normalize_early_era_rebounds Tests
# =============================================================================


class TestNormalizeEarlyEraRebounds:
    """Property-based tests for _normalize_early_era_rebounds function."""

    @given(early_era_rebound_stats())
    def test_early_era_normalization(self, stats):
        """
        Property: When oreb==0, dreb==0, and reb>0, both should become None.
        """
        row = {"oreb": stats["oreb"], "dreb": stats["dreb"], "reb": stats["reb"]}
        result = _normalize_early_era_rebounds(row)

        assert result["oreb"] is None
        assert result["dreb"] is None
        assert result["reb"] == stats["reb"]

    @given(rebound_stats())
    def test_modern_era_not_normalized(self, stats):
        """
        Property: When oreb or dreb is non-zero, values should remain unchanged.
        """
        assume(stats["oreb"] > 0 or stats["dreb"] > 0)

        row = {"oreb": stats["oreb"], "dreb": stats["dreb"], "reb": stats["reb"]}
        result = _normalize_early_era_rebounds(row)

        assert result["oreb"] == stats["oreb"]
        assert result["dreb"] == stats["dreb"]
        assert result["reb"] == stats["reb"]

    @given(
        st.integers(min_value=0, max_value=20),
        st.integers(min_value=0, max_value=20),
    )
    def test_zero_rebounds_not_normalized(self, oreb, dreb):
        """
        Property: When reb==0, normalization should not occur even if oreb==dreb==0.
        """
        row = {"oreb": oreb, "dreb": dreb, "reb": 0}
        result = _normalize_early_era_rebounds(row)

        assert result["oreb"] == oreb
        assert result["dreb"] == dreb
        assert result["reb"] == 0

    @given(
        st.integers(min_value=1, max_value=20),
        st.integers(min_value=1, max_value=20),
        st.integers(min_value=1, max_value=40),
    )
    def test_partial_rebounds_not_normalized(self, oreb, dreb, reb):
        """
        Property: Rows with any non-zero oreb or dreb should not be normalized.
        """
        row = {"oreb": oreb, "dreb": dreb, "reb": reb}
        result = _normalize_early_era_rebounds(row)

        assert result["oreb"] == oreb
        assert result["dreb"] == dreb


# =============================================================================
# build_player_rows Tests
# =============================================================================


class TestBuildPlayerRows:
    """Property-based tests for build_player_rows function."""

    @given(
        st.lists(
            st.fixed_dictionaries(
                {
                    "GAME_ID": game_ids,
                    "PLAYER_ID": player_ids,
                    "TEAM_ID": team_ids,
                    "MIN": st.floats(min_value=0.0, max_value=60.0),
                    "PTS": positive_ints,
                }
            ),
            min_size=1,
            max_size=10,
        )
    )
    def test_build_player_rows_preserves_count(self, records):
        """
        Property: Output row count equals input record count.
        """
        df = pd.DataFrame(records)
        rows = build_player_rows(df)

        assert len(rows) == len(records)

    @given(
        st.lists(
            st.fixed_dictionaries(
                {
                    "GAME_ID": game_ids,
                    "PLAYER_ID": player_ids,
                    "TEAM_ID": team_ids,
                }
            ),
            min_size=1,
            max_size=5,
        )
    )
    def test_build_player_rows_string_ids(self, records):
        """
        Property: game_id, player_id, and team_id should be strings.
        """
        df = pd.DataFrame(records)
        rows = build_player_rows(df)

        for row in rows:
            assert isinstance(row["game_id"], str)
            assert isinstance(row["player_id"], str)
            assert isinstance(row["team_id"], str)

    @given(
        st.lists(
            st.fixed_dictionaries(
                {
                    "GAME_ID": game_ids,
                    "PLAYER_ID": player_ids,
                    "TEAM_ID": team_ids,
                }
            ),
            min_size=1,
            max_size=5,
        )
    )
    def test_build_player_rows_adds_starter_column(self, records):
        """
        Property: All rows should have a 'starter' column added.
        """
        df = pd.DataFrame(records)
        rows = build_player_rows(df)

        for row in rows:
            assert "starter" in row
            assert row["starter"] is None

    @given(
        st.lists(
            st.fixed_dictionaries(
                {
                    "GAME_ID": game_ids,
                    "PLAYER_ID": player_ids,
                    "TEAM_ID": team_ids,
                    "OREB": st.just(0),
                    "DREB": st.just(0),
                    "REB": positive_ints,
                }
            ),
            min_size=1,
            max_size=5,
        )
    )
    def test_build_player_rows_normalizes_early_era(self, records):
        """
        Property: Early-era rows (oreb=0, dreb=0, reb>0) should be normalized.
        """
        assume(all(r["REB"] > 0 for r in records))

        df = pd.DataFrame(records)
        rows = build_player_rows(df)

        for row in rows:
            if row.get("reb", 0) > 0:
                assert row.get("oreb") is None
                assert row.get("dreb") is None


# =============================================================================
# build_game_rows Tests
# =============================================================================


class TestBuildGameRows:
    """Property-based tests for build_game_rows function."""

    @given(
        season_ids,
        st.sampled_from(["Regular Season", "Playoffs", "Pre Season"]),
        st.lists(
            st.tuples(game_ids, team_ids, matchups),
            min_size=2,
            max_size=10,
            unique_by=lambda x: x[0],  # Unique game_ids
        ),
    )
    def test_build_game_rows_creates_valid_games(self, season_id, season_type, game_data):
        """
        Property: Generated game rows should have valid structure.
        """
        assume(len(game_data) >= 2)

        # Need at least one home and one away team per game
        records = []
        for i, (game_id, team_id, matchup) in enumerate(game_data):
            records.append(
                {
                    "GAME_ID": game_id,
                    "TEAM_ID": team_id,
                    "MATCHUP": matchup,
                    "GAME_DATE": "2023-10-24",
                }
            )

        df = pd.DataFrame(records)
        rows = build_game_rows(df, season_id, season_type)

        # Each game should have both home and away teams identified
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
            st.fixed_dictionaries(
                {
                    "GAME_ID": game_ids,
                    "TEAM_ID": team_ids,
                    "MATCHUP": st.just("LAL vs. BOS"),
                    "GAME_DATE": st.just("2023-10-24"),
                }
            ),
            min_size=2,
            max_size=4,
        ),
    )
    def test_build_game_rows_vs_matchup_identifies_home_team(self, season_id, season_type, records):
        """
        Property: "vs." matchup should identify first team as home team.
        """
        assume(len(records) >= 2)

        df = pd.DataFrame(records)
        rows = build_game_rows(df, season_id, season_type)

        for row in rows:
            assert row["home_team_id"] is not None

    @given(
        st.just("2023-24"),
        st.just("Regular Season"),
        st.lists(
            st.fixed_dictionaries(
                {
                    "GAME_ID": game_ids,
                    "TEAM_ID": team_ids,
                    "MATCHUP": st.just("LAL @ BOS"),
                    "GAME_DATE": st.just("2023-10-24"),
                }
            ),
            min_size=2,
            max_size=4,
        ),
    )
    def test_build_game_rows_at_matchup_identifies_away_team(self, season_id, season_type, records):
        """
        Property: "@" matchup should identify first team as away team.
        """
        assume(len(records) >= 2)

        df = pd.DataFrame(records)
        rows = build_game_rows(df, season_id, season_type)

        for row in rows:
            assert row["away_team_id"] is not None


# =============================================================================
# build_team_rows Tests
# =============================================================================


class TestBuildTeamRows:
    @given(
        st.lists(
            st.fixed_dictionaries(
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
            ),
            min_size=1,
            max_size=20,
        )
    )
    def test_build_team_rows_aggregates_stats(self, records):
        """
        Property: Team rows should aggregate player stats correctly.
        """
        df = pd.DataFrame(records)
        rows = build_team_rows(df)

        # Each unique game/team combo should produce one row
        expected_count = df.groupby(["GAME_ID", "TEAM_ID"]).ngroups
        assert len(rows) == expected_count

    @given(
        st.lists(
            st.fixed_dictionaries(
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
            ),
            min_size=1,
            max_size=10,
        )
    )
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

        # Calculate expected sums
        expected_sums = df.groupby(["GAME_ID", "TEAM_ID"])["PTS"].sum().to_dict()

        rows = build_team_rows(df)

        # Verify each team's points sum matches
        for row in rows:
            key = (row["game_id"], row["team_id"])
            if key in expected_sums:
                assert row["pts"] == expected_sums[key]


# =============================================================================
# Round-trip Property Tests
# =============================================================================


class TestRoundTripProperties:
    """Tests that verify data integrity through transformations."""

    @given(
        st.lists(
            st.fixed_dictionaries(
                {
                    "GAME_ID": game_ids,
                    "PLAYER_ID": player_ids,
                    "TEAM_ID": team_ids,
                    "PTS": positive_ints,
                }
            ),
            min_size=1,
            max_size=5,
        )
    )
    def test_player_rows_preserve_game_ids(self, records):
        """
        Property: Game IDs should be preserved through build_player_rows.
        """
        original_ids = {r["GAME_ID"] for r in records}

        df = pd.DataFrame(records)
        rows = build_player_rows(df)

        result_ids = {row["game_id"] for row in rows}
        assert result_ids == original_ids

    @given(
        st.lists(
            st.fixed_dictionaries(
                {
                    "GAME_ID": game_ids,
                    "PLAYER_ID": player_ids,
                    "TEAM_ID": team_ids,
                    "PTS": positive_ints,
                }
            ),
            min_size=1,
            max_size=5,
        )
    )
    def test_player_rows_preserve_player_ids(self, records):
        """
        Property: Player IDs should be preserved through build_player_rows.
        """
        original_ids = {r["PLAYER_ID"] for r in records}

        df = pd.DataFrame(records)
        rows = build_player_rows(df)

        result_ids = {row["player_id"] for row in rows}
        assert result_ids == original_ids


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
