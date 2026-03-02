"""
Property-based tests for _normalize_early_era_rebounds and build_player_rows.

These tests verify that player-level transformation functions maintain expected
properties across a wide range of inputs.
"""

import pandas as pd
from hypothesis import assume, given
from hypothesis import strategies as st

from src.etl.transform._game_logs import (
    _normalize_early_era_rebounds,
    build_player_rows,
)
from tests.hypothesis_strategies import (
    early_era_rebound_stats,
    game_ids,
    player_ids,
    positive_ints,
    rebound_stats,
    team_ids,
)

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
