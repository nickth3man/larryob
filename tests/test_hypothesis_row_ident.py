"""
Property-based tests for _row_ident function.
"""

import pytest
from hypothesis import given
from hypothesis import strategies as st

from src.etl.validation import _row_ident
from tests.hypothesis_strategies import (
    game_ids,
    player_ids,
    season_ids,
    team_ids,
)


class TestRowIdent:
    """Property-based tests for _row_ident function."""

    @given(
        st.fixed_dictionaries(
            {
                "game_id": game_ids,
                "player_id": player_ids,
                "team_id": team_ids,
            }
        )
    )
    def test_row_ident_extracts_known_keys(self, row):
        """
        Property: _row_ident should extract all known identification keys.
        """
        result = _row_ident(row)

        assert "game_id" in result
        assert "player_id" in result
        assert "team_id" in result
        assert result["game_id"] == row["game_id"]
        assert result["player_id"] == row["player_id"]
        assert result["team_id"] == row["team_id"]

    @given(
        st.fixed_dictionaries(
            {
                "game_id": game_ids,
                "player_id": player_ids,
                "team_id": team_ids,
                "season_id": season_ids,
            }
        )
    )
    def test_row_ident_includes_season_id(self, row):
        """
        Property: _row_ident should include season_id if present.
        """
        result = _row_ident(row)

        assert "season_id" in result
        assert result["season_id"] == row["season_id"]

    @given(
        st.fixed_dictionaries(
            {
                "other_field": st.text(),
                "another_field": st.integers(),
            }
        )
    )
    def test_row_ident_skips_unknown_keys(self, row):
        """
        Property: _row_ident should only include known identification keys.
        """
        result = _row_ident(row)

        assert "other_field" not in result
        assert "another_field" not in result

    @given(st.dictionaries(st.text(), st.text()))
    def test_row_ident_never_raises(self, row):
        """
        Property: _row_ident should never raise, regardless of input.
        """
        try:
            result = _row_ident(row)
            assert isinstance(result, dict)
        except Exception:
            pytest.fail("_row_ident raised an exception")
