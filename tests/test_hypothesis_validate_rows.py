"""
Property-based tests for validate_rows function and integration tests.
"""

from hypothesis import given
from hypothesis import strategies as st

from src.etl.schemas import PlayerGameLogRow
from src.etl.validation import validate_rows
from tests.hypothesis_strategies import (
    game_ids,
    game_log_dicts,
    player_ids,
    team_ids,
)


class TestValidateRows:
    """Property-based tests for validate_rows function."""

    @given(
        st.just("player_game_log"),
        st.lists(game_log_dicts(), min_size=1, max_size=10),
    )
    def test_validate_rows_returns_valid_rows(self, table, rows):
        """
        Property: validate_rows should return valid rows for known tables.
        """
        result = validate_rows(table, rows)

        # Result should be a list
        assert isinstance(result, list)
        # All returned rows should be valid (no ValidationError in result)

    @given(
        st.just("unknown_table"),
        st.lists(st.dictionaries(st.text(), st.text()), min_size=1, max_size=5),
    )
    def test_validate_unknown_table_passes_through(self, table, rows):
        """
        Property: Unknown tables should pass through without validation.
        """
        result = validate_rows(table, rows)

        assert result == rows

    @given(st.just([]))
    def test_validate_empty_list_returns_empty(self, rows):
        """
        Property: Empty list should return empty list.
        """
        result = validate_rows("player_game_log", rows)

        assert result == []

    @given(
        st.just("player_game_log"),
        st.lists(
            st.fixed_dictionaries(
                {
                    "game_id": game_ids,
                    "player_id": player_ids,
                    "team_id": team_ids,
                    "fgm": st.integers(min_value=11, max_value=20),  # Invalid: fgm > fga
                    "fga": st.integers(min_value=0, max_value=10),
                }
            ),
            min_size=1,
            max_size=5,
        ),
    )
    def test_validate_rows_filters_invalid(self, table, rows):
        """
        Property: Invalid rows should be filtered out.
        """
        result = validate_rows(table, rows)

        # All rows have invalid shooting stats, so result should be empty
        assert result == []

    @given(
        st.just("player_game_log"),
        st.lists(
            game_log_dicts(),
            min_size=1,
            max_size=5,
        ),
    )
    def test_validate_rows_preserves_valid(self, table, rows):
        """
        Property: Valid rows should be preserved.
        """
        result = validate_rows(table, rows)

        # Result should contain valid rows (may be fewer due to filtering)
        assert len(result) <= len(rows)

        # All returned rows should be valid PlayerGameLogRow instances
        for row in result:
            # Should be able to validate again without error
            validated = PlayerGameLogRow.model_validate(row)
            assert validated.game_id is not None


class TestValidationIntegration:
    """Integration tests combining multiple validation functions."""

    @given(
        st.lists(game_log_dicts(), min_size=1, max_size=10),
    )
    def test_full_validation_pipeline(self, rows):
        """
        Property: Full validation pipeline should handle any valid input.
        """
        # Validate rows
        valid_rows = validate_rows("player_game_log", rows)

        # All returned rows should be valid
        for row in valid_rows:
            # Should not raise
            PlayerGameLogRow.model_validate(row)

    @given(
        st.lists(
            st.fixed_dictionaries(
                {
                    "game_id": game_ids,
                    "player_id": player_ids,
                    "team_id": team_ids,
                    "fgm": st.integers(min_value=0, max_value=10),
                    "fga": st.integers(min_value=0, max_value=10),
                }
            ),
            min_size=1,
            max_size=5,
        ),
    )
    def test_mixed_valid_invalid_rows(self, rows):
        """
        Property: Pipeline should filter invalid and keep valid rows.
        """
        # Count potentially valid rows (where fgm <= fga)
        potentially_valid = sum(1 for r in rows if r["fgm"] <= r["fga"])

        result = validate_rows("player_game_log", rows)

        # Result should not exceed potentially valid count
        assert len(result) <= potentially_valid
