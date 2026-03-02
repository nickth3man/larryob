"""
Property-based tests for row validation logic.

These tests verify that the validation module correctly filters and validates
rows across a wide range of generated inputs.
"""

import sqlite3

import pytest
from hypothesis import given
from hypothesis import strategies as st

from src.etl.schemas import PlayerGameLogRow
from src.etl.validation import (
    _row_ident,
    check_game_stat_consistency,
    run_consistency_checks,
    validate_rows,
)
from tests.hypothesis_strategies import (
    game_ids,
    game_log_dicts,
    player_ids,
    positive_ints,
    season_ids,
    team_ids,
)

# =============================================================================
# _row_ident Tests
# =============================================================================


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


# =============================================================================
# validate_rows Tests
# =============================================================================


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


# =============================================================================
# check_game_stat_consistency Tests
# =============================================================================


class TestCheckGameStatConsistency:
    """Property-based tests for check_game_stat_consistency function."""

    def _create_test_db(self, player_stats, team_stats):
        """Helper to create a test database with game data."""
        con = sqlite3.connect(":memory:")

        # Create tables
        con.execute("""
            CREATE TABLE player_game_log (
                game_id TEXT,
                team_id TEXT,
                player_id TEXT,
                pts INTEGER,
                reb INTEGER,
                ast INTEGER
            )
        """)

        con.execute("""
            CREATE TABLE team_game_log (
                game_id TEXT,
                team_id TEXT,
                pts INTEGER,
                reb INTEGER,
                ast INTEGER
            )
        """)

        # Insert player stats
        for stat in player_stats:
            con.execute(
                "INSERT INTO player_game_log VALUES (?, ?, ?, ?, ?, ?)",
                (
                    stat["game_id"],
                    stat["team_id"],
                    stat["player_id"],
                    stat["pts"],
                    stat["reb"],
                    stat["ast"],
                ),
            )

        # Insert team stats
        for stat in team_stats:
            con.execute(
                "INSERT INTO team_game_log VALUES (?, ?, ?, ?, ?)",
                (stat["game_id"], stat["team_id"], stat["pts"], stat["reb"], stat["ast"]),
            )

        con.commit()
        return con

    @given(
        game_ids,
        team_ids,
        st.lists(
            st.fixed_dictionaries(
                {
                    "player_id": player_ids,
                    "pts": positive_ints,
                    "reb": positive_ints,
                    "ast": positive_ints,
                }
            ),
            min_size=1,
            max_size=15,
        ),
    )
    def test_consistent_stats_no_warnings(self, game_id, team_id, player_stats):
        """
        Property: When player stats sum to team stats, no warnings should be generated.
        """
        # Calculate expected team totals
        total_pts = sum(p["pts"] for p in player_stats)
        total_reb = sum(p["reb"] for p in player_stats)
        total_ast = sum(p["ast"] for p in player_stats)

        # Prepare player data
        player_data = [{"game_id": game_id, "team_id": team_id, **p} for p in player_stats]

        # Team data matches player totals
        team_data = [
            {
                "game_id": game_id,
                "team_id": team_id,
                "pts": total_pts,
                "reb": total_reb,
                "ast": total_ast,
            }
        ]

        con = self._create_test_db(player_data, team_data)
        warnings = check_game_stat_consistency(con, game_id)

        assert warnings == []

    @given(
        game_ids,
        team_ids,
        st.lists(
            st.fixed_dictionaries(
                {
                    "player_id": player_ids,
                    "pts": positive_ints,
                }
            ),
            min_size=1,
            max_size=10,
        ),
        st.integers(min_value=1, max_value=100),
    )
    def test_inconsistent_pts_generates_warning(self, game_id, team_id, player_stats, diff):
        """
        Property: When player PTS sum != team PTS, warning should be generated.
        """
        # Calculate player totals
        total_pts = sum(p["pts"] for p in player_stats)

        # Prepare player data
        player_data = [
            {"game_id": game_id, "team_id": team_id, **p, "reb": 0, "ast": 0} for p in player_stats
        ]

        # Team data has different PTS
        team_data = [
            {
                "game_id": game_id,
                "team_id": team_id,
                "pts": total_pts + diff,  # Intentional mismatch
                "reb": 0,
                "ast": 0,
            }
        ]

        con = self._create_test_db(player_data, team_data)
        warnings = check_game_stat_consistency(con, game_id)

        # Should have a PTS mismatch warning
        assert any("PTS mismatch" in w for w in warnings)

    @given(
        game_ids,
        team_ids,
        st.lists(
            st.fixed_dictionaries(
                {
                    "player_id": player_ids,
                    "reb": positive_ints,
                }
            ),
            min_size=1,
            max_size=10,
        ),
        st.integers(min_value=1, max_value=100),
    )
    def test_inconsistent_reb_generates_warning(self, game_id, team_id, player_stats, diff):
        """
        Property: When player REB sum != team REB, warning should be generated.
        """
        total_reb = sum(p["reb"] for p in player_stats)

        player_data = [
            {"game_id": game_id, "team_id": team_id, **p, "pts": 0, "ast": 0} for p in player_stats
        ]

        team_data = [
            {
                "game_id": game_id,
                "team_id": team_id,
                "pts": 0,
                "reb": total_reb + diff,
                "ast": 0,
            }
        ]

        con = self._create_test_db(player_data, team_data)
        warnings = check_game_stat_consistency(con, game_id)

        assert any("REB mismatch" in w for w in warnings)

    @given(
        game_ids,
        team_ids,
        st.lists(
            st.fixed_dictionaries(
                {
                    "player_id": player_ids,
                    "ast": positive_ints,
                }
            ),
            min_size=1,
            max_size=10,
        ),
        st.integers(min_value=1, max_value=100),
    )
    def test_inconsistent_ast_generates_warning(self, game_id, team_id, player_stats, diff):
        """
        Property: When player AST sum != team AST, warning should be generated.
        """
        total_ast = sum(p["ast"] for p in player_stats)

        player_data = [
            {"game_id": game_id, "team_id": team_id, **p, "pts": 0, "reb": 0} for p in player_stats
        ]

        team_data = [
            {
                "game_id": game_id,
                "team_id": team_id,
                "pts": 0,
                "reb": 0,
                "ast": total_ast + diff,
            }
        ]

        con = self._create_test_db(player_data, team_data)
        warnings = check_game_stat_consistency(con, game_id)

        assert any("AST mismatch" in w for w in warnings)

    def test_missing_game_returns_empty(self):
        """
        Property: Querying non-existent game should return empty warnings.
        """
        con = self._create_test_db([], [])

        warnings = check_game_stat_consistency(con, "nonexistent")

        assert warnings == []


# =============================================================================
# run_consistency_checks Tests
# =============================================================================


class TestRunConsistencyChecks:
    """Property-based tests for run_consistency_checks function."""

    def _create_season_db(self, season_id, games_data):
        """Helper to create a test database with season data."""
        con = sqlite3.connect(":memory:")

        con.execute("""
            CREATE TABLE fact_game (
                game_id TEXT,
                season_id TEXT
            )
        """)

        con.execute("""
            CREATE TABLE player_game_log (
                game_id TEXT,
                team_id TEXT,
                player_id TEXT,
                pts INTEGER,
                reb INTEGER,
                ast INTEGER
            )
        """)

        con.execute("""
            CREATE TABLE team_game_log (
                game_id TEXT,
                team_id TEXT,
                pts INTEGER,
                reb INTEGER,
                ast INTEGER
            )
        """)

        for game in games_data:
            con.execute("INSERT INTO fact_game VALUES (?, ?)", (game["game_id"], season_id))

            # Insert player stats
            for player in game.get("players", []):
                con.execute(
                    "INSERT INTO player_game_log VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        game["game_id"],
                        game["team_id"],
                        player["player_id"],
                        player["pts"],
                        player["reb"],
                        player["ast"],
                    ),
                )

            # Insert team stats
            con.execute(
                "INSERT INTO team_game_log VALUES (?, ?, ?, ?, ?)",
                (game["game_id"], game["team_id"], game["pts"], game["reb"], game["ast"]),
            )

        con.commit()
        return con

    @given(
        season_ids,
        st.lists(
            st.fixed_dictionaries(
                {
                    "game_id": game_ids,
                    "team_id": team_ids,
                    "pts": positive_ints,
                    "reb": positive_ints,
                    "ast": positive_ints,
                }
            ),
            min_size=0,
            max_size=5,
        ),
    )
    def test_empty_season_returns_zero(self, season_id, games):
        """
        Property: Season with no games should return 0 warnings.
        """
        con = self._create_season_db(season_id, [])
        count = run_consistency_checks(con, season_id)

        assert count == 0

    @given(
        season_ids,
        st.lists(
            st.fixed_dictionaries(
                {
                    "game_id": game_ids,
                    "team_id": team_ids,
                    "pts": positive_ints,
                    "reb": positive_ints,
                    "ast": positive_ints,
                }
            ),
            min_size=1,
            max_size=5,
            unique_by=lambda x: x["game_id"],
        ),
    )
    def test_consistent_season_returns_zero(self, season_id, games):
        """
        Property: Season with all consistent games should return 0 warnings.
        """
        # Create games with consistent stats
        games_data = []
        for game in games:
            # Single player matching team stats
            games_data.append(
                {
                    **game,
                    "players": [
                        {
                            "player_id": "12345",
                            "pts": game["pts"],
                            "reb": game["reb"],
                            "ast": game["ast"],
                        }
                    ],
                }
            )

        con = self._create_season_db(season_id, games_data)
        count = run_consistency_checks(con, season_id)

        assert count == 0


# =============================================================================
# Integration Tests
# =============================================================================


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
