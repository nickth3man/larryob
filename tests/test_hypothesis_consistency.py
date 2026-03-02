"""
Property-based tests for check_game_stat_consistency and run_consistency_checks.
"""

import sqlite3

from hypothesis import given
from hypothesis import strategies as st

from src.etl.validation import check_game_stat_consistency, run_consistency_checks
from tests.hypothesis_strategies import (
    game_ids,
    player_ids,
    positive_ints,
    season_ids,
    team_ids,
)


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

    @given(season_ids)
    def test_empty_season_returns_zero(self, season_id):
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
