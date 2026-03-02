"""
Property-based tests for parse_matchup transformation function.

These tests verify that parse_matchup correctly handles "vs." and "@" formats,
malformed strings, and whitespace.
"""

from hypothesis import assume, given
from hypothesis import strategies as st

from src.etl.transform._game_logs import parse_matchup

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
        assert is_home is True

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
    )
    def test_parse_matchup_with_extra_whitespace(self, team):
        """
        Property: Matchup strings with extra whitespace should be handled.
        """
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
