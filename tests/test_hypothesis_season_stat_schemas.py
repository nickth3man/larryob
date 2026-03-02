"""
Property-based tests for season statistics schema validators.

Tests FactPlayerSeasonStatsRow and FactPlayerShootingSeasonRow, covering
season shooting percentages and shot-zone distribution validators.
"""

import pytest
from hypothesis import assume, given
from hypothesis import strategies as st
from pydantic import ValidationError

from src.etl.schemas import (
    FactPlayerSeasonStatsRow,
    FactPlayerShootingSeasonRow,
)

# =============================================================================
# FactPlayerSeasonStatsRow Tests
# =============================================================================


class TestFactPlayerSeasonStatsRow:
    """Tests for FactPlayerSeasonStatsRow shooting validators."""

    @given(
        st.tuples(
            st.integers(min_value=0, max_value=2000), st.integers(min_value=0, max_value=2000)
        ).map(lambda x: (min(x), max(x))),
        st.tuples(
            st.integers(min_value=0, max_value=1000), st.integers(min_value=0, max_value=1000)
        ).map(lambda x: (min(x), max(x))),
        st.tuples(
            st.integers(min_value=0, max_value=1000), st.integers(min_value=0, max_value=1000)
        ).map(lambda x: (min(x), max(x))),
    )
    def test_season_stats_valid_shooting(self, fg_data, x3p_data, ft_data):
        """
        Property: Season stats with valid shooting percentages should validate.
        """
        fg, fga = fg_data
        x3p, x3pa = x3p_data
        ft, fta = ft_data

        row = FactPlayerSeasonStatsRow(fg=fg, fga=fga, x3p=x3p, x3pa=x3pa, ft=ft, fta=fta)
        assert row.fg <= row.fga
        assert row.x3p <= row.x3pa
        assert row.ft <= row.fta

    @given(
        st.integers(min_value=101, max_value=200),
        st.integers(min_value=0, max_value=100),
    )
    def test_season_stats_invalid_fg_raises(self, fg, fga):
        """
        Property: When fg > fga, validation should raise ValueError.
        """
        with pytest.raises(ValidationError) as exc_info:
            FactPlayerSeasonStatsRow(fg=fg, fga=fga)
        assert "fg > fga" in str(exc_info.value)


# =============================================================================
# FactPlayerShootingSeasonRow Tests
# =============================================================================


class TestFactPlayerShootingSeasonRow:
    """Tests for FactPlayerShootingSeasonRow zone validators."""

    @given(
        st.floats(min_value=0.0, max_value=0.5),
        st.floats(min_value=0.0, max_value=0.3),
        st.floats(min_value=0.0, max_value=0.2),
        st.floats(min_value=0.0, max_value=0.3),
        st.floats(min_value=0.0, max_value=0.5),
    )
    def test_zone_percentages_sum_to_one(self, pct_0_3, pct_3_10, pct_10_16, pct_16_3p, pct_3p):
        """
        Property: When all zone percentages are provided and sum to ~1.0, validation passes.
        """
        # Normalize to sum to 1.0
        total = pct_0_3 + pct_3_10 + pct_10_16 + pct_16_3p + pct_3p
        assume(total > 0)  # Avoid division by zero

        row = FactPlayerShootingSeasonRow(
            pct_fga_0_3=pct_0_3 / total,
            pct_fga_3_10=pct_3_10 / total,
            pct_fga_10_16=pct_10_16 / total,
            pct_fga_16_3p=pct_16_3p / total,
            pct_fga_3p=pct_3p / total,
        )
        # Sum should be within 0.05 of 1.0
        zone_sum = sum(
            [
                row.pct_fga_0_3 or 0,
                row.pct_fga_3_10 or 0,
                row.pct_fga_10_16 or 0,
                row.pct_fga_16_3p or 0,
                row.pct_fga_3p or 0,
            ]
        )
        assert abs(zone_sum - 1.0) <= 0.05

    @given(
        st.floats(min_value=0.1, max_value=0.5),
        st.floats(min_value=0.1, max_value=0.5),
        st.floats(min_value=0.1, max_value=0.5),
        st.floats(min_value=0.1, max_value=0.5),
        st.floats(min_value=0.1, max_value=0.5),
    )
    def test_zone_percentages_too_high_raises(self, a, b, c, d, e):
        """
        Property: When zone percentages sum to more than 1.05, validation raises.
        """
        assume(a + b + c + d + e > 1.05)

        with pytest.raises(ValidationError) as exc_info:
            FactPlayerShootingSeasonRow(
                pct_fga_0_3=a,
                pct_fga_3_10=b,
                pct_fga_10_16=c,
                pct_fga_16_3p=d,
                pct_fga_3p=e,
            )
        assert "zone_sum" in str(exc_info.value)
