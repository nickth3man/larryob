"""
Shared Hypothesis strategies for property-based testing.

This module provides reusable strategies for generating test data
that conforms to the project's schema and validation rules.
"""

from datetime import date
from typing import Any

from hypothesis import strategies as st

# ------------------------------------------------------------------
# Basic type strategies
# ------------------------------------------------------------------

positive_ints = st.integers(min_value=0, max_value=1000)
"""Strategy for non-negative integers (e.g., counts, stats)."""

positive_floats = st.floats(min_value=0.0, max_value=1000.0, allow_nan=False, allow_infinity=False)
"""Strategy for non-negative floats."""

percentages = st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False)
"""Strategy for percentage values (0.0 to 1.0)."""

game_ids = st.from_regex(r"^\d{10}$", fullmatch=True).map(lambda x: f"{int(x):010d}")
"""Strategy for valid 10-digit game IDs."""

player_ids = st.from_regex(r"^\d{1,10}$", fullmatch=True)
"""Strategy for player IDs (numeric strings)."""

team_ids = st.from_regex(r"^16106\d{5}$", fullmatch=True)
"""Strategy for NBA team IDs (start with 16106)."""

season_ids = st.from_regex(r"^\d{4}-\d{2}$", fullmatch=True)
"""Strategy for season IDs (e.g., '2023-24')."""

dates = st.dates(min_value=date(1946, 1, 1), max_value=date(2030, 12, 31))
"""Strategy for dates within NBA history range."""

matchups = st.one_of(
    st.tuples(st.text(min_size=2, max_size=5), st.text(min_size=2, max_size=5)).map(
        lambda t: f"{t[0]} vs. {t[1]}"
    ),
    st.tuples(st.text(min_size=2, max_size=5), st.text(min_size=2, max_size=5)).map(
        lambda t: f"{t[0]} @ {t[1]}"
    ),
)
"""Strategy for matchup strings (HOME vs. AWAY or AWAY @ HOME)."""


# ------------------------------------------------------------------
# Composite strategies for complex data
# ------------------------------------------------------------------


@st.composite
def shooting_stats(draw) -> dict[str, int]:
    """
    Generate valid shooting statistics where made <= attempted.
    """
    fga = draw(st.integers(min_value=0, max_value=50))
    fg3a = draw(st.integers(min_value=0, max_value=fga))
    fta = draw(st.integers(min_value=0, max_value=30))

    fgm = draw(st.integers(min_value=0, max_value=fga))
    fg3m = draw(st.integers(min_value=0, max_value=min(fg3a, fgm)))
    ftm = draw(st.integers(min_value=0, max_value=fta))

    return {
        "fgm": fgm,
        "fga": fga,
        "fg3m": fg3m,
        "fg3a": fg3a,
        "ftm": ftm,
        "fta": fta,
    }


@st.composite
def rebound_stats(draw) -> dict[str, int | None]:
    """
    Generate valid rebound statistics where oreb + dreb == reb.
    """
    oreb = draw(st.integers(min_value=0, max_value=20))
    dreb = draw(st.integers(min_value=0, max_value=30))
    reb = oreb + dreb

    return {
        "oreb": oreb,
        "dreb": dreb,
        "reb": reb,
    }


@st.composite
def early_era_rebound_stats(draw) -> dict[str, int | None]:
    """
    Generate rebound stats for early-era games (pre-1974) where
    oreb and dreb were not tracked separately.
    """
    reb = draw(st.integers(min_value=1, max_value=50))
    return {
        "oreb": 0,
        "dreb": 0,
        "reb": reb,
    }


@st.composite
def game_log_dicts(draw, player_id: str | None = None) -> dict[str, Any]:
    """
    Generate valid player game log dictionaries.
    """
    gid = draw(game_ids)
    pid = player_id if player_id is not None else draw(player_ids)
    tid = draw(team_ids)

    shooting = draw(shooting_stats())
    rebounds = draw(rebound_stats())

    return {
        "game_id": gid,
        "player_id": pid,
        "team_id": tid,
        "minutes_played": draw(st.one_of(st.none(), st.floats(min_value=0.0, max_value=60.0))),
        "starter": draw(st.one_of(st.none(), st.integers(min_value=0, max_value=1))),
        **shooting,
        **rebounds,
        "ast": draw(st.integers(min_value=0, max_value=30)),
        "stl": draw(st.integers(min_value=0, max_value=15)),
        "blk": draw(st.integers(min_value=0, max_value=15)),
        "tov": draw(st.integers(min_value=0, max_value=15)),
        "pf": draw(st.integers(min_value=0, max_value=6)),
        "pts": draw(st.integers(min_value=0, max_value=100)),
        "plus_minus": draw(st.one_of(st.none(), st.integers(min_value=-50, max_value=50))),
    }


@st.composite
def invalid_shooting_stats(draw) -> dict[str, int]:
    """
    Generate invalid shooting statistics where made > attempted.
    Used to test validation catches bad data.
    """
    fga = draw(st.integers(min_value=0, max_value=50))
    # Intentionally make fgm > fga to trigger validation error
    fgm = draw(st.integers(min_value=fga + 1, max_value=fga + 10)) if fga < 50 else 51

    return {
        "fgm": fgm,
        "fga": fga,
        "fg3m": 0,
        "fg3a": 0,
        "ftm": 0,
        "fta": 0,
    }


@st.composite
def invalid_rebound_stats(draw) -> dict[str, int]:
    """
    Generate invalid rebound statistics where oreb + dreb != reb.
    """
    oreb = draw(st.integers(min_value=0, max_value=20))
    dreb = draw(st.integers(min_value=0, max_value=30))
    # Intentionally make reb != oreb + dreb
    reb = oreb + dreb + draw(st.integers(min_value=1, max_value=10))

    return {
        "oreb": oreb,
        "dreb": dreb,
        "reb": reb,
    }


@st.composite
def award_votes(draw) -> tuple[int, int]:
    """
    Generate valid (votes_received, votes_possible) pairs where received <= possible.
    """
    possible = draw(st.integers(min_value=0, max_value=1000))
    received = draw(st.integers(min_value=0, max_value=possible))
    return received, possible


@st.composite
def invalid_award_votes(draw) -> tuple[int, int]:
    """
    Generate invalid (votes_received, votes_possible) pairs where received > possible.
    """
    possible = draw(st.integers(min_value=0, max_value=100))
    received = possible + draw(st.integers(min_value=1, max_value=50))
    return received, possible


@st.composite
def draft_picks(draw) -> dict[str, Any]:
    """
    Generate valid draft pick data.
    """
    draft_round = draw(st.one_of(st.none(), st.integers(min_value=1, max_value=3)))
    overall_pick = draw(st.one_of(st.none(), st.integers(min_value=1, max_value=90)))

    return {
        "season_id": draw(season_ids),
        "draft_round": draft_round,
        "overall_pick": overall_pick,
        "bref_team_abbrev": draw(
            st.one_of(st.none(), st.sampled_from(["LAL", "BOS", "CHI", "GSW", "NYK"]))
        ),
        "bref_player_id": draw(st.one_of(st.none(), player_ids)),
        "player_name": draw(st.one_of(st.none(), st.text(min_size=2, max_size=50))),
        "college": draw(st.one_of(st.none(), st.text(min_size=2, max_size=50))),
        "lg": draw(st.one_of(st.none(), st.just("NBA"))),
    }


@st.composite
def all_nba_teams(draw) -> dict[str, Any]:
    """
    Generate valid All-NBA team data.
    """
    return {
        "player_id": draw(player_ids),
        "season_id": draw(season_ids),
        "team_type": draw(
            st.sampled_from(
                [
                    "First Team",
                    "Second Team",
                    "Third Team",
                    "Rookie First Team",
                    "Rookie Second Team",
                ]
            )
        ),
        "team_number": draw(st.one_of(st.none(), st.sampled_from([1, 2, 3]))),
        "position": draw(st.one_of(st.none(), st.sampled_from(["G", "F", "C", "GF", "FC"]))),
    }


@st.composite
def all_nba_votes(draw) -> dict[str, Any]:
    """
    Generate valid All-NBA vote data.
    """
    pts_max = draw(st.integers(min_value=0, max_value=1000))
    pts_won = draw(st.integers(min_value=0, max_value=pts_max))

    return {
        "player_id": draw(player_ids),
        "season_id": draw(season_ids),
        "team_type": draw(st.sampled_from(["First Team", "Second Team", "Third Team"])),
        "team_number": draw(st.one_of(st.none(), st.sampled_from([1, 2, 3]))),
        "position": draw(st.one_of(st.none(), st.sampled_from(["G", "F", "C"]))),
        "pts_won": pts_won,
        "pts_max": pts_max,
        "share": draw(st.one_of(st.none(), percentages)),
        "first_team_votes": draw(st.one_of(st.none(), st.integers(min_value=0, max_value=200))),
        "second_team_votes": draw(st.one_of(st.none(), st.integers(min_value=0, max_value=200))),
        "third_team_votes": draw(st.one_of(st.none(), st.integers(min_value=0, max_value=200))),
    }


@st.composite
def roster_entries(draw) -> dict[str, Any]:
    """
    Generate valid roster entries with valid date ranges.
    """
    start_year = draw(st.integers(min_value=2020, max_value=2024))
    start_month = draw(st.integers(min_value=1, max_value=12))
    start_day = draw(st.integers(min_value=1, max_value=28))
    start_date = f"{start_year}-{start_month:02d}-{start_day:02d}"

    # end_date is either None or after start_date
    end_date = draw(
        st.one_of(
            st.none(),
            st.dates(
                min_value=date(start_year, start_month, start_day), max_value=date(2025, 12, 31)
            )
            .filter(lambda d: d > date(start_year, start_month, start_day))
            .map(lambda d: d.isoformat()),
        )
    )

    return {
        "player_id": draw(player_ids),
        "team_id": draw(team_ids),
        "season_id": draw(season_ids),
        "start_date": start_date,
        "end_date": end_date,
    }
