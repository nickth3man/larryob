"""Tests: _resolve_player_id pure-logic cases in _player_career.

These tests exercise the resolution logic directly, without a database.
The name_lookup dict is built manually using _norm_name so the keys
match exactly what the real code produces.
"""

from src.etl.backfill._player_career import _resolve_player_id
from src.etl.helpers import _norm_name

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _lookup(*entries: tuple[str, str, str | None]) -> dict[str, list[tuple[str, str | None]]]:
    """
    Build a name_lookup dict from (player_id, full_name, birth_date) tuples.

    This mirrors how enrich_player_career builds the lookup:
        name_lookup.setdefault(_norm_name(full_name), []).append((player_id, birth_date))
    """
    result: dict[str, list[tuple[str, str | None]]] = {}
    for player_id, full_name, birth_date in entries:
        result.setdefault(_norm_name(full_name), []).append((player_id, birth_date))
    return result


# ---------------------------------------------------------------------------
# Single-candidate resolution
# ---------------------------------------------------------------------------


def test_resolve_single_candidate_returns_player_id() -> None:
    """Unique name → returns the only matching player_id."""
    lookup = _lookup(("2544", "LeBron James", "1984-12-30"))
    result = _resolve_player_id("LeBron James", "1984-12-30", lookup)
    assert result == "2544"


def test_resolve_no_candidates_returns_none() -> None:
    """Name not present in lookup → returns None."""
    lookup = _lookup(("2544", "LeBron James", "1984-12-30"))
    result = _resolve_player_id("Nobody Here", "1990-01-01", lookup)
    assert result is None


def test_resolve_accent_normalized_name() -> None:
    """Accented characters in raw_name are normalised to match ASCII lookup key."""
    # DB has "Nikola Jokic" (ASCII); CSV has "Nikolá Jokić"
    lookup = _lookup(("203999", "Nikola Jokic", "1995-02-19"))
    result = _resolve_player_id("Nikolá Jokić", "1995-02-19", lookup)
    assert result == "203999"


# ---------------------------------------------------------------------------
# Suffix stripping
# ---------------------------------------------------------------------------


def test_resolve_strips_jr_suffix() -> None:
    """'John Smith Jr.' matches 'John Smith' in the lookup after suffix strip."""
    lookup = _lookup(("9001", "John Smith", "1985-05-15"))
    result = _resolve_player_id("John Smith Jr.", "1985-05-15", lookup)
    assert result == "9001"


def test_resolve_strips_sr_suffix() -> None:
    """'Kevin Willis Sr.' matches 'Kevin Willis'."""
    lookup = _lookup(("9002", "Kevin Willis", "1962-09-06"))
    result = _resolve_player_id("Kevin Willis Sr.", "1962-09-06", lookup)
    assert result == "9002"


def test_resolve_strips_ii_suffix() -> None:
    """'Gary Payton II' is treated as a suffix strip case.

    Note: 'II' is in the suffix list, so 'Gary Payton II' strips to
    'Gary Payton'.  If the DB only has 'Gary Payton', this should resolve.
    """
    lookup = _lookup(("9003", "Gary Payton", "1968-07-23"))
    result = _resolve_player_id("Gary Payton II", None, lookup)
    # With birth_date=None and a single candidate, it resolves to that player.
    assert result == "9003"


def test_resolve_suffix_strip_no_match_returns_none() -> None:
    """Suffix stripped but still no match → None."""
    lookup = _lookup(("2544", "LeBron James", "1984-12-30"))
    result = _resolve_player_id("Nobody Known Jr.", None, lookup)
    assert result is None


# ---------------------------------------------------------------------------
# Ambiguous candidates — multiple same-named players
# ---------------------------------------------------------------------------


def test_resolve_ambiguous_no_birth_date_returns_none() -> None:
    """Two players with the same name and no birth date → None, appended to ambiguous_out."""
    lookup = _lookup(
        ("1000", "John Smith", "1990-01-01"),
        ("1001", "John Smith", "1992-02-02"),
    )
    ambiguous: list[str] = []
    result = _resolve_player_id("John Smith", None, lookup, ambiguous_out=ambiguous)

    assert result is None
    assert "John Smith" in ambiguous


def test_resolve_ambiguous_wrong_birth_date_returns_none() -> None:
    """Two same-named players but birth_date matches neither → None."""
    lookup = _lookup(
        ("1000", "John Smith", "1990-01-01"),
        ("1001", "John Smith", "1992-02-02"),
    )
    ambiguous: list[str] = []
    result = _resolve_player_id("John Smith", "1985-06-15", lookup, ambiguous_out=ambiguous)

    assert result is None
    assert "John Smith" in ambiguous


def test_resolve_ambiguous_birth_date_disambiguates() -> None:
    """Two same-named players, birth_date matches exactly one → returns that player_id."""
    lookup = _lookup(
        ("1000", "John Smith", "1990-01-01"),
        ("1001", "John Smith", "1992-02-02"),
    )
    result = _resolve_player_id("John Smith", "1992-02-02", lookup)
    assert result == "1001"


def test_resolve_ambiguous_birth_date_picks_first_of_two_matches() -> None:
    """If two candidates share the same birth_date, the first match is returned."""
    lookup = _lookup(
        ("1000", "John Smith", "1990-01-01"),
        ("1001", "John Smith", "1990-01-01"),
    )
    result = _resolve_player_id("John Smith", "1990-01-01", lookup)
    # Both match — first one wins (implementation returns matched[0])
    assert result == "1000"


def test_resolve_ambiguous_out_not_mutated_when_none() -> None:
    """Passing ambiguous_out=None with an ambiguous case does not error."""
    lookup = _lookup(
        ("1000", "John Smith", "1990-01-01"),
        ("1001", "John Smith", "1992-02-02"),
    )
    # Should not raise — just returns None
    result = _resolve_player_id("John Smith", None, lookup, ambiguous_out=None)
    assert result is None


def test_resolve_ambiguous_out_accumulates_multiple() -> None:
    """Multiple unresolved names accumulate in the same ambiguous_out list."""
    lookup = _lookup(
        ("1000", "John Smith", "1990-01-01"),
        ("1001", "John Smith", "1992-02-02"),
        ("2000", "Mike Brown", "1988-03-03"),
        ("2001", "Mike Brown", "1991-04-04"),
    )
    ambiguous: list[str] = []
    _resolve_player_id("John Smith", None, lookup, ambiguous_out=ambiguous)
    _resolve_player_id("Mike Brown", "1985-01-01", lookup, ambiguous_out=ambiguous)

    assert len(ambiguous) == 2
    assert "John Smith" in ambiguous
    assert "Mike Brown" in ambiguous
