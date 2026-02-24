"""
Helper functions for dimension table mapping.

These are pure utility functions for converting and mapping NBA API data
to dimension table rows.
"""

from .config import get_team_metadata

# ------------------------------------------------------------------ #
# Conversion Helpers                                                  #
# ------------------------------------------------------------------ #


def _height_to_cm(height_str: str | None) -> float | None:
    """Convert NBA height string '6-8' (feet-inches) to cm."""
    if not height_str or not isinstance(height_str, str):
        return None
    parts = height_str.strip().split("-")
    if len(parts) != 2:
        return None
    try:
        feet, inches = int(parts[0]), int(parts[1])
        total_inches = feet * 12 + inches
        return round(total_inches * 2.54, 1)
    except (ValueError, IndexError):
        return None


def _weight_to_kg(weight_str: str | int | None) -> float | None:
    """Convert weight in lbs to kg."""
    if weight_str is None:
        return None
    try:
        lbs = float(weight_str) if isinstance(weight_str, str) else weight_str
        return round(lbs * 0.453592, 1)
    except (ValueError, TypeError):
        return None


def _parse_birth_date(date_str: str | None) -> str | None:
    """Extract YYYY-MM-DD from date string like '1989-12-09T00:00:00'."""
    if not date_str or not isinstance(date_str, str):
        return None
    return date_str[:10] if len(date_str) >= 10 else None


def _normalize_position(pos: str | None) -> str | None:
    """Map API position strings to schema values."""
    if not pos or not isinstance(pos, str):
        return None
    p = pos.strip().upper()
    schema_positions = ("PG", "SG", "SF", "PF", "C", "G", "F", "G-F", "F-G", "F-C", "C-F")
    if p in schema_positions:
        return p
    if p in ("GUARD",):
        return "G"
    if p in ("FORWARD",):
        return "F"
    if p in ("CENTER",):
        return "C"
    return None


# ------------------------------------------------------------------ #
# Mapping Functions                                                   #
# ------------------------------------------------------------------ #


def _map_nba_team(t: dict) -> dict:
    """Map nba_api static team dict → dim_team row."""
    team_id = str(t["id"])
    base = {
        "team_id": team_id,
        "abbreviation": t["abbreviation"],
        "full_name": t["full_name"],
        "city": t["city"],
        "nickname": t["nickname"],
    }
    meta = get_team_metadata(team_id) or {}
    base["conference"] = meta.get("conference")
    base["division"] = meta.get("division")
    base["color_primary"] = meta.get("color_primary")
    base["color_secondary"] = meta.get("color_secondary")
    base["arena_name"] = meta.get("arena_name")
    base["founded_year"] = meta.get("founded_year")
    return base


def _map_nba_player_static(p: dict) -> dict:
    """Map nba_api static player dict → partial dim_player row."""
    full = p.get("full_name", "")
    parts = full.split(" ", 1)
    return {
        "player_id": str(p["id"]),
        "first_name": parts[0] if parts else "",
        "last_name": parts[1] if len(parts) > 1 else "",
        "full_name": full,
        "birth_date": None,
        "birth_city": None,
        "birth_country": None,
        "height_cm": None,
        "weight_kg": None,
        "position": None,
        "draft_year": None,
        "draft_round": None,
        "draft_number": None,
        "is_active": 1 if p.get("is_active") else 0,
    }


def _map_common_all_player(row: dict) -> dict:
    """
    Map a row from CommonAllPlayers endpoint → dim_player row.
    Column names come from the nba_api DataFrame columns (lowercased).
    CommonAllPlayers does not return bio fields; use load_players_bio_enrichment for those.
    """
    full = row.get("display_first_last") or row.get("player_slug", "")
    parts = full.split(" ", 1)
    return {
        "player_id": str(row.get("person_id", "")),
        "first_name": parts[0] if parts else "",
        "last_name": parts[1] if len(parts) > 1 else "",
        "full_name": full,
        "birth_date": None,
        "birth_city": None,
        "birth_country": None,
        "height_cm": None,
        "weight_kg": None,
        "position": None,
        "draft_year": None,
        "draft_round": None,
        "draft_number": None,
        "is_active": 1 if str(row.get("rosterstatus", "0")) == "1" else 0,
    }


def _map_common_player_info(row: dict) -> dict:
    """Map CommonPlayerInfo row → dim_player row (with bio fields)."""
    row = {k.lower(): v for k, v in row.items()}
    full = row.get("display_first_last") or ""
    parts = full.split(" ", 1)
    birth = _parse_birth_date(row.get("birthdate"))
    height = _height_to_cm(row.get("height"))
    weight = _weight_to_kg(row.get("weight"))
    draft_year = row.get("draft_year")
    if draft_year is not None and str(draft_year).strip().lower() in ("", "undrafted"):
        draft_year = None
    try:
        draft_year = int(draft_year) if draft_year is not None else None
    except (ValueError, TypeError):
        draft_year = None
    draft_round = row.get("draft_round")
    try:
        draft_round = int(draft_round) if draft_round not in (None, "") else None
    except (ValueError, TypeError):
        draft_round = None
    draft_number = row.get("draft_number")
    try:
        draft_number = int(draft_number) if draft_number not in (None, "") else None
    except (ValueError, TypeError):
        draft_number = None
    if draft_year is None:
        draft_round = None
        draft_number = None
    return {
        "player_id": str(row.get("person_id", "")),
        "first_name": parts[0] if parts else "",
        "last_name": parts[1] if len(parts) > 1 else "",
        "full_name": full,
        "birth_date": birth,
        "birth_city": None,
        "birth_country": row.get("country") or None,
        "height_cm": height,
        "weight_kg": weight,
        "position": _normalize_position(row.get("position")),
        "draft_year": draft_year,
        "draft_round": draft_round,
        "draft_number": draft_number,
        "is_active": 1 if str(row.get("rosterstatus", "0")).lower() == "active" else 0,
    }
