"""
Helper functions for dimension table mapping.

These are pure utility functions for converting and mapping NBA API data
to dimension table rows.
"""

from ..config import get_team_metadata

# ------------------------------------------------------------------ #
# Conversion Helpers                                                  #
# ------------------------------------------------------------------ #


def _height_to_cm(height_str: str | None) -> float | None:
    """
    Convert a height string in "feet-inches" format (e.g., "6-8") to centimeters.

    Parameters:
        height_str (str | None): Height in "feet-inches" (feet and inches separated by a hyphen). May be None.

    Returns:
        height_cm (float | None): Height in centimeters rounded to one decimal place, or `None` if the input is missing or not a valid "feet-inches" string.
    """
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
    """
    Convert a weight value in pounds to kilograms.

    Parameters:
        weight_str (str | int | None): Weight in pounds as a number or numeric string. If None or an unparsable value is provided, the function returns None.

    Returns:
        float | None: Weight converted to kilograms, rounded to one decimal place, or `None` if input is None or invalid.
    """
    if weight_str is None:
        return None
    try:
        lbs = float(weight_str) if isinstance(weight_str, str) else weight_str
        return round(lbs * 0.453592, 1)
    except (ValueError, TypeError):
        return None


def _parse_birth_date(date_str: str | None) -> str | None:
    """
    Extracts a YYYY-MM-DD date substring from an ISO-like datetime string.

    Parameters:
        date_str (str | None): Input date/time string (e.g., '1989-12-09T00:00:00').

    Returns:
        str | None: The first 10 characters as 'YYYY-MM-DD' when present and valid, otherwise `None`.
    """
    if not date_str or not isinstance(date_str, str):
        return None
    return date_str[:10] if len(date_str) >= 10 else None


def _normalize_position(pos: str | None) -> str | None:
    """
    Normalize an NBA position string to a canonical schema code.

    Recognizes canonical position codes (e.g., "PG", "SG", "SF", "PF", "C", "G", "F", "G-F", "F-G", "F-C", "C-F")
    and maps common synonyms: "GUARD" -> "G", "FORWARD" -> "F", "CENTER" -> "C".
    Returns None for missing, invalid, or unrecognized inputs.

    Parameters:
        pos (str | None): Position string from the API; may be a code, full word, or None.

    Returns:
        str | None: Canonical position code or `None` if the input is not recognized.
    """
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
    """
    Convert a static nba_api team dictionary into a dim_team row dictionary.

    Enriches the base team fields with metadata from get_team_metadata when available.

    Parameters:
        t (dict): Team object from the nba_api static teams endpoint.

    Returns:
        dict: A dim_team-style dictionary containing keys:
            team_id, abbreviation, full_name, city, nickname,
            conference, division, color_primary, color_secondary,
            arena_name, founded_year.
    """
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
    """
    Create a partial dim_player row from a static NBA player dictionary.

    Parameters:
        p (dict): NBA API player object containing at least the keys "id", "full_name", and optionally "is_active".

    Returns:
        dict: A mapping representing a partial dim_player record with keys:
            - player_id (str): Player identifier as a string from p["id"].
            - first_name (str): First name derived from `full_name`.
            - last_name (str): Last name derived from `full_name` (empty string if absent).
            - full_name (str): Unmodified `full_name` from the input (empty string if missing).
            - birth_date (None): Placeholder for birth date.
            - birth_city (None): Placeholder for birth city.
            - birth_country (None): Placeholder for birth country.
            - height_cm (None): Placeholder for height in centimeters.
            - weight_kg (None): Placeholder for weight in kilograms.
            - position (None): Placeholder for normalized position.
            - draft_year (None): Placeholder for draft year.
            - draft_round (None): Placeholder for draft round.
            - draft_number (None): Placeholder for draft pick number.
            - is_active (int): 1 if p.get("is_active") is truthy, otherwise 0.
    """
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
    Convert a CommonAllPlayers endpoint row into a dim_player dictionary.

    Parameters:
        row (dict): Input row with nba_api-style, lowercased column names. Expected keys include
            "person_id", and either "display_first_last" or "player_slug", and "rosterstatus".

    Returns:
        dict: A dim_player-formatted dictionary with these keys:
            player_id (str): Player identifier coerced to string from "person_id".
            first_name (str): First name extracted from display name.
            last_name (str): Last name extracted from display name (empty string if absent).
            full_name (str): Full display name from "display_first_last" or "player_slug".
            birth_date: None (CommonAllPlayers does not provide bio fields).
            birth_city: None
            birth_country: None
            height_cm: None
            weight_kg: None
            position: None
            draft_year: None
            draft_round: None
            draft_number: None
            is_active (int): 1 if "rosterstatus" equals "1", otherwise 0.
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
    """
    Map a CommonPlayerInfo API row to a dim_player dictionary including biographical and draft fields.

    Parameters:
        row (dict): Mapping of CommonPlayerInfo fields (expects keys such as
            "display_first_last", "person_id", "birthdate", "height", "weight",
            "country", "position", "draft_year", "draft_round", "draft_number",
            and "rosterstatus").

    Returns:
        dict: A dictionary representing a dim_player row with the following keys:
            - player_id (str): Player identifier coerced to a string.
            - first_name (str): First name extracted from display_first_last.
            - last_name (str): Last name extracted from display_first_last.
            - full_name (str): Full display name from the input.
            - birth_date (str | None): YYYY-MM-DD birth date or `None` if unavailable or invalid.
            - birth_city (None): Always set to `None`.
            - birth_country (str | None): Country value or `None`.
            - height_cm (float | None): Height in centimeters rounded to one decimal, or `None`.
            - weight_kg (float | None): Weight in kilograms rounded to one decimal, or `None`.
            - position (str | None): Normalized position code ("G", "F", "C") or `None`.
            - draft_year (int | None): Draft year as an integer or `None` if undrafted/invalid.
            - draft_round (int | None): Draft round as an integer or `None` if unavailable.
            - draft_number (int | None): Draft pick number as an integer or `None` if unavailable.
            - is_active (int): `1` if rosterstatus equals "active" (case-insensitive), otherwise `0`.
    """
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
