"""
Raw data backfill: seeds all SQLite tables from the raw/ CSV directory.

Load order (respects FK dependencies):
  1.  dim_team_history       ← TeamHistories.csv
  2.  dim_team  (enrich)     ← Team Abbrev.csv       (bref_abbrev column)
  3.  dim_player (enrich)    ← Players.csv + Player Career Info.csv
  4.  fact_game              ← Games.csv
  5.  fact_game  (schedule)  ← LeagueSchedule24_25.csv + LeagueSchedule25_26.csv
  6.  player_game_log        ← PlayerStatistics.csv   (chunked, 1.6 M rows)
  7.  team_game_log          ← TeamStatistics.csv
  8.  fact_team_season       ← Team Summaries.csv
  9.  dim_league_season      ← Team Summaries.csv + Team Stats Per Game.csv
 10.  fact_draft             ← Draft Pick History.csv
 11.  fact_player_season_stats    ← Player Totals.csv
 12.  fact_player_advanced_season ← Advanced.csv
 13.  fact_player_shooting_season ← Player Shooting.csv
 14.  fact_player_pbp_season      ← Player Play By Play.csv
 15.  fact_player_award           ← Player Award Shares.csv
                                  + All-Star Selections.csv
                                  + End of Season Teams (Voting).csv
                                  + End of Season Teams.csv
"""

from __future__ import annotations

import logging
import sqlite3
import unicodedata
from pathlib import Path
from typing import Any

import pandas as pd  # type: ignore[import-untyped]

from src.etl.utils import upsert_rows

logger = logging.getLogger(__name__)

RAW_DIR = Path(__file__).parent.parent.parent / "raw"


def _isna(v: Any) -> bool:
    """Scalar-safe NA check that always returns a plain bool."""
    if v is None:
        return True
    try:
        return bool(_isna(v))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return False

# --------------------------------------------------------------------------- #
# Season / game-ID helpers                                                     #
# --------------------------------------------------------------------------- #

def int_season_to_id(s: int | float) -> str:
    """
    Convert a Basketball-Reference ending-year integer to our season_id format.

    Examples
    --------
    2026  → '2025-26'
    2000  → '1999-00'
    1950  → '1949-50'
    1947  → '1946-47'
    """
    s = int(s)
    start = s - 1
    end_suffix = str(s)[2:]
    return f"{start}-{end_suffix}"


def pad_game_id(game_id: int | str) -> str:
    """Zero-pad a raw NBA game ID integer to the 10-char TEXT format."""
    return str(int(game_id)).zfill(10)


def season_type_from_game_id(padded: str) -> str:
    """
    Derive season_type from the 2-digit type code embedded in a padded game ID.

    NBA encoding: digits [2:4] of the zero-padded 10-char ID.
    """
    code = padded[2:4]
    return {
        "11": "Preseason",
        "22": "Regular Season",
        "52": "Play-In",
        "42": "Playoffs",
    }.get(code, "Regular Season")


def season_id_from_game_id(padded: str) -> str:
    """
    Derive season_id from a padded 10-char NBA game ID.

    The NBA embeds the season start year in digits [3:5] (0-indexed) of the
    10-character zero-padded game ID.  For example:
        '0022500686'  →  padded[3:5] = '25'  →  start_year = 2025  →  '2025-26'
        '0022301001'  →  padded[3:5] = '23'  →  start_year = 2023  →  '2023-24'
    """
    start_year = 2000 + int(padded[3:5])
    end_suffix = str(start_year + 1)[2:]
    return f"{start_year}-{end_suffix}"


def season_id_from_date(date_str: str) -> str:
    """
    Derive season_id from an ISO-8601 date string.

    NBA seasons run roughly October–June.
    July–September belong to the following season's start.
    """
    date_str = str(date_str)[:10]  # keep 'YYYY-MM-DD'
    year = int(date_str[:4])
    month = int(date_str[5:7])
    start_year = year if month >= 7 else year - 1
    end_suffix = str(start_year + 1)[2:]
    return f"{start_year}-{end_suffix}"


def _norm_name(name: str) -> str:
    """Lowercase, strip accents and extra whitespace for fuzzy name matching."""
    nfkd = unicodedata.normalize("NFKD", str(name))
    ascii_str = nfkd.encode("ascii", "ignore").decode("ascii")
    return " ".join(ascii_str.lower().split())


# --------------------------------------------------------------------------- #
# 1. dim_team_history                                                          #
# --------------------------------------------------------------------------- #

def load_team_history(con: sqlite3.Connection, raw_dir: Path = RAW_DIR) -> None:
    path = raw_dir / "TeamHistories.csv"
    if not path.exists():
        logger.warning("TeamHistories.csv not found, skipping")
        return

    df = pd.read_csv(path)
    rows = []
    for row in df.to_dict("records"):
        rows.append({
            "team_id":            str(int(row["teamId"])),
            "team_city":          str(row["teamCity"]).strip(),
            "team_name":          str(row["teamName"]).strip(),
            "team_abbrev":        str(row["teamAbbrev"]).strip(),
            "season_founded":     int(row["seasonFounded"]),
            "season_active_till": int(row["seasonActiveTill"]),
            "league":             str(row["league"]).strip(),
        })

    inserted = upsert_rows(con, "dim_team_history", rows)
    logger.info("dim_team_history: %d rows inserted/ignored", inserted)


# --------------------------------------------------------------------------- #
# 2. dim_team enrichment (bref_abbrev)                                         #
# --------------------------------------------------------------------------- #

def enrich_dim_team(con: sqlite3.Connection, raw_dir: Path = RAW_DIR) -> None:
    path = raw_dir / "Team Abbrev.csv"
    if not path.exists():
        logger.warning("Team Abbrev.csv not found, skipping")
        return

    df = pd.read_csv(path)
    # Keep only the most recent season's abbreviation for each team name.
    latest = (
        df.sort_values("season", ascending=False)
        .drop_duplicates(subset=["team"], keep="first")
    )

    # Build a full_name → bref_abbrev lookup.
    abbrev_map: dict[str, str] = {
        str(row["team"]).strip(): str(row["abbreviation"]).strip()
        for row in latest.to_dict("records")
    }

    updated = 0
    for full_name, bref_abbrev in abbrev_map.items():
        cur = con.execute(
            "UPDATE dim_team SET bref_abbrev = ? WHERE full_name = ?",
            (bref_abbrev, full_name),
        )
        updated += cur.rowcount
    con.commit()
    logger.info("dim_team bref_abbrev: %d teams updated", updated)


# --------------------------------------------------------------------------- #
# 3. dim_player enrichment                                                     #
# --------------------------------------------------------------------------- #

def enrich_dim_player(con: sqlite3.Connection, raw_dir: Path = RAW_DIR) -> None:
    _enrich_from_players_csv(con, raw_dir)
    _enrich_from_career_info(con, raw_dir)


def _enrich_from_players_csv(
    con: sqlite3.Connection, raw_dir: Path
) -> None:
    """Enrich dim_player with bio data from NBA API Players.csv."""
    path = raw_dir / "Players.csv"
    if not path.exists():
        logger.warning("Players.csv not found, skipping")
        return

    df = pd.read_csv(path, low_memory=False)
    updated = 0

    def _ht_to_cm(ht: str | float | None) -> float | None:
        """Convert 'feet-inches' string or numeric (inches) to cm."""
        if _isna(ht):
            return None
        s = str(ht).strip()
        if "-" in s:
            parts = s.split("-")
            try:
                return (int(parts[0]) * 12 + int(parts[1])) * 2.54
            except (ValueError, IndexError):
                return None
        try:
            return float(s) * 2.54  # assume already inches
        except ValueError:
            return None

    def _lbs_to_kg(w: Any) -> float | None:
        if _isna(w):
            return None
        try:
            return float(w) * 0.453592
        except (TypeError, ValueError):
            return None

    for row in df.to_dict("records"):
        pid = str(int(row["personId"])) if not _isna(row["personId"]) else None
        if pid is None:
            continue

        height_cm = _ht_to_cm(row.get("height"))
        weight_kg = _lbs_to_kg(row.get("bodyWeight"))
        college = (
            str(row["lastAttended"]).strip()
            if not _isna(row.get("lastAttended"))
            else None
        )
        draft_year   = int(row["draftYear"])   if not _isna(row.get("draftYear"))   else None
        draft_round  = int(row["draftRound"])  if not _isna(row.get("draftRound"))  else None
        draft_number = int(row["draftNumber"]) if not _isna(row.get("draftNumber")) else None

        cur = con.execute(
            """
            UPDATE dim_player SET
                height_cm    = COALESCE(height_cm,    ?),
                weight_kg    = COALESCE(weight_kg,    ?),
                college      = COALESCE(college,      ?),
                draft_year   = COALESCE(draft_year,   ?),
                draft_round  = COALESCE(draft_round,  ?),
                draft_number = COALESCE(draft_number, ?)
            WHERE player_id = ?
            """,
            (height_cm, weight_kg, college, draft_year, draft_round, draft_number, pid),
        )
        updated += cur.rowcount

    con.commit()
    logger.info("dim_player (Players.csv): %d rows enriched", updated)


def _enrich_from_career_info(
    con: sqlite3.Connection, raw_dir: Path
) -> None:
    """
    Match Basketball-Reference players to dim_player by normalised name
    and populate bref_id, college, hof.
    """
    path = raw_dir / "Player Career Info.csv"
    if not path.exists():
        logger.warning("Player Career Info.csv not found, skipping")
        return

    bref_df = pd.read_csv(path)

    # Load existing dim_player names for matching.
    rows = con.execute(
        "SELECT player_id, full_name, birth_date FROM dim_player"
    ).fetchall()
    name_to_ids: dict[str, list[tuple[str, str | None]]] = {}
    for pid, full_name, birth_date in rows:
        key = _norm_name(full_name)
        name_to_ids.setdefault(key, []).append((pid, birth_date))

    updated = 0
    skipped = 0
    for row in bref_df.to_dict("records"):
        bref_id  = str(row["player_id"]).strip()
        raw_name = str(row["player"]).strip()
        key      = _norm_name(raw_name)

        candidates = name_to_ids.get(key, [])
        if not candidates:
            skipped += 1
            continue

        if len(candidates) == 1:
            pid = candidates[0][0]
        else:
            # Tiebreak on birth date.
            bref_bd = str(row.get("birth_date", "")).strip()[:10]
            matched = [
                p for p, bd in candidates if bd and str(bd)[:10] == bref_bd
            ]
            pid = matched[0][0] if matched else candidates[0][0]

        height_cm = (
            float(row["ht_in_in"]) * 2.54  # type: ignore[arg-type]
            if not _isna(row.get("ht_in_in"))
            else None
        )
        weight_kg = (
            float(row["wt"]) * 0.453592  # type: ignore[arg-type]
            if not _isna(row.get("wt"))
            else None
        )
        college = (
            str(row["colleges"]).strip()
            if not _isna(row.get("colleges"))
            else None
        )
        hof = 1 if str(row.get("hof", "False")).lower() not in ("false", "nan", "0", "") else 0

        cur = con.execute(
            """
            UPDATE dim_player SET
                bref_id   = COALESCE(bref_id,   ?),
                college   = COALESCE(college,   ?),
                hof       = COALESCE(hof,       ?),
                height_cm = COALESCE(height_cm, ?),
                weight_kg = COALESCE(weight_kg, ?)
            WHERE player_id = ?
            """,
            (bref_id, college, hof, height_cm, weight_kg, pid),
        )
        updated += cur.rowcount

    con.commit()
    logger.info(
        "dim_player (Career Info): %d enriched, %d unmatched", updated, skipped
    )


# --------------------------------------------------------------------------- #
# 4. fact_game ← Games.csv                                                     #
# --------------------------------------------------------------------------- #

def load_games(con: sqlite3.Connection, raw_dir: Path = RAW_DIR) -> None:
    path = raw_dir / "Games.csv"
    if not path.exists():
        logger.warning("Games.csv not found, skipping")
        return

    df = pd.read_csv(path, low_memory=False)

    # Build set of valid season_ids and team_ids to skip orphan rows.
    valid_seasons = {r[0] for r in con.execute("SELECT season_id FROM dim_season")}
    valid_teams   = {r[0] for r in con.execute("SELECT team_id   FROM dim_team")}

    rows: list[dict] = []
    skipped = 0
    for row in df.to_dict("records"):
        game_id     = pad_game_id(row["gameId"])
        season_type = season_type_from_game_id(game_id)
        home_id     = str(int(row["hometeamId"]))
        away_id     = str(int(row["awayteamId"]))
        # Derive season from game date — more reliable than game-ID encoding for
        # historical records where the ID format may have differed.
        raw_date    = str(row["gameDateTimeEst"])
        season_id   = season_id_from_date(raw_date)

        if season_id not in valid_seasons or home_id not in valid_teams or away_id not in valid_teams:
            skipped += 1
            continue

        game_date = raw_date[:10]

        home_score = int(row["homeScore"]) if not _isna(row.get("homeScore")) else None
        away_score = int(row["awayScore"]) if not _isna(row.get("awayScore")) else None
        attendance = int(row["attendance"]) if not _isna(row.get("attendance")) else None

        rows.append({
            "game_id":      game_id,
            "season_id":    season_id,
            "game_date":    game_date,
            "home_team_id": home_id,
            "away_team_id": away_id,
            "home_score":   home_score,
            "away_score":   away_score,
            "season_type":  season_type,
            "status":       "Final",
            "arena":        None,
            "attendance":   attendance,
        })

    inserted = upsert_rows(con, "fact_game", rows)
    logger.info(
        "fact_game (Games.csv): %d inserted/ignored, %d skipped", inserted, skipped
    )


# --------------------------------------------------------------------------- #
# 5. fact_game ← LeagueSchedule CSVs                                           #
# --------------------------------------------------------------------------- #

def load_schedule(con: sqlite3.Connection, raw_dir: Path = RAW_DIR) -> None:
    files = [
        raw_dir / "LeagueSchedule24_25.csv",
        raw_dir / "LeagueSchedule25_26.csv",
    ]
    valid_seasons = {r[0] for r in con.execute("SELECT season_id FROM dim_season")}
    valid_teams   = {r[0] for r in con.execute("SELECT team_id   FROM dim_team")}

    total_inserted = 0
    for path in files:
        if not path.exists():
            logger.warning("%s not found, skipping", path.name)
            continue

        df = pd.read_csv(path)
        # Normalise column names — the two files have slightly different casing.
        df.columns = [c.lower() for c in df.columns]
        home_col = "hometeamid" if "hometeamid" in df.columns else "hometeamid"
        away_col = "awayteamid" if "awayteamid" in df.columns else "awayteamid"

        rows: list[dict] = []
        for row in df.to_dict("records"):
            game_id     = pad_game_id(row["gameid"])
            raw_date_s  = str(row["gamedatetimeest"])
            season_id   = season_id_from_date(raw_date_s)
            home_id     = str(int(row[home_col]))
            away_id     = str(int(row[away_col]))

            if season_id not in valid_seasons or home_id not in valid_teams or away_id not in valid_teams:
                continue

            label = str(row.get("gamelabel", "")).strip().lower()
            if "preseason" in label:
                season_type = "Preseason"
            elif "play-in" in label or "playin" in label:
                season_type = "Play-In"
            elif "playoff" in label:
                season_type = "Playoffs"
            else:
                season_type = season_type_from_game_id(game_id)

            game_date = raw_date_s[:10]

            arena = str(row.get("arenaname", "")).strip() or None

            rows.append({
                "game_id":      game_id,
                "season_id":    season_id,
                "game_date":    game_date,
                "home_team_id": home_id,
                "away_team_id": away_id,
                "home_score":   None,
                "away_score":   None,
                "season_type":  season_type,
                "status":       "Scheduled",
                "arena":        arena,
                "attendance":   None,
            })

        inserted = upsert_rows(con, "fact_game", rows)
        total_inserted += inserted
        logger.info("%s: %d rows inserted/ignored", path.name, inserted)

    logger.info("fact_game (schedule): %d total inserted/ignored", total_inserted)


# --------------------------------------------------------------------------- #
# 6. player_game_log ← PlayerStatistics.csv (chunked)                          #
# --------------------------------------------------------------------------- #

def load_player_game_logs(
    con: sqlite3.Connection, raw_dir: Path = RAW_DIR
) -> None:
    path = raw_dir / "PlayerStatistics.csv"
    ts_path = raw_dir / "TeamStatistics.csv"
    if not path.exists():
        logger.warning("PlayerStatistics.csv not found, skipping")
        return

    # Build (game_id_int, home_flag) → team_id lookup from TeamStatistics.csv.
    team_lookup: dict[tuple[int, int], str] = {}
    if ts_path.exists():
        ts_df = pd.read_csv(ts_path, usecols=["gameId", "teamId", "home"])
        for r in ts_df.to_dict("records"):
            team_lookup[(int(r["gameId"]), int(r["home"]))] = str(int(r["teamId"]))
    else:
        logger.warning("TeamStatistics.csv not found; team_id may be missing")

    # Valid game and player IDs already in DB.
    valid_games   = {r[0] for r in con.execute("SELECT game_id   FROM fact_game")}
    valid_players = {r[0] for r in con.execute("SELECT player_id FROM dim_player")}

    total, skipped = 0, 0
    chunk_size = 50_000

    for chunk in pd.read_csv(path, chunksize=chunk_size, low_memory=False):
        rows: list[dict] = []
        for row in chunk.to_dict("records"):
            game_id   = pad_game_id(row["gameId"])
            player_id = str(int(row["personId"]))

            if game_id not in valid_games or player_id not in valid_players:
                skipped += 1
                continue

            home_flag = int(row["home"]) if not _isna(row.get("home")) else None
            team_id   = team_lookup.get(
                (int(row["gameId"]), home_flag if home_flag is not None else -1)
            )
            if team_id is None:
                skipped += 1
                continue

            def _int(v: Any) -> int | None:
                return int(v) if not _isna(v) else None

            def _flt(v: Any) -> float | None:
                return float(v) if not _isna(v) else None

            rows.append({
                "game_id":        game_id,
                "player_id":      player_id,
                "team_id":        team_id,
                "minutes_played": _flt(row.get("numMinutes")),
                "fgm":  _int(row.get("fieldGoalsMade")),
                "fga":  _int(row.get("fieldGoalsAttempted")),
                "fg3m": _int(row.get("threePointersMade")),
                "fg3a": _int(row.get("threePointersAttempted")),
                "ftm":  _int(row.get("freeThrowsMade")),
                "fta":  _int(row.get("freeThrowsAttempted")),
                "oreb": _int(row.get("reboundsOffensive")),
                "dreb": _int(row.get("reboundsDefensive")),
                "reb":  _int(row.get("reboundsTotal")),
                "ast":  _int(row.get("assists")),
                "stl":  _int(row.get("steals")),
                "blk":  _int(row.get("blocks")),
                "tov":  _int(row.get("turnovers")),
                "pf":   _int(row.get("foulsPersonal")),
                "pts":  _int(row.get("points")),
                "plus_minus": _int(row.get("plusMinusPoints")),
                "starter": None,
            })

        inserted = upsert_rows(con, "player_game_log", rows)
        total += inserted
        logger.debug("player_game_log chunk: +%d rows", inserted)

    logger.info(
        "player_game_log (PlayerStatistics.csv): %d inserted/ignored, %d skipped",
        total, skipped,
    )


# --------------------------------------------------------------------------- #
# 7. team_game_log ← TeamStatistics.csv                                        #
# --------------------------------------------------------------------------- #

def load_team_game_logs(
    con: sqlite3.Connection, raw_dir: Path = RAW_DIR
) -> None:
    path = raw_dir / "TeamStatistics.csv"
    if not path.exists():
        logger.warning("TeamStatistics.csv not found, skipping")
        return

    df = pd.read_csv(path, low_memory=False)
    valid_games = {r[0] for r in con.execute("SELECT game_id FROM fact_game")}
    valid_teams = {r[0] for r in con.execute("SELECT team_id FROM dim_team")}

    rows: list[dict] = []
    skipped = 0

    def _int(v: Any) -> int | None:
        return int(v) if not _isna(v) else None

    for row in df.to_dict("records"):
        game_id = pad_game_id(row["gameId"])
        team_id = str(int(row["teamId"]))

        if game_id not in valid_games or team_id not in valid_teams:
            skipped += 1
            continue

        rows.append({
            "game_id":     game_id,
            "team_id":     team_id,
            "fgm":  _int(row.get("fieldGoalsMade")),
            "fga":  _int(row.get("fieldGoalsAttempted")),
            "fg3m": _int(row.get("threePointersMade")),
            "fg3a": _int(row.get("threePointersAttempted")),
            "ftm":  _int(row.get("freeThrowsMade")),
            "fta":  _int(row.get("freeThrowsAttempted")),
            "oreb": _int(row.get("reboundsOffensive")),
            "dreb": _int(row.get("reboundsDefensive")),
            "reb":  _int(row.get("reboundsTotal")),
            "ast":  _int(row.get("assists")),
            "stl":  _int(row.get("steals")),
            "blk":  _int(row.get("blocks")),
            "tov":  _int(row.get("turnovers")),
            "pf":   _int(row.get("foulsPersonal")),
            "pts":  _int(row.get("teamScore")),
            "plus_minus": _int(row.get("plusMinusPoints")),
        })

    inserted = upsert_rows(con, "team_game_log", rows)
    logger.info(
        "team_game_log (TeamStatistics.csv): %d inserted/ignored, %d skipped",
        inserted, skipped,
    )


# --------------------------------------------------------------------------- #
# 8. fact_team_season ← Team Summaries.csv                                     #
# --------------------------------------------------------------------------- #

def load_team_season(con: sqlite3.Connection, raw_dir: Path = RAW_DIR) -> None:
    path = raw_dir / "Team Summaries.csv"
    if not path.exists():
        logger.warning("Team Summaries.csv not found, skipping")
        return

    df = pd.read_csv(path)
    valid_seasons = {r[0] for r in con.execute("SELECT season_id FROM dim_season")}

    rows: list[dict] = []
    skipped = 0

    def _flt(v: Any) -> float | None:
        try:
            return float(v) if not _isna(v) else None
        except (TypeError, ValueError):
            return None

    def _int(v: Any) -> int | None:
        try:
            return int(v) if not _isna(v) else None
        except (TypeError, ValueError):
            return None

    for row in df.to_dict("records"):
        season_id = int_season_to_id(row["season"])
        if season_id not in valid_seasons:
            skipped += 1
            continue

        rows.append({
            "season_id":   season_id,
            "bref_abbrev": str(row["abbreviation"]).strip(),
            "lg":          str(row.get("lg", "NBA")).strip(),
            "playoffs":    1 if str(row.get("playoffs", "False")).lower() == "true" else 0,
            "w":     _int(row.get("w")),
            "l":     _int(row.get("l")),
            "pw":    _flt(row.get("pw")),
            "pl":    _flt(row.get("pl")),
            "mov":   _flt(row.get("mov")),
            "sos":   _flt(row.get("sos")),
            "srs":   _flt(row.get("srs")),
            "o_rtg": _flt(row.get("o_rtg")),
            "d_rtg": _flt(row.get("d_rtg")),
            "n_rtg": _flt(row.get("n_rtg")),
            "pace":  _flt(row.get("pace")),
            "ts_pct":     _flt(row.get("ts_percent")),
            "e_fg_pct":   _flt(row.get("e_fg_percent")),
            "tov_pct":    _flt(row.get("tov_percent")),
            "orb_pct":    _flt(row.get("orb_percent")),
            "ft_fga":     _flt(row.get("ft_fga")),
            "opp_e_fg_pct": _flt(row.get("opp_e_fg_percent")),
            "opp_tov_pct":  _flt(row.get("opp_tov_percent")),
            "drb_pct":      _flt(row.get("drb_percent")),
            "opp_ft_fga":   _flt(row.get("opp_ft_fga")),
            "arena":     str(row["arena"]).strip() if not _isna(row.get("arena")) else None,
            "attend":    _int(row.get("attend")),
            "attend_g":  _int(row.get("attend_g")),
        })

    inserted = upsert_rows(con, "fact_team_season", rows)
    logger.info(
        "fact_team_season: %d inserted/ignored, %d skipped", inserted, skipped
    )


# --------------------------------------------------------------------------- #
# 9. dim_league_season (aggregated from Team Summaries + Team Stats Per Game)  #
# --------------------------------------------------------------------------- #

def load_league_season(con: sqlite3.Connection, raw_dir: Path = RAW_DIR) -> None:
    summaries_path = raw_dir / "Team Summaries.csv"
    per_game_path  = raw_dir / "Team Stats Per Game.csv"

    if not summaries_path.exists():
        logger.warning("Team Summaries.csv not found, skipping league_season")
        return

    summaries = pd.read_csv(summaries_path)
    valid_seasons = {r[0] for r in con.execute("SELECT season_id FROM dim_season")}

    # Pace and ortg from Team Summaries.
    pace_ortg = (
        summaries.groupby("season")
        .agg(num_teams=("team", "count"), avg_pace=("pace", "mean"), avg_ortg=("o_rtg", "mean"))
        .reset_index()
    )

    # Per-game scoring/volume averages from Team Stats Per Game.
    per_game_cols = {
        "avg_pts": "pts_per_game",
        "avg_fga": "fga_per_game",
        "avg_fta": "fta_per_game",
        "avg_trb": "trb_per_game",
        "avg_ast": "ast_per_game",
        "avg_stl": "stl_per_game",
        "avg_blk": "blk_per_game",
        "avg_tov": "tov_per_game",
    }
    if per_game_path.exists():
        pg_df = pd.read_csv(per_game_path)
        per_game_agg = pg_df.groupby("season").agg(
            **{out: (src, "mean") for out, src in per_game_cols.items()}
        ).reset_index()
        merged = pace_ortg.merge(per_game_agg, on="season", how="left")
    else:
        merged = pace_ortg
        for col in per_game_cols:
            merged[col] = None

    rows: list[dict] = []
    for row in merged.to_dict("records"):
        season_id = int_season_to_id(row["season"])
        if season_id not in valid_seasons:
            continue

        def _flt(v: Any) -> float | None:
            try:
                return round(float(v), 2) if not _isna(v) else None  # type: ignore[arg-type]
            except (TypeError, ValueError):
                return None

        rows.append({
            "season_id": season_id,
            "num_teams": int(row["num_teams"]),
            "avg_pace":  _flt(row.get("avg_pace")),
            "avg_ortg":  _flt(row.get("avg_ortg")),
            "avg_pts":   _flt(row.get("avg_pts")),
            "avg_fga":   _flt(row.get("avg_fga")),
            "avg_fta":   _flt(row.get("avg_fta")),
            "avg_trb":   _flt(row.get("avg_trb")),
            "avg_ast":   _flt(row.get("avg_ast")),
            "avg_stl":   _flt(row.get("avg_stl")),
            "avg_blk":   _flt(row.get("avg_blk")),
            "avg_tov":   _flt(row.get("avg_tov")),
        })

    inserted = upsert_rows(con, "dim_league_season", rows, conflict="REPLACE")
    logger.info("dim_league_season: %d rows upserted", inserted)


# --------------------------------------------------------------------------- #
# 10. fact_draft ← Draft Pick History.csv                                      #
# --------------------------------------------------------------------------- #

def load_draft(con: sqlite3.Connection, raw_dir: Path = RAW_DIR) -> None:
    path = raw_dir / "Draft Pick History.csv"
    if not path.exists():
        logger.warning("Draft Pick History.csv not found, skipping")
        return

    df = pd.read_csv(path, low_memory=False)
    valid_seasons = {r[0] for r in con.execute("SELECT season_id FROM dim_season")}

    rows: list[dict] = []
    skipped = 0
    for row in df.to_dict("records"):
        season_id = int_season_to_id(row["season"])
        if season_id not in valid_seasons:
            skipped += 1
            continue

        rows.append({
            "season_id":        season_id,
            "draft_round":      int(row["round"])         if not _isna(row.get("round"))        else None,
            "overall_pick":     int(row["overall_pick"])  if not _isna(row.get("overall_pick")) else None,
            "bref_team_abbrev": str(row["tm"]).strip()    if not _isna(row.get("tm"))           else None,
            "bref_player_id":   str(row["player_id"]).strip() if not _isna(row.get("player_id")) else None,
            "player_name":      str(row["player"]).strip()    if not _isna(row.get("player"))    else None,
            "college":          str(row["college"]).strip()   if not _isna(row.get("college"))   else None,
            "lg":               str(row["lg"]).strip()        if not _isna(row.get("lg"))        else None,
        })

    inserted = upsert_rows(con, "fact_draft", rows)
    logger.info(
        "fact_draft: %d inserted/ignored, %d skipped", inserted, skipped
    )


# --------------------------------------------------------------------------- #
# 11. fact_player_season_stats ← Player Totals.csv                             #
# --------------------------------------------------------------------------- #

def load_player_season_stats(
    con: sqlite3.Connection, raw_dir: Path = RAW_DIR
) -> None:
    path = raw_dir / "Player Totals.csv"
    if not path.exists():
        logger.warning("Player Totals.csv not found, skipping")
        return

    df = pd.read_csv(path, low_memory=False)
    valid_seasons = {r[0] for r in con.execute("SELECT season_id FROM dim_season")}

    rows: list[dict] = []
    skipped = 0

    def _int(v: Any) -> int | None:
        try:
            return int(v) if not _isna(v) else None
        except (TypeError, ValueError):
            return None

    for row in df.to_dict("records"):
        season_id = int_season_to_id(row["season"])
        if season_id not in valid_seasons:
            skipped += 1
            continue

        rows.append({
            "bref_player_id": str(row["player_id"]).strip(),
            "season_id":   season_id,
            "lg":          str(row.get("lg", "NBA")).strip(),
            "team_abbrev": str(row["team"]).strip() if not _isna(row.get("team")) else None,
            "pos":         str(row["pos"]).strip()  if not _isna(row.get("pos"))  else None,
            "age":   _int(row.get("age")),
            "g":     _int(row.get("g")),
            "gs":    _int(row.get("gs")),
            "mp":    _int(row.get("mp")),
            "fg":    _int(row.get("fg")),
            "fga":   _int(row.get("fga")),
            "x3p":   _int(row.get("x3p")),
            "x3pa":  _int(row.get("x3pa")),
            "ft":    _int(row.get("ft")),
            "fta":   _int(row.get("fta")),
            "orb":   _int(row.get("orb")),
            "drb":   _int(row.get("drb")),
            "reb":   _int(row.get("trb")),
            "ast":   _int(row.get("ast")),
            "stl":   _int(row.get("stl")),
            "blk":   _int(row.get("blk")),
            "tov":   _int(row.get("tov")),
            "pf":    _int(row.get("pf")),
            "pts":   _int(row.get("pts")),
        })

    inserted = upsert_rows(con, "fact_player_season_stats", rows)
    logger.info(
        "fact_player_season_stats: %d inserted/ignored, %d skipped", inserted, skipped
    )


# --------------------------------------------------------------------------- #
# 12. fact_player_advanced_season ← Advanced.csv                               #
# --------------------------------------------------------------------------- #

def load_player_advanced(
    con: sqlite3.Connection, raw_dir: Path = RAW_DIR
) -> None:
    path = raw_dir / "Advanced.csv"
    if not path.exists():
        logger.warning("Advanced.csv not found, skipping")
        return

    df = pd.read_csv(path, low_memory=False)
    valid_seasons = {r[0] for r in con.execute("SELECT season_id FROM dim_season")}

    rows: list[dict] = []
    skipped = 0

    def _flt(v: Any) -> float | None:
        try:
            return float(v) if not _isna(v) else None
        except (TypeError, ValueError):
            return None

    def _int(v: Any) -> int | None:
        try:
            return int(v) if not _isna(v) else None
        except (TypeError, ValueError):
            return None

    for row in df.to_dict("records"):
        season_id = int_season_to_id(row["season"])
        if season_id not in valid_seasons:
            skipped += 1
            continue

        rows.append({
            "bref_player_id": str(row["player_id"]).strip(),
            "season_id":   season_id,
            "team_abbrev": str(row["team"]).strip() if not _isna(row.get("team")) else None,
            "pos": str(row["pos"]).strip() if not _isna(row.get("pos")) else None,
            "age": _int(row.get("age")),
            "g":   _int(row.get("g")),
            "gs":  _int(row.get("gs")),
            "mp":  _int(row.get("mp")),
            "per":     _flt(row.get("per")),
            "ts_pct":  _flt(row.get("ts_percent")),
            "x3p_ar":  _flt(row.get("x3p_ar")),
            "f_tr":    _flt(row.get("f_tr")),
            "orb_pct": _flt(row.get("orb_percent")),
            "drb_pct": _flt(row.get("drb_percent")),
            "trb_pct": _flt(row.get("trb_percent")),
            "ast_pct": _flt(row.get("ast_percent")),
            "stl_pct": _flt(row.get("stl_percent")),
            "blk_pct": _flt(row.get("blk_percent")),
            "tov_pct": _flt(row.get("tov_percent")),
            "usg_pct": _flt(row.get("usg_percent")),
            "ows":    _flt(row.get("ows")),
            "dws":    _flt(row.get("dws")),
            "ws":     _flt(row.get("ws")),
            "ws_48":  _flt(row.get("ws_48")),
            "obpm":   _flt(row.get("obpm")),
            "dbpm":   _flt(row.get("dbpm")),
            "bpm":    _flt(row.get("bpm")),
            "vorp":   _flt(row.get("vorp")),
        })

    inserted = upsert_rows(con, "fact_player_advanced_season", rows)
    logger.info(
        "fact_player_advanced_season: %d inserted/ignored, %d skipped", inserted, skipped
    )


# --------------------------------------------------------------------------- #
# 13. fact_player_shooting_season ← Player Shooting.csv                        #
# --------------------------------------------------------------------------- #

def load_player_shooting(
    con: sqlite3.Connection, raw_dir: Path = RAW_DIR
) -> None:
    path = raw_dir / "Player Shooting.csv"
    if not path.exists():
        logger.warning("Player Shooting.csv not found, skipping")
        return

    df = pd.read_csv(path, low_memory=False)
    valid_seasons = {r[0] for r in con.execute("SELECT season_id FROM dim_season")}

    rows: list[dict] = []
    skipped = 0

    def _flt(v: Any) -> float | None:
        try:
            return float(v) if not _isna(v) else None
        except (TypeError, ValueError):
            return None

    for row in df.to_dict("records"):
        season_id = int_season_to_id(row["season"])
        if season_id not in valid_seasons:
            skipped += 1
            continue

        rows.append({
            "bref_player_id": str(row["player_id"]).strip(),
            "season_id":   season_id,
            "team_abbrev": str(row["team"]).strip() if not _isna(row.get("team")) else None,
            "g":  int(row["g"])  if not _isna(row.get("g"))  else None,
            "mp": int(row["mp"]) if not _isna(row.get("mp")) else None,
            "avg_dist_fga":   _flt(row.get("avg_dist_fga")),
            "pct_fga_2p":     _flt(row.get("percent_fga_from_x2p_range")),
            "pct_fga_0_3":    _flt(row.get("percent_fga_from_x0_3_range")),
            "pct_fga_3_10":   _flt(row.get("percent_fga_from_x3_10_range")),
            "pct_fga_10_16":  _flt(row.get("percent_fga_from_x10_16_range")),
            "pct_fga_16_3p":  _flt(row.get("percent_fga_from_x16_3p_range")),
            "pct_fga_3p":     _flt(row.get("percent_fga_from_x3p_range")),
            "fg_pct_2p":      _flt(row.get("fg_percent_from_x2p_range")),
            "fg_pct_0_3":     _flt(row.get("fg_percent_from_x0_3_range")),
            "fg_pct_3_10":    _flt(row.get("fg_percent_from_x3_10_range")),
            "fg_pct_10_16":   _flt(row.get("fg_percent_from_x10_16_range")),
            "fg_pct_16_3p":   _flt(row.get("fg_percent_from_x16_3p_range")),
            "fg_pct_3p":      _flt(row.get("fg_percent_from_x3p_range")),
            "pct_ast_2p":     _flt(row.get("percent_assisted_x2p_fg")),
            "pct_ast_3p":     _flt(row.get("percent_assisted_x3p_fg")),
            "pct_dunks_fga":  _flt(row.get("percent_dunks_of_fga")),
            "num_dunks":      int(row["num_of_dunks"]) if not _isna(row.get("num_of_dunks")) else None,
            "pct_corner3_3pa": _flt(row.get("percent_corner_3s_of_3pa")),
            "corner3_pct":     _flt(row.get("corner_3_point_percent")),
        })

    inserted = upsert_rows(con, "fact_player_shooting_season", rows)
    logger.info(
        "fact_player_shooting_season: %d inserted/ignored, %d skipped", inserted, skipped
    )


# --------------------------------------------------------------------------- #
# 14. fact_player_pbp_season ← Player Play By Play.csv                         #
# --------------------------------------------------------------------------- #

def load_player_pbp_season(
    con: sqlite3.Connection, raw_dir: Path = RAW_DIR
) -> None:
    path = raw_dir / "Player Play By Play.csv"
    if not path.exists():
        logger.warning("Player Play By Play.csv not found, skipping")
        return

    df = pd.read_csv(path, low_memory=False)
    valid_seasons = {r[0] for r in con.execute("SELECT season_id FROM dim_season")}

    rows: list[dict] = []
    skipped = 0

    def _flt(v: Any) -> float | None:
        try:
            return float(v) if not _isna(v) else None
        except (TypeError, ValueError):
            return None

    def _int(v: Any) -> int | None:
        try:
            return int(v) if not _isna(v) else None
        except (TypeError, ValueError):
            return None

    for row in df.to_dict("records"):
        season_id = int_season_to_id(row["season"])
        if season_id not in valid_seasons:
            skipped += 1
            continue

        rows.append({
            "bref_player_id": str(row["player_id"]).strip(),
            "season_id":   season_id,
            "team_abbrev": str(row["team"]).strip() if not _isna(row.get("team")) else None,
            "g":  _int(row.get("g")),
            "mp": _int(row.get("mp")),
            "pg_pct": _flt(row.get("pg_percent")),
            "sg_pct": _flt(row.get("sg_percent")),
            "sf_pct": _flt(row.get("sf_percent")),
            "pf_pct": _flt(row.get("pf_percent")),
            "c_pct":  _flt(row.get("c_percent")),
            "on_court_pm_per100": _flt(row.get("on_court_plus_minus_per_100_poss")),
            "net_pm_per100":      _flt(row.get("net_plus_minus_per_100_poss")),
            "bad_pass_tov":          _int(row.get("bad_pass_turnover")),
            "lost_ball_tov":         _int(row.get("lost_ball_turnover")),
            "shoot_foul_committed":  _int(row.get("shooting_foul_committed")),
            "off_foul_committed":    _int(row.get("offensive_foul_committed")),
            "shoot_foul_drawn":      _int(row.get("shooting_foul_drawn")),
            "off_foul_drawn":        _int(row.get("offensive_foul_drawn")),
            "pts_gen_by_ast": _int(row.get("points_generated_by_assists")),
            "and1":           _int(row.get("and1")),
            "fga_blocked":    _int(row.get("fga_blocked")),
        })

    inserted = upsert_rows(con, "fact_player_pbp_season", rows)
    logger.info(
        "fact_player_pbp_season: %d inserted/ignored, %d skipped", inserted, skipped
    )


# --------------------------------------------------------------------------- #
# 15. fact_player_award ← bref award CSVs                                      #
# --------------------------------------------------------------------------- #

# Maps bref award identifier → (canonical award_name, award_type)
_AWARD_MAP: dict[str, tuple[str, str]] = {
    "mvp":           ("MVP",           "individual"),
    "dpoy":          ("DPOY",          "individual"),
    "roy":           ("ROY",           "individual"),
    "mip":           ("MIP",           "individual"),
    "6moy":          ("6MOY",          "individual"),
    "smoy":          ("6MOY",          "individual"),
    "clutch_poy":    ("Clutch POY",    "individual"),
    "nba clutch_poy": ("Clutch POY",   "individual"),
    "twoway_player": ("Two-Way Player", "individual"),
    "coy":           ("COY",           "individual"),
    "eoy":           ("EOY",           "individual"),
    "sportsmanship": ("Sportsmanship", "individual"),
}


def _bref_to_player_id(
    con: sqlite3.Connection,
) -> dict[str, str]:
    """Build bref_id → player_id lookup from dim_player."""
    rows = con.execute(
        "SELECT bref_id, player_id FROM dim_player WHERE bref_id IS NOT NULL"
    ).fetchall()
    return {bref: pid for bref, pid in rows}


def load_awards(con: sqlite3.Connection, raw_dir: Path = RAW_DIR) -> None:
    bref_to_pid = _bref_to_player_id(con)
    valid_seasons = {r[0] for r in con.execute("SELECT season_id FROM dim_season")}

    total_inserted = 0

    # --- Player Award Shares ---
    aws_path = raw_dir / "Player Award Shares.csv"
    if aws_path.exists():
        df = pd.read_csv(aws_path)
        rows: list[dict] = []
        for row in df.to_dict("records"):
            season_id = int_season_to_id(row["season"])
            if season_id not in valid_seasons:
                continue
            bref_pid = str(row["player_id"]).strip()
            player_id = bref_to_pid.get(bref_pid)
            if not player_id:
                continue

            award_raw = str(row.get("award", "")).strip().lower()
            award_name, award_type = _AWARD_MAP.get(
                award_raw, (award_raw.upper(), "individual")
            )

            rows.append({
                "player_id":      player_id,
                "season_id":      season_id,
                "award_name":     award_name,
                "award_type":     award_type,
                "trophy_name":    None,
                "votes_received": int(row["pts_won"]) if not _isna(row.get("pts_won")) else None,
                "votes_possible": int(row["pts_max"]) if not _isna(row.get("pts_max")) else None,
            })
        inserted = upsert_rows(con, "fact_player_award", rows)
        total_inserted += inserted
        logger.info("fact_player_award (award shares): %d inserted/ignored", inserted)

    # --- All-Star Selections ---
    allstar_path = raw_dir / "All-Star Selections.csv"
    if allstar_path.exists():
        df = pd.read_csv(allstar_path)
        rows = []
        for row in df.to_dict("records"):
            season_id = int_season_to_id(row["season"])
            if season_id not in valid_seasons:
                continue
            bref_pid = str(row["player_id"]).strip()
            player_id = bref_to_pid.get(bref_pid)
            if not player_id:
                continue
            rows.append({
                "player_id":      player_id,
                "season_id":      season_id,
                "award_name":     "All-Star",
                "award_type":     "team_inclusion",
                "trophy_name":    None,
                "votes_received": None,
                "votes_possible": None,
            })
        inserted = upsert_rows(con, "fact_player_award", rows)
        total_inserted += inserted
        logger.info("fact_player_award (all-star): %d inserted/ignored", inserted)

    # --- End of Season Teams (Voting) — preferred source with vote data ---
    eos_voting_path = raw_dir / "End of Season Teams (Voting).csv"
    eos_path        = raw_dir / "End of Season Teams.csv"

    def _eos_award_name(type_: str, number_tm: str) -> str:
        type_clean = str(type_).strip().replace("_", "-").title()
        return f"{type_clean} {str(number_tm).strip()}"

    if eos_voting_path.exists():
        df = pd.read_csv(eos_voting_path)
        rows = []
        for row in df.to_dict("records"):
            season_id = int_season_to_id(row["season"])
            if season_id not in valid_seasons:
                continue
            bref_pid = str(row["player_id"]).strip()
            player_id = bref_to_pid.get(bref_pid)
            if not player_id:
                continue
            award_name = _eos_award_name(row["type"], row["number_tm"])
            rows.append({
                "player_id":      player_id,
                "season_id":      season_id,
                "award_name":     award_name,
                "award_type":     "team_inclusion",
                "trophy_name":    None,
                "votes_received": int(row["pts_won"]) if not _isna(row.get("pts_won")) else None,
                "votes_possible": int(row["pts_max"]) if not _isna(row.get("pts_max")) else None,
            })
        inserted = upsert_rows(con, "fact_player_award", rows)
        total_inserted += inserted
        logger.info("fact_player_award (EOS voting): %d inserted/ignored", inserted)

    elif eos_path.exists():
        df = pd.read_csv(eos_path)
        rows = []
        for row in df.to_dict("records"):
            season_id = int_season_to_id(row["season"])
            if season_id not in valid_seasons:
                continue
            bref_pid = str(row["player_id"]).strip()
            player_id = bref_to_pid.get(bref_pid)
            if not player_id:
                continue
            award_name = _eos_award_name(row["type"], row["number_tm"])
            rows.append({
                "player_id":      player_id,
                "season_id":      season_id,
                "award_name":     award_name,
                "award_type":     "team_inclusion",
                "trophy_name":    None,
                "votes_received": None,
                "votes_possible": None,
            })
        inserted = upsert_rows(con, "fact_player_award", rows)
        total_inserted += inserted
        logger.info("fact_player_award (EOS teams): %d inserted/ignored", inserted)

    logger.info("fact_player_award total: %d inserted/ignored", total_inserted)


# --------------------------------------------------------------------------- #
# Orchestrator                                                                 #
# --------------------------------------------------------------------------- #

def run_raw_backfill(
    con: sqlite3.Connection, raw_dir: Path = RAW_DIR
) -> None:
    """
    Execute all raw-data loaders in dependency order.
    Safe to re-run (all inserts use INSERT OR IGNORE or REPLACE).
    """
    logger.info("=== Raw backfill starting (raw_dir=%s) ===", raw_dir)

    load_team_history(con, raw_dir)
    enrich_dim_team(con, raw_dir)
    enrich_dim_player(con, raw_dir)
    load_games(con, raw_dir)
    load_schedule(con, raw_dir)
    load_player_game_logs(con, raw_dir)
    load_team_game_logs(con, raw_dir)
    load_team_season(con, raw_dir)
    load_league_season(con, raw_dir)
    load_draft(con, raw_dir)
    load_player_season_stats(con, raw_dir)
    load_player_advanced(con, raw_dir)
    load_player_shooting(con, raw_dir)
    load_player_pbp_season(con, raw_dir)
    load_awards(con, raw_dir)

    logger.info("=== Raw backfill complete ===")
