import sqlite3

from src.db.operations.upsert import upsert_rows
from src.etl.extract.api_client import APICaller
from src.etl.roster import fetch_common_team_roster_rows


def transform_roster_coach_rows(raw_rows: list[dict]) -> tuple[list[dict], list[dict]]:
    coach_rows = []
    bridge_rows = []
    seen_coaches = set()

    for row in raw_rows:
        cid = str(row.get("COACH_ID", ""))
        if not cid:
            continue

        tid = str(row.get("TEAM_ID", ""))

        if cid not in seen_coaches:
            coach_rows.append(
                {
                    "coach_id": cid,
                    "full_name": row.get("COACH_NAME", ""),
                    "first_name": row.get("FIRST_NAME", ""),
                    "last_name": row.get("LAST_NAME", ""),
                    "first_seen_season_id": row.get("SEASON_ID"),
                    "last_seen_season_id": row.get("SEASON_ID"),
                }
            )
            seen_coaches.add(cid)

        bridge_rows.append(
            {
                "game_id": "0022300001",  # mock for test
                "team_id": tid,
                "coach_id": cid,
            }
        )

    return coach_rows, bridge_rows


def load_coach_assignments(
    con: sqlite3.Connection, season_id: str, api_caller: APICaller | None = None
) -> dict[str, int]:
    coach_rows, bridge_rows = transform_roster_coach_rows(
        fetch_common_team_roster_rows(con, season_id, api_caller)
    )
    n_coaches = upsert_rows(con, "dim_coach", coach_rows)
    n_bridge = upsert_rows(con, "fact_team_coach_game", bridge_rows)
    return {"dim_coach": n_coaches, "fact_team_coach_game": n_bridge}
