import sqlite3

from nba_api.stats.endpoints import boxscoretraditionalv3

from src.db.operations.upsert import transaction, upsert_rows
from src.etl.extract.api_client import APICaller
from src.etl.transform._game_logs import (
    transform_boxscore_player_rows,
    transform_boxscore_team_rows,
)
from src.etl.validation import validate_rows


def load_canonical_boxscores_for_game(
    con: sqlite3.Connection, game_id: str, api_caller: APICaller | None = None
) -> dict[str, int]:
    def _fetch() -> dict:
        ep = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id)
        return {
            "player_stats": ep.player_stats.get_data_frame().to_dict(orient="records"),  # type: ignore
            "team_stats": ep.team_stats.get_data_frame().to_dict(orient="records"),  # type: ignore
        }

    payload = (
        api_caller.call_with_backoff(_fetch, label="BoxScoreTraditionalV3")
        if api_caller
        else _fetch()
    )
    player_rows = transform_boxscore_player_rows(payload["player_stats"], game_id)
    team_rows = transform_boxscore_team_rows(payload["team_stats"], game_id)
    player_rows = validate_rows("player_game_log", player_rows)
    team_rows = validate_rows("team_game_log", team_rows)
    with transaction(con):
        n_players = upsert_rows(
            con, "player_game_log", player_rows, conflict="REPLACE", autocommit=False
        )
        n_teams = upsert_rows(con, "team_game_log", team_rows, conflict="REPLACE", autocommit=False)
    return {"player_game_log": n_players, "team_game_log": n_teams}
