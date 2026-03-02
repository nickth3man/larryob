from nba_api.stats.endpoints import boxscoretraditionalv3

from src.etl.extract.api_client import APICaller


def fetch_boxscore_traditional_v3(game_id: str, api_caller: APICaller | None = None) -> dict:
    def _fetch() -> dict:
        ep = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id)
        return {
            "player_stats": ep.player_stats.get_data_frame().to_dict(orient="records"),  # type: ignore
            "team_stats": ep.team_stats.get_data_frame().to_dict(orient="records"),  # type: ignore
        }

    return (
        api_caller.call_with_backoff(_fetch, label="BoxScoreTraditionalV3")
        if api_caller
        else _fetch()
    )
    return (
        api_caller.call_with_backoff(_fetch, label="BoxScoreTraditionalV3")
        if api_caller
        else _fetch()
    )
