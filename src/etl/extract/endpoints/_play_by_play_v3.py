from nba_api.stats.endpoints import playbyplayv3

from src.etl.extract.api_client import APICaller


def fetch_play_by_play_v3(game_id: str, api_caller: APICaller | None = None) -> list[dict]:
    def _fetch():
        ep = playbyplayv3.PlayByPlayV3(game_id=game_id)
        return ep.play_by_play.get_data_frame()

    df = api_caller.call_with_backoff(_fetch, label="PlayByPlayV3") if api_caller else _fetch()
    df = df.rename(columns={"actionNumber": "action_number"})
    df["game_id"] = game_id
    return df.to_dict(orient="records")
