from src.etl.extract.api_client import APICaller
from src.etl.extract.endpoints import fetch_boxscore_traditional_v3, fetch_play_by_play_v3


class FakeCaller(APICaller):
    def call_with_backoff(self, fn, **kwargs):
        # depending on the endpoint, return fake df
        if "play_by_play" in fn.__qualname__.lower() or "PlayByPlay" in kwargs.get("label", ""):
            import pandas as pd

            return pd.DataFrame([{"actionNumber": 1}])
        if "boxscore" in fn.__qualname__.lower() or "BoxScore" in kwargs.get("label", ""):
            return {"player_stats": [{"id": 1}], "team_stats": [{"id": 1}]}
        return fn()


def test_fetch_play_by_play_v3_normalizes_action_number():
    rows = fetch_play_by_play_v3(game_id="0022300001", api_caller=FakeCaller())
    assert rows[0]["game_id"] == "0022300001"
    assert rows[0]["action_number"] == 1


def test_fetch_boxscore_v3_uses_named_dataset_attributes_not_indexes():
    payload = fetch_boxscore_traditional_v3("0022300001", api_caller=FakeCaller())
    assert "player_stats" in payload and "team_stats" in payload
