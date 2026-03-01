from src.etl.canonical._boxscore import load_canonical_boxscores_for_game
from src.etl.extract.api_client import APICaller


class FakeCaller(APICaller):
    def call_with_backoff(self, fn, **kwargs):
        return {
            "player_stats": [
                {"personId": "2544", "teamId": "1610612747", "minutes": "24:00", "points": 10}
            ],
            "team_stats": [
                {"teamId": "1610612747", "points": 100},
                {"teamId": "1610612744", "points": 90},
            ],
        }


def test_load_boxscores_v3_writes_two_team_rows_per_final_game(sqlite_con_with_data):
    counts = load_canonical_boxscores_for_game(
        sqlite_con_with_data, "0022300001", api_caller=FakeCaller()
    )
    assert counts["team_game_log"] == 2
