from src.etl.canonical._game_inventory import load_canonical_game_inventory
from src.etl.extract.api_client import APICaller
from src.etl.extract.endpoints import fetch_schedule_league_v2


class FakeCaller(APICaller):
    def call_with_backoff(self, fn, **kwargs):
        label = kwargs.get("label", "")
        if "Schedule" in label:
            return [
                {
                    "game_date": "2023-10-24",
                    "game_id": "0022300001",
                    "status": "Scheduled",
                    "season_id": "2023-24",
                    "home_team_id": "1610612747",
                    "away_team_id": "1610612744",
                    "season_type": "Regular Season",
                }
            ]
        if "Scoreboard" in label:
            return [{"game_id": "0022300001", "status": "Final"}]
        return fn()


class FakeCallerEmptyWeeks(APICaller):
    def call_with_backoff(self, fn, **kwargs):
        return []


def test_load_canonical_game_inventory_applies_scoreboard_corrections(sqlite_con_with_data):
    counts = load_canonical_game_inventory(sqlite_con_with_data, "2023-24", api_caller=FakeCaller())
    assert counts["fact_game"] > 0
    row = sqlite_con_with_data.execute(
        "SELECT status FROM fact_game WHERE game_id='0022300001'"
    ).fetchone()
    assert row[0] == "Final"


def test_load_canonical_game_inventory_handles_empty_season_weeks():
    rows = fetch_schedule_league_v2("1947-48", api_caller=FakeCallerEmptyWeeks())
    assert isinstance(rows, list)
