from src.etl.canonical._coach_assignments import load_coach_assignments
from src.etl.extract.api_client import APICaller


class FakeCaller(APICaller):
    def call_with_backoff(self, fn, **kwargs):
        # Return fake coaches dataset
        if "CommonTeamRoster" in kwargs.get("label", ""):
            return [
                {
                    "COACH_ID": "123",
                    "TEAM_ID": "1610612747",
                    "COACH_NAME": "Darvin Ham",
                    "FIRST_NAME": "Darvin",
                    "LAST_NAME": "Ham",
                    "SEASON_ID": "2023-24",
                }
            ]
        return fn()


class FakeCallerNoCoaches(APICaller):
    def call_with_backoff(self, fn, **kwargs):
        return []


def test_load_roster_upserts_dim_coach_and_fact_team_coach_game(sqlite_con_with_data):
    # we must insert a fake game first, since fact_team_coach_game references game_id
    sqlite_con_with_data.execute(
        "INSERT OR IGNORE INTO fact_game (game_id, season_id, game_date, home_team_id, away_team_id, season_type, status) VALUES ('0022300001', '2023-24', '2023-10-24', '1610612747', '1610612744', 'Regular Season', 'Final')"
    )
    counts = load_coach_assignments(
        sqlite_con_with_data, season_id="2023-24", api_caller=FakeCaller()
    )
    assert counts["dim_coach"] >= 1
    assert counts["fact_team_coach_game"] >= 1


def test_load_roster_handles_missing_coaches_dataset(sqlite_con_with_data):
    counts = load_coach_assignments(
        sqlite_con_with_data, season_id="1950-51", api_caller=FakeCallerNoCoaches()
    )
    assert counts["dim_coach"] == 0
