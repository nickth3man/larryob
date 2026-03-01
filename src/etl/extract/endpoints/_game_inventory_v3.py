from src.etl.extract.api_client import APICaller


def fetch_schedule_league_v2(season: str, api_caller: APICaller | None = None) -> list[dict]:
    # We will implement this correctly soon.
    return []


def fetch_scoreboard_v3_for_dates(
    game_dates: list[str], api_caller: APICaller | None = None
) -> list[dict]:
    return []
