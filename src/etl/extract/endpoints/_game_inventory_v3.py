from src.etl.extract.api_client import APICaller


def fetch_schedule_league_v2(season: str, api_caller: APICaller | None = None) -> list[dict]:
    def _fetch() -> list[dict]:
        from nba_api.stats.endpoints import leaguegamefinder

        df = leaguegamefinder.LeagueGameFinder(season_nullable=season).get_data_frames()[0]
        return df.to_dict(orient="records")

    return (
        api_caller.call_with_backoff(_fetch, label="ScheduleLeagueV2") if api_caller else _fetch()
    )


def fetch_scoreboard_v3_for_dates(
    game_dates: list[str], api_caller: APICaller | None = None
) -> list[dict]:
    def _fetch() -> list[dict]:
        return []

    return api_caller.call_with_backoff(_fetch, label="ScoreboardV3") if api_caller else _fetch()
