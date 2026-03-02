"""Tests: nba_api V3 endpoint adapters."""

from unittest.mock import MagicMock, patch

import pandas as pd

from src.etl.extract.endpoints._boxscore_v3 import fetch_boxscore_traditional_v3
from src.etl.extract.endpoints._game_inventory_v3 import (
    fetch_schedule_league_v2,
    fetch_scoreboard_v3_for_dates,
)
from src.etl.extract.endpoints._play_by_play_v3 import fetch_play_by_play_v3

# ------------------------------------------------------------------ #

# FakeCaller helpers                                                   #

# ------------------------------------------------------------------ #


class FakeCaller:
    """APICaller stand-in that executes the provided callable directly."""

    def call_with_backoff(self, fn, *, label="", base_sleep=None):
        return fn()

    def call_with_backoff_custom_delay(self, fn, *, label="", base_sleep, max_retries=None):
        return fn()

    def sleep_between_calls(self):
        pass


# ------------------------------------------------------------------ #

# _play_by_play_v3                                                     #

# ------------------------------------------------------------------ #


def _make_pbp_v3_df() -> pd.DataFrame:
    """Minimal PlayByPlayV3 play_by_play dataset."""

    return pd.DataFrame(
        {
            "gameId": ["0022300001", "0022300001", "0022300001"],
            "actionNumber": [1, 2, 3],
            "clock": ["PT11M48.00S", "PT11M32.00S", "PT11M15.00S"],
            "period": [1, 1, 1],
            "teamId": [1610612747, 1610612747, 1610612744],
            "teamTricode": ["LAL", "LAL", "GSW"],
            "personId": [2544, 203999, 0],
            "playerName": ["LeBron James", "Nikola Jokic", ""],
            "actionType": ["2pt", "miss", "rebound"],
            "description": ["LeBron layup", "Miss", "Jokic rebound"],
        }
    )


def test_fetch_play_by_play_v3_normalizes_action_number():
    mock_ep = MagicMock()

    mock_ep.play_by_play.get_data_frame.return_value = _make_pbp_v3_df()

    with patch(
        "src.etl.extract.endpoints._play_by_play_v3.playbyplayv3.PlayByPlayV3",
        return_value=mock_ep,
    ):
        rows = fetch_play_by_play_v3(game_id="0022300001", api_caller=FakeCaller())

    assert len(rows) == 3

    assert rows[0]["game_id"] == "0022300001"

    assert rows[0]["action_number"] == 1


def test_fetch_play_by_play_v3_all_rows_have_game_id():
    mock_ep = MagicMock()

    mock_ep.play_by_play.get_data_frame.return_value = _make_pbp_v3_df()

    with patch(
        "src.etl.extract.endpoints._play_by_play_v3.playbyplayv3.PlayByPlayV3",
        return_value=mock_ep,
    ):
        rows = fetch_play_by_play_v3(game_id="0022300001", api_caller=FakeCaller())

    assert all(r["game_id"] == "0022300001" for r in rows)


def test_fetch_play_by_play_v3_uses_named_dataset_attribute():
    """Must call ep.play_by_play.get_data_frame(), not ep.get_data_frames()[0]."""

    mock_ep = MagicMock()

    mock_ep.play_by_play.get_data_frame.return_value = _make_pbp_v3_df()

    with patch(
        "src.etl.extract.endpoints._play_by_play_v3.playbyplayv3.PlayByPlayV3",
        return_value=mock_ep,
    ):
        fetch_play_by_play_v3(game_id="0022300001", api_caller=FakeCaller())

    mock_ep.play_by_play.get_data_frame.assert_called_once()

    mock_ep.get_data_frames.assert_not_called()


def test_fetch_play_by_play_v3_returns_empty_list_for_empty_df():
    mock_ep = MagicMock()

    mock_ep.play_by_play.get_data_frame.return_value = pd.DataFrame()

    with patch(
        "src.etl.extract.endpoints._play_by_play_v3.playbyplayv3.PlayByPlayV3",
        return_value=mock_ep,
    ):
        rows = fetch_play_by_play_v3(game_id="0022300001", api_caller=FakeCaller())

    assert rows == []


# ------------------------------------------------------------------ #

# _boxscore_v3                                                         #

# ------------------------------------------------------------------ #


def _make_player_stats_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "personId": ["2544"],
            "teamId": ["1610612747"],
            "teamTricode": ["LAL"],
            "points": [25],
            "assists": [5],
            "reboundsTotal": [8],
        }
    )


def _make_team_stats_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "teamId": ["1610612747"],
            "teamTricode": ["LAL"],
            "points": [120],
            "assists": [25],
            "reboundsTotal": [40],
        }
    )


def test_fetch_boxscore_v3_uses_named_dataset_attributes_not_indexes():
    mock_ep = MagicMock()

    mock_ep.player_stats.get_data_frame.return_value = _make_player_stats_df()

    mock_ep.team_stats.get_data_frame.return_value = _make_team_stats_df()

    with patch(
        "src.etl.extract.endpoints._boxscore_v3.boxscoretraditionalv3.BoxScoreTraditionalV3",
        return_value=mock_ep,
    ):
        payload = fetch_boxscore_traditional_v3("0022300001", api_caller=FakeCaller())

    assert "player_stats" in payload and "team_stats" in payload

    mock_ep.player_stats.get_data_frame.assert_called_once()

    mock_ep.team_stats.get_data_frame.assert_called_once()

    mock_ep.get_data_frames.assert_not_called()


def test_fetch_boxscore_v3_returns_dataframes():
    mock_ep = MagicMock()

    mock_ep.player_stats.get_data_frame.return_value = _make_player_stats_df()

    mock_ep.team_stats.get_data_frame.return_value = _make_team_stats_df()

    with patch(
        "src.etl.extract.endpoints._boxscore_v3.boxscoretraditionalv3.BoxScoreTraditionalV3",
        return_value=mock_ep,
    ):
        payload = fetch_boxscore_traditional_v3("0022300001", api_caller=FakeCaller())

    assert isinstance(payload["player_stats"], pd.DataFrame)

    assert isinstance(payload["team_stats"], pd.DataFrame)


def test_fetch_boxscore_v3_player_stats_has_rows():
    mock_ep = MagicMock()

    mock_ep.player_stats.get_data_frame.return_value = _make_player_stats_df()

    mock_ep.team_stats.get_data_frame.return_value = _make_team_stats_df()

    with patch(
        "src.etl.extract.endpoints._boxscore_v3.boxscoretraditionalv3.BoxScoreTraditionalV3",
        return_value=mock_ep,
    ):
        payload = fetch_boxscore_traditional_v3("0022300001", api_caller=FakeCaller())

    assert len(payload["player_stats"]) == 1

    assert len(payload["team_stats"]) == 1


# ------------------------------------------------------------------ #

# _game_inventory_v3                                                   #

# ------------------------------------------------------------------ #


def _make_game_header_df() -> pd.DataFrame:
    """ScoreboardV3 game_header dataset."""

    return pd.DataFrame(
        {
            "gameId": ["0022300001"],
            "gameStatus": [3],
            "gameStatusText": ["Final"],
            "gameClock": [""],
            "period": [4],
        }
    )


def _make_line_score_df() -> pd.DataFrame:
    """ScoreboardV3 line_score dataset — two rows per game (home/away)."""

    return pd.DataFrame(
        {
            "gameId": ["0022300001", "0022300001"],
            "teamId": ["1610612747", "1610612744"],
            "teamCity": ["Los Angeles", "Golden State"],
            "teamName": ["Lakers", "Warriors"],
            "teamTricode": ["LAL", "GSW"],
            "score": [120, 110],
        }
    )


def test_fetch_scoreboard_v3_for_dates_returns_list():
    mock_ep = MagicMock()

    mock_ep.game_header.get_data_frame.return_value = _make_game_header_df()

    mock_ep.line_score.get_data_frame.return_value = _make_line_score_df()

    with patch(
        "src.etl.extract.endpoints._game_inventory_v3.scoreboardv3.ScoreboardV3",
        return_value=mock_ep,
    ):
        rows = fetch_scoreboard_v3_for_dates(["2023-10-24"], api_caller=FakeCaller())

    assert isinstance(rows, list)

    assert len(rows) >= 1

    assert rows[0]["game_id"] == "0022300001"


def test_fetch_scoreboard_v3_for_dates_handles_empty_dates():
    rows = fetch_scoreboard_v3_for_dates([], api_caller=FakeCaller())

    assert rows == []


def test_fetch_scoreboard_v3_uses_named_attributes():
    """Must use named attributes (game_header, line_score), not get_data_frames()."""

    mock_ep = MagicMock()

    mock_ep.game_header.get_data_frame.return_value = _make_game_header_df()

    mock_ep.line_score.get_data_frame.return_value = _make_line_score_df()

    with patch(
        "src.etl.extract.endpoints._game_inventory_v3.scoreboardv3.ScoreboardV3",
        return_value=mock_ep,
    ):
        fetch_scoreboard_v3_for_dates(["2023-10-24"], api_caller=FakeCaller())

    mock_ep.game_header.get_data_frame.assert_called()

    mock_ep.get_data_frames.assert_not_called()


def _make_season_games_df() -> pd.DataFrame:
    """ScheduleLeagueV2 season_games dataset."""

    return pd.DataFrame(
        {
            "gameId": ["0022300001"],
            "gameDate": ["2023-10-24"],
            "gameStatus": [3],
            "gameStatusText": ["Final"],
            "weekNumber": [1],
            "homeTeam_teamId": [1610612747],
            "awayTeam_teamId": [1610612744],
            "homeTeam_score": [120],
            "awayTeam_score": [110],
        }
    )


def test_fetch_schedule_league_v2_returns_list():
    mock_ep = MagicMock()

    mock_ep.season_games.get_data_frame.return_value = _make_season_games_df()

    with patch(
        "src.etl.extract.endpoints._game_inventory_v3.scheduleleaguev2.ScheduleLeagueV2",
        return_value=mock_ep,
    ):
        rows = fetch_schedule_league_v2("2023-24", api_caller=FakeCaller())

    assert isinstance(rows, list)

    assert len(rows) >= 1

    assert rows[0]["game_id"] == "0022300001"

    assert rows[0]["game_date"] == "2023-10-24"


def test_fetch_schedule_league_v2_uses_named_dataset_attribute():
    """Must call ep.season_games.get_data_frame(), not ep.get_data_frames()[index]."""

    mock_ep = MagicMock()

    mock_ep.season_games.get_data_frame.return_value = _make_season_games_df()

    with patch(
        "src.etl.extract.endpoints._game_inventory_v3.scheduleleaguev2.ScheduleLeagueV2",
        return_value=mock_ep,
    ):
        fetch_schedule_league_v2("2023-24", api_caller=FakeCaller())

    mock_ep.season_games.get_data_frame.assert_called_once()

    mock_ep.get_data_frames.assert_not_called()


def test_fetch_schedule_league_v2_handles_empty_response():
    mock_ep = MagicMock()

    mock_ep.season_games.get_data_frame.return_value = pd.DataFrame()

    with patch(
        "src.etl.extract.endpoints._game_inventory_v3.scheduleleaguev2.ScheduleLeagueV2",
        return_value=mock_ep,
    ):
        rows = fetch_schedule_league_v2("1947-48", api_caller=FakeCaller())

    assert isinstance(rows, list)

    assert rows == []
