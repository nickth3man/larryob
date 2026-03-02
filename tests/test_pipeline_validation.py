import pytest

from src.pipeline.exceptions import ReconciliationError
from src.pipeline.parity import run_blocking_parity_gates


def test_blocking_parity_gate_raises_on_score_mismatch(sqlite_con_with_data):
    # insert fake mismatch
    sqlite_con_with_data.execute(
        "INSERT OR IGNORE INTO fact_game (game_id, season_id, game_date, home_team_id, away_team_id, season_type, status) VALUES ('0022300001', '2023-24', '2023-10-24', '1610612747', '1610612744', 'Regular Season', 'Final')"
    )
    sqlite_con_with_data.execute(
        "INSERT INTO team_game_log (game_id, team_id, pts) VALUES ('0022300001', '1610612747', 100)"
    )
    sqlite_con_with_data.execute(
        "INSERT INTO player_game_log (game_id, team_id, player_id, pts) VALUES ('0022300001', '1610612747', '2544', 99)"
    )

    with pytest.raises(ReconciliationError):
        run_blocking_parity_gates(sqlite_con_with_data, seasons=("2023-24",))
