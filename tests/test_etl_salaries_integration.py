import sqlite3
from unittest.mock import MagicMock, patch

from src.etl.salaries import load_player_salaries


def test_load_player_salaries_historical_season(
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path,
    monkeypatch,
):
    import src.etl.utils as utils_mod
    monkeypatch.setattr(utils_mod, "CACHE_DIR", tmp_path)

    mock_html = """
    <html>
      <body>
        <!-- 
        <table id="salaries2">
            <thead>
                <tr><th>Rk</th><th>Player</th><th>Salary</th></tr>
            </thead>
            <tbody>
                <tr><td>1</td><td>LeBron James</td><td>$47,607,350</td></tr>
                <tr><td>2</td><td>Unknown Player</td><td>$1,000,000</td></tr>
            </tbody>
        </table>
        -->
      </body>
    </html>
    """

    with patch("src.etl.salaries._get_html", return_value=mock_html):
        with patch("src.etl.salaries.time.sleep"):
            inserted = load_player_salaries(sqlite_con_with_data, "2023-24")

    row = sqlite_con_with_data.execute("SELECT player_id, salary FROM fact_salary").fetchone()
    assert row[0] == "2544"
    assert row[1] == 47607350


def test_load_player_salaries_current_season(
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path,
    monkeypatch,
):
    import src.etl.utils as utils_mod
    monkeypatch.setattr(utils_mod, "CACHE_DIR", tmp_path)
    
    import datetime as dt
    current_year = dt.date.today().year
    season = f"{current_year}-{str(current_year+1)[-2:]}"

    mock_html = f"""
    <html>
      <body>
        <table id="contracts">
            <thead>
                <tr>
                    <th>Unnamed: 0_level_0</th>
                    <th>Unnamed: 1_level_0</th>
                    <th>Salary</th>
                    <th>Salary</th>
                </tr>
                <tr>
                    <th>Rk</th>
                    <th>Player</th>
                    <th>{season}</th>
                    <th>2030-31</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td>1</td>
                    <td>LeBron James</td>
                    <td>$50,000,000</td>
                    <td>$55,000,000</td>
                </tr>
                <tr>
                    <td>2</td>
                    <td>Unknown Player</td>
                    <td>$2,000,000</td>
                    <td>$0</td>
                </tr>
            </tbody>
        </table>
      </body>
    </html>
    """

    with patch("src.etl.salaries._get_html", return_value=mock_html):
        with patch("src.etl.salaries.time.sleep"):
            inserted = load_player_salaries(sqlite_con_with_data, season)

    row = sqlite_con_with_data.execute(f"SELECT player_id, salary FROM fact_salary WHERE season_id = '{season}'").fetchone()
    assert row[0] == "2544"
    assert row[1] == 50000000
