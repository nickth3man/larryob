import math

import numpy as np
import pandas as pd

from src.etl.helpers import (
    _flt,
    _int,
    _isna,
    _norm_name,
    int_season_to_id,
    pad_game_id,
    season_id_from_date,
    season_id_from_game_id,
    season_type_from_game_id,
)


def test_isna():
    assert _isna(None) is True
    assert _isna(pd.NA) is True
    assert _isna(np.nan) is True
    assert _isna(math.nan) is True
    assert _isna("") is False
    assert _isna(0) is False
    assert _isna(1) is False
    assert _isna("test") is False


def test_int():
    assert _int(5) == 5
    assert _int("5") == 5
    assert _int(5.0) == 5
    assert _int(None) is None
    assert _int(pd.NA) is None
    assert _int(np.nan) is None
    assert _int("invalid") is None


def test_flt():
    assert _flt(5.5) == 5.5
    assert _flt("5.5") == 5.5
    assert _flt(5) == 5.0
    assert _flt(None) is None
    assert _flt(pd.NA) is None
    assert _flt(np.nan) is None
    assert _flt("invalid") is None


def test_int_season_to_id():
    assert int_season_to_id(2026) == "2025-26"
    assert int_season_to_id(2000) == "1999-00"
    assert int_season_to_id(1950) == "1949-50"
    assert int_season_to_id(1947) == "1946-47"
    assert int_season_to_id(2024.0) == "2023-24"


def test_pad_game_id():
    assert pad_game_id(22300001) == "0022300001"
    assert pad_game_id("22300001") == "0022300001"


def test_season_type_from_game_id():
    assert season_type_from_game_id("0011000001") == "Preseason"
    assert season_type_from_game_id("0022300001") == "Regular Season"
    assert season_type_from_game_id("0052300001") == "Play-In"
    assert season_type_from_game_id("0042300001") == "Playoffs"
    assert season_type_from_game_id("0099000001") == "Regular Season"  # Fallback


def test_season_id_from_game_id():
    assert season_id_from_game_id("0022500686") == "2025-26"
    assert season_id_from_game_id("0022301001") == "2023-24"
    assert season_id_from_game_id("0011900001") == "2019-20"


def test_season_id_from_date():
    assert season_id_from_date("2023-10-24") == "2023-24"
    assert season_id_from_date("2024-04-15") == "2023-24"
    assert season_id_from_date("2024-07-01") == "2024-25"
    assert season_id_from_date("2024-09-30") == "2024-25"
    assert season_id_from_date("1999-02-05T00:00:00") == "1998-99"


def test_norm_name():
    assert _norm_name("Nikola Jokić") == "nikola jokic"
    assert _norm_name("  LeBron James  ") == "lebron james"
    assert _norm_name("D'Angelo Russell") == "d'angelo russell"
    assert _norm_name("Luka Dončić") == "luka doncic"
