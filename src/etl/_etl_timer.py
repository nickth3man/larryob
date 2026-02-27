"""ETL duration timing context manager."""

import time
from typing import Any

from src.etl.metrics import record_etl_duration


class ETLTimer:
    """Context manager that records ETL duration on exit."""

    def __init__(self, table: str, season_id: str | None = None):
        self.table = table
        self.season_id = season_id
        self.start_time: float | None = None

    def __enter__(self) -> "ETLTimer":
        self.start_time = time.monotonic()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.start_time is not None:
            record_etl_duration(self.table, self.season_id, time.monotonic() - self.start_time)
