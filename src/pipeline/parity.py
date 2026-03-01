import sqlite3
from collections.abc import Sequence

from src.etl.validation import query_score_mismatches
from src.pipeline.exceptions import ReconciliationError


def run_blocking_parity_gates(con: sqlite3.Connection, seasons: Sequence[str]) -> None:
    score_mismatches = query_score_mismatches(con, seasons)
    if score_mismatches:
        raise ReconciliationError(len(score_mismatches), seasons=list(seasons))
