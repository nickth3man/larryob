import logging
import sqlite3
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.etl.backfill._advanced_stats import (
    load_player_advanced,
    load_player_pbp_season,
    load_player_shooting,
)
from src.etl.backfill._awards import load_awards
from src.etl.backfill._dims import enrich_dim_player, enrich_dim_team, load_team_history
from src.etl.backfill._draft import load_draft
from src.etl.backfill._game_logs import load_player_game_logs, load_team_game_logs
from src.etl.backfill._games import load_games, load_schedule
from src.etl.backfill._season_stats import (
    load_league_season,
    load_player_season_stats,
    load_team_season,
)
from src.etl.utils import already_loaded, record_run

logger = logging.getLogger(__name__)
RAW_DIR = Path("raw")

_LOADERS: list[tuple[str, str, str]] = [
    ("team_history", "dim_team_history", load_team_history.__name__),
    ("dim_team_enrich", "dim_team", enrich_dim_team.__name__),
    ("dim_player_enrich", "dim_player", enrich_dim_player.__name__),
    ("games", "fact_game", load_games.__name__),
    ("schedule", "fact_game", load_schedule.__name__),
    ("player_game_logs", "player_game_log", load_player_game_logs.__name__),
    ("team_game_logs", "team_game_log", load_team_game_logs.__name__),
    ("team_season", "fact_team_season", load_team_season.__name__),
    ("league_season", "dim_league_season", load_league_season.__name__),
    ("draft", "fact_draft", load_draft.__name__),
    ("player_season_stats", "fact_player_season_stats", load_player_season_stats.__name__),
    ("player_advanced", "fact_player_advanced_season", load_player_advanced.__name__),
    ("player_shooting", "fact_player_shooting_season", load_player_shooting.__name__),
    ("player_pbp_season", "fact_player_pbp_season", load_player_pbp_season.__name__),
    ("awards", "fact_player_award", load_awards.__name__),
]


def _table_count(con: sqlite3.Connection, table_name: str) -> int | None:
    try:
        return con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    except sqlite3.OperationalError:
        return None


def run_raw_backfill(
    con: sqlite3.Connection,
    raw_dir: Path = RAW_DIR,
    *,
    fail_fast: bool = False,
) -> dict[str, Any]:
    """
    Execute all raw-data loaders in dependency order.

    Returns a machine-readable run summary:
    {
        "ok": [...loader names...],
        "skipped": [...loader names...],
        "failed": [...loader names...],
        "details": [{"loader": ..., "status": ..., "table": ..., "row_count": ..., "error": ...}]
    }
    """
    logger.info("=== Raw backfill starting (raw_dir=%s) ===", raw_dir)

    summary: dict[str, Any] = {"ok": [], "skipped": [], "failed": [], "details": []}
    total = len(_LOADERS)

    for idx, (name, table_name, loader_name) in enumerate(_LOADERS, start=1):
        loader = globals()[loader_name]
        loader_id = f"backfill.{name}"
        started_at = datetime.now(UTC).isoformat()
        started_perf = time.perf_counter()
        before_count = _table_count(con, table_name)
        logger.info(
            "Raw backfill [%d/%d] starting loader=%s target_table=%s before_row_count=%s",
            idx,
            total,
            name,
            table_name,
            before_count if before_count is not None else "n/a",
        )
        if already_loaded(con, table_name, None, loader_id):
            elapsed = time.perf_counter() - started_perf
            logger.info(
                "Raw backfill [%d/%d] skipped loader=%s (already loaded) elapsed=%.2fs",
                idx,
                total,
                name,
                elapsed,
            )
            summary["skipped"].append(name)
            summary["details"].append(
                {
                    "loader": name,
                    "status": "skipped",
                    "table": table_name,
                    "before_row_count": before_count,
                    "after_row_count": before_count,
                    "delta_row_count": 0,
                    "elapsed_sec": round(elapsed, 3),
                    "row_count": None,
                    "error": None,
                }
            )
            continue

        try:
            loader(con, raw_dir)
            row_count = _table_count(con, table_name)
            elapsed = time.perf_counter() - started_perf
            delta = (
                row_count - before_count
                if row_count is not None and before_count is not None
                else None
            )
            record_run(con, table_name, None, loader_id, row_count, "ok", started_at)
            logger.info(
                "Raw backfill [%d/%d] completed loader=%s row_count=%s before=%s delta=%s elapsed=%.2fs",
                idx,
                total,
                name,
                row_count if row_count is not None else "n/a",
                before_count if before_count is not None else "n/a",
                delta if delta is not None else "n/a",
                elapsed,
            )
            summary["ok"].append(name)
            summary["details"].append(
                {
                    "loader": name,
                    "status": "ok",
                    "table": table_name,
                    "before_row_count": before_count,
                    "after_row_count": row_count,
                    "delta_row_count": delta,
                    "elapsed_sec": round(elapsed, 3),
                    "row_count": row_count,
                    "error": None,
                }
            )
        except Exception as exc:
            elapsed = time.perf_counter() - started_perf
            record_run(con, table_name, None, loader_id, None, "error", started_at)
            logger.exception("Loader %s failed during raw backfill:", name)
            logger.error(
                "Raw backfill [%d/%d] failed loader=%s before=%s elapsed=%.2fs",
                idx,
                total,
                name,
                before_count if before_count is not None else "n/a",
                elapsed,
            )
            summary["failed"].append(name)
            summary["details"].append(
                {
                    "loader": name,
                    "status": "error",
                    "table": table_name,
                    "before_row_count": before_count,
                    "after_row_count": None,
                    "delta_row_count": None,
                    "elapsed_sec": round(elapsed, 3),
                    "row_count": None,
                    "error": str(exc),
                }
            )
            if fail_fast:
                break

    logger.info(
        "=== Raw backfill complete: ok=%d skipped=%d failed=%d ===",
        len(summary["ok"]),
        len(summary["skipped"]),
        len(summary["failed"]),
    )
    for detail in summary["details"]:
        logger.info(
            "Raw backfill detail: loader=%s status=%s table=%s before=%s after=%s delta=%s elapsed_sec=%s error=%s",
            detail["loader"],
            detail["status"],
            detail["table"],
            detail.get("before_row_count"),
            detail.get("after_row_count"),
            detail.get("delta_row_count"),
            detail.get("elapsed_sec"),
            detail.get("error"),
        )
    return summary
