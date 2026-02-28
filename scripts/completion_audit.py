"""completion_audit.py – Evaluate the completion state of nba_raw_data.db.

Outputs:
    research/completion_report.md
    research/players_missing_bref_id.txt
"""

from __future__ import annotations

import argparse
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ALL_TABLES: list[str] = [
    # dimensions
    "dim_season",
    "dim_team",
    "dim_player",
    "dim_salary_cap",
    "dim_league_season",
    "dim_team_history",
    # facts
    "fact_roster",
    "fact_game",
    "fact_play_by_play",
    "fact_player_award",
    "fact_all_star",
    "fact_all_nba",
    "fact_all_nba_vote",
    "fact_salary",
    "fact_team_season",
    "fact_draft",
    "fact_player_season_stats",
    "fact_player_advanced_season",
    "fact_player_shooting_season",
    "fact_player_pbp_season",
    # logs
    "team_game_log",
    "player_game_log",
    "etl_run_log",
]

ALL_TABLES: list[str] = [
    # dimensions
    "dim_season",
    "dim_team",
    "dim_player",
    "dim_salary_cap",
    "dim_league_season",
    "dim_team_history",
    # facts
    "fact_roster",
    "fact_game",
    "fact_play_by_play",
    "fact_player_award",
    "fact_all_star",
    "fact_all_nba",
    "fact_all_nba_vote",
    "fact_salary",
    "fact_team_season",
    "fact_draft",
    "fact_player_season_stats",
    "fact_player_advanced_season",
    "fact_player_shooting_season",
    "fact_player_pbp_season",
    # logs
    "team_game_log",
    "player_game_log",
    "etl_run_log",
]

# Completeness contract constants
REQUIRED_GAME_TYPES: tuple[str, ...] = ("Preseason", "Regular Season", "Play-In", "Playoffs")
CONTRACT_FIRST_SEASON = "1946-47"
# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _table_exists(con: sqlite3.Connection, table: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row is not None


def _count(con: sqlite3.Connection, table: str) -> int | None:
    """Return row count for *table*, or None if the table doesn't exist."""
    if not _table_exists(con, table):
        return None
    return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # noqa: S608


def _column_exists(con: sqlite3.Connection, table: str, column: str) -> bool:
    if not _table_exists(con, table):
        return False
    cols = [row[1] for row in con.execute(f"PRAGMA table_info({table})").fetchall()]
    return column in cols


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------


def evaluate_completion(con: sqlite3.Connection) -> dict:
    """Run all queries and return a structured result dict."""
    data: dict = {}

    # 1. Row counts
    row_counts: dict[str, int | None] = {}
    for table in ALL_TABLES:
        row_counts[table] = _count(con, table)
    data["row_counts"] = row_counts

    # 2. dim_player.bref_id coverage
    bref_id: dict = {"available": False}
    if _column_exists(con, "dim_player", "bref_id"):
        bref_id["available"] = True
        row = con.execute(
            "SELECT COUNT(*) FILTER (WHERE bref_id IS NULL),"
            "       COUNT(*) FILTER (WHERE bref_id IS NOT NULL),"
            "       COUNT(*)"
            " FROM dim_player"
        ).fetchone()
        bref_id["null_count"] = row[0]
        bref_id["non_null_count"] = row[1]
        bref_id["total"] = row[2]
        pct = (row[1] / row[2] * 100) if row[2] else 0.0
        bref_id["coverage_pct"] = round(pct, 1)
    data["bref_id"] = bref_id

    # 3. dim_team.bref_abbrev coverage
    bref_abbrev: dict = {"available": False}
    if _column_exists(con, "dim_team", "bref_abbrev"):
        bref_abbrev["available"] = True
        row = con.execute(
            "SELECT COUNT(*) FILTER (WHERE bref_abbrev IS NULL),"
            "       COUNT(*) FILTER (WHERE bref_abbrev IS NOT NULL),"
            "       COUNT(*)"
            " FROM dim_team"
        ).fetchone()
        bref_abbrev["null_count"] = row[0]
        bref_abbrev["non_null_count"] = row[1]
        bref_abbrev["total"] = row[2]
        pct = (row[1] / row[2] * 100) if row[2] else 0.0
        bref_abbrev["coverage_pct"] = round(pct, 1)
    data["bref_abbrev"] = bref_abbrev

    # 4. ETL run log summary
    etl_summary: dict = {"available": False, "status_counts": {}}
    if _table_exists(con, "etl_run_log"):
        etl_summary["available"] = True
        has_table_name = _column_exists(con, "etl_run_log", "table_name")
        has_status = _column_exists(con, "etl_run_log", "status")
        if has_table_name and has_status:
            rows = con.execute(
                "SELECT table_name, status, COUNT(*) AS cnt"
                " FROM etl_run_log"
                " GROUP BY table_name, status"
                " ORDER BY table_name, status"
            ).fetchall()
            status_map: dict[str, dict[str, int]] = {}
            for table_name, status, cnt in rows:
                status_map.setdefault(table_name, {})[status] = cnt
            etl_summary["status_counts"] = status_map

            # overall totals
            overall: dict[str, int] = {}
            for tbl_statuses in status_map.values():
                for status, cnt in tbl_statuses.items():
                    overall[status] = overall.get(status, 0) + cnt
            etl_summary["overall"] = overall
        else:
            # Fallback: just count total rows
            etl_summary["total_rows"] = _count(con, "etl_run_log")
    data["etl_summary"] = etl_summary

    # 5. fact_play_by_play status
    pbp_count = row_counts.get("fact_play_by_play")
    data["play_by_play_empty"] = pbp_count == 0 if pbp_count is not None else None

    # 6. fact_salary season coverage
    salary_seasons: dict = {"available": False, "seasons": []}
    if _table_exists(con, "fact_salary") and _column_exists(con, "fact_salary", "season_id"):
        salary_seasons["available"] = True
        rows = con.execute(
            "SELECT season_id, COUNT(*) AS cnt"
            " FROM fact_salary"
            " GROUP BY season_id"
            " ORDER BY season_id"
        ).fetchall()
        salary_seasons["seasons"] = [(str(r[0]), r[1]) for r in rows]
    data["salary_seasons"] = salary_seasons

    # 7. Players missing bref_id
    missing_players: list[str] = []
    if _column_exists(con, "dim_player", "bref_id"):
        rows = con.execute(
            "SELECT full_name FROM dim_player WHERE bref_id IS NULL ORDER BY full_name"
        ).fetchall()
        missing_players = [r[0] for r in rows if r[0] is not None]
    data["missing_players"] = missing_players

    # 8. Missing required game types
    missing_game_types: list[str] = []
    if _table_exists(con, "fact_game") and _column_exists(con, "fact_game", "season_type"):
        rows = con.execute("SELECT DISTINCT season_type FROM fact_game").fetchall()
        present_types = {r[0] for r in rows if r[0]}
        missing_game_types = [gt for gt in REQUIRED_GAME_TYPES if gt not in present_types]
    else:
        missing_game_types = list(REQUIRED_GAME_TYPES)
    data["missing_required_game_types"] = missing_game_types

    # 9. Season range compliance
    season_range: dict = {
        "expected_start": CONTRACT_FIRST_SEASON,
        "complete": False,
        "actual_start": None,
        "actual_end": None,
    }
    if _table_exists(con, "dim_season"):
        rows = con.execute("SELECT MIN(season_id), MAX(season_id) FROM dim_season").fetchone()
        if rows and rows[0]:
            season_range["actual_start"] = rows[0]
            season_range["actual_end"] = rows[1]
            season_range["complete"] = rows[0] == CONTRACT_FIRST_SEASON
    data["season_range"] = season_range

    # 10. Unresolved entities
    unresolved: dict = {"players_without_identifier": 0, "teams_without_identifier": 0}
    if _table_exists(con, "dim_player"):
        if _table_exists(con, "dim_player_identifier"):
            row = con.execute(
                "SELECT COUNT(*) FROM dim_player p "
                "WHERE NOT EXISTS (SELECT 1 FROM dim_player_identifier pi WHERE pi.player_id = p.player_id)"
            ).fetchone()
            unresolved["players_without_identifier"] = row[0] if row else 0
        else:
            # No identifier table means all players are unresolved
            unresolved["players_without_identifier"] = _count(con, "dim_player") or 0
    if _table_exists(con, "dim_team"):
        if _table_exists(con, "dim_team_identifier"):
            row = con.execute(
                "SELECT COUNT(*) FROM dim_team t "
                "WHERE NOT EXISTS (SELECT 1 FROM dim_team_identifier ti WHERE ti.team_id = t.team_id)"
            ).fetchone()
            unresolved["teams_without_identifier"] = row[0] if row else 0
    data["unresolved_entities"] = unresolved

    data["generated_at"] = datetime.now(tz=UTC).isoformat()
    return data


# ---------------------------------------------------------------------------
# Report writing
# ---------------------------------------------------------------------------


def _fmt_count(val: int | None) -> str:
    if val is None:
        return "— (table missing)"
    return f"{val:,}"


def write_report(data: dict, output_dir: Path) -> Path:
    """Write research/completion_report.md and return the path."""
    output_dir.mkdir(parents=True, exist_ok=True)
    out = output_dir / "completion_report.md"

    lines: list[str] = []
    a = lines.append

    a("# NBA Database Completion Report")
    a("")
    a(f"_Generated: {data['generated_at']}_")
    a("")

    # --- Table row counts ---
    a("## Table Row Counts")
    a("")
    a("| Table | Rows |")
    a("|-------|-----:|")
    for table, count in data["row_counts"].items():
        a(f"| `{table}` | {_fmt_count(count)} |")
    a("")

    # --- Coverage ---
    a("## Column Coverage")
    a("")

    bref = data["bref_id"]
    if bref["available"]:
        a(f"### `dim_player.bref_id` ({bref['coverage_pct']}% populated)")
        a("")
        a(f"- Total players: {bref['total']:,}")
        a(f"- Non-NULL (have bref_id): {bref['non_null_count']:,}")
        a(f"- NULL (missing bref_id): {bref['null_count']:,}")
    else:
        a("### `dim_player.bref_id`")
        a("")
        a("- Column not found (table may be empty or missing).")
    a("")

    bba = data["bref_abbrev"]
    if bba["available"]:
        a(f"### `dim_team.bref_abbrev` ({bba['coverage_pct']}% populated)")
        a("")
        a(f"- Total teams: {bba['total']:,}")
        a(f"- Non-NULL (have bref_abbrev): {bba['non_null_count']:,}")
        a(f"- NULL (missing bref_abbrev): {bba['null_count']:,}")
    else:
        a("### `dim_team.bref_abbrev`")
        a("")
        a("- Column not found (table may be empty or missing).")
    a("")

    # --- ETL run log ---
    a("## ETL Run Log Summary")
    a("")
    etl = data["etl_summary"]
    if not etl["available"]:
        a("_`etl_run_log` table not found._")
    elif not etl["status_counts"]:
        total = etl.get("total_rows", 0)
        a(f"_Table exists but status/table_name columns not found. Total rows: {total:,}_")
    else:
        overall = etl.get("overall", {})
        if overall:
            a("### Overall status totals")
            a("")
            for status, cnt in sorted(overall.items()):
                a(f"- `{status}`: {cnt:,}")
            a("")

        a("### Per-table breakdown")
        a("")
        a(
            "| table_name | "
            + " | ".join(sorted({s for tbl in etl["status_counts"].values() for s in tbl}))
            + " |"
        )
        all_statuses = sorted({s for tbl in etl["status_counts"].values() for s in tbl})
        header = "| table_name | " + " | ".join(all_statuses) + " |"
        sep = "|------------|" + "|".join(["------:"] * len(all_statuses)) + "|"
        # Replace the last two lines (header was added twice - fix)
        lines.pop()  # remove the bad header
        a(header)
        a(sep)
        for tname, statuses in sorted(etl["status_counts"].items()):
            cols = " | ".join(str(statuses.get(s, 0)) for s in all_statuses)
            a(f"| `{tname}` | {cols} |")
    a("")

    # --- Key gaps ---
    a("## Key Gaps")
    a("")

    gaps: list[str] = []

    pbp = data["play_by_play_empty"]
    if pbp is True:
        gaps.append("`fact_play_by_play` is **empty** — play-by-play data not yet ingested.")
    elif pbp is None:
        gaps.append("`fact_play_by_play` table is **missing**.")

    sal = data["salary_seasons"]
    total_sal = data["row_counts"].get("fact_salary") or 0
    if sal["available"] and total_sal == 0:
        gaps.append("`fact_salary` is **empty** — no salary data ingested.")
    elif sal["available"] and len(sal["seasons"]) < 5:
        gaps.append(
            f"`fact_salary` has only {len(sal['seasons'])} season(s) with data — may be sparse."
        )

    bref = data["bref_id"]
    if bref.get("available") and bref.get("null_count", 0) > 0:
        gaps.append(
            f"{bref['null_count']:,} player(s) missing `bref_id` "
            f"({100 - bref['coverage_pct']:.1f}% uncovered). "
            "See `players_missing_bref_id.txt`."
        )

    bba = data["bref_abbrev"]
    if bba.get("available") and bba.get("null_count", 0) > 0:
        gaps.append(f"{bba['null_count']:,} team(s) missing `bref_abbrev`.")

    missing_tables = [t for t, c in data["row_counts"].items() if c is None]
    if missing_tables:
        gaps.append(
            f"{len(missing_tables)} table(s) not yet created: {', '.join(f'`{t}`' for t in missing_tables)}."
        )

    zero_tables = [t for t, c in data["row_counts"].items() if c == 0]
    if zero_tables:
        gaps.append(
            f"{len(zero_tables)} table(s) exist but have 0 rows: {', '.join(f'`{t}`' for t in zero_tables)}."
        )

    if gaps:
        for gap in gaps:
            a(f"- {gap}")
    else:
        a("_No significant gaps detected._")
    a("")

    # --- Salary season detail ---
    sal = data["salary_seasons"]
    if sal["available"] and sal["seasons"]:
        a("## Salary Season Coverage")
        a("")
        a("| Season | Rows |")
        a("|--------|-----:|")
        for season_id, cnt in sal["seasons"]:
            a(f"| {season_id} | {cnt:,} |")
        a("")

    out.write_text("\n".join(lines), encoding="utf-8")
    return out


def write_missing_players(data: dict, output_dir: Path) -> Path:
    """Write players_missing_bref_id.txt and return the path."""
    output_dir.mkdir(parents=True, exist_ok=True)
    out = output_dir / "players_missing_bref_id.txt"
    players = data["missing_players"]
    out.write_text("\n".join(players) + ("\n" if players else ""), encoding="utf-8")
    return out


# ---------------------------------------------------------------------------
# Stdout summary
# ---------------------------------------------------------------------------


def print_summary(data: dict) -> None:
    rc = data["row_counts"]
    total_rows = sum(c for c in rc.values() if c is not None)
    missing_tables = sum(1 for c in rc.values() if c is None)
    zero_tables = sum(1 for c in rc.values() if c == 0)

    print("=" * 60)
    print("NBA Database Completion Audit")
    print("=" * 60)
    print(f"  Generated : {data['generated_at']}")
    print(f"  Tables    : {len(rc)} expected, {missing_tables} missing, {zero_tables} empty")
    print(f"  Total rows: {total_rows:,}")

    bref = data["bref_id"]
    if bref["available"]:
        print(
            f"  bref_id   : {bref['non_null_count']:,}/{bref['total']:,} players "
            f"({bref['coverage_pct']}%)"
        )

    bba = data["bref_abbrev"]
    if bba["available"]:
        print(
            f"  bref_abbrev: {bba['non_null_count']:,}/{bba['total']:,} teams "
            f"({bba['coverage_pct']}%)"
        )

    pbp = data["play_by_play_empty"]
    pbp_label = "empty" if pbp else ("has data" if pbp is False else "missing")
    print(f"  play_by_play: {pbp_label}")

    sal = data["salary_seasons"]
    sal_seasons = len(sal["seasons"]) if sal["available"] else 0
    print(f"  salary seasons: {sal_seasons}")

    missing = data["missing_players"]
    print(f"  Players missing bref_id: {len(missing)}")
    print("=" * 60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate NBA database completion state.")
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path(__file__).parent.parent / "nba_raw_data.db",
        help="Path to nba_raw_data.db (default: repo root)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).parent.parent / "research",
        help="Directory for output files (default: research/)",
    )
    parser.add_argument(
        "--enforce",
        action="store_true",
        help="Exit with non-zero status if completeness contract is violated",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not args.db_path.exists():
        print(f"Warning: database not found at {args.db_path}. Proceeding with empty results.")
        con = sqlite3.connect(":memory:")
    else:
        con = sqlite3.connect(args.db_path)

    try:
        data = evaluate_completion(con)
    finally:
        con.close()

    report_path = write_report(data, args.output_dir)
    players_path = write_missing_players(data, args.output_dir)

    print_summary(data)
    print(f"  Report  -> {report_path}")
    print(f"  Players -> {players_path}")

    # Enforcement mode: exit non-zero on violations
    if args.enforce:
        violations = []

        # Check season range
        if not data.get("season_range", {}).get("complete", False):
            violations.append("Season range incomplete")

        # Check missing game types
        if data.get("missing_required_game_types"):
            violations.append(f"Missing game types: {data['missing_required_game_types']}")

        # Check unresolved entities
        unresolved = data.get("unresolved_entities", {})
        if unresolved.get("players_without_identifier", 0) > 0:
            violations.append(f"Unresolved players: {unresolved['players_without_identifier']}")

        if violations:
            print("\n[ENFORCE] Completeness violations detected:")
            for v in violations:
                print(f"  - {v}")
            raise SystemExit(1)


if __name__ == "__main__":
    main()
