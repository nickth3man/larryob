"""
Core pipeline data models: Stage enum, IngestConfig, and CheckpointState.

This module defines the core data structures used throughout the pipeline:

- Stage: StrEnum of pipeline stages for checkpoint tracking
- IngestConfig: Immutable configuration built from CLI arguments
- CheckpointState: Mutable state for tracking progress between stages

Design Decisions
----------------
- IngestConfig uses __slots__ via dataclass for memory efficiency
- All config fields have sensible defaults for minimal CLI usage
- CheckpointState is deliberately mutable (updated after each stage)
- Stage values use "post-" prefix for clear checkpoint semantics

Usage
-----
    # From CLI arguments
    config = IngestConfig.from_args(args)

    # Direct construction
    config = IngestConfig(seasons=["2023-24", "2024-25"], awards=True)

    # Checkpoint tracking
    state = CheckpointState()
    state.update(status_map, table_counts, timestamp)
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Literal, cast

from src.etl.config import MetricsConfig
from src.pipeline.validation import _normalize_seasons


class Stage(StrEnum):
    """ETL pipeline stages for checkpoint tracking.

    Stage values use "post-" prefix to clearly indicate these are
    checkpoint markers recorded AFTER each stage completes.

    Attributes:
        DIMENSIONS: After seeding dim_season, dim_team, dim_player
        RAW_BACKFILL: After loading from raw/ CSVs
        AWARDS: After loading fact_player_award
        SALARIES: After loading dim_salary_cap and fact_salary
        ROSTERS: After loading fact_roster
        GAME_LOGS: After loading box scores (player_game_log, team_game_log)
        PBP: After loading fact_play_by_play
    """

    DIMENSIONS = "post-dimensions"
    RAW_BACKFILL = "post-raw-backfill"
    AWARDS = "post-awards"
    SALARIES = "post-salaries"
    ROSTERS = "post-rosters"
    GAME_LOGS = "post-game-logs"
    PBP = "post-pbp"


@dataclass(slots=True)
class IngestConfig:
    """Configuration for the ingest pipeline.

    This is the single source of truth for all runtime flags. It is built
    once from parsed CLI arguments and threaded through the pipeline unchanged.

    Attributes:
        seasons: List of season IDs to ingest (e.g., ["2023-24", "2024-25"])
        dims_only: If True, only seed dimension tables (no box scores)
        enrich_bio: If True, enrich dim_player with CommonPlayerInfo
        awards: If True, load fact_player_award
        salaries: If True, load dim_salary_cap and fact_salary
        rosters: If True, load fact_roster
        include_playoffs: If True, also ingest Playoffs game logs
        pbp_limit: Max games to load PBP for (0 = skip PBP)
        pbp_source: PBP data source strategy ("api", "bulk", or "auto")
        pbp_bulk_dir: Path to raw/pbp/ directory for bulk PBP loading
        salary_source: Salary data source strategy ("bref", "open", or "auto")
        salary_open_file: Path to open-source salary CSV
        skip_reconciliation: If True, skip post-boxscore reconciliation
        reconciliation_warn_only: If True, don't fail on reconciliation errors
        raw_backfill: If True, seed tables from raw/ CSVs
        raw_dir: Path to raw/ directory (defaults to repo_root/raw)
        raw_backfill_fail_fast: If True, stop on first backfill error
        analytics_view: DuckDB view name to query after ingest
        analytics_limit: Row limit for analytics view queries
        analytics_output: Optional path to export analytics results
        analytics_only: If True, skip ingest and only run analytics query
        metrics_enabled: If True, collect in-memory metrics
        metrics_summary: If True, log metrics summary at end
        metrics_export_endpoint: Optional HTTP endpoint for metrics export
        runlog_tail: Number of etl_run_log rows to show at checkpoints
    """

    seasons: tuple[str, ...]
    correction_window_days: int = 14
    dims_only: bool = False
    enrich_bio: bool = False
    awards: bool = True
    salaries: bool = True
    rosters: bool = True
    include_playoffs: bool = True
    pbp_limit: int = 0
    pbp_source: Literal["api", "bulk", "auto"] = "auto"
    pbp_bulk_dir: Path | None = None
    salary_source: Literal["bref", "open", "auto"] = "auto"
    salary_open_file: Path | None = None
    skip_reconciliation: bool = False
    reconciliation_warn_only: bool = False
    raw_backfill: bool = False
    raw_dir: Path | None = None
    raw_backfill_fail_fast: bool = False
    analytics_view: str | None = None
    analytics_limit: int = 20
    analytics_output: Path | None = None
    analytics_only: bool = False
    metrics_enabled: bool = False
    metrics_summary: bool = False
    metrics_export_endpoint: str | None = None
    runlog_tail: int = 12

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> IngestConfig:
        """Create config from parsed CLI arguments.

        Args:
            args: Parsed argparse Namespace containing all CLI flags.

        Returns:
            IngestConfig instance with validated and normalized values.

        Note:
            Seasons are normalized (trimmed, de-duplicated) before storage.
            Metrics flags are OR'd with environment variable defaults.
        """
        metrics_enabled = args.metrics or MetricsConfig.enabled()
        metrics_export = args.metrics_export_endpoint or MetricsConfig.export_endpoint()
        seasons = tuple(_normalize_seasons(args.seasons))

        return cls(
            seasons=seasons,
            dims_only=args.dims_only,
            enrich_bio=args.enrich_bio,
            awards=args.awards,
            salaries=args.salaries,
            rosters=args.rosters,
            include_playoffs=args.include_playoffs,
            pbp_limit=args.pbp_limit,
            pbp_source=cast(Literal["api", "bulk", "auto"], args.pbp_source),
            pbp_bulk_dir=Path(args.pbp_bulk_dir) if args.pbp_bulk_dir else None,
            salary_source=cast(Literal["bref", "open", "auto"], args.salary_source),
            salary_open_file=Path(args.salary_open_file) if args.salary_open_file else None,
            skip_reconciliation=args.skip_reconciliation,
            reconciliation_warn_only=args.reconciliation_warn_only,
            raw_backfill=args.raw_backfill,
            raw_dir=Path(args.raw_dir) if args.raw_dir else None,
            raw_backfill_fail_fast=args.raw_backfill_fail_fast,
            analytics_view=args.analytics_view,
            analytics_limit=args.analytics_limit,
            analytics_output=Path(args.analytics_output) if args.analytics_output else None,
            analytics_only=args.analytics_only,
            metrics_enabled=metrics_enabled,
            metrics_summary=args.metrics_summary,
            metrics_export_endpoint=metrics_export,
            runlog_tail=args.runlog_tail,
        )

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if self.pbp_limit < 0:
            raise ValueError(f"pbp_limit must be >= 0, got {self.pbp_limit}")
        if self.analytics_limit <= 0:
            raise ValueError(f"analytics_limit must be > 0, got {self.analytics_limit}")
        if self.runlog_tail <= 0:
            raise ValueError(f"runlog_tail must be > 0, got {self.runlog_tail}")


@dataclass
class CheckpointState:
    """Mutable state for checkpoint tracking.

    This class accumulates row-count snapshots between pipeline stages.
    It is deliberately mutable to allow in-place updates without
    creating new objects at each checkpoint.

    Attributes:
        status_map: Counts of etl_run_log rows by status (e.g., {"DONE": 42})
        table_counts: Row counts per table (None if table doesn't exist)
        last_timestamp: perf_counter() value at last checkpoint
    """

    status_map: dict[str, int] = field(default_factory=dict)
    table_counts: dict[str, int | None] = field(default_factory=dict)
    last_timestamp: float | None = None

    def update(
        self,
        status_map: dict[str, int],
        table_counts: dict[str, int | None],
        timestamp: float,
    ) -> None:
        """Update checkpoint state atomically.

        Args:
            status_map: New status counts from etl_run_log.
            table_counts: New row counts per table.
            timestamp: Current perf_counter() value.
        """
        self.status_map = status_map.copy()
        self.table_counts = table_counts.copy()
        self.last_timestamp = timestamp
