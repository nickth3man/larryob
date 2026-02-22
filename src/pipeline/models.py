"""
Core pipeline data models: Stage enum, IngestConfig, and CheckpointState.

IngestConfig is the single source of truth for all runtime flags; it is built
once from parsed CLI arguments and threaded through the pipeline unchanged.
CheckpointState is mutable, accumulating row-count snapshots between stages.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path

from src.etl.config import MetricsConfig
from src.pipeline.validation import _normalize_seasons


class Stage(StrEnum):
    """ETL pipeline stages for checkpoint tracking."""

    DIMENSIONS = "post-dimensions"
    RAW_BACKFILL = "post-raw-backfill"
    AWARDS = "post-awards"
    SALARIES = "post-salaries"
    ROSTERS = "post-rosters"
    GAME_LOGS = "post-game-logs"
    PBP = "post-pbp"


@dataclass
class IngestConfig:
    """Configuration for the ingest pipeline."""

    seasons: list[str]
    dims_only: bool = False
    enrich_bio: bool = False
    awards: bool = False
    salaries: bool = False
    rosters: bool = False
    include_playoffs: bool = False
    pbp_limit: int = 0
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
        """Create config from parsed arguments."""
        metrics_enabled = args.metrics or MetricsConfig.enabled()
        metrics_export = args.metrics_export_endpoint or MetricsConfig.export_endpoint()
        seasons = _normalize_seasons(args.seasons)

        return cls(
            seasons=seasons,
            dims_only=args.dims_only,
            enrich_bio=args.enrich_bio,
            awards=args.awards,
            salaries=args.salaries,
            rosters=args.rosters,
            include_playoffs=args.include_playoffs,
            pbp_limit=args.pbp_limit,
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


@dataclass
class CheckpointState:
    """Mutable state for checkpoint tracking."""

    status_map: dict[str, int] = field(default_factory=dict)
    table_counts: dict[str, int | None] = field(default_factory=dict)
    last_timestamp: float | None = None

    def update(
        self,
        status_map: dict[str, int],
        table_counts: dict[str, int | None],
        timestamp: float,
    ) -> None:
        """Update checkpoint state atomically."""
        self.status_map = status_map
        self.table_counts = table_counts
        self.last_timestamp = timestamp
