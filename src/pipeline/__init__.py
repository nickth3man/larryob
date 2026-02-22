"""
src.pipeline — ingest pipeline orchestration package.

This package provides the CLI and orchestration layer for the NBA analytics
ingest pipeline. It coordinates ETL loaders, checkpoint logging, and analytics
view execution.

Public API
----------
The following symbols are re-exported for external callers (tests, notebooks):

- ``IngestConfig``: Configuration dataclass built from CLI arguments
- ``Stage``: StrEnum of pipeline stages (for checkpoint tracking)
- ``CheckpointState``: Mutable state for tracking progress between stages
- ``run_ingest_pipeline``: Main entry point for programmatic pipeline execution

Usage
-----
    # Programmatic usage (from tests or notebooks)
    from src.pipeline import IngestConfig, run_ingest_pipeline

    config = IngestConfig(seasons=["2023-24"], awards=True)
    run_ingest_pipeline(con, config)

    # CLI usage
    uv run ingest --seasons 2023-24 --awards

Module Layout
-------------
- ``cli.py``: Argument parser and main() entry point
- ``executor.py``: Stage plan construction and execution
- ``stages.py``: Individual stage runner functions
- ``models.py``: IngestConfig, Stage, CheckpointState
- ``checkpoint.py``: Row-count logging and etl_run_log tailing
- ``analytics.py``: DuckDB view querying and export
- ``validation.py``: Input validation helpers
- ``exceptions.py``: Pipeline exception hierarchy
- ``constants.py``: Default values and stage-to-table mappings
"""

from src.pipeline.executor import run_ingest_pipeline
from src.pipeline.models import CheckpointState, IngestConfig, Stage

__all__ = [
    "CheckpointState",
    "IngestConfig",
    "Stage",
    "run_ingest_pipeline",
]

__version__ = "1.0.0"
