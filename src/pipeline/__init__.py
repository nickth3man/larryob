"""
src.pipeline — ingest pipeline orchestration package.

Public API re-exported here so that external callers (tests, notebooks, etc.)
can import from a single stable location without knowing the internal layout.
"""

from src.pipeline.executor import run_ingest_pipeline
from src.pipeline.models import CheckpointState, IngestConfig, Stage

__all__ = [
    "CheckpointState",
    "IngestConfig",
    "Stage",
    "run_ingest_pipeline",
]
