"""
Module entry point for the ingest pipeline.

Allows the pipeline to be invoked as:
    python -m src.pipeline [options]

This is equivalent to running:
    uv run ingest [options]

Exit Codes
----------
    0: Success
    1: Validation error (invalid arguments)
    2: Ingest error (pipeline failure)
    3: Unexpected error

See Also
--------
    src.pipeline.cli : CLI argument parsing and main entry point
"""

import sys

from src.pipeline.cli import main

if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
