"""
Shared ETL utilities: caching, upsert helpers, logging, and ETL run tracking.

BACKWARD COMPATIBILITY LAYER: This module re-exports functions from their new locations.
After all imports are migrated, this file will be removed.
"""

# Re-export from new locations for backward compatibility
from ..db.cache.file_cache import cache_path, load_cache, save_cache
from ..db.operations.upsert import (
    _chunked,
    _validate_identifier,
    transaction,
    upsert_rows,
)
from ..db.tracking.etl_log import already_loaded, log_load_summary, record_run
from .logging import LOG_DATE_FORMAT, LOG_FORMAT, setup_logging

__all__ = [
    "setup_logging",
    "LOG_FORMAT",
    "LOG_DATE_FORMAT",
    "load_cache",
    "save_cache",
    "cache_path",
    "transaction",
    "upsert_rows",
    "_validate_identifier",
    "_chunked",
    "already_loaded",
    "record_run",
    "log_load_summary",
]

# Note: Original implementations removed - now re-exported from:
# - src/etl/logging.py (setup_logging, LOG_FORMAT, LOG_DATE_FORMAT)
# - src/db/cache/file_cache.py (cache_path, load_cache, save_cache)
# - src/db/operations/upsert.py (transaction, upsert_rows, _validate_identifier, _chunked)
# - src/db/tracking/etl_log.py (already_loaded, record_run, log_load_summary)
