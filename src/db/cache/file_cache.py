"""File-based caching for API responses and computed data."""

import json
import logging
import os
import tempfile
import time
from pathlib import Path
from typing import Any

from ...etl.config import CacheConfig

logger = logging.getLogger(__name__)

CACHE_DIR = CacheConfig.cache_dir()
CACHE_DIR.mkdir(exist_ok=True)

CACHE_VERSION = CacheConfig.CACHE_VERSION


def cache_path(key: str) -> Path:
    """
    Compute the filesystem path for the cache file corresponding to the given cache key.
    
    Parameters:
        key (str): Cache key used as the filename; ".json" is appended.
    
    Returns:
        Path: Path to the cache file located inside CACHE_DIR.
    """
    return CACHE_DIR / f"{key}.json"


def load_cache(key: str, ttl_days: float | None = None) -> Any | None:
    """
    Retrieve the cached value for a given key, validating stored cache version and optional TTL.
    
    Parameters:
        key (str): Cache key (filename without the ".json" extension).
        ttl_days (float | None): Time-to-live in days; if None, the cached entry does not expire.
    
    Returns:
        Any | None: The cached data if present, matches the current cache version, and is not expired; `None` if the file is missing, expired, has a different version, or is corrupted.
    """
    p = cache_path(key)
    if not p.exists():
        return None

    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(data, dict) and "v" in data and "ts" in data and "data" in data:
            if data["v"] != CACHE_VERSION:
                return None
            if ttl_days is not None:
                age_seconds = time.time() - data["ts"]
                if age_seconds > ttl_days * 86400:
                    return None
            return data["data"]
    except json.JSONDecodeError as e:
        logger.warning("Cache file %s is corrupted, treating as cache miss: %s", p, e)
        return None
    return None


def save_cache(key: str, data: Any) -> None:
    """
    Persist `data` under the given cache `key` on disk with an atomic update and include cache version and timestamp.
    
    Parameters:
        key (str): Cache key used to derive the on-disk filename (stored as "<key>.json" in the cache directory).
        data (Any): JSON-serializable value to store as the cached payload; the saved file will contain the cache `v` (version), `ts` (timestamp), and `data` fields.
    """
    payload = {
        "v": CACHE_VERSION,
        "ts": time.time(),
        "data": data,
    }
    target_path = cache_path(key)

    # Write to a temporary file in the same directory, then rename atomically
    with tempfile.NamedTemporaryFile(
        "w", dir=target_path.parent, delete=False, encoding="utf-8"
    ) as tf:
        json.dump(payload, tf)
        tmp_name = tf.name

    os.replace(tmp_name, target_path)
