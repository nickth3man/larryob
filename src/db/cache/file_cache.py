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
    """Get the cache file path for a given key."""
    return CACHE_DIR / f"{key}.json"


def load_cache(key: str, ttl_days: float | None = None) -> Any | None:
    """
    Load cached data from disk.

    Parameters
    ----------
    key : str
        Cache key (filename without .json extension)
    ttl_days : float | None
        Time-to-live in days. If None, cache never expires.

    Returns
    -------
    Any | None
        Cached data if valid, None if expired or not found
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
    Save data to cache using atomic file replacement.

    Parameters
    ----------
    key : str
        Cache key
    data : Any
        Data to cache
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
