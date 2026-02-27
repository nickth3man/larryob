"""Application settings and configuration."""

import os
from pathlib import Path


class Settings:
    """Application settings."""

    def __init__(self):
        self.project_root = Path(__file__).parent.parent.parent
        self.data_dir = self.project_root / "data"
        self.db_path = self.data_dir / "databases" / "nba_raw_data.db"
        self.cache_dir = self.data_dir / "cache"


def get_settings() -> Settings:
    """Get application settings."""
    return Settings()
