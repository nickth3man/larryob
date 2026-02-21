"""Tests for centralized configuration (src.etl.config)."""

import os

from src.etl.config import (
    APIConfig,
    CacheConfig,
    MetricsConfig,
    get_salary_cap,
    get_team_metadata,
    nba_abbr_to_bref,
)


class TestAPIConfig:
    """Test API rate limiting configuration."""

    def test_base_sleep_default(self) -> None:
        """Default base sleep should be 3.0 seconds."""
        # Clear any existing env var
        os.environ.pop("LARRYOB_API_DELAY_SECONDS", None)
        assert APIConfig.base_sleep() == 3.0

    def test_base_sleep_from_env(self) -> None:
        """Base sleep can be overridden via environment variable."""
        os.environ["LARRYOB_API_DELAY_SECONDS"] = "5.0"
        try:
            assert APIConfig.base_sleep() == 5.0
        finally:
            os.environ.pop("LARRYOB_API_DELAY_SECONDS", None)

    def test_max_retries_default(self) -> None:
        """Default max retries should be 5."""
        os.environ.pop("LARRYOB_API_MAX_RETRIES", None)
        assert APIConfig.max_retries() == 5

    def test_max_retries_from_env(self) -> None:
        """Max retries can be overridden via environment variable."""
        os.environ["LARRYOB_API_MAX_RETRIES"] = "10"
        try:
            assert APIConfig.max_retries() == 10
        finally:
            os.environ.pop("LARRYOB_API_MAX_RETRIES", None)

    def test_inter_call_sleep_default(self) -> None:
        """Default inter-call sleep should be 2.0 seconds."""
        os.environ.pop("LARRYOB_INTER_CALL_SLEEP", None)
        assert APIConfig.inter_call_sleep() == 2.0


class TestCacheConfig:
    """Test cache configuration."""

    def test_cache_version(self) -> None:
        """Cache version should be 2."""
        assert CacheConfig.CACHE_VERSION == 2

    def test_cache_dir_default(self) -> None:
        """Default cache directory should be .cache/ at project root."""
        os.environ.pop("LARRYOB_CACHE_DIR", None)
        cache_dir = CacheConfig.cache_dir()
        assert cache_dir.name == ".cache"

    def test_cache_dir_from_env(self) -> None:
        """Cache directory can be overridden via environment variable."""
        os.environ["LARRYOB_CACHE_DIR"] = "/tmp/test_cache"
        try:
            cache_dir = CacheConfig.cache_dir()
            # Use as_posix() for cross-platform compatibility
            assert cache_dir.as_posix() == "/tmp/test_cache"
        finally:
            os.environ.pop("LARRYOB_CACHE_DIR", None)


class TestMetricsConfig:
    """Test metrics configuration."""

    def test_enabled_default(self) -> None:
        """Metrics should be disabled by default."""
        os.environ.pop("LARRYOB_METRICS_ENABLED", None)
        assert MetricsConfig.enabled() is False

    def test_enabled_from_env(self) -> None:
        """Metrics can be enabled via environment variable."""
        os.environ["LARRYOB_METRICS_ENABLED"] = "true"
        try:
            assert MetricsConfig.enabled() is True
        finally:
            os.environ.pop("LARRYOB_METRICS_ENABLED", None)

    def test_export_endpoint_default(self) -> None:
        """Export endpoint should be None by default."""
        os.environ.pop("LARRYOB_METRICS_ENDPOINT", None)
        assert MetricsConfig.export_endpoint() is None

    def test_export_endpoint_from_env(self) -> None:
        """Export endpoint can be set via environment variable."""
        os.environ["LARRYOB_METRICS_ENDPOINT"] = "http://localhost:9090/metrics"
        try:
            assert MetricsConfig.export_endpoint() == "http://localhost:9090/metrics"
        finally:
            os.environ.pop("LARRYOB_METRICS_ENDPOINT", None)


class TestTeamMetadata:
    """Test team metadata configuration."""

    def test_get_team_metadata_existing(self) -> None:
        """Should return metadata for existing teams."""
        metadata = get_team_metadata("1610612747")  # Lakers
        assert metadata is not None
        # Team metadata contains conference, division, arena_name, colors, founded_year
        assert "conference" in metadata
        assert metadata["conference"] == "West"
        assert "arena_name" in metadata
        assert metadata["arena_name"] == "Crypto.com Arena"

    def test_get_team_metadata_missing(self) -> None:
        """Should return None for non-existent teams."""
        metadata = get_team_metadata("0000000000")
        assert metadata is None


class TestSalaryCap:
    """Test salary cap configuration."""

    def test_get_salary_cap_existing(self) -> None:
        """Should return salary cap for existing seasons."""
        cap = get_salary_cap("2023-24")
        assert cap == 136_021_000

    def test_get_salary_cap_future(self) -> None:
        """Should return salary cap for future seasons."""
        cap = get_salary_cap("2024-25")
        assert cap == 140_588_000

    def test_get_salary_cap_missing(self) -> None:
        """Should return None for non-existent seasons."""
        cap = get_salary_cap("1950-51")
        assert cap is None


class TestAbbreviationMapping:
    """Test NBA to Basketball-Reference abbreviation mapping."""

    def test_nba_abbr_to_bref_direct(self) -> None:
        """Direct mappings should work."""
        assert nba_abbr_to_bref("ATL") == "ATL"
        assert nba_abbr_to_bref("BOS") == "BOS"

    def test_nba_abbr_to_bref_different(self) -> None:
        """Teams with different abbreviations should map correctly."""
        assert nba_abbr_to_bref("BKN") == "BRK"  # Nets
        assert nba_abbr_to_bref("CHA") == "CHO"  # Hornets
        assert nba_abbr_to_bref("PHX") == "PHO"  # Suns

    def test_nba_abbr_to_bref_missing(self) -> None:
        """Non-existent abbreviations should return None."""
        assert nba_abbr_to_bref("XXX") is None
