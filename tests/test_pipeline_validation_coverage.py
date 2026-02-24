"""
Coverage tests for src.pipeline.validation module.

These tests import the actual validation module to measure coverage properly.
"""

from pathlib import Path

import pytest

from src.pipeline.exceptions import AnalyticsError, ValidationError
from src.pipeline.validation import (
    SUPPORTED_ANALYTICS_EXTENSIONS,
    _normalize_seasons,
    _validate_analytics_output_path,
    _validate_log_level,
    _validate_seasons,
    validate_view_name,
)


class TestNormalizeSeasonsCoverage:
    """Coverage tests for _normalize_seasons - hitting all branches."""

    def test_normalize_seasons_basic(self):
        result = _normalize_seasons(["2023-24", "2024-25"])
        assert result == ["2023-24", "2024-25"]

    def test_normalize_seasons_trims_whitespace(self):
        """Hit line 54: cleaned = season.strip()"""
        result = _normalize_seasons(["  2023-24  ", "\t2024-25\n"])
        assert result == ["2023-24", "2024-25"]

    def test_normalize_seasons_filters_empty_strings(self):
        """Hit line 56: continue for empty strings"""
        result = _normalize_seasons(["", "2023-24", "", "  ", "2024-25"])
        assert result == ["2023-24", "2024-25"]

    def test_normalize_seasons_removes_duplicates(self):
        """Hit line 56: continue for duplicates"""
        result = _normalize_seasons(["2023-24", "2024-25", "2023-24"])
        assert result == ["2023-24", "2024-25"]

    def test_normalize_seasons_empty_input(self):
        result = _normalize_seasons([])
        assert result == []


class TestValidateViewNameCoverage:
    """Coverage tests for validate_view_name - hitting all branches."""

    def test_validate_view_name_valid(self):
        """Hit line 86: return name"""
        result = validate_view_name("vw_player_totals")
        assert result == "vw_player_totals"

    def test_validate_view_name_invalid_characters(self):
        """Hit line 85: raise AnalyticsError for invalid pattern"""
        with pytest.raises(AnalyticsError) as exc_info:
            validate_view_name("invalid-name")
        assert exc_info.value.view_name == "invalid-name"

    def test_validate_view_name_sql_injection(self):
        """Hit line 86: _validate_sql_identifier for SQLi patterns"""
        with pytest.raises(Exception):  # Could be AnalyticsError or ValueError from SQL validator
            validate_view_name("vw; DROP TABLE--")


class TestValidateLogLevelCoverage:
    """Coverage tests for _validate_log_level - hitting all branches."""

    def test_validate_log_level_valid_lowercase(self):
        """Hit line 116: return candidate"""
        assert _validate_log_level("debug") == "DEBUG"
        assert _validate_log_level("info") == "INFO"
        assert _validate_log_level("warning") == "WARNING"
        assert _validate_log_level("error") == "ERROR"
        assert _validate_log_level("critical") == "CRITICAL"

    def test_validate_log_level_valid_uppercase(self):
        assert _validate_log_level("DEBUG") == "DEBUG"
        assert _validate_log_level("INFO") == "INFO"
        assert _validate_log_level("WARNING") == "WARNING"
        assert _validate_log_level("ERROR") == "ERROR"
        assert _validate_log_level("CRITICAL") == "CRITICAL"

    def test_validate_log_level_invalid(self):
        """Hit line 110: raise ValidationError for invalid level"""
        with pytest.raises(ValidationError) as exc_info:
            _validate_log_level("INVALID")
        assert exc_info.value.argument == "--log-level"
        assert exc_info.value.value == "INVALID"

    def test_validate_log_level_empty_string(self):
        """Hit line 110: raise ValidationError for empty string"""
        with pytest.raises(ValidationError) as exc_info:
            _validate_log_level("")
        assert exc_info.value.argument == "--log-level"
        assert exc_info.value.value == ""

    def test_validate_log_level_partial_match(self):
        """Hit line 110: raise ValidationError for partial match"""
        with pytest.raises(ValidationError) as exc_info:
            _validate_log_level("INF")
        assert exc_info.value.argument == "--log-level"
        assert exc_info.value.value == "INF"


class TestValidateAnalyticsOutputPathCoverage:
    """Coverage tests for _validate_analytics_output_path - hitting all branches."""

    def test_validate_output_path_csv(self):
        """Hit no-raise path for valid extensions"""
        path = Path("output.csv")
        _validate_analytics_output_path(path)

    def test_validate_output_path_parquet(self):
        path = Path("output.parquet")
        _validate_analytics_output_path(path)

    def test_validate_output_path_json(self):
        path = Path("output.json")
        _validate_analytics_output_path(path)

    def test_validate_output_path_uppercase_extension(self):
        """Hit line 134: suffix = path.suffix.lower()"""
        path = Path("output.CSV")
        _validate_analytics_output_path(path)

    def test_validate_output_path_unsupported_extension(self):
        """Hit line 135: raise ValidationError for unsupported extension"""
        path = Path("output.xlsx")
        with pytest.raises(ValidationError) as exc_info:
            _validate_analytics_output_path(path)
        assert exc_info.value.argument == "--analytics-output"
        assert str(path) in exc_info.value.value

    def test_validate_output_path_no_extension(self):
        """Hit line 135: raise ValidationError for no extension"""
        path = Path("output")
        with pytest.raises(ValidationError) as exc_info:
            _validate_analytics_output_path(path)
        assert exc_info.value.argument == "--analytics-output"

    def test_validate_output_path_lists_supported_formats(self):
        """Verify error message includes all supported formats"""
        path = Path("output.invalid")
        with pytest.raises(ValidationError) as exc_info:
            _validate_analytics_output_path(path)
        error_msg = str(exc_info.value)
        for ext in SUPPORTED_ANALYTICS_EXTENSIONS:
            assert ext in error_msg


class TestValidateSeasonsCoverage:
    """Coverage tests for _validate_seasons - hitting all branches."""

    def test_validate_seasons_valid(self):
        """Hit line 177: return list(seasons)"""
        result = _validate_seasons(["2023-24", "2024-25"])
        assert result == ["2023-24", "2024-25"]

    def test_validate_seasons_empty_list(self):
        """Hit line 164: raise ValidationError for empty list"""
        with pytest.raises(ValidationError) as exc_info:
            _validate_seasons([])
        assert exc_info.value.argument == "--seasons"
        assert exc_info.value.value is None

    def test_validate_seasons_invalid_format(self):
        """Hit line 172: raise ValidationError for invalid format"""
        with pytest.raises(ValidationError) as exc_info:
            _validate_seasons(["2023"])
        assert exc_info.value.argument == "--seasons"
        assert exc_info.value.value == ["2023"]

    def test_validate_seasons_multiple_invalid(self):
        """Hit line 172: raise ValidationError with multiple invalid"""
        with pytest.raises(ValidationError) as exc_info:
            _validate_seasons(["2023", "2024", "2025"])
        assert exc_info.value.value == ["2023", "2024", "2025"]

    def test_validate_seasons_mixed_valid_invalid(self):
        """Hit line 170: invalid = [s for s in seasons if not pattern.match(s)]"""
        with pytest.raises(ValidationError) as exc_info:
            _validate_seasons(["2023-24", "bad", "2024-25", "also-bad"])
        assert exc_info.value.value == ["bad", "also-bad"]

    def test_validate_seasons_wrong_separator(self):
        with pytest.raises(ValidationError) as exc_info:
            _validate_seasons(["2023/24"])
        assert exc_info.value.value == ["2023/24"]

    def test_validate_seasons_missing_century(self):
        with pytest.raises(ValidationError) as exc_info:
            _validate_seasons(["23-24"])
        assert exc_info.value.value == ["23-24"]

    def test_validate_seasons_missing_dash(self):
        with pytest.raises(ValidationError) as exc_info:
            _validate_seasons(["202324"])
        assert exc_info.value.value == ["202324"]

    def test_validate_seasons_with_extra_text(self):
        with pytest.raises(ValidationError) as exc_info:
            _validate_seasons(["2023-24-season"])
        assert exc_info.value.value == ["2023-24-season"]

    def test_validate_seasons_single_digit_end_year(self):
        with pytest.raises(ValidationError) as exc_info:
            _validate_seasons(["2023-4"])
        assert exc_info.value.value == ["2023-4"]

    def test_validate_seasons_too_many_digits(self):
        with pytest.raises(ValidationError) as exc_info:
            _validate_seasons(["20234-567"])
        assert exc_info.value.value == ["20234-567"]

    def test_validate_seasons_special_chars(self):
        with pytest.raises(ValidationError) as exc_info:
            _validate_seasons(["2023-24!"])
        assert exc_info.value.value == ["2023-24!"]

    def test_validate_seasons_returns_copy(self):
        """Verify a copy is returned, not the original list"""
        original = ["2023-24", "2024-25"]
        result = _validate_seasons(original)
        assert result == original
        assert result is not original
