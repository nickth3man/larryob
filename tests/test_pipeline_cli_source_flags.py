"""Tests: CLI argument parser source-dispatch flags (PBP and salary)."""

import pytest

from src.pipeline.cli import create_argument_parser, validate_arguments
from src.pipeline.models import IngestConfig

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse(args: list[str]) -> tuple:
    """Return (parser, namespace) for the given CLI args."""
    parser = create_argument_parser()
    namespace = parser.parse_args(args)
    return parser, namespace


# ---------------------------------------------------------------------------
# --pbp-source bulk + --pbp-bulk-dir validation
# ---------------------------------------------------------------------------


def test_pbp_source_bulk_nonexistent_dir_raises(tmp_path) -> None:
    """--pbp-source bulk with a missing --pbp-bulk-dir → parser.error (SystemExit)."""
    missing = str(tmp_path / "no_such_dir")
    parser, args = _parse(
        [
            "--pbp-source",
            "bulk",
            "--pbp-bulk-dir",
            missing,
            "--seasons",
            "2023-24",
        ]
    )
    with pytest.raises(SystemExit):
        validate_arguments(parser, args)


def test_pbp_source_bulk_existing_dir_accepted(tmp_path) -> None:
    """--pbp-source bulk with an existing directory passes validation."""
    pbp_dir = tmp_path / "pbp"
    pbp_dir.mkdir()
    parser, args = _parse(
        [
            "--pbp-source",
            "bulk",
            "--pbp-bulk-dir",
            str(pbp_dir),
            "--seasons",
            "2023-24",
        ]
    )
    # Should not raise
    validate_arguments(parser, args)


def test_pbp_source_api_no_dir_required(tmp_path) -> None:
    """--pbp-source api never requires --pbp-bulk-dir."""
    parser, args = _parse(["--pbp-source", "api", "--seasons", "2023-24"])
    validate_arguments(parser, args)  # no SystemExit


# ---------------------------------------------------------------------------
# IngestConfig.pbp_source field
# ---------------------------------------------------------------------------


def test_ingest_config_pbp_source_api() -> None:
    """IngestConfig created with pbp_source='api' stores the value correctly."""
    parser, args = _parse(["--pbp-source", "api", "--seasons", "2023-24"])
    validate_arguments(parser, args)
    config = IngestConfig.from_args(args)
    assert config.pbp_source == "api"


def test_ingest_config_pbp_source_auto_default() -> None:
    """Default pbp_source is 'auto' when flag is omitted."""
    parser, args = _parse(["--seasons", "2023-24"])
    validate_arguments(parser, args)
    config = IngestConfig.from_args(args)
    assert config.pbp_source == "auto"


# ---------------------------------------------------------------------------
# --salary-source open + --salary-open-file validation
# ---------------------------------------------------------------------------


def test_salary_source_open_nonexistent_file_raises(tmp_path) -> None:
    """--salary-source open with a missing --salary-open-file → SystemExit."""
    missing = str(tmp_path / "no_such.csv")
    parser, args = _parse(
        [
            "--salary-source",
            "open",
            "--salary-open-file",
            missing,
            "--seasons",
            "2023-24",
        ]
    )
    with pytest.raises(SystemExit):
        validate_arguments(parser, args)


def test_salary_source_open_file_is_directory_raises(tmp_path) -> None:
    """--salary-open-file pointing to a directory → parser.error (SystemExit)."""
    a_dir = tmp_path / "actually_a_dir"
    a_dir.mkdir()
    parser, args = _parse(
        [
            "--salary-source",
            "open",
            "--salary-open-file",
            str(a_dir),
            "--seasons",
            "2023-24",
        ]
    )
    with pytest.raises(SystemExit):
        validate_arguments(parser, args)


def test_salary_source_open_valid_file_accepted(tmp_path) -> None:
    """--salary-source open with a real file passes validation."""
    csv_file = tmp_path / "salaries.csv"
    csv_file.write_text("player_name,salary\nLeBron James,47000000\n")
    parser, args = _parse(
        [
            "--salary-source",
            "open",
            "--salary-open-file",
            str(csv_file),
            "--seasons",
            "2023-24",
        ]
    )
    validate_arguments(parser, args)  # no SystemExit


# ---------------------------------------------------------------------------
# IngestConfig.salary_source field
# ---------------------------------------------------------------------------


def test_ingest_config_salary_source_bref() -> None:
    """IngestConfig stores salary_source='bref' correctly."""
    parser, args = _parse(["--salary-source", "bref", "--seasons", "2023-24"])
    validate_arguments(parser, args)
    config = IngestConfig.from_args(args)
    assert config.salary_source == "bref"


def test_ingest_config_salary_source_auto_default() -> None:
    """Default salary_source is 'auto' when flag is omitted."""
    parser, args = _parse(["--seasons", "2023-24"])
    validate_arguments(parser, args)
    config = IngestConfig.from_args(args)
    assert config.salary_source == "auto"


def test_ingest_config_salary_open_file_stored_as_path(tmp_path) -> None:
    """--salary-open-file value is stored as Path on IngestConfig."""
    csv_file = tmp_path / "salaries.csv"
    csv_file.write_text("x\n")
    parser, args = _parse(
        [
            "--salary-source",
            "open",
            "--salary-open-file",
            str(csv_file),
            "--seasons",
            "2023-24",
        ]
    )
    validate_arguments(parser, args)
    config = IngestConfig.from_args(args)
    assert config.salary_open_file == csv_file
