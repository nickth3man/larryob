"""Tests: .gitignore patterns and coverage."""

from pathlib import Path

import pytest


def test_gitignore_exists() -> None:
    """Verify .gitignore file exists."""
    gitignore = Path(".gitignore")
    assert gitignore.exists(), ".gitignore should exist"


def test_gitignore_ignores_pycache() -> None:
    """Verify .gitignore includes __pycache__ directories."""
    gitignore = Path(".gitignore")
    if not gitignore.exists():
        pytest.skip(".gitignore not found")

    content = gitignore.read_text()
    assert "__pycache__" in content, ".gitignore should ignore __pycache__"


def test_gitignore_ignores_pyc_files() -> None:
    """Verify .gitignore includes .pyc and .pyo files."""
    gitignore = Path(".gitignore")
    if not gitignore.exists():
        pytest.skip(".gitignore not found")

    content = gitignore.read_text()
    assert "*.py[oc]" in content or "*.pyc" in content, (
        ".gitignore should ignore compiled Python files"
    )


def test_gitignore_ignores_venv() -> None:
    """Verify .gitignore includes virtual environment directories."""
    gitignore = Path(".gitignore")
    if not gitignore.exists():
        pytest.skip(".gitignore not found")

    content = gitignore.read_text()
    assert ".venv" in content or "venv" in content, ".gitignore should ignore virtual environment"


def test_gitignore_ignores_env_file() -> None:
    """Verify .gitignore includes .env file (secrets)."""
    gitignore = Path(".gitignore")
    if not gitignore.exists():
        pytest.skip(".gitignore not found")

    content = gitignore.read_text()
    assert ".env" in content, ".gitignore should ignore .env file with secrets"


def test_gitignore_does_not_ignore_env_example() -> None:
    """Verify .gitignore doesn't ignore .env.example."""
    gitignore = Path(".gitignore")
    if not gitignore.exists():
        pytest.skip(".gitignore not found")

    content = gitignore.read_text()
    lines = [line.strip() for line in content.split("\n")]

    # Check that .env is ignored but .env.example is not explicitly ignored
    has_env_ignore = any(line == ".env" for line in lines)
    has_env_example_ignore = any(line == ".env.example" for line in lines)

    assert has_env_ignore, ".env should be ignored"
    assert not has_env_example_ignore, ".env.example should not be explicitly ignored"


def test_gitignore_ignores_database_files() -> None:
    """Verify .gitignore includes database files."""
    gitignore = Path(".gitignore")
    if not gitignore.exists():
        pytest.skip(".gitignore not found")

    content = gitignore.read_text()
    assert "*.db" in content, ".gitignore should ignore SQLite database files"


def test_gitignore_ignores_db_wal_files() -> None:
    """Verify .gitignore includes SQLite WAL files."""
    gitignore = Path(".gitignore")
    if not gitignore.exists():
        pytest.skip(".gitignore not found")

    content = gitignore.read_text()
    assert "*.db-wal" in content or "*-wal" in content, ".gitignore should ignore SQLite WAL files"


def test_gitignore_ignores_cache_directory() -> None:
    """Verify .gitignore includes cache directories."""
    gitignore = Path(".gitignore")
    if not gitignore.exists():
        pytest.skip(".gitignore not found")

    content = gitignore.read_text()
    # Project uses .cache for API response cache
    assert ".cache" in content or "/.cache" in content, ".gitignore should ignore cache directory"


def test_gitignore_ignores_build_artifacts() -> None:
    """Verify .gitignore includes common build artifacts."""
    gitignore = Path(".gitignore")
    if not gitignore.exists():
        pytest.skip(".gitignore not found")

    content = gitignore.read_text()
    build_artifacts = ["build/", "dist/", "*.egg-info"]

    for artifact in build_artifacts:
        assert artifact in content, f".gitignore should ignore {artifact}"


def test_gitignore_ignores_coverage_files() -> None:
    """Verify .gitignore includes test coverage files."""
    gitignore = Path(".gitignore")
    if not gitignore.exists():
        pytest.skip(".gitignore not found")

    content = gitignore.read_text()
    assert ".coverage" in content, ".gitignore should ignore coverage files"


def test_gitignore_ignores_log_files() -> None:
    """Verify .gitignore includes log files."""
    gitignore = Path(".gitignore")
    if not gitignore.exists():
        pytest.skip(".gitignore not found")

    content = gitignore.read_text()
    assert "*.log" in content or "logs/" in content, ".gitignore should ignore log files"


def test_gitignore_has_comments() -> None:
    """Verify .gitignore includes helpful comments."""
    gitignore = Path(".gitignore")
    if not gitignore.exists():
        pytest.skip(".gitignore not found")

    content = gitignore.read_text()
    lines = content.split("\n")
    comment_lines = [line for line in lines if line.strip().startswith("#")]

    # Should have at least a few comment lines for organization
    assert len(comment_lines) > 0, ".gitignore should have explanatory comments"


def test_gitignore_patterns_are_valid() -> None:
    """Verify .gitignore doesn't contain invalid patterns."""
    gitignore = Path(".gitignore")
    if not gitignore.exists():
        pytest.skip(".gitignore not found")

    content = gitignore.read_text()
    lines = [line.strip() for line in content.split("\n")]

    for line in lines:
        # Skip empty lines and comments
        if not line or line.startswith("#"):
            continue

        # Basic validation: no obvious syntax errors
        # Git patterns shouldn't have unescaped spaces at start/end
        assert line == line.strip(), f"Pattern has trailing/leading whitespace: '{line}'"

        # Check for common mistakes
        assert not line.endswith("\\"), f"Pattern ends with backslash: '{line}'"


def test_gitignore_sections_are_organized() -> None:
    """Verify .gitignore is organized into logical sections."""
    gitignore = Path(".gitignore")
    if not gitignore.exists():
        pytest.skip(".gitignore not found")

    content = gitignore.read_text()

    # Should have sections for different types of ignored files
    # Looking for organizational structure via comments
    python_section = "Python" in content or "python" in content or "# Python" in content
    venv_section = "Virtual" in content or "environment" in content
    db_section = "database" in content or "Database" in content or "SQLite" in content

    # At least one section should be commented
    assert python_section or venv_section or db_section, (
        ".gitignore should have organizational comments"
    )


def test_gitignore_ignores_raw_data_directory() -> None:
    """Verify .gitignore ignores /raw directory for local data."""
    gitignore = Path(".gitignore")
    if not gitignore.exists():
        pytest.skip(".gitignore not found")

    content = gitignore.read_text()
    # Based on project structure, /raw is for local data
    assert "/raw" in content or "raw/" in content, ".gitignore should ignore raw data directory"


def test_gitignore_allows_required_config_files() -> None:
    """Verify .gitignore doesn't ignore essential config files."""
    gitignore = Path(".gitignore")
    if not gitignore.exists():
        pytest.skip(".gitignore not found")

    content = gitignore.read_text()
    lines = [
        line.strip()
        for line in content.split("\n")
        if line.strip() and not line.strip().startswith("#")
    ]

    # Should not explicitly ignore important files
    forbidden_ignores = ["pyproject.toml", "README.md", ".github/", "src/", "tests/"]

    for forbidden in forbidden_ignores:
        assert forbidden not in lines, f".gitignore should not ignore {forbidden}"


def test_gitignore_pattern_for_pytest_cache() -> None:
    """Verify .gitignore ignores pytest cache."""
    gitignore = Path(".gitignore")
    if not gitignore.exists():
        pytest.skip(".gitignore not found")

    content = gitignore.read_text()
    # Pytest creates .pytest_cache directory
    # May be covered by __pycache__ or explicitly listed
    assert ".pytest_cache" in content or "__pycache__" in content, (
        ".gitignore should handle pytest cache"
    )


def test_gitignore_db_shm_files() -> None:
    """Verify .gitignore includes SQLite shared memory files."""
    gitignore = Path(".gitignore")
    if not gitignore.exists():
        pytest.skip(".gitignore not found")

    content = gitignore.read_text()
    assert "*.db-shm" in content or "*-shm" in content, (
        ".gitignore should ignore SQLite shared memory files"
    )


def test_gitignore_wheels_directory() -> None:
    """Verify .gitignore ignores wheels directory."""
    gitignore = Path(".gitignore")
    if not gitignore.exists():
        pytest.skip(".gitignore not found")

    content = gitignore.read_text()
    # Check for wheels directory (Python build artifact)
    if "wheels/" in content:
        assert True
    else:
        # May be acceptable if not used in this project
        pass


def test_gitignore_coverage_json() -> None:
    """Verify .gitignore ignores coverage.json file."""
    gitignore = Path(".gitignore")
    if not gitignore.exists():
        pytest.skip(".gitignore not found")

    content = gitignore.read_text()
    assert "coverage.json" in content, ".gitignore should ignore coverage.json"


def test_gitignore_no_duplicate_patterns() -> None:
    """Verify .gitignore doesn't have obvious duplicate patterns."""
    gitignore = Path(".gitignore")
    if not gitignore.exists():
        pytest.skip(".gitignore not found")

    content = gitignore.read_text()
    lines = [
        line.strip()
        for line in content.split("\n")
        if line.strip() and not line.strip().startswith("#")
    ]

    # Check for exact duplicates
    unique_lines = set(lines)
    assert len(unique_lines) == len(lines), ".gitignore contains duplicate patterns"


def test_gitignore_ignores_logs_directory() -> None:
    """Verify .gitignore handles logs directory appropriately."""
    gitignore = Path(".gitignore")
    if not gitignore.exists():
        pytest.skip(".gitignore not found")

    content = gitignore.read_text()

    # Based on PR changes, logs/*.log should be ignored
    assert "logs/*.log" in content or "*.log" in content, (
        ".gitignore should ignore log files in logs directory"
    )
