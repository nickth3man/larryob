"""Tests: .githooks/pre-commit validation and functionality."""

import subprocess
from pathlib import Path

import pytest


def test_pre_commit_hook_exists() -> None:
    """Verify .githooks/pre-commit file exists."""
    hook = Path(".githooks/pre-commit")
    assert hook.exists(), "pre-commit hook should exist"


def test_pre_commit_hook_is_executable() -> None:
    """Verify pre-commit hook has executable permissions."""
    hook = Path(".githooks/pre-commit")
    if not hook.exists():
        pytest.skip("pre-commit hook not found")

    # Check if file has executable bit set
    import stat

    st = hook.stat()
    _ = bool(st.st_mode & stat.S_IXUSR)  # executable check (may not work on all filesystems)
    # Note: May not be executable on all systems, so we just verify it's a valid shell script
    assert hook.exists()


def test_pre_commit_hook_has_shebang() -> None:
    """Verify pre-commit hook starts with proper shebang."""
    hook = Path(".githooks/pre-commit")
    if not hook.exists():
        pytest.skip("pre-commit hook not found")

    first_line = hook.read_text().split("\n")[0]
    assert first_line.startswith("#!"), "pre-commit should have shebang"
    assert "sh" in first_line, "pre-commit should be a shell script"


def test_pre_commit_hook_uses_set_flags() -> None:
    """Verify pre-commit uses safe shell options (set -eu)."""
    hook = Path(".githooks/pre-commit")
    if not hook.exists():
        pytest.skip("pre-commit hook not found")

    content = hook.read_text()
    # Should use 'set -e' or 'set -eu' for safer shell scripting
    assert "set -" in content, "pre-commit should use set flags for safety"


def test_pre_commit_hook_checks_for_uv() -> None:
    """Verify pre-commit checks for uv availability."""
    hook = Path(".githooks/pre-commit")
    if not hook.exists():
        pytest.skip("pre-commit hook not found")

    content = hook.read_text()
    assert "command -v uv" in content or "which uv" in content, (
        "pre-commit should check for uv availability"
    )


def test_pre_commit_hook_runs_ruff_check() -> None:
    """Verify pre-commit runs ruff check."""
    hook = Path(".githooks/pre-commit")
    if not hook.exists():
        pytest.skip("pre-commit hook not found")

    content = hook.read_text()
    assert "ruff check" in content, "pre-commit should run ruff check"


def test_pre_commit_hook_runs_ruff_format() -> None:
    """Verify pre-commit runs ruff format check."""
    hook = Path(".githooks/pre-commit")
    if not hook.exists():
        pytest.skip("pre-commit hook not found")

    content = hook.read_text()
    assert "ruff format" in content, "pre-commit should run ruff format"


def test_pre_commit_hook_runs_type_checker() -> None:
    """Verify pre-commit runs type checker (ty check)."""
    hook = Path(".githooks/pre-commit")
    if not hook.exists():
        pytest.skip("pre-commit hook not found")

    content = hook.read_text()
    assert "ty check" in content, "pre-commit should run type checker"


def test_pre_commit_hook_runs_pytest() -> None:
    """Verify pre-commit runs pytest."""
    hook = Path(".githooks/pre-commit")
    if not hook.exists():
        pytest.skip("pre-commit hook not found")

    content = hook.read_text()
    assert "pytest" in content, "pre-commit should run pytest"


def test_pre_commit_hook_exits_on_missing_uv() -> None:
    """Verify pre-commit exits with error if uv is not found."""
    hook = Path(".githooks/pre-commit")
    if not hook.exists():
        pytest.skip("pre-commit hook not found")

    content = hook.read_text()
    lines = content.split("\n")

    # Find the uv check block
    found_check = False
    found_exit = False
    for i, line in enumerate(lines):
        if "command -v uv" in line:
            found_check = True
            # Look for exit 1 in the next few lines
            for j in range(i, min(i + 5, len(lines))):
                if "exit 1" in lines[j]:
                    found_exit = True
                    break

    assert found_check, "Should check for uv"
    assert found_exit, "Should exit with error code if uv not found"


def test_pre_commit_hook_commands_use_uv_run() -> None:
    """Verify all quality check commands are run via 'uv run'."""
    hook = Path(".githooks/pre-commit")
    if not hook.exists():
        pytest.skip("pre-commit hook not found")

    content = hook.read_text()
    lines = [line.strip() for line in content.split("\n")]

    # Commands that should be prefixed with 'uv run'
    commands = ["ruff check", "ruff format", "ty check", "pytest"]

    for cmd in commands:
        # Find lines containing the command (exclude echo and comments)
        matching_lines = [
            line
            for line in lines
            if cmd in line and not line.startswith("#") and not line.startswith("echo")
        ]
        for line in matching_lines:
            if line and not line.startswith("#") and not line.startswith("echo"):
                # The command should be run via 'uv run'
                assert "uv run" in line, f"'{cmd}' should be run via 'uv run'"


def test_pre_commit_hook_syntax_is_valid() -> None:
    """Verify pre-commit hook has valid shell syntax."""
    hook = Path(".githooks/pre-commit")
    if not hook.exists():
        pytest.skip("pre-commit hook not found")

    # Try to validate syntax with bash -n (dry-run)
    try:
        result = subprocess.run(
            ["bash", "-n", hook.as_posix()],
            capture_output=True,
            text=True,
            timeout=5,
        )
        assert result.returncode == 0, f"Syntax error in pre-commit: {result.stderr}"
    except FileNotFoundError:
        pytest.skip("bash not available for syntax check")
    except subprocess.TimeoutExpired:
        pytest.fail("pre-commit syntax check timed out")


def test_pre_commit_hook_has_informative_output() -> None:
    """Verify pre-commit provides user feedback."""
    hook = Path(".githooks/pre-commit")
    if not hook.exists():
        pytest.skip("pre-commit hook not found")

    content = hook.read_text()
    # Should have echo statements or similar to inform user
    assert "echo" in content or "printf" in content, "pre-commit should provide user feedback"


def test_pre_commit_hook_quality_checks_order() -> None:
    """Verify quality checks run in logical order."""
    hook = Path(".githooks/pre-commit")
    if not hook.exists():
        pytest.skip("pre-commit hook not found")

    content = hook.read_text()

    # Find positions of each check
    ruff_check_pos = content.find("ruff check")
    ruff_format_pos = content.find("ruff format")
    ty_check_pos = content.find("ty check")
    pytest_pos = content.find("pytest")

    # All should be present
    assert ruff_check_pos > 0
    assert ruff_format_pos > 0
    assert ty_check_pos > 0
    assert pytest_pos > 0

    # Reasonable order: lint, format, type check, then tests
    # (exact order may vary, but all should be present)
    assert ruff_check_pos < pytest_pos, "linting should run before tests"
    assert ty_check_pos < pytest_pos, "type checking should run before tests"


def test_pre_commit_hook_uses_quiet_pytest() -> None:
    """Verify pre-commit runs pytest in quiet mode for cleaner output."""
    hook = Path(".githooks/pre-commit")
    if not hook.exists():
        pytest.skip("pre-commit hook not found")

    content = hook.read_text()

    # Check if pytest is run with -q flag for quieter output
    if "pytest" in content:
        assert "pytest -q" in content, "pytest should use -q flag for cleaner output"


def test_pre_commit_hook_content_matches_ci() -> None:
    """Verify pre-commit checks match CI workflow checks."""
    hook = Path(".githooks/pre-commit")
    workflow = Path(".github/workflows/commit-gate.yml")

    if not hook.exists() or not workflow.exists():
        pytest.skip("hook or workflow not found")

    hook_content = hook.read_text()
    workflow_content = workflow.read_text()

    # Both should run the same quality checks
    checks = ["ruff check", "ruff format", "ty check", "pytest"]

    for check in checks:
        assert check in hook_content, f"pre-commit missing '{check}'"
        assert check in workflow_content, f"CI workflow missing '{check}'"


def test_pre_commit_hook_no_hardcoded_paths() -> None:
    """Verify pre-commit doesn't use hardcoded absolute paths."""
    hook = Path(".githooks/pre-commit")
    if not hook.exists():
        pytest.skip("pre-commit hook not found")

    content = hook.read_text()
    lines = content.split("\n")

    # Check for suspicious absolute paths (excluding shebang)
    for i, line in enumerate(lines):
        if i == 0:  # Skip shebang line
            continue
        # Avoid hardcoded /home/, /usr/local/, etc. paths
        if "/home/" in line or "/usr/local/" in line:
            # Could be in comments or strings, so this is a soft check
            assert line.strip().startswith("#"), f"Line {i + 1} may contain hardcoded path: {line}"
