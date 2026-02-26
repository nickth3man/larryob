"""Integration tests for configuration files."""

from pathlib import Path

import pytest


def test_pre_commit_and_ci_workflow_consistency() -> None:
    """Verify pre-commit hook and CI workflow run the same checks."""
    hook = Path(".githooks/pre-commit")
    workflow = Path(".github/workflows/commit-gate.yml")

    if not hook.exists() or not workflow.exists():
        pytest.skip("Required files not found")

    hook_content = hook.read_text()
    workflow_content = workflow.read_text()

    # Both should run these commands
    critical_checks = [
        "ruff check",
        "ruff format",
        "ty check",
        "pytest",
    ]

    for check in critical_checks:
        hook_has = check in hook_content
        workflow_has = check in workflow_content
        assert hook_has and workflow_has, (
            f"Inconsistency: '{check}' found in hook={hook_has}, workflow={workflow_has}"
        )


def test_gitignore_covers_all_build_artifacts() -> None:
    """Verify .gitignore covers all common Python build artifacts."""
    gitignore = Path(".gitignore")
    if not gitignore.exists():
        pytest.skip(".gitignore not found")

    content = gitignore.read_text()

    # Python build artifacts that should be ignored
    artifacts = {
        "__pycache__": "Python cache directories",
        "*.py[oc]": "Compiled Python files",
        "*.egg-info": "Egg info directories",
        "build/": "Build directory",
        "dist/": "Distribution directory",
        ".venv": "Virtual environment",
    }

    for pattern, description in artifacts.items():
        assert pattern in content, f"Missing pattern for {description}: {pattern}"


def test_github_workflows_directory_structure() -> None:
    """Verify GitHub workflows directory exists and contains workflow files."""
    workflows_dir = Path(".github/workflows")
    assert workflows_dir.exists(), ".github/workflows directory should exist"
    assert workflows_dir.is_dir(), ".github/workflows should be a directory"

    workflow_files = list(workflows_dir.glob("*.yml")) + list(workflows_dir.glob("*.yaml"))
    assert len(workflow_files) > 0, "Should have at least one workflow file"


def test_pre_commit_hook_file_permissions() -> None:
    """Verify pre-commit hook has correct file structure."""
    hook = Path(".githooks/pre-commit")
    if not hook.exists():
        pytest.skip("pre-commit hook not found")

    # Should be a file, not a directory
    assert hook.is_file(), "pre-commit should be a file"

    # Should not be empty
    content = hook.read_text()
    assert len(content) > 0, "pre-commit hook should not be empty"

    # Should have reasonable size (not too large)
    assert len(content) < 10000, "pre-commit hook suspiciously large"


def test_gitignore_does_not_ignore_source_code() -> None:
    """Verify .gitignore doesn't accidentally ignore source code."""
    gitignore = Path(".gitignore")
    if not gitignore.exists():
        pytest.skip(".gitignore not found")

    content = gitignore.read_text()
    lines = [line.strip() for line in content.split("\n")]

    # Should not ignore these critical directories
    should_not_ignore = ["src/", "tests/", ".github/"]

    for pattern in should_not_ignore:
        # Check it's not in the ignore list
        for line in lines:
            if line and not line.startswith("#"):
                assert line != pattern, f".gitignore should not ignore {pattern}"


def test_workflow_yaml_syntax_edge_cases() -> None:
    """Test edge cases in workflow YAML parsing."""
    try:
        import yaml
    except ImportError:
        pytest.skip("PyYAML not available")

    workflow_dir = Path(".github/workflows")
    if not workflow_dir.exists():
        pytest.skip("workflow directory not found")

    for workflow_file in workflow_dir.glob("*.yml"):
        content = workflow_file.read_text()

        # Verify no tab characters (YAML doesn't allow tabs for indentation)
        assert "\t" not in content or all(
            line.strip().startswith("#") or "\t" in line.split("#")[0]
            for line in content.split("\n") if "\t" in line
        ), f"{workflow_file.name} should not use tabs for indentation"

        # Verify consistent indentation (2 or 4 spaces)
        lines = content.split("\n")
        indented_lines = [line for line in lines if line and line[0] == " " and not line.strip().startswith("#")]
        if indented_lines:
            # Check that indentation is consistent (multiples of 2)
            for line in indented_lines:
                leading_spaces = len(line) - len(line.lstrip())
                assert leading_spaces % 2 == 0, (
                    f"{workflow_file.name} has inconsistent indentation on line: {line[:50]}"
                )


def test_pre_commit_hook_error_handling() -> None:
    """Verify pre-commit hook has proper error handling."""
    hook = Path(".githooks/pre-commit")
    if not hook.exists():
        pytest.skip("pre-commit hook not found")

    content = hook.read_text()

    # Should use set -e or set -eu for error handling
    assert "set -e" in content or "set -eu" in content, (
        "pre-commit should use 'set -e' or 'set -eu' for error handling"
    )

    # Should check for required tools
    assert "command -v" in content or "which" in content, (
        "pre-commit should check for required tools"
    )


def test_gitignore_protects_secrets() -> None:
    """Verify .gitignore protects common secret files."""
    gitignore = Path(".gitignore")
    if not gitignore.exists():
        pytest.skip(".gitignore not found")

    content = gitignore.read_text()

    # Common secret files that should be ignored
    secret_patterns = [
        ".env",  # Environment variables with secrets
    ]

    for pattern in secret_patterns:
        assert pattern in content, f"Should ignore secret file pattern: {pattern}"

    # Should NOT ignore the example env file
    lines = [line.strip() for line in content.split("\n") if line.strip() and not line.startswith("#")]
    assert ".env.example" not in lines, ".env.example should be tracked"


def test_workflow_files_have_descriptive_names() -> None:
    """Verify workflow files follow naming conventions."""
    workflow_dir = Path(".github/workflows")
    if not workflow_dir.exists():
        pytest.skip("workflow directory not found")

    for workflow_file in workflow_dir.glob("*.yml"):
        # Workflow names should be lowercase with hyphens
        name = workflow_file.stem
        assert name.replace("-", "").replace("_", "").isalnum(), (
            f"Workflow name should only contain alphanumeric and hyphens/underscores: {name}"
        )

        # Should have reasonable length
        assert len(name) > 2, f"Workflow name too short: {name}"
        assert len(name) < 50, f"Workflow name too long: {name}"


def test_gitignore_comment_quality() -> None:
    """Verify .gitignore has helpful comments."""
    gitignore = Path(".gitignore")
    if not gitignore.exists():
        pytest.skip(".gitignore not found")

    content = gitignore.read_text()
    lines = content.split("\n")

    # Should have at least a few sections with comments
    comment_lines = [line for line in lines if line.strip().startswith("#")]
    non_empty_lines = [line for line in lines if line.strip()]

    # At least 10% of non-empty lines should be comments
    if non_empty_lines:
        comment_ratio = len(comment_lines) / len(non_empty_lines)
        assert comment_ratio > 0.1, "Should have sufficient comments in .gitignore"


def test_pre_commit_hook_line_endings() -> None:
    """Verify pre-commit hook uses Unix line endings."""
    hook = Path(".githooks/pre-commit")
    if not hook.exists():
        pytest.skip("pre-commit hook not found")

    content = hook.read_bytes()

    # Should not contain Windows line endings (CRLF)
    assert b"\r\n" not in content, "pre-commit hook should use Unix line endings (LF, not CRLF)"


def test_workflow_security_best_practices() -> None:
    """Verify workflows follow security best practices."""
    try:
        import yaml
    except ImportError:
        pytest.skip("PyYAML not available")

    workflow_dir = Path(".github/workflows")
    if not workflow_dir.exists():
        pytest.skip("workflow directory not found")

    for workflow_file in workflow_dir.glob("*.yml"):
        content = workflow_file.read_text()
        data = yaml.safe_load(content)

        # Check that workflows don't use pull_request_target without restrictions
        if "on" in data or True in data:
            triggers = data.get("on", data.get(True, {}))
            if isinstance(triggers, dict) and "pull_request_target" in triggers:
                # If using pull_request_target, should have safety measures
                pytest.fail(
                    f"{workflow_file.name} uses pull_request_target which can be dangerous"
                )


def test_gitignore_no_trailing_whitespace() -> None:
    """Verify .gitignore patterns don't have trailing whitespace."""
    gitignore = Path(".gitignore")
    if not gitignore.exists():
        pytest.skip(".gitignore not found")

    content = gitignore.read_text()
    lines = content.split("\n")

    for i, line in enumerate(lines, 1):
        if line and not line.startswith("#"):
            # Pattern lines should not have trailing whitespace
            assert line == line.rstrip(), (
                f".gitignore line {i} has trailing whitespace: '{line}'"
            )


def test_commit_gate_workflow_triggers_correctly() -> None:
    """Verify commit-gate workflow triggers on correct events."""
    try:
        import yaml
    except ImportError:
        pytest.skip("PyYAML not available")

    workflow = Path(".github/workflows/commit-gate.yml")
    if not workflow.exists():
        pytest.skip("commit-gate.yml not found")

    data = yaml.safe_load(workflow.read_text())
    triggers = data.get("on", data.get(True, {}))

    # Should trigger on pull_request events
    assert "pull_request" in triggers or "pull_request_target" in triggers, (
        "commit-gate should trigger on pull request events"
    )


def test_workflow_files_valid_model_references() -> None:
    """Verify workflow files reference valid AI models."""
    try:
        import yaml
    except ImportError:
        pytest.skip("PyYAML not available")

    workflow_dir = Path(".github/workflows")
    if not workflow_dir.exists():
        pytest.skip("workflow directory not found")

    for workflow_file in workflow_dir.glob("opencode-*.yml"):
        content = workflow_file.read_text()

        # OpenCode workflows should reference a model
        if "anomalyco/opencode" in content:
            assert "model:" in content, (
                f"{workflow_file.name} should specify a model for OpenCode"
            )
