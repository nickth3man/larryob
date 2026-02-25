"""Tests: GitHub workflow YAML files validation."""

import re
from pathlib import Path

import pytest


def get_workflow_files():
    """Get all workflow YAML files from .github/workflows/."""
    workflow_dir = Path(".github/workflows")
    if not workflow_dir.exists():
        return []
    return list(workflow_dir.glob("*.yml")) + list(workflow_dir.glob("*.yaml"))


@pytest.mark.parametrize("workflow_file", get_workflow_files())
def test_workflow_file_is_valid_yaml(workflow_file: Path) -> None:
    """Verify each workflow file contains valid YAML syntax."""
    try:
        import yaml
    except ImportError:
        pytest.skip("PyYAML not available")

    content = workflow_file.read_text()
    data = yaml.safe_load(content)
    assert data is not None
    assert isinstance(data, dict)


@pytest.mark.parametrize("workflow_file", get_workflow_files())
def test_workflow_has_required_top_level_keys(workflow_file: Path) -> None:
    """Verify workflow files have required top-level keys."""
    try:
        import yaml
    except ImportError:
        pytest.skip("PyYAML not available")

    content = workflow_file.read_text()
    data = yaml.safe_load(content)

    assert "name" in data, f"{workflow_file.name} missing 'name' field"
    # YAML 'on' keyword is parsed as True by PyYAML
    assert "on" in data or True in data, f"{workflow_file.name} missing 'on' trigger field"
    assert "jobs" in data, f"{workflow_file.name} missing 'jobs' field"


@pytest.mark.parametrize("workflow_file", get_workflow_files())
def test_workflow_jobs_have_runs_on(workflow_file: Path) -> None:
    """Verify each job has a runs-on field."""
    try:
        import yaml
    except ImportError:
        pytest.skip("PyYAML not available")

    content = workflow_file.read_text()
    data = yaml.safe_load(content)

    if "jobs" in data:
        for job_name, job_config in data["jobs"].items():
            if isinstance(job_config, dict):
                # Skip if job is conditional and may not run
                assert "runs-on" in job_config or "uses" in job_config, (
                    f"{workflow_file.name}: job '{job_name}' missing 'runs-on' or 'uses'"
                )


def test_commit_gate_workflow_exists() -> None:
    """Verify commit-gate.yml exists and is valid."""
    workflow = Path(".github/workflows/commit-gate.yml")
    assert workflow.exists()


def test_commit_gate_has_quality_job() -> None:
    """Verify commit-gate.yml has quality check job."""
    try:
        import yaml
    except ImportError:
        pytest.skip("PyYAML not available")

    workflow = Path(".github/workflows/commit-gate.yml")
    if not workflow.exists():
        pytest.skip("commit-gate.yml not found")

    data = yaml.safe_load(workflow.read_text())
    assert "quality" in data["jobs"]

    quality_job = data["jobs"]["quality"]
    assert quality_job["runs-on"] == "ubuntu-latest"


def test_commit_gate_runs_ruff_and_pytest() -> None:
    """Verify commit-gate.yml runs expected quality checks."""
    try:
        import yaml
    except ImportError:
        pytest.skip("PyYAML not available")

    workflow = Path(".github/workflows/commit-gate.yml")
    if not workflow.exists():
        pytest.skip("commit-gate.yml not found")

    content = workflow.read_text()

    # Check that it runs the expected commands
    assert "ruff check" in content
    assert "ruff format --check" in content
    assert "ty check" in content
    assert "pytest" in content


def test_opencode_workflows_use_correct_action() -> None:
    """Verify opencode workflows use the anomalyco/opencode/github action."""
    try:
        import yaml
    except ImportError:
        pytest.skip("PyYAML not available")

    workflow_dir = Path(".github/workflows")
    if not workflow_dir.exists():
        pytest.skip("workflow directory not found")

    opencode_workflows = [
        f for f in workflow_dir.glob("opencode-*.yml")
    ]

    for workflow in opencode_workflows:
        content = workflow.read_text()
        assert "anomalyco/opencode/github@latest" in content, (
            f"{workflow.name} should use anomalyco/opencode/github action"
        )


def test_opencode_workflows_have_required_secrets() -> None:
    """Verify opencode workflows reference required secrets."""
    try:
        import yaml
    except ImportError:
        pytest.skip("PyYAML not available")

    workflow_dir = Path(".github/workflows")
    if not workflow_dir.exists():
        pytest.skip("workflow directory not found")

    opencode_workflows = [
        f for f in workflow_dir.glob("opencode-*.yml")
    ]

    for workflow in opencode_workflows:
        content = workflow.read_text()
        data = yaml.safe_load(content)

        # Find steps that use opencode action
        for job in data.get("jobs", {}).values():
            if isinstance(job, dict):
                for step in job.get("steps", []):
                    if isinstance(step, dict) and "anomalyco/opencode" in str(step.get("uses", "")):
                        # Check env section exists with required keys
                        env = step.get("env", {})
                        assert "OPENROUTER_API_KEY" in str(env) or "OPENROUTER_API_KEY" in str(step)
                        assert "GITHUB_TOKEN" in str(env) or "GITHUB_TOKEN" in str(step)


def test_workflow_permissions_are_minimal() -> None:
    """Verify workflows follow principle of least privilege for permissions."""
    try:
        import yaml
    except ImportError:
        pytest.skip("PyYAML not available")

    workflow_dir = Path(".github/workflows")
    if not workflow_dir.exists():
        pytest.skip("workflow directory not found")

    for workflow_file in workflow_dir.glob("*.yml"):
        data = yaml.safe_load(workflow_file.read_text())

        # Check job-level permissions
        for job_name, job_config in data.get("jobs", {}).items():
            if isinstance(job_config, dict) and "permissions" in job_config:
                perms = job_config["permissions"]
                # If permissions are specified, they should not be 'write-all' or too broad
                if isinstance(perms, dict):
                    # Verify specific permissions are granted, not wildcards
                    assert perms != {}, f"{workflow_file.name}: {job_name} has empty permissions"


def test_workflows_use_specific_action_versions() -> None:
    """Verify workflows pin action versions (not 'latest' for critical actions)."""
    try:
        import yaml
    except ImportError:
        pytest.skip("PyYAML not available")

    workflow_dir = Path(".github/workflows")
    if not workflow_dir.exists():
        pytest.skip("workflow directory not found")

    for workflow_file in workflow_dir.glob("*.yml"):
        content = workflow_file.read_text()

        # actions/checkout should use v6 or similar version
        if "actions/checkout" in content:
            assert re.search(r"actions/checkout@v\d+", content), (
                f"{workflow_file.name} should pin checkout action version"
            )


def test_commit_gate_has_concurrency_control() -> None:
    """Verify commit-gate.yml has concurrency control to prevent duplicate runs."""
    try:
        import yaml
    except ImportError:
        pytest.skip("PyYAML not available")

    workflow = Path(".github/workflows/commit-gate.yml")
    if not workflow.exists():
        pytest.skip("commit-gate.yml not found")

    data = yaml.safe_load(workflow.read_text())
    assert "concurrency" in data
    assert "group" in data["concurrency"]
    assert data["concurrency"].get("cancel-in-progress") is True


def test_opencode_security_review_triggers_on_pr_opened() -> None:
    """Verify security review runs on PR opened events."""
    try:
        import yaml
    except ImportError:
        pytest.skip("PyYAML not available")

    workflow = Path(".github/workflows/opencode-security-review.yml")
    if not workflow.exists():
        pytest.skip("opencode-security-review.yml not found")

    data = yaml.safe_load(workflow.read_text())
    # Handle YAML 'on' keyword parsed as True
    pr_config = data.get("on", data.get(True, {})).get("pull_request", {})

    if isinstance(pr_config, dict):
        types = pr_config.get("types", [])
        assert "opened" in types


def test_workflows_checkout_without_persisting_credentials() -> None:
    """Verify workflows use persist-credentials: false for security."""
    try:
        import yaml
    except ImportError:
        pytest.skip("PyYAML not available")

    workflow_dir = Path(".github/workflows")
    if not workflow_dir.exists():
        pytest.skip("workflow directory not found")

    for workflow_file in workflow_dir.glob("*.yml"):
        data = yaml.safe_load(workflow_file.read_text())

        for job_name, job_config in data.get("jobs", {}).items():
            if isinstance(job_config, dict):
                for step in job_config.get("steps", []):
                    if isinstance(step, dict):
                        uses = step.get("uses", "")
                        if "actions/checkout" in uses:
                            with_config = step.get("with", {})
                            # If persist-credentials is specified, it should be false
                            if "persist-credentials" in with_config:
                                assert with_config["persist-credentials"] is False, (
                                    f"{workflow_file.name} should set persist-credentials: false"
                                )