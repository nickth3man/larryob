"""Salary transformation helpers."""

from ..config import get_all_salary_caps
from ..helpers import _norm_name

# Use centralized config for salary cap data.
_SALARY_CAP_BY_SEASON = get_all_salary_caps()


def _normalize_name(name: str) -> str:
    """Normalize a person name for matching."""
    return _norm_name(name, strip_non_alpha=True)
