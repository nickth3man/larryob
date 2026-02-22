"""
Pipeline exception hierarchy.

All exceptions raised by the ingest pipeline are defined here so callers can
import from a single, stable location. This enables:

1. Catching all pipeline errors with a single `except IngestError` clause
2. Granular error handling by catching specific subclasses
3. Stable import paths for external callers (tests, notebooks)

Design Decisions
----------------
- All exceptions inherit from IngestError (which inherits from RuntimeError)
- __slots__ used for memory efficiency and to prevent accidental attribute addition
- Each exception includes relevant context attributes for programmatic access

Usage
-----
    try:
        run_ingest_pipeline(con, config)
    except ReconciliationError as e:
        logger.warning("Reconciliation failed with %d warnings", e.warning_count)
    except IngestError as e:
        logger.error("Pipeline failed: %s", e)
"""

from __future__ import annotations

from typing import Any


class IngestError(RuntimeError):
    """Base exception for ingest pipeline errors.

    All pipeline-specific exceptions inherit from this class, allowing
    callers to catch all ingest-related errors with a single except clause.

    Attributes:
        message: Human-readable error description.
        context: Optional dictionary of additional context for debugging.
    """

    __slots__ = ("context", "message")

    def __init__(
        self,
        message: str,
        context: dict[str, Any] | None = None,
    ) -> None:
        self.message = message
        self.context = context
        super().__init__(message)

    def __repr__(self) -> str:
        if self.context:
            return f"{self.__class__.__name__}({self.message!r}, context={self.context!r})"
        return f"{self.__class__.__name__}({self.message!r})"


class ReconciliationError(IngestError):
    """Raised when reconciliation checks find discrepancies.

    Player-sum vs team-total reconciliation checks may find mismatches
    in PTS/REB/AST columns. This error is raised unless --reconciliation-warn-only
    is set.

    Attributes:
        warning_count: Number of discrepancy warnings found.
        seasons: Optional list of seasons that had discrepancies.
    """

    __slots__ = ("seasons", "warning_count")

    def __init__(
        self,
        warning_count: int,
        seasons: list[str] | None = None,
    ) -> None:
        self.warning_count = warning_count
        self.seasons = seasons
        message = (
            f"Reconciliation checks found {warning_count} discrepancy warning(s). "
            "Re-run with --reconciliation-warn-only to continue despite mismatches."
        )
        super().__init__(message, context={"warning_count": warning_count, "seasons": seasons})


class AnalyticsError(IngestError):
    """Raised when analytics view operations fail.

    This includes invalid view names, unsupported export formats,
    and query execution failures.

    Attributes:
        view_name: The view name that caused the error (if applicable).
        output_path: The output path that caused the error (if applicable).
    """

    __slots__ = ("output_path", "view_name")

    def __init__(
        self,
        message: str,
        view_name: str | None = None,
        output_path: str | None = None,
    ) -> None:
        self.view_name = view_name
        self.output_path = output_path
        context = {}
        if view_name:
            context["view_name"] = view_name
        if output_path:
            context["output_path"] = output_path
        super().__init__(message, context=context if context else None)


class ValidationError(IngestError):
    """Raised when ingest CLI arguments are invalid.

    This includes invalid season formats, unsupported file extensions,
    and invalid log levels.

    Attributes:
        argument: The argument name that failed validation.
        value: The invalid value that was provided.
    """

    __slots__ = ("argument", "value")

    def __init__(
        self,
        message: str,
        argument: str | None = None,
        value: Any = None,
    ) -> None:
        self.argument = argument
        self.value = value
        context = {}
        if argument:
            context["argument"] = argument
        if value is not None:
            context["value"] = value
        super().__init__(message, context=context if context else None)
