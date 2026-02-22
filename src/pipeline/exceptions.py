"""
Pipeline exception hierarchy.

All exceptions raised by the ingest pipeline are defined here so callers can
import from a single, stable location.
"""

from __future__ import annotations


class IngestError(RuntimeError):
    """Base exception for ingest pipeline errors."""


class ReconciliationError(IngestError):
    """Raised when reconciliation checks find discrepancies."""

    def __init__(self, warning_count: int) -> None:
        self.warning_count = warning_count
        super().__init__(
            f"Reconciliation checks found {warning_count} discrepancy warning(s). "
            "Re-run with --reconciliation-warn-only to continue despite mismatches."
        )


class AnalyticsError(IngestError):
    """Raised when analytics view operations fail."""


class ValidationError(IngestError):
    """Raised when ingest CLI arguments are invalid."""
