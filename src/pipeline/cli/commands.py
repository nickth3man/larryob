"""CLI exit-code constants."""

EXIT_SUCCESS = 0
EXIT_VALIDATION_ERROR = 1
EXIT_INGEST_ERROR = 2
EXIT_UNEXPECTED_ERROR = 3

COMMANDS = {
    "ingest": "run_ingest_pipeline",
    "analytics": "run_analytics_view",
}
