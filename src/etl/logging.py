"""Logging configuration for ETL processes."""

import logging
import sys
from pathlib import Path

LOG_FORMAT = "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s"
LOG_DATE_FORMAT = "%H:%M:%S"


def setup_logging(
    level: str = "INFO",
    log_file: Path | None = None,
) -> None:
    """
    Configure the process-wide root logger.
    
    Call early (for example in main()) before any other logging. Sets the root logger level, replaces existing handlers, attaches a stream handler that writes to stdout using the module's log format, and—if provided—adds a UTF-8 file handler that also receives log records.
    
    Parameters:
    	level (str): Logging level name (e.g., "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"); case-insensitive.
    	log_file (Path | None): Optional path to a log file. When provided, logs are written to the file in addition to stdout using UTF-8 encoding.
    """
    root = logging.getLogger()
    root.setLevel(level.upper())
    for h in root.handlers[:]:
        root.removeHandler(h)
    fmt = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    root.addHandler(sh)
    if log_file:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        root.addHandler(fh)
