"""
Logging utilities for the NBA stats data pipeline.
"""
import logging
import os
import pipeline.constants as _constants


def create_logger(run_id: str) -> logging.Logger:
    """
    Create a logger with dual sinks: StreamHandler (terminal) and FileHandler.

    Args:
        run_id: Timestamp-based run identifier (YYYYMMDD_HHMMSS)

    Returns:
        Configured logging.Logger instance
    """
    logger = logging.getLogger(f"pipeline.run.{run_id}")
    logger.setLevel(logging.DEBUG)

    # Avoid duplicate handlers if called multiple times with same run_id
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Stream handler (terminal)
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    # File handler (log file)
    os.makedirs(_constants.LOGS_DIR, exist_ok=True)
    log_path = os.path.join(_constants.LOGS_DIR, f"run_{run_id}.log")
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def log_validation_block(logger: logging.Logger, failures: list[str]) -> None:
    """
    Emit all validation failures in a single WARNING log call.

    Args:
        logger: Logger instance
        failures: List of failure message strings
    """
    combined = "\n  ".join(failures)
    logger.warning(f"Validation failures:\n  {combined}")
