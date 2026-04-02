"""
Unit tests for pipeline.logging_utils.
"""
import logging
import os
import re
import tempfile
import pytest


def test_log_file_created_at_correct_path(tmp_path, monkeypatch):
    """Log file created at backend/logs/run_{run_id}.log."""
    import pipeline.constants as constants
    monkeypatch.setattr(constants, "LOGS_DIR", str(tmp_path))

    # Re-import logging_utils after patching constants
    import importlib
    import pipeline.logging_utils as logging_utils
    importlib.reload(logging_utils)

    run_id = "20260101_120000"
    logger = logging_utils.create_logger(run_id)

    expected_log_path = tmp_path / f"run_{run_id}.log"
    assert expected_log_path.exists(), f"Log file not found at {expected_log_path}"

    # Clean up logger handlers
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)
    logging.getLogger(f"pipeline.run.{run_id}").handlers.clear()


def test_message_appears_in_both_handlers(tmp_path, monkeypatch, capsys):
    """Log messages appear in both StreamHandler (terminal) and FileHandler."""
    import pipeline.constants as constants
    monkeypatch.setattr(constants, "LOGS_DIR", str(tmp_path))

    import importlib
    import pipeline.logging_utils as logging_utils
    importlib.reload(logging_utils)

    run_id = "20260101_120001"
    logger = logging_utils.create_logger(run_id)
    test_message = "test message for dual sink verification"
    logger.info(test_message)

    # Flush handlers
    for handler in logger.handlers:
        handler.flush()

    # Check file handler
    log_path = tmp_path / f"run_{run_id}.log"
    file_content = log_path.read_text(encoding="utf-8")
    assert test_message in file_content

    # Check stream handler (captured output)
    captured = capsys.readouterr()
    assert test_message in captured.err

    # Clean up
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)


def test_log_validation_block_emits_one_warning(tmp_path, monkeypatch, caplog):
    """log_validation_block emits one WARNING call with all failures joined."""
    import pipeline.constants as constants
    monkeypatch.setattr(constants, "LOGS_DIR", str(tmp_path))

    import importlib
    import pipeline.logging_utils as logging_utils
    importlib.reload(logging_utils)

    run_id = "20260101_120002"
    logger = logging_utils.create_logger(run_id)

    failures = ["missing column: pts", "wrong dtype: game_id", "too few rows: 5 < 10"]

    with caplog.at_level(logging.WARNING, logger=f"pipeline.run.{run_id}"):
        logging_utils.log_validation_block(logger, failures)

    # Should be exactly one WARNING record
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1, f"Expected 1 WARNING, got {len(warnings)}"

    # All failure messages should be in that single warning
    warning_msg = warnings[0].message
    for failure in failures:
        assert failure in warning_msg, f"Expected '{failure}' in warning message"

    # Clean up
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)


def test_format_string_contains_timestamp_pattern(tmp_path, monkeypatch):
    """Log entries contain expected timestamp format YYYY-MM-DD HH:MM:SS."""
    import pipeline.constants as constants
    monkeypatch.setattr(constants, "LOGS_DIR", str(tmp_path))

    import importlib
    import pipeline.logging_utils as logging_utils
    importlib.reload(logging_utils)

    run_id = "20260101_120003"
    logger = logging_utils.create_logger(run_id)
    logger.info("timestamp format test")

    for handler in logger.handlers:
        handler.flush()

    log_path = tmp_path / f"run_{run_id}.log"
    file_content = log_path.read_text(encoding="utf-8")

    # Timestamp pattern: YYYY-MM-DD HH:MM:SS
    timestamp_pattern = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"
    assert re.search(timestamp_pattern, file_content), (
        f"Expected timestamp pattern '{timestamp_pattern}' in log output: {file_content}"
    )

    # Also check separator format
    assert " | " in file_content

    # Clean up
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)
