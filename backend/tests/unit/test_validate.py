"""
Unit tests for pipeline.validate.
"""
import logging
import pytest
import pandas as pd
import numpy as np

from pipeline.validate import validate
from pipeline.exceptions import ValidationError


def make_logger():
    """Create a basic logger for testing."""
    logger = logging.getLogger("test_validate")
    logger.setLevel(logging.DEBUG)
    if not logger.handlers:
        logger.addHandler(logging.NullHandler())
    return logger


def make_valid_game_logs_df(rows=15):
    """Create a valid game_logs DataFrame."""
    return pd.DataFrame({
        "game_id": list(range(rows)),
        "player_name": ["LeBron James"] * rows,
        "date": ["2023-10-01"] * rows,
        "min": [32.5] * rows,
        "pts": [28] * rows,
        "reb": [8] * rows,
        "ast": [7] * rows,
        "stl": [1] * rows,
        "blk": [1] * rows,
    })


def test_missing_column_raises_validation_error():
    """Missing required column raises ValidationError naming the column."""
    logger = make_logger()
    df = make_valid_game_logs_df()
    df = df.drop(columns=["pts"])

    with pytest.raises(ValidationError) as exc_info:
        validate(df, "game_logs", logger)

    assert "pts" in str(exc_info.value)


def test_wrong_dtype_raises_validation_error():
    """Wrong dtype (non-numeric for int column) raises ValidationError."""
    logger = make_logger()
    df = make_valid_game_logs_df()
    # Make pts a string (non-numeric)
    df["pts"] = df["pts"].astype(str).replace({"28": "twenty-eight"})

    with pytest.raises(ValidationError) as exc_info:
        validate(df, "game_logs", logger)

    assert "pts" in str(exc_info.value)


def test_fewer_than_10_rows_raises_for_game_logs():
    """Fewer than 10 rows raises ValidationError for game_logs."""
    logger = make_logger()
    df = make_valid_game_logs_df(rows=5)

    with pytest.raises(ValidationError) as exc_info:
        validate(df, "game_logs", logger)

    assert "10" in str(exc_info.value) or "rows" in str(exc_info.value)


def test_fewer_than_10_rows_does_not_raise_for_box_scores():
    """Fewer than 10 rows does NOT raise for box_scores (10-row rule only for game_logs)."""
    logger = make_logger()
    df = pd.DataFrame({
        "game_id": [1, 2, 3],
        "player_name": ["LeBron James"] * 3,
        "team": ["LAL"] * 3,
        "date": ["2023-10-01"] * 3,
        "min": [32.5] * 3,
        "pts": [28] * 3,
        "reb": [8] * 3,
        "ast": [7] * 3,
        "stl": [1] * 3,
        "blk": [1] * 3,
    })

    # Should not raise
    validate(df, "box_scores", logger)


def test_multiple_failures_collected_into_one_validation_error():
    """Multiple validation failures are all included in a single ValidationError."""
    logger = make_logger()
    # Missing pts column AND only 3 rows (both fail)
    df = pd.DataFrame({
        "game_id": [1, 2, 3],
        "player_name": ["LeBron James"] * 3,
        "date": ["2023-10-01"] * 3,
        "min": [32.5] * 3,
        # pts missing
        "reb": [8] * 3,
        "ast": [7] * 3,
        "stl": [1] * 3,
        "blk": [1] * 3,
    })

    with pytest.raises(ValidationError) as exc_info:
        validate(df, "game_logs", logger)

    error_msg = str(exc_info.value)
    # Both failures should be mentioned
    assert "pts" in error_msg
    assert "rows" in error_msg or "10" in error_msg


def test_valid_dataframe_passes_silently():
    """A valid DataFrame does not raise and logs validation passed."""
    logger = make_logger()
    df = make_valid_game_logs_df(rows=15)

    # Should not raise
    validate(df, "game_logs", logger)


def test_float64_int_columns_safe_cast_accepted():
    """float64 columns with whole number values are accepted for int expected dtypes."""
    logger = make_logger()
    df = make_valid_game_logs_df(rows=15)
    # Make pts float (as returned by BallDontLie API)
    df["pts"] = df["pts"].astype(float)
    df["reb"] = df["reb"].astype(float)
    df["ast"] = df["ast"].astype(float)
    df["stl"] = df["stl"].astype(float)
    df["blk"] = df["blk"].astype(float)
    df["game_id"] = df["game_id"].astype(float)

    # Should not raise
    validate(df, "game_logs", logger)


def test_float64_with_fractional_values_fails_for_int_column():
    """float64 with fractional values fails for columns expected to be int."""
    logger = make_logger()
    df = make_valid_game_logs_df(rows=15)
    # pts should be int but has fractional value
    df["pts"] = [28.5] * 15

    with pytest.raises(ValidationError) as exc_info:
        validate(df, "game_logs", logger)

    assert "pts" in str(exc_info.value)
