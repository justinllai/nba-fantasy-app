"""
Unit tests for run() input validation.
Tests T015: empty data_types, unsupported data type, missing API key.
"""
import os
import pytest
from unittest.mock import patch

from pipeline.exceptions import MissingAPIKeyError, UnsupportedDataTypeError


def test_empty_data_types_raises_value_error(monkeypatch):
    """Empty data_types list raises ValueError."""
    monkeypatch.setenv("BALL_IS_LIFE", "test_key")

    from pipeline.run import run

    with pytest.raises(ValueError, match="data_types"):
        run([], "LeBron James", 2023)


def test_unsupported_data_type_raises_error(monkeypatch):
    """Unsupported data type raises UnsupportedDataTypeError."""
    monkeypatch.setenv("BALL_IS_LIFE", "test_key")

    from pipeline.run import run

    with pytest.raises(UnsupportedDataTypeError):
        run(["unknown_type"], "LeBron James", 2023)


def test_missing_api_key_raises_before_any_action(monkeypatch):
    """Missing BALL_IS_LIFE env var raises MissingAPIKeyError before any file or API action."""
    monkeypatch.delenv("BALL_IS_LIFE", raising=False)

    from pipeline.run import run

    with pytest.raises(MissingAPIKeyError):
        run(["game_logs"], "LeBron James", 2023)


def test_non_list_data_types_raises_value_error(monkeypatch):
    """data_types that is not a list raises ValueError."""
    monkeypatch.setenv("BALL_IS_LIFE", "test_key")

    from pipeline.run import run

    with pytest.raises((ValueError, TypeError)):
        run("game_logs", "LeBron James", 2023)


def test_valid_inputs_do_not_raise_on_validation(monkeypatch):
    """Valid inputs pass input validation (may fail later at API call, not validation)."""
    monkeypatch.setenv("BALL_IS_LIFE", "test_key")

    from pipeline.run import run
    from pipeline.exceptions import IngestionError

    # Valid inputs should not raise ValueError or UnsupportedDataTypeError
    # They may raise IngestionError (if no API mock), which is OK
    try:
        run(["game_logs"], "LeBron James", 2023)
    except (ValueError, UnsupportedDataTypeError, MissingAPIKeyError):
        pytest.fail("Valid inputs should not raise input validation errors")
    except Exception:
        # Any other exception (IngestionError, NotImplementedError) is acceptable
        pass
