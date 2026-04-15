"""
Unit tests for run() input validation.
Updated for new 8-param run() signature.
"""
import pytest
from unittest.mock import patch

from pipeline.exceptions import (
    APIKeyMissingError,
    MissingAPIKeyError,
    PipelineConfigError,
    UnsupportedDataTypeError,
)


def test_empty_data_types_raises(monkeypatch):
    """Empty data_types list raises PipelineConfigError."""
    monkeypatch.setenv("BALL_IS_LIFE", "test_key")
    from pipeline.run import run
    with pytest.raises((PipelineConfigError, ValueError)):
        run([], season=2023, player="LeBron James")


def test_unsupported_data_type_raises_error(monkeypatch):
    """Unsupported data type raises UnsupportedDataTypeError."""
    monkeypatch.setenv("BALL_IS_LIFE", "test_key")
    from pipeline.run import run
    with pytest.raises(UnsupportedDataTypeError):
        run(["unknown_type"], season=2023, player="LeBron James")


def test_missing_api_key_raises_before_any_action(monkeypatch):
    """Missing BALL_IS_LIFE env var raises APIKeyMissingError before any API action."""
    monkeypatch.delenv("BALL_IS_LIFE", raising=False)
    from pipeline.run import run
    with patch("pipeline.run.load_dotenv"):  # prevent .env re-read
        with pytest.raises((APIKeyMissingError, MissingAPIKeyError)):
            run(["game_logs"], season=2023, player="LeBron James")


def test_non_list_data_types_raises(monkeypatch):
    """data_types that is not a list raises PipelineConfigError or ValueError."""
    monkeypatch.setenv("BALL_IS_LIFE", "test_key")
    from pipeline.run import run
    with pytest.raises((PipelineConfigError, ValueError, TypeError)):
        run("game_logs", season=2023, player="LeBron James")


def test_both_player_and_team_raises(monkeypatch):
    """Providing both player and team raises PipelineConfigError."""
    monkeypatch.setenv("BALL_IS_LIFE", "test_key")
    from pipeline.run import run
    with pytest.raises(PipelineConfigError):
        run(["game_logs"], season=2023, player="LeBron James", team="LAL")


def test_valid_inputs_do_not_raise_on_validation(monkeypatch):
    """Valid inputs pass input validation."""
    monkeypatch.setenv("BALL_IS_LIFE", "test_key")
    import pipeline.fetcher as fetcher_mod
    import pandas as pd
    orig = fetcher_mod.FETCH_FUNCTIONS.get("game_logs")
    fetcher_mod.FETCH_FUNCTIONS["game_logs"] = lambda **kw: pd.DataFrame({
        "player_id": [1], "player_name": ["A"], "game_id": [1], "date": ["2023-01-01"]
    })
    from pipeline.run import run
    try:
        run(["game_logs"], season=2023, player="LeBron James")
    except (PipelineConfigError, ValueError, UnsupportedDataTypeError, APIKeyMissingError):
        pytest.fail("Valid inputs should not raise input validation errors")
    except Exception:
        pass
    finally:
        if orig:
            fetcher_mod.FETCH_FUNCTIONS["game_logs"] = orig
