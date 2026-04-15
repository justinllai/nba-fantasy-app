"""
Quickstart.md validation tests.
Updated for new 8-param run() signature.
"""
import json
import os
import pytest
import pandas as pd
import pipeline.fetcher as fetcher_mod

from pipeline.exceptions import MissingAPIKeyError, UnsupportedDataTypeError, IngestionError


def _game_logs_df(rows=15):
    return pd.DataFrame({
        "player_id": list(range(rows)),
        "player_name": ["LeBron James"] * rows,
        "game_id": list(range(rows)),
        "date": [f"2023-01-{str(i+1).zfill(2)}" for i in range(rows)],
        "season": [2023] * rows,
        "min": [32.5] * rows,
        "pts": [28] * rows,
        "reb": [8] * rows,
        "ast": [7] * rows,
        "stl": [1] * rows,
        "blk": [1] * rows,
    })


def _season_avg_df():
    return pd.DataFrame({
        "player_id": [1], "player_name": ["Stephen Curry"],
        "season": [2023], "pts": [30.1], "reb": [5.3], "ast": [6.4],
        "min": [34.2],
    })


@pytest.fixture(autouse=True)
def restore_fetch_functions():
    original = dict(fetcher_mod.FETCH_FUNCTIONS)
    yield
    fetcher_mod.FETCH_FUNCTIONS.update(original)


@pytest.fixture
def api_key(monkeypatch):
    monkeypatch.setenv("BALL_IS_LIFE", "test_key_123")


def test_basic_usage(api_key, tmp_path):
    """Quickstart: run with player + season."""
    fetcher_mod.FETCH_FUNCTIONS["game_logs"] = lambda **kw: _game_logs_df()
    from pipeline import run
    result = run(data_types=["game_logs"], season=2023, player="LeBron James", output_dir=tmp_path)
    assert "game_logs" in result
    assert result["game_logs"]["status"] == "success"
    assert "file_path" in result["game_logs"]


def test_multiple_data_types(api_key, tmp_path):
    """Quickstart: multiple data types in one call."""
    fetcher_mod.FETCH_FUNCTIONS["game_logs"] = lambda **kw: _game_logs_df()
    fetcher_mod.FETCH_FUNCTIONS["season_averages"] = lambda **kw: _season_avg_df()
    from pipeline import run
    result = run(
        data_types=["game_logs", "season_averages"],
        season=2023, player="LeBron James", output_dir=tmp_path,
    )
    for dt in ["game_logs", "season_averages"]:
        assert result[dt]["status"] == "success"


def test_reading_output_parquet_and_sidecar(api_key, tmp_path):
    """Quickstart: reading output parquet and sidecar JSON."""
    fetcher_mod.FETCH_FUNCTIONS["game_logs"] = lambda **kw: _game_logs_df()
    from pipeline import run
    result = run(["game_logs"], season=2023, player="LeBron James", output_dir=tmp_path)
    file_path = result["game_logs"]["file_path"]

    df = pd.read_parquet(file_path)
    assert "is_outlier" in df.columns

    sidecar_path = file_path.replace(".parquet", ".sidecar.json")
    with open(sidecar_path) as f:
        meta = json.load(f)
    assert "rows_after" in meta
    assert "schema_drift" in meta


def test_error_handling_missing_api_key(tmp_path, monkeypatch):
    """Quickstart: missing API key raises APIKeyMissingError."""
    from unittest.mock import patch as _patch
    monkeypatch.delenv("BALL_IS_LIFE", raising=False)
    from pipeline.exceptions import APIKeyMissingError
    from pipeline import run
    # Prevent load_dotenv() inside run() from re-reading the .env file
    with _patch("pipeline.run.load_dotenv"):
        with pytest.raises(APIKeyMissingError):
            run(["game_logs"], season=2023, player="LeBron James", output_dir=tmp_path)


def test_error_handling_unsupported_type(api_key, tmp_path):
    """Quickstart: unsupported data type raises UnsupportedDataTypeError."""
    from pipeline import run
    with pytest.raises(UnsupportedDataTypeError):
        run(["bad_type"], season=2023, player="LeBron James", output_dir=tmp_path)


def test_per_type_failure_in_result(api_key, tmp_path):
    """Quickstart: per-type failure appears in result dict with status='failed'."""
    fetcher_mod.FETCH_FUNCTIONS["game_logs"] = lambda **kw: (_ for _ in ()).throw(RuntimeError("API down"))
    from pipeline import run
    result = run(["game_logs"], season=2023, player="LeBron James", output_dir=tmp_path)
    assert result["game_logs"]["status"] == "failed"
    assert "error" in result["game_logs"]
