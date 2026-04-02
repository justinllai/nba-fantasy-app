"""
T031: Quickstart.md validation tests.
Executes all code examples from quickstart.md against mocked API.
"""
import os
import json
import pytest
import pandas as pd
from unittest.mock import patch


def make_game_logs_df(rows=15):
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


def make_season_averages_df():
    return pd.DataFrame({
        "player_name": ["Stephen Curry"],
        "season": [2023],
        "pts": [30.1],
        "reb": [5.3],
        "ast": [6.4],
        "stl": [1.3],
        "blk": [0.4],
        "min": [34.2],
    })


def make_box_scores_df(rows=5):
    return pd.DataFrame({
        "game_id": list(range(rows)),
        "player_name": ["LeBron James"] * rows,
        "team": ["LAL"] * rows,
        "date": ["2023-10-01"] * rows,
        "min": [32.5] * rows,
        "pts": [28] * rows,
        "reb": [8] * rows,
        "ast": [7] * rows,
        "stl": [1] * rows,
        "blk": [1] * rows,
    })


@pytest.fixture
def setup_dirs(tmp_path, monkeypatch):
    monkeypatch.setenv("BALL_IS_LIFE", "test_key_123")

    import pipeline.constants as constants
    monkeypatch.setattr(constants, "RAW_DIR", str(tmp_path / "raw"))
    monkeypatch.setattr(constants, "CLEAN_DIR", str(tmp_path / "clean"))
    monkeypatch.setattr(constants, "LOGS_DIR", str(tmp_path / "logs"))

    os.makedirs(str(tmp_path / "raw"), exist_ok=True)
    os.makedirs(str(tmp_path / "clean"), exist_ok=True)
    os.makedirs(str(tmp_path / "logs"), exist_ok=True)

    return tmp_path


def test_quickstart_basic_usage(setup_dirs):
    """Quickstart basic usage: run(["game_logs"], "LeBron James", 2023)."""
    import importlib

    # Quickstart example 1: Basic usage
    from pipeline import run

    with patch("pipeline.ingest.ingest_game_logs", return_value=make_game_logs_df()):
        result = run(
            data_types=["game_logs"],
            player_or_team="LeBron James",
            season=2023,
        )

    assert "game_logs" in result
    assert "error" not in result["game_logs"]
    assert "rows_after" in result["game_logs"]
    assert "file_path" in result["game_logs"]


def test_quickstart_multiple_data_types(setup_dirs):
    """Quickstart: multiple data types in one call."""
    import importlib

    from pipeline import run

    with patch("pipeline.ingest.ingest_game_logs", return_value=make_game_logs_df()), \
         patch("pipeline.ingest.ingest_season_averages", return_value=make_season_averages_df()):
        result = run(
            data_types=["game_logs", "season_averages"],
            player_or_team="Stephen Curry",
            season=2023,
        )

    for dtype, summary in result.items():
        if "error" in summary:
            pytest.fail(f"{dtype} FAILED unexpectedly: {summary['error']}")
        else:
            assert "rows_after" in summary
            assert "file_path" in summary


def test_quickstart_box_scores_team_season(setup_dirs):
    """Quickstart: box scores via team+season."""
    import importlib

    from pipeline import run

    with patch("pipeline.ingest.ingest_box_scores", return_value=make_box_scores_df()):
        result = run(
            data_types=["box_scores"],
            player_or_team="LAL",
            season=2023,
        )

    assert "box_scores" in result
    assert "error" not in result["box_scores"]


def test_quickstart_box_scores_game_id(setup_dirs):
    """Quickstart: box scores via game_id string."""
    import importlib

    from pipeline import run

    with patch("pipeline.ingest.ingest_box_scores", return_value=make_box_scores_df()):
        result = run(
            data_types=["box_scores"],
            player_or_team="18370647",
            season=2023,
        )

    assert "box_scores" in result
    assert "error" not in result["box_scores"]


def test_quickstart_reading_output(setup_dirs):
    """Quickstart: reading output parquet and sidecar JSON."""
    import importlib

    from pipeline import run

    with patch("pipeline.ingest.ingest_game_logs", return_value=make_game_logs_df()):
        result = run(["game_logs"], "LeBron James", 2023)

    file_path = result["game_logs"]["file_path"]

    # Reading output as shown in quickstart
    df = pd.read_parquet(file_path)
    assert "is_outlier" in df.columns

    sidecar_path = file_path.replace(".parquet", ".json")
    with open(sidecar_path) as f:
        meta = json.load(f)

    assert "rows_after" in meta
    assert "outliers_flagged" in meta


def test_quickstart_error_handling_missing_api_key(setup_dirs, monkeypatch):
    """Quickstart: error handling for missing API key."""
    monkeypatch.delenv("BALL_IS_LIFE", raising=False)

    import importlib

    from pipeline import run
    from pipeline.exceptions import MissingAPIKeyError

    with pytest.raises(MissingAPIKeyError):
        run(["game_logs"], "LeBron James", 2023)


def test_quickstart_error_handling_unsupported_type(setup_dirs):
    """Quickstart: error handling for unsupported data type."""
    import importlib

    from pipeline import run
    from pipeline.exceptions import UnsupportedDataTypeError

    with pytest.raises(UnsupportedDataTypeError):
        run(["bad_type"], "LeBron James", 2023)


def test_quickstart_per_type_failure_in_result(setup_dirs):
    """Quickstart: per-type failure appears in result dict with 'error' key."""
    import importlib

    from pipeline import run
    from pipeline.exceptions import IngestionError

    with patch("pipeline.ingest.ingest_game_logs", side_effect=IngestionError("API down")):
        result = run(["game_logs"], "LeBron James", 2023)

    assert "error" in result.get("game_logs", {})
    assert "API down" in result["game_logs"]["error"]
