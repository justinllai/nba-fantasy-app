"""
Integration tests for run() with box_scores data type.
T021: Tests for both lookup paths.
"""
import os
import pytest
import pandas as pd
from unittest.mock import patch


def make_mock_box_scores_df(rows=5):
    """Create a mock box_scores DataFrame."""
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
    """Set up temp directories and env for tests."""
    monkeypatch.setenv("BALL_IS_LIFE", "test_key_123")

    import pipeline.constants as constants
    monkeypatch.setattr(constants, "RAW_DIR", str(tmp_path / "raw"))
    monkeypatch.setattr(constants, "CLEAN_DIR", str(tmp_path / "clean"))
    monkeypatch.setattr(constants, "LOGS_DIR", str(tmp_path / "logs"))

    os.makedirs(str(tmp_path / "raw"), exist_ok=True)
    os.makedirs(str(tmp_path / "clean"), exist_ok=True)
    os.makedirs(str(tmp_path / "logs"), exist_ok=True)

    return tmp_path


def test_team_season_path_produces_output(setup_dirs):
    """Team+season path produces clean parquet + sidecar."""
    tmp_path = setup_dirs
    mock_df = make_mock_box_scores_df(5)

    import importlib

    from pipeline.run import run

    with patch("pipeline.ingest.ingest_box_scores", return_value=mock_df):
        result = run(["box_scores"], "LAL", 2023)

    assert "box_scores" in result
    assert "error" not in result["box_scores"], f"Unexpected error: {result['box_scores'].get('error')}"

    clean_dir = tmp_path / "clean"
    parquet_files = list(clean_dir.glob("*.parquet"))
    sidecar_files = list(clean_dir.glob("*.json"))
    assert len(parquet_files) >= 1
    assert len(sidecar_files) >= 1


def test_game_id_path_produces_output(setup_dirs):
    """Game ID path produces clean parquet + sidecar."""
    tmp_path = setup_dirs
    mock_df = make_mock_box_scores_df(5)

    import importlib

    from pipeline.run import run

    with patch("pipeline.ingest.ingest_box_scores", return_value=mock_df):
        result = run(["box_scores"], "18370647", 2023)

    assert "box_scores" in result
    assert "error" not in result["box_scores"], f"Unexpected error: {result['box_scores'].get('error')}"


def test_team_path_filename_contains_team_name(setup_dirs):
    """Filename for team+season path contains normalized team name."""
    tmp_path = setup_dirs
    mock_df = make_mock_box_scores_df(5)

    import importlib

    from pipeline.run import run

    with patch("pipeline.ingest.ingest_box_scores", return_value=mock_df):
        result = run(["box_scores"], "LAL", 2023)

    file_path = result["box_scores"]["file_path"]
    assert "lal" in file_path.lower()
    assert "box_scores" in file_path.lower()


def test_game_id_path_filename_contains_game_id(setup_dirs):
    """Filename for game_id path contains the game_id."""
    tmp_path = setup_dirs
    mock_df = make_mock_box_scores_df(5)

    import importlib

    from pipeline.run import run

    with patch("pipeline.ingest.ingest_box_scores", return_value=mock_df):
        result = run(["box_scores"], "18370647", 2023)

    file_path = result["box_scores"]["file_path"]
    assert "18370647" in file_path
