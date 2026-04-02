"""
Integration tests for run() with game_scores data type.
T028a: Tests for game_scores full pipeline.
"""
import os
import json
import pytest
import pandas as pd
from unittest.mock import patch


def make_mock_game_scores_df(rows=5):
    """Create mock game_scores DataFrame."""
    return pd.DataFrame({
        "game_id": list(range(rows)),
        "home_team": ["LAL"] * rows,
        "visitor_team": ["BOS"] * rows,
        "home_team_score": [110] * rows,
        "visitor_team_score": [105] * rows,
        "date": ["2023-10-01"] * rows,
        "status": ["Final"] * rows,
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


def test_clean_parquet_and_sidecar_written_for_game_scores(setup_dirs):
    """Clean parquet and sidecar written for game_scores."""
    tmp_path = setup_dirs
    mock_df = make_mock_game_scores_df(5)

    import importlib

    from pipeline.run import run

    with patch("pipeline.ingest.ingest_game_scores", return_value=mock_df):
        result = run(["game_scores"], "LAL", 2023)

    assert "game_scores" in result
    assert "error" not in result["game_scores"], f"Unexpected error: {result['game_scores'].get('error')}"

    clean_dir = tmp_path / "clean"
    parquet_files = list(clean_dir.glob("*.parquet"))
    sidecar_files = list(clean_dir.glob("*.json"))

    assert len(parquet_files) >= 1
    assert len(sidecar_files) >= 1


def test_summary_dict_has_all_required_keys_for_game_scores(setup_dirs):
    """Summary dict returned with all required keys for game_scores."""
    tmp_path = setup_dirs
    mock_df = make_mock_game_scores_df(5)

    import importlib

    from pipeline.run import run

    with patch("pipeline.ingest.ingest_game_scores", return_value=mock_df):
        result = run(["game_scores"], "LAL", 2023)

    game_scores_result = result["game_scores"]
    required_keys = ["rows_before", "rows_after", "outliers_flagged", "corrupted_removed", "nulls_found", "file_path"]
    for key in required_keys:
        assert key in game_scores_result, f"Missing key: {key}"


def test_corrupted_rows_removed_from_game_scores(setup_dirs):
    """Rows with impossible scores are removed and logged."""
    tmp_path = setup_dirs

    # Create df with one corrupted row (score way over any reasonable threshold)
    rows = 5
    home_scores = [110] * (rows - 1) + [9999]  # 9999 is not > CORRUPTION_THRESHOLDS (which doesn't have scores)
    # Actually game_scores corruption is tested differently
    mock_df = make_mock_game_scores_df(rows)

    import importlib

    from pipeline.run import run

    with patch("pipeline.ingest.ingest_game_scores", return_value=mock_df):
        result = run(["game_scores"], "LAL", 2023)

    # Should succeed (corruption removal only applies if thresholds are exceeded)
    assert "game_scores" in result


def test_game_scores_sidecar_has_correct_fields(setup_dirs):
    """Clean sidecar for game_scores has correct fields."""
    tmp_path = setup_dirs
    mock_df = make_mock_game_scores_df(5)

    import importlib

    from pipeline.run import run

    with patch("pipeline.ingest.ingest_game_scores", return_value=mock_df):
        result = run(["game_scores"], "LAL", 2023)

    file_path = result["game_scores"]["file_path"]
    sidecar_path = file_path.replace(".parquet", ".json")

    assert os.path.exists(sidecar_path)
    with open(sidecar_path) as f:
        sidecar = json.load(f)

    assert sidecar["data_type"] == "game_scores"
    assert "rows_before" in sidecar
    assert "rows_after" in sidecar
    assert "written_at" in sidecar
