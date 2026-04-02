"""
Integration tests for run() with season_averages data type.
T027: Tests for season_averages full pipeline.
"""
import os
import json
import pytest
import pandas as pd
from unittest.mock import patch


def make_mock_season_averages_df(rows=1):
    """Create mock season_averages DataFrame."""
    return pd.DataFrame({
        "player_name": ["Stephen Curry"] * rows,
        "season": [2023] * rows,
        "pts": [30.1] * rows,
        "reb": [5.3] * rows,
        "ast": [6.4] * rows,
        "stl": [1.3] * rows,
        "blk": [0.4] * rows,
        "min": [34.2] * rows,
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


def test_clean_parquet_and_sidecar_written_for_season_averages(setup_dirs):
    """Clean parquet and sidecar written for season_averages."""
    tmp_path = setup_dirs
    mock_df = make_mock_season_averages_df(1)

    import importlib

    from pipeline.run import run

    with patch("pipeline.ingest.ingest_season_averages", return_value=mock_df):
        result = run(["season_averages"], "Stephen Curry", 2023)

    assert "season_averages" in result
    assert "error" not in result["season_averages"], (
        f"Unexpected error: {result['season_averages'].get('error')}"
    )

    clean_dir = tmp_path / "clean"
    parquet_files = list(clean_dir.glob("*.parquet"))
    sidecar_files = list(clean_dir.glob("*.json"))

    assert len(parquet_files) >= 1
    assert len(sidecar_files) >= 1


def test_dedup_skipped_true_in_sidecar_for_season_averages(setup_dirs):
    """dedup_skipped is true in clean sidecar for season_averages (no game_id)."""
    tmp_path = setup_dirs
    mock_df = make_mock_season_averages_df(1)

    import importlib

    from pipeline.run import run

    with patch("pipeline.ingest.ingest_season_averages", return_value=mock_df):
        result = run(["season_averages"], "Stephen Curry", 2023)

    file_path = result["season_averages"]["file_path"]
    sidecar_path = file_path.replace(".parquet", ".json")

    with open(sidecar_path) as f:
        sidecar = json.load(f)

    assert sidecar["dedup_skipped"] is True, "dedup_skipped should be True for season_averages"
    assert sidecar["dedup_reason"] is not None


def test_summary_dict_has_all_required_keys_for_season_averages(setup_dirs):
    """Summary dict returned with all required keys for season_averages."""
    tmp_path = setup_dirs
    mock_df = make_mock_season_averages_df(1)

    import importlib

    from pipeline.run import run

    with patch("pipeline.ingest.ingest_season_averages", return_value=mock_df):
        result = run(["season_averages"], "Stephen Curry", 2023)

    season_avg_result = result["season_averages"]
    required_keys = ["rows_before", "rows_after", "outliers_flagged", "corrupted_removed", "nulls_found", "file_path"]
    for key in required_keys:
        assert key in season_avg_result, f"Missing key: {key}"


def test_no_minimum_row_validation_failure_for_season_averages(setup_dirs):
    """10-row minimum does not apply to season_averages (1 row is valid)."""
    tmp_path = setup_dirs
    # Only 1 row — would fail for game_logs but not for season_averages
    mock_df = make_mock_season_averages_df(rows=1)

    import importlib

    from pipeline.run import run

    with patch("pipeline.ingest.ingest_season_averages", return_value=mock_df):
        result = run(["season_averages"], "Stephen Curry", 2023)

    assert "season_averages" in result
    # Should NOT have validation error about minimum rows
    if "error" in result["season_averages"]:
        error_msg = result["season_averages"]["error"]
        assert "10" not in error_msg or "row" not in error_msg.lower(), (
            f"season_averages should not fail 10-row minimum check: {error_msg}"
        )
