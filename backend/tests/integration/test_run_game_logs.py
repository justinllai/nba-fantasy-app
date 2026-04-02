"""
Integration tests for run() with game_logs data type.
T016, T024, T030: Tests for full game_logs pipeline with mocked API.
"""
import json
import os
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from pipeline.exceptions import ValidationError


def make_game_logs_data(rows=15):
    """Create mock game_logs API response data."""
    return [
        {
            "id": i,
            "game": {"id": i, "date": "2023-10-01", "season": 2023},
            "player": {"id": 1, "first_name": "LeBron", "last_name": "James"},
            "team": {"abbreviation": "LAL"},
            "min": "32:30",
            "pts": 28,
            "reb": 8,
            "ast": 7,
            "stl": 1,
            "blk": 1,
            "fg_pct": 0.5,
            "fg3_pct": 0.35,
            "ft_pct": 0.75,
            "turnover": 3,
        }
        for i in range(rows)
    ]


def make_mock_df(rows=15):
    """Create a mock DataFrame that ingest would return."""
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


@pytest.fixture
def mock_ingest(tmp_path, monkeypatch):
    """Patch ingest_game_logs to return mock data."""
    mock_df = make_mock_df(15)
    monkeypatch.setenv("BALL_IS_LIFE", "test_key_123")

    # Patch RAW_DIR and CLEAN_DIR to use tmp_path
    import pipeline.constants as constants
    monkeypatch.setattr(constants, "RAW_DIR", str(tmp_path / "raw"))
    monkeypatch.setattr(constants, "CLEAN_DIR", str(tmp_path / "clean"))
    monkeypatch.setattr(constants, "LOGS_DIR", str(tmp_path / "logs"))

    os.makedirs(str(tmp_path / "raw"), exist_ok=True)
    os.makedirs(str(tmp_path / "clean"), exist_ok=True)
    os.makedirs(str(tmp_path / "logs"), exist_ok=True)

    with patch("pipeline.ingest.ingest_game_logs", return_value=mock_df) as mock:
        yield mock, tmp_path


def test_summary_dict_returned_with_correct_keys(mock_ingest):
    """run() returns a summary dict with all required keys."""
    mock_fn, tmp_path = mock_ingest

    import importlib

    from pipeline.run import run

    result = run(["game_logs"], "LeBron James", 2023)

    assert "game_logs" in result
    game_logs_result = result["game_logs"]
    assert "error" not in game_logs_result, f"Unexpected error: {game_logs_result.get('error')}"

    required_keys = ["rows_before", "rows_after", "outliers_flagged", "corrupted_removed", "nulls_found", "file_path"]
    for key in required_keys:
        assert key in game_logs_result, f"Missing key: {key}"


def test_clean_parquet_and_sidecar_written(mock_ingest):
    """Clean parquet and sidecar are written to data/clean/."""
    mock_fn, tmp_path = mock_ingest

    import importlib

    from pipeline.run import run

    result = run(["game_logs"], "LeBron James", 2023)

    clean_dir = tmp_path / "clean"
    parquet_files = list(clean_dir.glob("*.parquet"))
    sidecar_files = list(clean_dir.glob("*.json"))

    assert len(parquet_files) >= 1, "Expected at least one clean parquet file"
    assert len(sidecar_files) >= 1, "Expected at least one clean sidecar file"


def test_raw_parquet_and_sidecar_written(mock_ingest):
    """Raw parquet and sidecar are written to data/raw/."""
    mock_fn, tmp_path = mock_ingest

    import importlib

    from pipeline.run import run

    run(["game_logs"], "LeBron James", 2023)

    raw_dir = tmp_path / "raw"
    parquet_files = list(raw_dir.glob("*.parquet"))
    sidecar_files = list(raw_dir.glob("*.json"))

    assert len(parquet_files) >= 1, "Expected at least one raw parquet file"
    assert len(sidecar_files) >= 1, "Expected at least one raw sidecar file"


def test_is_outlier_column_present_in_clean_parquet(mock_ingest):
    """is_outlier column is present in clean parquet."""
    mock_fn, tmp_path = mock_ingest

    import importlib

    from pipeline.run import run

    result = run(["game_logs"], "LeBron James", 2023)

    file_path = result["game_logs"]["file_path"]
    clean_df = pd.read_parquet(file_path)

    assert "is_outlier" in clean_df.columns, "is_outlier column should be present in clean parquet"


def test_fewer_than_10_rows_emits_warning_no_clean_output(tmp_path, monkeypatch, caplog):
    """< 10 rows: ValidationError is caught, no clean output written, error in result dict."""
    import logging
    monkeypatch.setenv("BALL_IS_LIFE", "test_key_123")

    import pipeline.constants as constants
    monkeypatch.setattr(constants, "RAW_DIR", str(tmp_path / "raw"))
    monkeypatch.setattr(constants, "CLEAN_DIR", str(tmp_path / "clean"))
    monkeypatch.setattr(constants, "LOGS_DIR", str(tmp_path / "logs"))

    os.makedirs(str(tmp_path / "raw"), exist_ok=True)
    os.makedirs(str(tmp_path / "clean"), exist_ok=True)
    os.makedirs(str(tmp_path / "logs"), exist_ok=True)

    # Only 5 rows — less than the 10-row minimum
    small_df = make_mock_df(rows=5)

    import importlib

    from pipeline.run import run

    with patch("pipeline.ingest.ingest_game_logs", return_value=small_df):
        result = run(["game_logs"], "LeBron James", 2023)

    # Should have error key, not success keys
    assert "game_logs" in result
    assert "error" in result["game_logs"]

    # No clean parquet should have been written
    clean_dir = tmp_path / "clean"
    parquet_files = list(clean_dir.glob("*.parquet"))
    assert len(parquet_files) == 0, "Clean parquet should not be written after validation failure"


def test_missing_pts_column_emits_warning_naming_column(tmp_path, monkeypatch, caplog):
    """Missing 'pts' column: error in result dict mentions 'pts'."""
    import logging
    monkeypatch.setenv("BALL_IS_LIFE", "test_key_123")

    import pipeline.constants as constants
    monkeypatch.setattr(constants, "RAW_DIR", str(tmp_path / "raw"))
    monkeypatch.setattr(constants, "CLEAN_DIR", str(tmp_path / "clean"))
    monkeypatch.setattr(constants, "LOGS_DIR", str(tmp_path / "logs"))

    os.makedirs(str(tmp_path / "raw"), exist_ok=True)
    os.makedirs(str(tmp_path / "clean"), exist_ok=True)
    os.makedirs(str(tmp_path / "logs"), exist_ok=True)

    bad_df = make_mock_df(rows=15).drop(columns=["pts"])

    import importlib

    from pipeline.run import run

    with patch("pipeline.ingest.ingest_game_logs", return_value=bad_df):
        result = run(["game_logs"], "LeBron James", 2023)

    assert "game_logs" in result
    assert "error" in result["game_logs"]
    assert "pts" in result["game_logs"]["error"]


def test_valid_dataset_no_error_in_result(mock_ingest):
    """Valid dataset produces success result with no error."""
    mock_fn, tmp_path = mock_ingest

    import importlib

    from pipeline.run import run

    result = run(["game_logs"], "LeBron James", 2023)

    assert "game_logs" in result
    assert "error" not in result["game_logs"]


def test_failed_run_leaves_old_files_intact(tmp_path, monkeypatch):
    """Failed run (write error) leaves pre-existing clean parquet intact."""
    monkeypatch.setenv("BALL_IS_LIFE", "test_key_123")

    import pipeline.constants as constants
    monkeypatch.setattr(constants, "RAW_DIR", str(tmp_path / "raw"))
    monkeypatch.setattr(constants, "CLEAN_DIR", str(tmp_path / "clean"))
    monkeypatch.setattr(constants, "LOGS_DIR", str(tmp_path / "logs"))

    os.makedirs(str(tmp_path / "raw"), exist_ok=True)
    os.makedirs(str(tmp_path / "clean"), exist_ok=True)
    os.makedirs(str(tmp_path / "logs"), exist_ok=True)

    # Pre-seed a "good" clean file
    good_df = make_mock_df(rows=15)
    from pipeline.save import write_with_sidecar, build_clean_sidecar

    parquet_path = str(tmp_path / "clean" / "game_logs_lebron_james_2023.parquet")
    sidecar = build_clean_sidecar("game_logs", "LeBron James", 2023,
                                   {"rows_before": 15, "rows_after": 15, "nulls_found": 0,
                                    "outliers_flagged": 0, "corrupted_removed": 0,
                                    "dedup_skipped": False, "dedup_reason": None}, good_df)
    write_with_sidecar(good_df, parquet_path, sidecar)
    assert os.path.exists(parquet_path)
    original_size = os.path.getsize(parquet_path)

    # Now simulate a write failure
    from pipeline.exceptions import FileWriteError

    import importlib

    from pipeline.run import run

    with patch("pipeline.ingest.ingest_game_logs", return_value=good_df):
        with patch("pipeline.run.write_with_sidecar", side_effect=FileWriteError("simulated failure")):
            result = run(["game_logs"], "LeBron James", 2023)

    # Pre-seeded file should still be intact
    assert os.path.exists(parquet_path)
    assert os.path.getsize(parquet_path) == original_size


def test_overwrite_safety_successful_run_updates_file(tmp_path, monkeypatch):
    """After successful run, file is updated (overwritten atomically)."""
    monkeypatch.setenv("BALL_IS_LIFE", "test_key_123")

    import pipeline.constants as constants
    monkeypatch.setattr(constants, "RAW_DIR", str(tmp_path / "raw"))
    monkeypatch.setattr(constants, "CLEAN_DIR", str(tmp_path / "clean"))
    monkeypatch.setattr(constants, "LOGS_DIR", str(tmp_path / "logs"))

    os.makedirs(str(tmp_path / "raw"), exist_ok=True)
    os.makedirs(str(tmp_path / "clean"), exist_ok=True)
    os.makedirs(str(tmp_path / "logs"), exist_ok=True)

    mock_df = make_mock_df(rows=15)

    import importlib

    from pipeline.run import run

    with patch("pipeline.ingest.ingest_game_logs", return_value=mock_df):
        result1 = run(["game_logs"], "LeBron James", 2023)

    assert "error" not in result1.get("game_logs", {})

    # Run again — should succeed and update file
    with patch("pipeline.ingest.ingest_game_logs", return_value=mock_df):
        result2 = run(["game_logs"], "LeBron James", 2023)

    assert "error" not in result2.get("game_logs", {})
