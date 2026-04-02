"""
File output safety tests.
T029: Tests for file write safety, overwrite behavior, and run log creation.
"""
import json
import os
import pytest
import pandas as pd
from unittest.mock import patch

from pipeline.save import write_with_sidecar, build_raw_sidecar, build_clean_sidecar
from pipeline.exceptions import FileWriteError


def make_mock_df(rows=5):
    return pd.DataFrame({
        "game_id": list(range(rows)),
        "player_name": ["LeBron James"] * rows,
        "pts": [28] * rows,
    })


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


@pytest.fixture
def setup_dirs(tmp_path, monkeypatch):
    import logging

    monkeypatch.setenv("BALL_IS_LIFE", "test_key_123")

    import pipeline.constants as constants
    monkeypatch.setattr(constants, "RAW_DIR", str(tmp_path / "raw"))
    monkeypatch.setattr(constants, "CLEAN_DIR", str(tmp_path / "clean"))
    monkeypatch.setattr(constants, "LOGS_DIR", str(tmp_path / "logs"))

    os.makedirs(str(tmp_path / "raw"), exist_ok=True)
    os.makedirs(str(tmp_path / "clean"), exist_ok=True)
    os.makedirs(str(tmp_path / "logs"), exist_ok=True)

    yield tmp_path

    # Clear cached pipeline loggers so handler/path leakage doesn't cross tests
    for name in list(logging.Logger.manager.loggerDict.keys()):
        if name.startswith("pipeline.run."):
            logger = logging.getLogger(name)
            for handler in logger.handlers[:]:
                handler.close()
                logger.removeHandler(handler)


def test_raw_parquet_and_sidecar_written_after_ingest(setup_dirs):
    """Raw parquet and sidecar are both written after successful ingest."""
    tmp_path = setup_dirs
    df = make_mock_df()
    parquet_path = str(tmp_path / "raw" / "test.parquet")
    sidecar = build_raw_sidecar("game_logs", "LeBron James", 2023, df)

    write_with_sidecar(df, parquet_path, sidecar)

    assert os.path.exists(parquet_path)
    assert os.path.exists(str(tmp_path / "raw" / "test.json"))


def test_clean_parquet_and_sidecar_both_written(setup_dirs):
    """Clean parquet and sidecar are both written after successful clean."""
    tmp_path = setup_dirs
    df = make_mock_df()
    parquet_path = str(tmp_path / "clean" / "test.parquet")
    metrics = {
        "rows_before": 5, "rows_after": 5, "nulls_found": 0,
        "outliers_flagged": 0, "corrupted_removed": 0,
        "dedup_skipped": False, "dedup_reason": None,
    }
    sidecar = build_clean_sidecar("game_logs", "LeBron James", 2023, metrics, df)

    write_with_sidecar(df, parquet_path, sidecar)

    assert os.path.exists(parquet_path)
    assert os.path.exists(str(tmp_path / "clean" / "test.json"))


def test_exactly_one_sidecar_per_parquet(setup_dirs):
    """Exactly one sidecar file per parquet (never zero, never two)."""
    tmp_path = setup_dirs
    df = make_mock_df()
    parquet_path = str(tmp_path / "raw" / "test.parquet")
    sidecar = {"test": True}

    write_with_sidecar(df, parquet_path, sidecar)

    parquet_files = list((tmp_path / "raw").glob("*.parquet"))
    sidecar_files = list((tmp_path / "raw").glob("*.json"))

    assert len(parquet_files) == 1
    assert len(sidecar_files) == 1


def test_filename_matches_convention(setup_dirs):
    """Filenames match {data_type}_{player_or_team}_{season}.parquet convention."""
    tmp_path = setup_dirs
    mock_df = make_game_logs_df(15)

    import importlib

    from pipeline.run import run

    with patch("pipeline.ingest.ingest_game_logs", return_value=mock_df):
        result = run(["game_logs"], "LeBron James", 2023)

    file_path = result["game_logs"]["file_path"]
    filename = os.path.basename(file_path)

    assert filename == "game_logs_lebron_james_2023.parquet"


def test_run_log_file_created_per_execution(setup_dirs):
    """Run log file created at logs/run_*.log for every invocation."""
    tmp_path = setup_dirs
    mock_df = make_game_logs_df(15)

    import importlib

    from pipeline.run import run

    with patch("pipeline.ingest.ingest_game_logs", return_value=mock_df):
        run(["game_logs"], "LeBron James", 2023)

    logs_dir = tmp_path / "logs"
    log_files = list(logs_dir.glob("run_*.log"))
    assert len(log_files) >= 1


def test_no_tmp_files_remain_after_successful_run(setup_dirs):
    """No .tmp files remain after successful run."""
    tmp_path = setup_dirs
    df = make_mock_df()
    parquet_path = str(tmp_path / "raw" / "test.parquet")
    sidecar = {"test": True}

    write_with_sidecar(df, parquet_path, sidecar)

    tmp_files = list((tmp_path / "raw").glob("*.tmp"))
    assert len(tmp_files) == 0


def test_no_tmp_files_remain_after_simulated_write_failure(tmp_path):
    """No .tmp files remain after a simulated write failure."""
    df = make_mock_df()
    parquet_path = str(tmp_path / "test_fail.parquet")
    sidecar = {"test": True}

    with patch.object(df, "to_parquet", side_effect=IOError("disk full")):
        with pytest.raises(FileWriteError):
            write_with_sidecar(df, parquet_path, sidecar)

    tmp_files = list(tmp_path.glob("*.tmp"))
    assert len(tmp_files) == 0


def test_clean_output_not_written_when_validation_fails(setup_dirs):
    """Clean output is NOT written when validation fails."""
    tmp_path = setup_dirs

    import importlib

    from pipeline.run import run

    # Only 3 rows — fails game_logs validation (< 10)
    small_df = make_game_logs_df(rows=3)

    with patch("pipeline.ingest.ingest_game_logs", return_value=small_df):
        result = run(["game_logs"], "LeBron James", 2023)

    assert "error" in result["game_logs"]
    clean_dir = tmp_path / "clean"
    parquet_files = list(clean_dir.glob("*.parquet"))
    assert len(parquet_files) == 0


def test_last_known_good_parquet_survives_failed_overwrite(setup_dirs):
    """Pre-seeded parquet survives a failed overwrite attempt."""
    tmp_path = setup_dirs

    # Pre-seed a good file
    good_df = make_mock_df(5)
    parquet_path = str(tmp_path / "clean" / "test_survive.parquet")
    sidecar = {"rows": 5}
    write_with_sidecar(good_df, parquet_path, sidecar)

    original_content = pd.read_parquet(parquet_path)

    # Attempt to write with failure
    bad_df = make_mock_df(10)
    with patch.object(bad_df, "to_parquet", side_effect=IOError("disk full")):
        with pytest.raises(FileWriteError):
            write_with_sidecar(bad_df, parquet_path, sidecar)

    # Original file should be unchanged
    surviving_content = pd.read_parquet(parquet_path)
    assert len(surviving_content) == len(original_content) == 5


def test_run_produces_correct_directory_structure(setup_dirs):
    """Full run creates parquet + sidecar in both raw/ and clean/ dirs."""
    tmp_path = setup_dirs
    mock_df = make_game_logs_df(15)

    import importlib

    from pipeline.run import run

    with patch("pipeline.ingest.ingest_game_logs", return_value=mock_df):
        run(["game_logs"], "LeBron James", 2023)

    raw_parquets = list((tmp_path / "raw").glob("*.parquet"))
    raw_sidecars = list((tmp_path / "raw").glob("*.json"))
    clean_parquets = list((tmp_path / "clean").glob("*.parquet"))
    clean_sidecars = list((tmp_path / "clean").glob("*.json"))

    assert len(raw_parquets) == 1
    assert len(raw_sidecars) == 1
    assert len(clean_parquets) == 1
    assert len(clean_sidecars) == 1
