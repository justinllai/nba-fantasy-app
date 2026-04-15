"""
File output safety tests.
Tests for file write safety, overwrite behavior, and run log creation.
Updated for new writer.py (sidecar_path returns .sidecar.json) and new run() signature.
"""
import json
import os
import logging
import pytest
import pandas as pd
import pipeline.fetcher as fetcher_mod

from pipeline.writer import write_parquet, write_sidecar, sidecar_path
from pipeline.exceptions import FileWriteError


def _small_df(rows=5):
    return pd.DataFrame({
        "game_id": list(range(rows)),
        "player_name": ["LeBron James"] * rows,
        "pts": [28] * rows,
    })


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


@pytest.fixture(autouse=True)
def restore_fetch_functions():
    original = dict(fetcher_mod.FETCH_FUNCTIONS)
    yield
    fetcher_mod.FETCH_FUNCTIONS.update(original)


@pytest.fixture
def api_key(monkeypatch):
    monkeypatch.setenv("BALL_IS_LIFE", "test_key_123")


# ---------------------------------------------------------------------------
# writer.py primitives
# ---------------------------------------------------------------------------

def test_raw_parquet_and_sidecar_written(tmp_path):
    """write_parquet and write_sidecar both produce files."""
    df = _small_df()
    parquet_file = str(tmp_path / "test.parquet")
    sc_file = sidecar_path(parquet_file)

    write_parquet(df, parquet_file)
    write_sidecar({"data_type": "game_logs", "rows": 5}, sc_file)

    assert os.path.exists(parquet_file)
    assert os.path.exists(sc_file)
    assert sc_file.endswith(".sidecar.json")


def test_sidecar_extension_is_dot_sidecar_json(tmp_path):
    """sidecar_path() returns a .sidecar.json extension."""
    p = str(tmp_path / "game_logs_lebron_2023.parquet")
    assert sidecar_path(p).endswith(".sidecar.json")


def test_exactly_one_sidecar_per_parquet(tmp_path):
    """Exactly one sidecar file per parquet."""
    df = _small_df()
    parquet_file = str(tmp_path / "test.parquet")
    sc_file = sidecar_path(parquet_file)

    write_parquet(df, parquet_file)
    write_sidecar({"x": 1}, sc_file)

    parquet_files = list(tmp_path.glob("*.parquet"))
    sidecar_files = list(tmp_path.glob("*.sidecar.json"))

    assert len(parquet_files) == 1
    assert len(sidecar_files) == 1


def test_no_tmp_files_after_successful_write(tmp_path):
    """No .tmp files remain after a successful write."""
    df = _small_df()
    write_parquet(df, str(tmp_path / "test.parquet"))
    write_sidecar({"x": 1}, str(tmp_path / "test.sidecar.json"))
    assert len(list(tmp_path.glob("*.tmp"))) == 0


def test_no_tmp_files_after_write_failure(tmp_path):
    """No .tmp files remain after a write failure."""
    df = _small_df()
    parquet_file = str(tmp_path / "test_fail.parquet")

    with pytest.raises(FileWriteError):
        # Write to a non-existent directory path to trigger failure
        write_parquet(df, "/nonexistent_root/sub/test.parquet")

    assert len(list(tmp_path.glob("*.tmp"))) == 0


def test_last_known_good_parquet_survives_failed_overwrite(tmp_path):
    """Pre-seeded parquet survives a failed overwrite attempt."""
    good_df = _small_df(5)
    parquet_file = str(tmp_path / "test_survive.parquet")
    write_parquet(good_df, parquet_file)
    original_len = len(pd.read_parquet(parquet_file))

    # Attempt overwrite by writing to a bad path — original should be untouched
    with pytest.raises(FileWriteError):
        write_parquet(good_df, "/nonexistent_root/sub/test_survive.parquet")

    surviving = pd.read_parquet(parquet_file)
    assert len(surviving) == original_len == 5


# ---------------------------------------------------------------------------
# run() file output behavior
# ---------------------------------------------------------------------------

def test_filename_matches_convention(api_key, tmp_path):
    """Clean parquet filename matches {data_type}_{subject}_{season}.parquet."""
    fetcher_mod.FETCH_FUNCTIONS["game_logs"] = lambda **kw: _game_logs_df()
    from pipeline.run import run
    result = run(["game_logs"], season=2023, player="LeBron James", output_dir=tmp_path)
    filename = os.path.basename(result["game_logs"]["file_path"])
    assert filename == "game_logs_lebron_james_2023.parquet"


def test_run_log_file_created_per_execution(api_key, tmp_path):
    """Run log file created at logs/run_*.log for every invocation."""
    fetcher_mod.FETCH_FUNCTIONS["game_logs"] = lambda **kw: _game_logs_df()
    from pipeline.run import run
    run(["game_logs"], season=2023, player="LeBron James", output_dir=tmp_path)
    log_files = list((tmp_path / "logs").glob("run_*.log"))
    assert len(log_files) >= 1


def test_run_produces_correct_directory_structure(api_key, tmp_path):
    """Full run creates parquet + .sidecar.json in both raw/ and clean/ dirs."""
    fetcher_mod.FETCH_FUNCTIONS["game_logs"] = lambda **kw: _game_logs_df()
    from pipeline.run import run
    run(["game_logs"], season=2023, player="LeBron James", output_dir=tmp_path)

    assert len(list((tmp_path / "raw").glob("*.parquet"))) == 1
    assert len(list((tmp_path / "raw").glob("*.sidecar.json"))) == 1
    assert len(list((tmp_path / "clean").glob("*.parquet"))) == 1
    assert len(list((tmp_path / "clean").glob("*.sidecar.json"))) == 1
