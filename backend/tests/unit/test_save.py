"""
Unit tests for pipeline.save.
"""
import json
import os
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from pipeline.save import write_with_sidecar, build_raw_sidecar, build_clean_sidecar
from pipeline.exceptions import FileWriteError


def make_test_df():
    """Create a small test DataFrame."""
    return pd.DataFrame({
        "game_id": [1, 2, 3],
        "player_name": ["LeBron James", "LeBron James", "LeBron James"],
        "pts": [28, 35, 22],
    })


def test_temp_file_replaced_by_final_on_success(tmp_path):
    """On successful write, temp file is replaced by final file."""
    df = make_test_df()
    parquet_path = str(tmp_path / "test.parquet")
    sidecar = {"data_type": "game_logs", "rows": 3}

    write_with_sidecar(df, parquet_path, sidecar)

    # Final file exists
    assert os.path.exists(parquet_path)
    # Sidecar exists
    assert os.path.exists(str(tmp_path / "test.json"))
    # No .tmp files remain
    assert not os.path.exists(parquet_path + ".tmp")
    assert not os.path.exists(str(tmp_path / "test.json") + ".tmp")


def test_no_final_file_created_on_write_failure(tmp_path):
    """If parquet write fails, no final parquet file is created."""
    df = make_test_df()
    parquet_path = str(tmp_path / "test_fail.parquet")
    sidecar = {"data_type": "game_logs", "rows": 3}

    # Mock to_parquet to raise an error
    with patch.object(df, "to_parquet", side_effect=IOError("disk full")):
        with pytest.raises(FileWriteError):
            write_with_sidecar(df, parquet_path, sidecar)

    # Final file should not exist
    assert not os.path.exists(parquet_path)


def test_no_sidecar_created_when_parquet_write_fails(tmp_path):
    """Sidecar is not created when parquet write fails."""
    df = make_test_df()
    parquet_path = str(tmp_path / "test_no_sidecar.parquet")
    sidecar_path = str(tmp_path / "test_no_sidecar.json")
    sidecar = {"data_type": "game_logs"}

    with patch.object(df, "to_parquet", side_effect=IOError("disk full")):
        with pytest.raises(FileWriteError):
            write_with_sidecar(df, parquet_path, sidecar)

    assert not os.path.exists(sidecar_path)


def test_both_tmp_files_deleted_after_failure(tmp_path):
    """Both .tmp files are deleted after any write failure."""
    df = make_test_df()
    parquet_path = str(tmp_path / "test_cleanup.parquet")
    sidecar = {"data_type": "game_logs"}

    parquet_tmp = parquet_path + ".tmp"
    sidecar_path = str(tmp_path / "test_cleanup.json")
    sidecar_tmp = sidecar_path + ".tmp"

    # Simulate failure after parquet tmp is written but before rename
    original_replace = os.replace

    replace_call_count = [0]

    def failing_replace(src, dst):
        replace_call_count[0] += 1
        if replace_call_count[0] == 1:
            raise OSError("rename failed")
        return original_replace(src, dst)

    with patch("os.replace", side_effect=failing_replace):
        with pytest.raises(FileWriteError):
            write_with_sidecar(df, parquet_path, sidecar)

    # Neither .tmp file should remain
    assert not os.path.exists(parquet_tmp)
    assert not os.path.exists(sidecar_tmp)


def test_idempotent_on_rerun_overwrites(tmp_path):
    """write_with_sidecar is idempotent — running twice overwrites with new content."""
    df1 = pd.DataFrame({"game_id": [1], "pts": [10]})
    df2 = pd.DataFrame({"game_id": [1, 2], "pts": [20, 30]})
    parquet_path = str(tmp_path / "idempotent.parquet")
    sidecar1 = {"rows": 1}
    sidecar2 = {"rows": 2}

    write_with_sidecar(df1, parquet_path, sidecar1)
    write_with_sidecar(df2, parquet_path, sidecar2)

    # Final parquet should reflect df2
    result_df = pd.read_parquet(parquet_path)
    assert len(result_df) == 2

    # Final sidecar should reflect sidecar2
    sidecar_path = str(tmp_path / "idempotent.json")
    with open(sidecar_path) as f:
        result_sidecar = json.load(f)
    assert result_sidecar["rows"] == 2


def test_build_raw_sidecar_structure():
    """build_raw_sidecar returns correct keys and types."""
    df = make_test_df()
    sidecar = build_raw_sidecar("game_logs", "LeBron James", 2023, df)

    assert sidecar["data_type"] == "game_logs"
    assert sidecar["player_or_team"] == "LeBron James"
    assert sidecar["season"] == 2023
    assert sidecar["rows"] == 3
    assert isinstance(sidecar["columns"], list)
    assert "written_at" in sidecar


def test_build_clean_sidecar_structure():
    """build_clean_sidecar returns correct keys and values from metrics."""
    df = make_test_df()
    metrics = {
        "rows_before": 10,
        "rows_after": 3,
        "nulls_found": 0,
        "outliers_flagged": 1,
        "corrupted_removed": 0,
        "dedup_skipped": False,
        "dedup_reason": None,
    }
    sidecar = build_clean_sidecar("game_logs", "LeBron James", 2023, metrics, df)

    assert sidecar["rows_before"] == 10
    assert sidecar["rows_after"] == 3
    assert sidecar["dedup_skipped"] is False
    assert sidecar["dedup_reason"] is None
    assert "written_at" in sidecar
