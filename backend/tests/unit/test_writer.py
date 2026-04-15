"""Unit tests for pipeline/writer.py"""
import json
import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import pandas as pd
from pipeline.writer import write_parquet, write_sidecar, sidecar_path, feature_sidecar_path
from pipeline.exceptions import FileWriteError


def _make_df():
    return pd.DataFrame({"game_id": [1, 2], "pts": [28, 30]})


def test_write_parquet_creates_file(tmp_path):
    df = _make_df()
    out = str(tmp_path / "test.parquet")
    write_parquet(df, out)
    assert os.path.exists(out)
    assert not os.path.exists(out + ".tmp")


def test_write_parquet_no_tmp_file_on_success(tmp_path):
    df = _make_df()
    out = str(tmp_path / "test.parquet")
    write_parquet(df, out)
    assert not os.path.exists(out + ".tmp")


def test_write_parquet_is_readable(tmp_path):
    df = _make_df()
    out = str(tmp_path / "test.parquet")
    write_parquet(df, out)
    result = pd.read_parquet(out)
    assert list(result.columns) == ["game_id", "pts"]
    assert len(result) == 2


def test_write_parquet_raises_file_write_error_on_bad_path():
    df = _make_df()
    with pytest.raises(FileWriteError):
        write_parquet(df, "/nonexistent_root_dir/sub/test.parquet")


def test_write_sidecar_creates_file(tmp_path):
    out = str(tmp_path / "test.sidecar.json")
    write_sidecar({"rows": 5}, out)
    assert os.path.exists(out)
    assert not os.path.exists(out + ".tmp")


def test_write_sidecar_content_is_valid_json(tmp_path):
    out = str(tmp_path / "test.sidecar.json")
    data = {"data_type": "game_logs", "rows": 82}
    write_sidecar(data, out)
    with open(out) as f:
        loaded = json.load(f)
    assert loaded["data_type"] == "game_logs"
    assert loaded["rows"] == 82


def test_write_sidecar_raises_file_write_error_on_bad_path():
    with pytest.raises(FileWriteError):
        write_sidecar({"x": 1}, "/nonexistent_root_dir/sub/test.sidecar.json")


def test_sidecar_path_replaces_parquet_extension():
    assert sidecar_path("data/clean/game_logs_lebron_2023.parquet") == \
        "data/clean/game_logs_lebron_2023.sidecar.json"


def test_feature_sidecar_path_uses_features_extension():
    assert feature_sidecar_path("data/clean/game_logs_lebron_2023.parquet") == \
        "data/clean/game_logs_lebron_2023.features.sidecar.json"


def test_parquet_write_is_idempotent(tmp_path):
    df = _make_df()
    out = str(tmp_path / "test.parquet")
    write_parquet(df, out)
    write_parquet(df, out)  # second write should succeed and overwrite
    result = pd.read_parquet(out)
    assert len(result) == 2
