"""Unit tests for pipeline/schema_drift.py"""
import json
import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import pandas as pd
from pipeline.schema_drift import check_drift


def _df_with_cols(*cols):
    return pd.DataFrame({c: [1] for c in cols})


def test_first_run_writes_baseline(tmp_path):
    df = _df_with_cols("player_id", "game_id", "pts")
    result = check_drift(df, "game_logs", tmp_path)
    assert result["first_run"] is True
    assert (tmp_path / "game_logs.json").exists()


def test_first_run_returns_no_drift(tmp_path):
    df = _df_with_cols("player_id", "game_id", "pts")
    result = check_drift(df, "game_logs", tmp_path)
    assert result["columns_added"] == []
    assert result["columns_missing"] == []


def test_baseline_file_contains_sorted_columns(tmp_path):
    df = _df_with_cols("pts", "game_id", "player_id")
    check_drift(df, "game_logs", tmp_path)
    with open(tmp_path / "game_logs.json") as f:
        baseline = json.load(f)
    assert baseline["columns"] == sorted(["pts", "game_id", "player_id"])


def test_second_run_no_drift_when_columns_same(tmp_path):
    df = _df_with_cols("player_id", "game_id", "pts")
    check_drift(df, "game_logs", tmp_path)
    result = check_drift(df, "game_logs", tmp_path)
    assert result["first_run"] is False
    assert result["columns_added"] == []
    assert result["columns_missing"] == []


def test_second_run_detects_added_column(tmp_path):
    df1 = _df_with_cols("player_id", "game_id", "pts")
    df2 = _df_with_cols("player_id", "game_id", "pts", "reb")
    check_drift(df1, "game_logs", tmp_path)
    result = check_drift(df2, "game_logs", tmp_path)
    assert "reb" in result["columns_added"]


def test_second_run_detects_missing_column(tmp_path):
    df1 = _df_with_cols("player_id", "game_id", "pts", "reb")
    df2 = _df_with_cols("player_id", "game_id", "pts")
    check_drift(df1, "game_logs", tmp_path)
    result = check_drift(df2, "game_logs", tmp_path)
    assert "reb" in result["columns_missing"]


def test_identity_columns_missing_reported(tmp_path):
    df = _df_with_cols("pts", "reb")  # no player_id, game_id, etc.
    result = check_drift(df, "game_logs", tmp_path)
    assert len(result["identity_columns_missing"]) > 0


def test_identity_columns_present_not_reported(tmp_path):
    df = _df_with_cols("player_id", "player_name", "game_id", "date", "season", "pts")
    result = check_drift(df, "game_logs", tmp_path)
    # Only identity cols in config that are absent should show up
    # Some may not be in this df but at least the ones present should not appear
    for col in ["player_id", "game_id"]:
        assert col not in result["identity_columns_missing"]


def test_separate_baselines_per_data_type(tmp_path):
    df1 = _df_with_cols("team_id", "game_id", "score")
    df2 = _df_with_cols("player_id", "game_id", "pts")
    check_drift(df1, "game_scores", tmp_path)
    check_drift(df2, "game_logs", tmp_path)
    assert (tmp_path / "game_scores.json").exists()
    assert (tmp_path / "game_logs.json").exists()


def test_baseline_dir_created_if_missing(tmp_path):
    nested = tmp_path / "deep" / "nested"
    df = _df_with_cols("player_id", "game_id", "pts")
    check_drift(df, "game_logs", nested)
    assert nested.exists()
    assert (nested / "game_logs.json").exists()
