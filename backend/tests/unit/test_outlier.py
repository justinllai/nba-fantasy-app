"""Unit tests for pipeline/outlier.py"""
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import pandas as pd
from pipeline.outlier import flag_outliers


def _normal_df():
    """8 rows with consistent pts values — no outliers."""
    return pd.DataFrame({
        "player_id": range(8),
        "game_id": range(8),
        "pts": [20, 22, 21, 19, 23, 20, 21, 22],
    })


def test_is_outlier_column_added():
    df = _normal_df()
    result, _ = flag_outliers(df)
    assert "is_outlier" in result.columns


def test_no_outliers_in_normal_data():
    df = _normal_df()
    result, count = flag_outliers(df)
    assert count == 0
    assert result["is_outlier"].sum() == 0


def test_extreme_value_flagged_as_outlier():
    df = pd.DataFrame({
        "player_id": range(8),
        "game_id": range(8),
        "pts": [20, 22, 21, 19, 23, 20, 21, 100],  # 100 pts is extreme
    })
    result, count = flag_outliers(df)
    assert count >= 1
    assert result.iloc[-1]["is_outlier"] == True


def test_rows_never_removed():
    df = _normal_df()
    result, _ = flag_outliers(df)
    assert len(result) == len(df)


def test_non_stat_columns_excluded_from_fence():
    """game_id, season, player_id, team_id must not be used for outlier detection."""
    df = pd.DataFrame({
        "player_id": [1, 2, 3, 4, 5, 6, 7, 999999],  # 999999 would be outlier if included
        "game_id": [100, 101, 102, 103, 104, 105, 106, 999999],
        "pts": [20, 22, 21, 19, 23, 20, 21, 22],
    })
    result, count = flag_outliers(df)
    assert count == 0  # no stat outliers; identity cols excluded


def test_multiple_stat_cols_any_outlier_flags_row():
    df = pd.DataFrame({
        "player_id": range(8),
        "game_id": range(8),
        "pts": [20, 22, 21, 19, 23, 20, 21, 22],
        "reb": [5, 6, 5, 4, 6, 5, 5, 50],  # 50 reb is extreme
    })
    result, count = flag_outliers(df)
    assert count >= 1
    assert result.iloc[-1]["is_outlier"] == True


def test_df_with_no_stat_cols_returns_all_false():
    df = pd.DataFrame({
        "player_id": [1, 2],
        "game_id": [100, 101],
    })
    result, count = flag_outliers(df)
    assert count == 0
    assert "is_outlier" in result.columns
    assert result["is_outlier"].sum() == 0


def test_small_series_skipped_for_fence():
    """Columns with fewer than 4 non-null values skip the fence — no crash."""
    df = pd.DataFrame({
        "player_id": [1, 2],
        "game_id": [100, 101],
        "pts": [20, 100],  # only 2 rows — fence skipped
    })
    result, count = flag_outliers(df)
    assert count == 0  # skipped, not crashed


def test_original_df_not_mutated():
    df = _normal_df()
    original_cols = list(df.columns)
    flag_outliers(df)
    assert list(df.columns) == original_cols


def test_season_excluded_from_fence():
    df = pd.DataFrame({
        "player_id": range(8),
        "game_id": range(8),
        "season": [2023] * 7 + [1900],  # extreme season value — must not flag
        "pts": [20, 22, 21, 19, 23, 20, 21, 22],
    })
    result, count = flag_outliers(df)
    assert count == 0
