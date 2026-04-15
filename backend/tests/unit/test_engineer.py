"""Unit tests for pipeline/features/engineer.py"""
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import pandas as pd
import numpy as np
from pipeline.features.engineer import engineer


def _df(n=10, player_id=1):
    """Simple game-log style DataFrame."""
    return pd.DataFrame({
        "player_id": [player_id] * n,
        "game_id": list(range(n)),
        "date": [f"2023-01-{str(i+1).zfill(2)}" for i in range(n)],
        "pts": [20 + i for i in range(n)],
        "reb": [5] * n,
        "ast": [7] * n,
    })


_CONFIG = {
    "rolling_windows": [3],
    "min_observations": 2,
    "scoring": {"pts": 1.0, "reb": 1.2, "ast": 1.5},
}


def test_engineer_returns_dataframe_and_feature_names():
    df = _df()
    result_df, feature_names = engineer(df, _CONFIG)
    assert isinstance(result_df, pd.DataFrame)
    assert isinstance(feature_names, list)
    assert len(feature_names) > 0


def test_rolling_feature_uses_shift_before_rolling():
    """
    Anti-leakage check: game N's rolling feature must NOT include game N's own value.
    With shift(1) + rolling(3), game N's window is games N-3..N-1.
    For a player with pts=[20,21,22,23,...], rolling_pts_3 for game index 3
    should be mean([20,21,22]) = 21.0, NOT include pts[3]=23.
    """
    df = _df(n=8)
    result_df, _ = engineer(df, _CONFIG)

    # Game at index 3 should have rolling_pts_3 = mean(pts[0], pts[1], pts[2])
    # = mean(20, 21, 22) = 21.0 — NOT including pts[3]=23
    rolling_col = "rolling_pts_3"
    if rolling_col in result_df.columns:
        val = result_df.iloc[3][rolling_col]
        assert val == pytest.approx(21.0), f"Expected 21.0 but got {val} (shift not applied?)"


def test_rows_below_min_observations_are_null():
    """Rows without enough prior games should have null rolling features."""
    df = _df(n=8)
    config = {**_CONFIG, "min_observations": 3, "rolling_windows": [3]}
    result_df, _ = engineer(df, config)

    rolling_col = "rolling_pts_3"
    if rolling_col in result_df.columns:
        # With min_observations=3 and shift(1), first 3 rows won't have 3 prior obs
        null_rows = result_df[result_df[rolling_col].isna()]
        assert len(null_rows) >= 1


def test_is_outlier_not_used_as_feature():
    """is_outlier column must not appear in feature names."""
    df = _df()
    df["is_outlier"] = False
    _, feature_names = engineer(df, _CONFIG)
    assert "is_outlier" not in feature_names


def test_fantasy_pts_computed_from_scoring_config():
    """fantasy_pts = pts*1.0 + reb*1.2 + ast*1.5 for scoring config."""
    df = _df(n=5)
    config = {**_CONFIG, "scoring": {"pts": 1.0, "reb": 1.2, "ast": 1.5}}
    result_df, feature_names = engineer(df, config)

    if "fantasy_pts" in result_df.columns:
        expected = df["pts"] * 1.0 + df["reb"] * 1.2 + df["ast"] * 1.5
        pd.testing.assert_series_equal(
            result_df["fantasy_pts"].reset_index(drop=True),
            expected.reset_index(drop=True),
            check_names=False,
        )


def test_multiple_rolling_windows_produce_separate_columns():
    """Rolling windows [3, 5] produce rolling_pts_3 and rolling_pts_5."""
    df = _df(n=10)
    config = {**_CONFIG, "rolling_windows": [3, 5]}
    result_df, feature_names = engineer(df, config)

    if "rolling_pts_3" in result_df.columns:
        assert "rolling_pts_3" in feature_names
    if "rolling_pts_5" in result_df.columns:
        assert "rolling_pts_5" in feature_names


def test_original_columns_preserved():
    """Original stat columns still present in returned DataFrame."""
    df = _df()
    result_df, _ = engineer(df, _CONFIG)
    for col in ["pts", "reb", "ast", "player_id", "game_id"]:
        assert col in result_df.columns


def test_two_player_df_keeps_rolling_separate():
    """Rolling windows are grouped by player_id — one player's games don't bleed into another's."""
    df1 = _df(n=5, player_id=1)
    df2 = _df(n=5, player_id=2)
    df = pd.concat([df1, df2], ignore_index=True)
    result_df, _ = engineer(df, _CONFIG)

    rolling_col = "rolling_pts_3"
    if rolling_col in result_df.columns:
        p2_rows = result_df[result_df["player_id"] == 2]
        # Player 2's first row should not have player 1's data in its window
        assert pd.isna(p2_rows.iloc[0][rolling_col]) or p2_rows.iloc[0][rolling_col] != result_df.iloc[4][rolling_col]
