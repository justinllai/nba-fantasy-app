"""Unit tests for pipeline/features/labels.py"""
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import pandas as pd
import numpy as np
from pipeline.features.labels import generate_labels


def _df(n=5, player_id=1):
    return pd.DataFrame({
        "player_id": [player_id] * n,
        "game_id": list(range(n)),
        "date": [f"2023-01-{str(i+1).zfill(2)}" for i in range(n)],
        "pts": [20 + i for i in range(n)],
        "reb": [5] * n,
    })


_CONFIG = {"targets": ["pts"]}


def test_generate_labels_returns_df_and_meta():
    df = _df()
    result_df, meta = generate_labels(df, _CONFIG)
    assert isinstance(result_df, pd.DataFrame)
    assert isinstance(meta, dict)


def test_next_game_pts_is_null_for_last_game():
    """The last game per player has null next_game_pts label."""
    df = _df(n=5)
    result_df, _ = generate_labels(df, _CONFIG)

    player_df = result_df[result_df["player_id"] == 1].sort_values("date")
    last_row = player_df.iloc[-1]
    assert pd.isna(last_row["next_game_pts"])


def test_is_end_of_series_true_for_last_game():
    """is_end_of_series=True for the last game per player."""
    df = _df(n=5)
    result_df, _ = generate_labels(df, _CONFIG)

    player_df = result_df[result_df["player_id"] == 1].sort_values("date")
    assert player_df.iloc[-1]["is_end_of_series"] == True


def test_is_end_of_series_false_for_non_last_games():
    """is_end_of_series=False for all non-last games."""
    df = _df(n=5)
    result_df, _ = generate_labels(df, _CONFIG)

    player_df = result_df[result_df["player_id"] == 1].sort_values("date")
    non_last = player_df.iloc[:-1]
    assert (non_last["is_end_of_series"] == False).all()


def test_label_for_game_n_is_stat_from_game_n_plus_1():
    """next_game_pts for game N is pts from game N+1 (shift(-1))."""
    df = _df(n=5)
    result_df, _ = generate_labels(df, _CONFIG)

    player_df = result_df[result_df["player_id"] == 1].sort_values("date").reset_index(drop=True)

    # Game 0 (pts=20) should have next_game_pts = pts[1] = 21
    assert player_df.iloc[0]["next_game_pts"] == pytest.approx(21.0)
    # Game 1 (pts=21) should have next_game_pts = pts[2] = 22
    assert player_df.iloc[1]["next_game_pts"] == pytest.approx(22.0)


def test_meta_contains_end_of_series_count():
    """Meta dict contains 'end_of_series_count' key."""
    df = _df(n=5)
    _, meta = generate_labels(df, _CONFIG)
    assert "end_of_series_count" in meta


def test_end_of_series_count_equals_number_of_players():
    """end_of_series_count == number of distinct players (one end per player)."""
    df = pd.concat([_df(n=5, player_id=1), _df(n=5, player_id=2)], ignore_index=True)
    _, meta = generate_labels(df, _CONFIG)
    assert meta["end_of_series_count"] == 2


def test_meta_contains_labels_generated():
    """Meta dict contains 'labels_generated' list."""
    df = _df()
    _, meta = generate_labels(df, _CONFIG)
    assert "labels_generated" in meta
    assert "next_game_pts" in meta["labels_generated"]


def test_multiple_targets_generate_multiple_labels():
    """Two targets produce two label columns."""
    df = _df()
    config = {"targets": ["pts", "reb"]}
    result_df, meta = generate_labels(df, config)
    assert "next_game_pts" in result_df.columns
    assert "next_game_reb" in result_df.columns


def test_labels_computed_on_sorted_time_series():
    """Labels must be computed on data sorted by date ASC (time-series order)."""
    # Reverse-ordered input — labels should still be computed chronologically
    df = _df(n=5)
    df_reversed = df.iloc[::-1].reset_index(drop=True)
    result_df, _ = generate_labels(df_reversed, _CONFIG)

    player_df = result_df.sort_values("date").reset_index(drop=True)
    # Game 0 should still have next_game_pts = game 1's pts
    assert player_df.iloc[0]["next_game_pts"] == pytest.approx(21.0)
