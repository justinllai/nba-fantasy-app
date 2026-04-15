"""Unit tests for pipeline/deduplicator.py"""
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import pandas as pd
from pipeline.deduplicator import deduplicate


def _game_logs_df():
    return pd.DataFrame({
        "player_id": [1, 1, 2],
        "player_name": ["LeBron James", "LeBron James", "Anthony Davis"],
        "game_id": [100, 100, 200],
        "pts": [28, 30, 20],
    })


def test_game_logs_deduplicates_by_player_id_and_game_id():
    df = _game_logs_df()
    result, meta = deduplicate(df, "game_logs")
    # Row with player_id=1, game_id=100 appears twice — should keep last (pts=30)
    assert len(result) == 2
    assert result[result["player_id"] == 1]["pts"].iloc[0] == 30
    assert meta["dedup_skipped"] is False


def test_game_logs_falls_back_to_player_name_when_player_id_absent():
    df = pd.DataFrame({
        "player_name": ["LeBron James", "LeBron James"],
        "game_id": [100, 100],
        "pts": [28, 30],
    })
    result, meta = deduplicate(df, "game_logs")
    assert len(result) == 1
    assert result.iloc[0]["pts"] == 30
    assert meta["dedup_skipped"] is False


def test_game_logs_conflict_count_correct():
    df = _game_logs_df()
    result, meta = deduplicate(df, "game_logs")
    assert meta["dedup_conflicts"] == 1  # one duplicate key group with differing pts


def test_box_scores_uses_player_id_and_game_id():
    df = pd.DataFrame({
        "player_id": [1, 1],
        "game_id": [100, 100],
        "pts": [20, 25],
    })
    result, meta = deduplicate(df, "box_scores")
    assert len(result) == 1
    assert result.iloc[0]["pts"] == 25


def test_game_scores_uses_team_id_and_game_id():
    df = pd.DataFrame({
        "team_id": [1, 1],
        "game_id": [100, 100],
        "home_team_score": [100, 105],
    })
    result, meta = deduplicate(df, "game_scores")
    assert len(result) == 1
    assert result.iloc[0]["home_team_score"] == 105


def test_game_scores_falls_back_to_team_and_game_id():
    df = pd.DataFrame({
        "team": ["LAL", "LAL"],
        "game_id": [100, 100],
        "home_team_score": [100, 105],
    })
    result, meta = deduplicate(df, "game_scores")
    assert len(result) == 1


def test_season_averages_skips_dedup():
    df = pd.DataFrame({
        "player_name": ["LeBron James"],
        "season": [2023],
        "pts": [28.0],
    })
    result, meta = deduplicate(df, "season_averages")
    assert meta["dedup_skipped"] is True
    assert len(result) == 1


def test_dedup_returns_correct_metadata_when_skipped():
    df = pd.DataFrame({"player_name": ["A"], "season": [2023]})
    _, meta = deduplicate(df, "season_averages")
    assert "dedup_skipped" in meta
    assert "dedup_reason" in meta
    assert "dedup_conflicts" in meta
    assert meta["dedup_conflicts"] == 0


def test_no_duplicates_returns_same_length():
    df = pd.DataFrame({
        "player_id": [1, 2, 3],
        "game_id": [100, 101, 102],
        "pts": [20, 25, 30],
    })
    result, meta = deduplicate(df, "game_logs")
    assert len(result) == 3
    assert meta["dedup_conflicts"] == 0


def test_dedup_skipped_when_keys_missing():
    df = pd.DataFrame({"pts": [20, 25]})  # no game_id, no player_id, no player_name
    _, meta = deduplicate(df, "game_logs")
    assert meta["dedup_skipped"] is True
    assert meta["dedup_reason"] is not None
