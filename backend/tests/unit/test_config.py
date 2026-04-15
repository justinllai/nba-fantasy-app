"""Unit tests for pipeline/config.py"""
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from pipeline.config import (
    SUPPORTED_DATA_TYPES,
    IDENTITY_COLUMNS,
    REQUIRED_STRUCTURAL,
    DEDUP_KEYS,
    DEDUP_FALLBACK_KEYS,
    TIME_SERIES_TYPES,
    IMPOSSIBLE_VALUE_THRESHOLDS,
    CONTEXT_FIELDS,
    NULL_PLACEHOLDERS,
)


def test_supported_data_types_has_all_four():
    assert set(SUPPORTED_DATA_TYPES) == {"game_logs", "box_scores", "game_scores", "season_averages"}


def test_identity_columns_has_all_eight():
    required = {"player_id", "player_name", "team_id", "team", "game_id", "date", "season", "opponent"}
    assert required == set(IDENTITY_COLUMNS)


def test_required_structural_covers_all_data_types():
    for dt in SUPPORTED_DATA_TYPES:
        assert dt in REQUIRED_STRUCTURAL, f"REQUIRED_STRUCTURAL missing entry for {dt}"
        assert isinstance(REQUIRED_STRUCTURAL[dt], list)
        assert len(REQUIRED_STRUCTURAL[dt]) > 0


def test_dedup_keys_covers_all_data_types():
    for dt in SUPPORTED_DATA_TYPES:
        assert dt in DEDUP_KEYS, f"DEDUP_KEYS missing entry for {dt}"


def test_dedup_keys_game_logs_uses_player_id_and_game_id():
    assert DEDUP_KEYS["game_logs"] == ("player_id", "game_id")


def test_dedup_keys_box_scores_uses_player_id_and_game_id():
    assert DEDUP_KEYS["box_scores"] == ("player_id", "game_id")


def test_dedup_keys_game_scores_uses_team_id_and_game_id():
    assert DEDUP_KEYS["game_scores"] == ("team_id", "game_id")


def test_dedup_keys_season_averages_is_none():
    assert DEDUP_KEYS["season_averages"] is None


def test_dedup_fallback_keys_present():
    assert DEDUP_FALLBACK_KEYS["game_logs"] == ("player_name", "game_id")
    assert DEDUP_FALLBACK_KEYS["game_scores"] == ("team", "game_id")


def test_time_series_types_excludes_season_averages():
    assert "season_averages" not in TIME_SERIES_TYPES
    assert "game_logs" in TIME_SERIES_TYPES
    assert "box_scores" in TIME_SERIES_TYPES
    assert "game_scores" in TIME_SERIES_TYPES


def test_impossible_value_thresholds_has_upper_and_lower():
    assert "upper" in IMPOSSIBLE_VALUE_THRESHOLDS
    assert "lower" in IMPOSSIBLE_VALUE_THRESHOLDS


def test_impossible_value_thresholds_upper_values():
    upper = IMPOSSIBLE_VALUE_THRESHOLDS["upper"]
    assert upper["pts"] == 100
    assert upper["min"] == 60
    assert upper["reb"] == 50
    assert upper["ast"] == 30
    assert upper["stl"] == 15
    assert upper["blk"] == 15
    assert upper["games_played"] == 82


def test_impossible_value_thresholds_lower_all_zero():
    lower = IMPOSSIBLE_VALUE_THRESHOLDS["lower"]
    for col, val in lower.items():
        assert val == 0, f"Lower bound for '{col}' should be 0, got {val}"


def test_null_placeholders_includes_common_values():
    assert "" in NULL_PLACEHOLDERS
    assert "N/A" in NULL_PLACEHOLDERS
    assert "null" in NULL_PLACEHOLDERS
    assert "None" in NULL_PLACEHOLDERS
