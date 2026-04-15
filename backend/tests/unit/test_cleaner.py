"""Unit tests for pipeline/cleaner.py"""
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import pandas as pd
from pipeline.cleaner import clean


def _base_df(**extra):
    data = {
        "player_id": [1, 2],
        "player_name": ["lebron james", "anthony davis"],
        "game_id": [100, 101],
        "date": ["2023-01-15", "2023-01-20"],
        "season": [2023, 2023],
        "pts": [28, 20],
        "reb": [8, 10],
        "min": ["32:00", "35:30"],
    }
    data.update(extra)
    return pd.DataFrame(data)


# --- String normalization ---

def test_player_name_title_cased():
    df = _base_df()
    result, _ = clean(df, "game_logs")
    assert result["player_name"].iloc[0] == "Lebron James"


def test_player_name_collapses_whitespace():
    df = pd.DataFrame({
        "player_id": [1], "game_id": [100], "date": ["2023-01-01"],
        "player_name": ["LeBron  James"],
    })
    result, _ = clean(df, "game_logs")
    assert result["player_name"].iloc[0] == "Lebron  James".title().replace("  ", " ")


def test_team_abbreviation_uppercased():
    df = pd.DataFrame({
        "player_id": [1], "game_id": [100], "date": ["2023-01-01"],
        "team": ["lal"],
    })
    result, _ = clean(df, "game_logs")
    assert result["team"].iloc[0] == "LAL"


def test_date_normalized_to_yyyy_mm_dd():
    df = pd.DataFrame({
        "player_id": [1], "game_id": [100],
        "date": ["01/15/2023"],
    })
    result, _ = clean(df, "game_logs")
    assert result["date"].iloc[0] == "2023-01-15"


def test_non_printable_chars_removed():
    df = pd.DataFrame({
        "player_id": [1], "game_id": [100], "date": ["2023-01-01"],
        "player_name": ["LeBron\x00James"],
    })
    result, _ = clean(df, "game_logs")
    assert "\x00" not in result["player_name"].iloc[0]


def test_null_placeholder_normalized():
    df = pd.DataFrame({
        "player_id": [1], "game_id": [100], "date": ["2023-01-01"],
        "player_name": ["N/A"],
    })
    result, _ = clean(df, "game_logs")
    assert pd.isna(result["player_name"].iloc[0])


# --- Minutes conversion ---

def test_minutes_converted_from_mm_ss():
    df = pd.DataFrame({
        "player_id": [1], "game_id": [100], "date": ["2023-01-01"],
        "min": ["32:30"],
    })
    result, _ = clean(df, "game_logs")
    assert abs(result["min"].iloc[0] - 32.5) < 0.01


def test_minutes_already_numeric_unchanged():
    df = pd.DataFrame({
        "player_id": [1], "game_id": [100], "date": ["2023-01-01"],
        "min": [32.5],
    })
    result, _ = clean(df, "game_logs")
    assert result["min"].iloc[0] == 32.5


# --- DNP removal ---

def test_dnp_rows_removed_when_min_zero():
    df = pd.DataFrame({
        "player_id": [1, 2], "game_id": [100, 101], "date": ["2023-01-01", "2023-01-02"],
        "min": [0.0, 32.0], "pts": [0, 25],
    })
    result, metrics = clean(df, "game_logs")
    assert len(result) == 1
    assert metrics["dnp_removed"] == 1


# --- Impossible-value threshold removal ---

def test_corrupted_rows_removed_upper_threshold():
    thresholds = {"upper": {"pts": 100}, "lower": {}}
    df = pd.DataFrame({
        "player_id": [1, 2], "game_id": [100, 101], "date": ["2023-01-01", "2023-01-02"],
        "pts": [28, 150],  # 150 exceeds upper bound of 100
    })
    result, metrics = clean(df, "game_logs", thresholds=thresholds)
    assert len(result) == 1
    assert metrics["corrupted_removed"] == 1


def test_corrupted_rows_removed_lower_threshold():
    thresholds = {"upper": {}, "lower": {"pts": 0}}
    df = pd.DataFrame({
        "player_id": [1, 2], "game_id": [100, 101], "date": ["2023-01-01", "2023-01-02"],
        "pts": [28, -5],  # -5 is below lower bound
    })
    result, metrics = clean(df, "game_logs", thresholds=thresholds)
    assert len(result) == 1
    assert metrics["corrupted_removed"] == 1


def test_default_thresholds_used_when_none_provided():
    df = pd.DataFrame({
        "player_id": [1], "game_id": [100], "date": ["2023-01-01"],
        "pts": [28],
    })
    _, metrics = clean(df, "game_logs")
    assert "thresholds_applied" in metrics
    assert "upper" in metrics["thresholds_applied"]


# --- Column filtering ---

def test_columns_filter_retains_only_specified_plus_identity():
    df = _base_df()
    result, _ = clean(df, "game_logs", columns=["pts"])
    assert "pts" in result.columns
    assert "player_id" in result.columns  # identity always kept
    assert "reb" not in result.columns


def test_columns_none_retains_all():
    df = _base_df()
    result, _ = clean(df, "game_logs", columns=None)
    assert "reb" in result.columns
    assert "pts" in result.columns


def test_identity_columns_always_retained():
    df = _base_df()
    result, _ = clean(df, "game_logs", columns=["pts"])
    for col in ["player_id", "game_id", "date", "season"]:
        if col in df.columns:
            assert col in result.columns


# --- Sorting ---

def test_time_series_sorted_by_date_asc():
    df = pd.DataFrame({
        "player_id": [1, 1], "game_id": [101, 100],
        "date": ["2023-01-20", "2023-01-15"],
        "pts": [28, 30],
    })
    result, _ = clean(df, "game_logs")
    assert result["date"].iloc[0] == "2023-01-15"
    assert result["date"].iloc[1] == "2023-01-20"


def test_season_averages_not_sorted(capsys):
    df = pd.DataFrame({
        "player_id": [2, 1],
        "season": [2023, 2023],
        "pts": [20, 28],
    })
    result, _ = clean(df, "season_averages")
    # Order should not change (no sort applied for non-time-series)
    assert result["player_id"].iloc[0] == 2


# --- Metrics ---

def test_metrics_contains_required_keys():
    df = _base_df()
    _, metrics = clean(df, "game_logs")
    for key in ["rows_before", "rows_after", "corrupted_removed", "nulls_found", "dnp_removed"]:
        assert key in metrics


def test_rows_before_and_after_correct():
    df = _base_df()
    _, metrics = clean(df, "game_logs")
    assert metrics["rows_before"] == 2
    assert metrics["rows_after"] == 2  # no corrupted or DNP rows
