"""Unit tests for pipeline/validator.py"""
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import pandas as pd
from pipeline.validator import validate
from pipeline.exceptions import ValidationError


def _game_logs_df(**overrides):
    data = {
        "player_id": [1, 2],
        "player_name": ["LeBron James", "Anthony Davis"],
        "game_id": [100, 101],
        "date": ["2023-01-15", "2023-01-20"],
        "pts": [28, 20],
        "reb": [8, 10],
        "ast": [7, 2],
    }
    data.update(overrides)
    return pd.DataFrame(data)


# ---------------------------------------------------------------------------
# game_logs — structural field checks
# ---------------------------------------------------------------------------

def test_game_logs_valid_passes():
    df = _game_logs_df()
    result = validate(df, "game_logs")
    assert result["passed"] is True


def test_game_logs_missing_date_raises():
    df = _game_logs_df()
    df = df.drop(columns=["date"])
    with pytest.raises(ValidationError, match="date"):
        validate(df, "game_logs")


def test_game_logs_missing_game_id_raises():
    df = _game_logs_df()
    df = df.drop(columns=["game_id"])
    with pytest.raises(ValidationError, match="game_id"):
        validate(df, "game_logs")


def test_game_logs_missing_both_player_fields_raises():
    df = _game_logs_df()
    df = df.drop(columns=["player_id", "player_name"])
    with pytest.raises(ValidationError, match="player"):
        validate(df, "game_logs")


def test_game_logs_player_name_only_passes():
    df = _game_logs_df()
    df = df.drop(columns=["player_id"])
    result = validate(df, "game_logs")
    assert result["passed"] is True


def test_game_logs_player_id_only_passes():
    df = _game_logs_df()
    df = df.drop(columns=["player_name"])
    result = validate(df, "game_logs")
    assert result["passed"] is True


# ---------------------------------------------------------------------------
# game_scores — team identity check
# ---------------------------------------------------------------------------

def test_game_scores_with_team_id_passes():
    df = pd.DataFrame({
        "game_id": [100], "date": ["2023-01-15"],
        "team_id": [1], "home_team_score": [110],
    })
    result = validate(df, "game_scores")
    assert result["passed"] is True


def test_game_scores_with_team_abbreviation_passes():
    df = pd.DataFrame({
        "game_id": [100], "date": ["2023-01-15"],
        "team": ["LAL"], "home_team_score": [110],
    })
    result = validate(df, "game_scores")
    assert result["passed"] is True


def test_game_scores_missing_both_team_fields_raises():
    df = pd.DataFrame({
        "game_id": [100], "date": ["2023-01-15"],
        "home_team_score": [110],
    })
    with pytest.raises(ValidationError, match="team"):
        validate(df, "game_scores")


def test_game_scores_missing_game_id_raises():
    df = pd.DataFrame({"date": ["2023-01-15"], "team_id": [1]})
    with pytest.raises(ValidationError, match="game_id"):
        validate(df, "game_scores")


# ---------------------------------------------------------------------------
# season_averages
# ---------------------------------------------------------------------------

def test_season_averages_valid_passes():
    df = pd.DataFrame({
        "player_id": [1], "player_name": ["LeBron James"],
        "season": [2023], "pts": [28.0],
    })
    result = validate(df, "season_averages")
    assert result["passed"] is True


def test_season_averages_missing_season_raises():
    df = pd.DataFrame({
        "player_id": [1], "pts": [28.0],
    })
    with pytest.raises(ValidationError, match="season"):
        validate(df, "season_averages")


# ---------------------------------------------------------------------------
# Optional warnings — never halt
# ---------------------------------------------------------------------------

def test_missing_optional_stat_warns_but_passes():
    df = pd.DataFrame({
        "player_id": [1], "game_id": [100], "date": ["2023-01-15"],
    })
    result = validate(df, "game_logs")
    assert result["passed"] is True
    assert len(result["optional_warnings"]) > 0


def test_structural_failures_list_empty_on_pass():
    df = _game_logs_df()
    result = validate(df, "game_logs")
    assert result["structural_failures"] == []


# ---------------------------------------------------------------------------
# box_scores
# ---------------------------------------------------------------------------

def test_box_scores_valid_passes():
    df = pd.DataFrame({
        "player_id": [1], "game_id": [100],
        "date": ["2023-01-15"], "pts": [25],
    })
    result = validate(df, "box_scores")
    assert result["passed"] is True


def test_box_scores_missing_date_raises():
    df = pd.DataFrame({"player_id": [1], "game_id": [100], "pts": [25]})
    with pytest.raises(ValidationError):
        validate(df, "box_scores")
