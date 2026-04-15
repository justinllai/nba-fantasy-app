"""Unit tests for pipeline/fetcher.py — all API calls mocked at the api module boundary."""
import sys
import os
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import pandas as pd
import pytest
from pipeline.fetcher import (
    fetch_game_logs,
    fetch_box_scores,
    fetch_game_scores,
    fetch_season_averages,
    _flatten_stat_record,
    _flatten_game_score_record,
    _flatten_season_average_record,
    FETCH_FUNCTIONS,
)
from pipeline.exceptions import IngestionError, EntityLookupError


# ---------------------------------------------------------------------------
# Shared sample API payloads
# ---------------------------------------------------------------------------

SAMPLE_STAT_RECORD = {
    "game": {
        "id": 100, "date": "2023-01-15", "season": 2023,
        "home_team_id": 1, "visitor_team_id": 2,
        "home_team_score": 110, "visitor_team_score": 105,
        "status": "Final", "postseason": False,
    },
    "player": {"id": 42, "first_name": "LeBron", "last_name": "James", "position": "F"},
    "team": {"id": 1, "abbreviation": "LAL"},
    "min": "32:30", "pts": 28, "reb": 8, "ast": 7, "stl": 1, "blk": 1,
    "fgm": 11, "fga": 20, "fg_pct": 0.55,
    "fg3m": 2, "fg3a": 5, "fg3_pct": 0.40,
    "ftm": 4, "fta": 5, "ft_pct": 0.80,
    "oreb": 1, "dreb": 7, "turnover": 3, "pf": 2,
}

SAMPLE_GAME_SCORE = {
    "id": 100, "date": "2023-01-15", "season": 2023,
    "status": "Final", "period": 4, "time": "0:00", "postseason": False,
    "home_team": {"id": 1, "abbreviation": "LAL", "full_name": "Los Angeles Lakers",
                  "city": "Los Angeles", "conference": "West", "division": "Pacific"},
    "visitor_team": {"id": 2, "abbreviation": "GSW", "full_name": "Golden State Warriors",
                     "city": "Golden State", "conference": "West", "division": "Pacific"},
    "home_team_score": 110, "visitor_team_score": 105,
}

SAMPLE_SEASON_AVG = {
    "player": {"id": 42, "first_name": "LeBron", "last_name": "James"},
    "season": 2023, "games_played": 55,
    "pts": 28.5, "reb": 8.0, "ast": 6.5,
    "min": "35:00", "fg_pct": 0.54, "fg3_pct": 0.35, "ft_pct": 0.73,
}


# ---------------------------------------------------------------------------
# Flatten helpers
# ---------------------------------------------------------------------------

def test_flatten_stat_record_extracts_player_fields():
    flat = _flatten_stat_record(SAMPLE_STAT_RECORD)
    assert flat["player_id"] == 42
    assert flat["player_name"] == "LeBron James"
    assert flat["game_id"] == 100
    assert flat["team"] == "LAL"
    assert flat["pts"] == 28


def test_flatten_stat_record_captures_position_from_player():
    flat = _flatten_stat_record(SAMPLE_STAT_RECORD)
    assert flat["position"] == "F"


def test_flatten_stat_record_position_injected_when_absent():
    record = {**SAMPLE_STAT_RECORD, "player": {"id": 42, "first_name": "A", "last_name": "B"}}
    flat = _flatten_stat_record(record, position="G")
    assert flat["position"] == "G"


def test_flatten_stat_record_captures_shooting_splits():
    flat = _flatten_stat_record(SAMPLE_STAT_RECORD)
    for col in ["fgm", "fga", "fg3m", "fg3a", "ftm", "fta", "oreb", "dreb", "pf"]:
        assert col in flat


def test_flatten_game_score_record_extracts_teams():
    flat = _flatten_game_score_record(SAMPLE_GAME_SCORE)
    assert flat["home_team"] == "LAL"
    assert flat["visitor_team"] == "GSW"
    assert flat["home_team_score"] == 110
    assert flat["game_id"] == 100


def test_flatten_game_score_captures_team_metadata():
    flat = _flatten_game_score_record(SAMPLE_GAME_SCORE)
    assert flat["home_team_conference"] == "West"
    assert flat["home_team_division"] == "Pacific"


def test_flatten_season_average_extracts_player():
    flat = _flatten_season_average_record(SAMPLE_SEASON_AVG)
    assert flat["player_id"] == 42
    assert flat["player_name"] == "LeBron James"
    assert flat["pts"] == 28.5


def test_flatten_stat_record_empty_dict_returns_all_keys():
    flat = _flatten_stat_record({})
    for col in ["game_id", "player_id", "player_name", "team", "pts", "reb", "ast"]:
        assert col in flat


# ---------------------------------------------------------------------------
# fetch_game_logs
# ---------------------------------------------------------------------------

@patch("pipeline.fetcher.get_game_logs", return_value=[SAMPLE_STAT_RECORD])
@patch("pipeline.fetcher.get_player_id", return_value=42)
@patch("pipeline.fetcher.load", return_value=[])
def test_fetch_game_logs_by_player_name(mock_load, mock_get_id, mock_get_logs):
    df = fetch_game_logs(player="LeBron James", season=2023)
    assert len(df) == 1
    assert df.iloc[0]["player_name"] == "LeBron James"


@patch("pipeline.fetcher.get_game_logs", return_value=[SAMPLE_STAT_RECORD])
@patch("pipeline.fetcher.get_player_id", return_value=42)
@patch("pipeline.fetcher.load", return_value=[])
def test_fetch_game_logs_by_player_id(mock_load, mock_get_id, mock_get_logs):
    df = fetch_game_logs(player=42, season=2023)
    assert len(df) == 1


@patch("pipeline.fetcher.load", return_value=[SAMPLE_STAT_RECORD])
def test_fetch_game_logs_by_team(mock_load):
    # team param resolves team_id then fetches via /stats
    # mock_load is called for both /teams lookup and /stats fetch
    mock_load.side_effect = [
        [{"id": 1, "abbreviation": "LAL"}],  # /teams call
        [],  # /players position call
        [SAMPLE_STAT_RECORD],  # /stats call
    ]
    df = fetch_game_logs(team="LAL", season=2023)
    assert "pts" in df.columns


def test_fetch_game_logs_no_player_or_team_raises():
    with pytest.raises(IngestionError):
        fetch_game_logs(season=2023)


@patch("pipeline.fetcher.get_game_logs", return_value=[])
@patch("pipeline.fetcher.get_player_id", return_value=42)
@patch("pipeline.fetcher.load", return_value=[])
def test_fetch_game_logs_empty_returns_df_with_columns(mock_load, mock_get_id, mock_get_logs):
    df = fetch_game_logs(player="LeBron James", season=2023)
    assert isinstance(df, pd.DataFrame)
    assert "pts" in df.columns


# ---------------------------------------------------------------------------
# fetch_game_scores
# ---------------------------------------------------------------------------

@patch("pipeline.fetcher.load", side_effect=[
    [{"id": 1, "abbreviation": "LAL"}],  # /teams
    [SAMPLE_GAME_SCORE],                  # /games
])
def test_fetch_game_scores_by_team(mock_load):
    df = fetch_game_scores(team="LAL", season=2023)
    assert len(df) == 1
    assert df.iloc[0]["home_team"] == "LAL"


def test_fetch_game_scores_no_team_raises():
    with pytest.raises(IngestionError):
        fetch_game_scores(season=2023)


# ---------------------------------------------------------------------------
# fetch_season_averages
# ---------------------------------------------------------------------------

@patch("pipeline.fetcher.load", return_value=[SAMPLE_SEASON_AVG])
@patch("pipeline.fetcher.get_player_id", return_value=42)
def test_fetch_season_averages_by_player(mock_get_id, mock_load):
    df = fetch_season_averages(player="LeBron James", season=2023)
    assert len(df) == 1
    assert df.iloc[0]["pts"] == 28.5


def test_fetch_season_averages_no_player_raises():
    with pytest.raises(IngestionError):
        fetch_season_averages(season=2023)


# ---------------------------------------------------------------------------
# FETCH_FUNCTIONS routing table
# ---------------------------------------------------------------------------

def test_fetch_functions_routing_table_complete():
    for key in ["game_logs", "box_scores", "game_scores", "season_averages"]:
        assert key in FETCH_FUNCTIONS
        assert callable(FETCH_FUNCTIONS[key])
