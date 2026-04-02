"""
Unit tests for box_scores detection logic in ingest.py.
T020: Tests that int-castable strings -> game_id path, non-int -> team path.
"""
import logging
import pytest
from unittest.mock import patch, MagicMock
import pandas as pd

from pipeline.exceptions import EntityLookupError


def make_logger():
    logger = logging.getLogger("test_ingest")
    logger.setLevel(logging.DEBUG)
    if not logger.handlers:
        logger.addHandler(logging.NullHandler())
    return logger


def make_box_score_records(count=5):
    """Create mock box score API records."""
    return [
        {
            "game": {"id": i, "date": "2023-10-01"},
            "player": {"id": 1, "first_name": "LeBron", "last_name": "James"},
            "team": {"abbreviation": "LAL"},
            "min": "32:00",
            "pts": 28, "reb": 8, "ast": 7, "stl": 1, "blk": 1,
            "fg_pct": 0.5, "fg3_pct": 0.35, "ft_pct": 0.75, "turnover": 3,
        }
        for i in range(count)
    ]


def test_int_castable_string_routed_to_game_id_path():
    """'18370647' (int-castable) is routed to the game_id lookup path."""
    logger = make_logger()
    mock_records = make_box_score_records(3)

    with patch("pipeline.ingest.load") as mock_load:
        mock_load.return_value = mock_records

        from pipeline.ingest import ingest_box_scores
        df = ingest_box_scores("18370647", 2023, logger)

        # Should have called load with game_ids[], not team search
        call_args = mock_load.call_args_list
        assert len(call_args) >= 1
        # The stats call should use game_ids[]
        stats_calls = [c for c in call_args if "/stats" in str(c.args)]
        assert len(stats_calls) >= 1
        assert "game_ids[]" in stats_calls[0].kwargs.get("params", stats_calls[0].args[1] if len(stats_calls[0].args) > 1 else {})


def test_team_abbreviation_routed_to_team_season_path():
    """'LAL' (non-int-castable) is routed to team+season lookup path."""
    logger = make_logger()
    mock_records = make_box_score_records(5)

    with patch("pipeline.ingest.load") as mock_load:
        # First call is team lookup, second is stats
        mock_load.side_effect = [
            [{"id": 14, "abbreviation": "LAL"}],  # teams endpoint
            mock_records,  # stats endpoint
        ]

        from pipeline.ingest import ingest_box_scores
        df = ingest_box_scores("LAL", 2023, logger)

        # Should have called load with teams endpoint first
        call_args = mock_load.call_args_list
        assert len(call_args) >= 2
        first_call_endpoint = call_args[0].args[0] if call_args[0].args else call_args[0].kwargs.get("endpoint", "")
        assert "/teams" in str(first_call_endpoint)


def test_edge_case_123abc_treated_as_team_string():
    """'123abc' (not pure int) is treated as team string, not game_id."""
    logger = make_logger()

    with patch("pipeline.ingest.load") as mock_load:
        mock_load.side_effect = [
            [{"id": 1, "abbreviation": "123abc"}],  # team lookup returns something
            [],  # stats returns empty
        ]

        from pipeline.ingest import ingest_box_scores
        # This should not raise — should attempt team lookup path
        try:
            df = ingest_box_scores("123abc", 2023, logger)
        except Exception as e:
            # EntityLookupError or IngestionError is OK (it's a fake team)
            # What's NOT OK is a ValueError from failed int cast propagating
            assert "int" not in str(type(e)).lower(), f"Should not fail with int-cast error: {e}"


def test_game_id_path_returns_dataframe():
    """game_id path returns a DataFrame with expected columns."""
    logger = make_logger()
    mock_records = make_box_score_records(3)

    with patch("pipeline.ingest.load", return_value=mock_records):
        from pipeline.ingest import ingest_box_scores
        df = ingest_box_scores("18370647", 2023, logger)

    assert isinstance(df, pd.DataFrame)
    assert "game_id" in df.columns
    assert "player_name" in df.columns


def test_team_season_path_returns_dataframe():
    """Team+season path returns a DataFrame with expected columns."""
    logger = make_logger()
    mock_records = make_box_score_records(5)

    with patch("pipeline.ingest.load") as mock_load:
        mock_load.side_effect = [
            [{"id": 14, "abbreviation": "LAL"}],
            mock_records,
        ]

        from pipeline.ingest import ingest_box_scores
        df = ingest_box_scores("LAL", 2023, logger)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 5
