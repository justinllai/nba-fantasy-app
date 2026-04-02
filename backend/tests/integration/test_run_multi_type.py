"""
Integration tests for multi-type run() behavior.
T022, T025: Multiple data types, partial failures, validation failures.
"""
import os
import pytest
import pandas as pd
from unittest.mock import patch

from pipeline.exceptions import IngestionError, ValidationError


def make_mock_game_logs_df(rows=15):
    return pd.DataFrame({
        "game_id": list(range(rows)),
        "player_name": ["LeBron James"] * rows,
        "date": ["2023-10-01"] * rows,
        "min": [32.5] * rows,
        "pts": [28] * rows,
        "reb": [8] * rows,
        "ast": [7] * rows,
        "stl": [1] * rows,
        "blk": [1] * rows,
    })


def make_mock_box_scores_df(rows=5):
    return pd.DataFrame({
        "game_id": list(range(rows)),
        "player_name": ["LeBron James"] * rows,
        "team": ["LAL"] * rows,
        "date": ["2023-10-01"] * rows,
        "min": [32.5] * rows,
        "pts": [28] * rows,
        "reb": [8] * rows,
        "ast": [7] * rows,
        "stl": [1] * rows,
        "blk": [1] * rows,
    })


def make_mock_game_scores_df(rows=5):
    return pd.DataFrame({
        "game_id": list(range(rows)),
        "home_team": ["LAL"] * rows,
        "visitor_team": ["BOS"] * rows,
        "home_team_score": [110] * rows,
        "visitor_team_score": [105] * rows,
        "date": ["2023-10-01"] * rows,
        "status": ["Final"] * rows,
    })


@pytest.fixture
def setup_dirs(tmp_path, monkeypatch):
    monkeypatch.setenv("BALL_IS_LIFE", "test_key_123")

    import pipeline.constants as constants
    monkeypatch.setattr(constants, "RAW_DIR", str(tmp_path / "raw"))
    monkeypatch.setattr(constants, "CLEAN_DIR", str(tmp_path / "clean"))
    monkeypatch.setattr(constants, "LOGS_DIR", str(tmp_path / "logs"))

    os.makedirs(str(tmp_path / "raw"), exist_ok=True)
    os.makedirs(str(tmp_path / "clean"), exist_ok=True)
    os.makedirs(str(tmp_path / "logs"), exist_ok=True)

    return tmp_path


def test_multi_type_run_returns_all_requested_types(setup_dirs):
    """run() with multiple types returns dict with keys for all requested types."""
    tmp_path = setup_dirs

    import importlib

    from pipeline.run import run

    with patch("pipeline.ingest.ingest_game_logs", return_value=make_mock_game_logs_df()), \
         patch("pipeline.ingest.ingest_box_scores", return_value=make_mock_box_scores_df()), \
         patch("pipeline.ingest.ingest_game_scores", return_value=make_mock_game_scores_df()):
        result = run(["game_logs", "box_scores", "game_scores"], "LeBron James", 2023)

    assert "game_logs" in result
    assert "box_scores" in result
    assert "game_scores" in result


def test_one_type_failure_does_not_stop_others(setup_dirs):
    """One data type failure does not prevent other types from completing."""
    tmp_path = setup_dirs

    import importlib

    from pipeline.run import run

    def failing_ingest(*args, **kwargs):
        raise IngestionError("Simulated API failure")

    with patch("pipeline.ingest.ingest_game_logs", side_effect=failing_ingest), \
         patch("pipeline.ingest.ingest_box_scores", return_value=make_mock_box_scores_df()):
        result = run(["game_logs", "box_scores"], "LeBron James", 2023)

    # Both keys should be present
    assert "game_logs" in result
    assert "box_scores" in result

    # game_logs failed
    assert "error" in result["game_logs"]

    # box_scores succeeded
    assert "error" not in result["box_scores"]


def test_result_dict_contains_error_for_failed_types(setup_dirs):
    """Failed types have {"error": str} in result dict."""
    tmp_path = setup_dirs

    import importlib

    from pipeline.run import run

    with patch("pipeline.ingest.ingest_game_logs", side_effect=IngestionError("test failure")):
        result = run(["game_logs"], "LeBron James", 2023)

    assert "error" in result["game_logs"]
    assert "test failure" in result["game_logs"]["error"]


def test_all_types_succeed_in_multi_type_run(setup_dirs):
    """All types produce success dicts when all succeed."""
    tmp_path = setup_dirs

    import importlib

    from pipeline.run import run

    with patch("pipeline.ingest.ingest_game_logs", return_value=make_mock_game_logs_df()), \
         patch("pipeline.ingest.ingest_box_scores", return_value=make_mock_box_scores_df()):
        result = run(["game_logs", "box_scores"], "LeBron James", 2023)

    for dt in ["game_logs", "box_scores"]:
        assert "error" not in result[dt], f"{dt} had unexpected error: {result[dt]}"
        assert "rows_before" in result[dt]


def test_one_validation_failure_does_not_stop_other_types(setup_dirs):
    """Validation failure for one type does not prevent other types from running."""
    tmp_path = setup_dirs

    import importlib

    from pipeline.run import run

    # game_logs with too few rows (validation failure)
    small_df = make_mock_game_logs_df(rows=3)

    with patch("pipeline.ingest.ingest_game_logs", return_value=small_df), \
         patch("pipeline.ingest.ingest_box_scores", return_value=make_mock_box_scores_df()):
        result = run(["game_logs", "box_scores"], "LeBron James", 2023)

    # game_logs should fail validation
    assert "error" in result["game_logs"]

    # box_scores should still succeed
    assert "error" not in result["box_scores"]


def test_validation_failure_result_has_error_key(setup_dirs):
    """ValidationError produces {"error": str} in result, not raised at run() level."""
    tmp_path = setup_dirs

    import importlib

    from pipeline.run import run

    small_df = make_mock_game_logs_df(rows=3)  # Too few rows

    with patch("pipeline.ingest.ingest_game_logs", return_value=small_df):
        result = run(["game_logs"], "LeBron James", 2023)

    assert "game_logs" in result
    assert "error" in result["game_logs"]
    # Should not raise ValidationError — it should be caught and put in result dict
