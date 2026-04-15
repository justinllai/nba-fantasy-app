"""
Integration tests for multi-type run() behavior.
Updated for new 8-param run() signature (T059).
"""
import pytest
import pandas as pd
import pipeline.fetcher as fetcher_mod

from pipeline.exceptions import IngestionError


def _game_logs_df(rows=15):
    return pd.DataFrame({
        "player_id": list(range(rows)),
        "player_name": ["LeBron James"] * rows,
        "game_id": list(range(rows)),
        "date": [f"2023-01-{str(i+1).zfill(2)}" for i in range(rows)],
        "season": [2023] * rows,
        "pts": [28] * rows, "reb": [8] * rows, "ast": [7] * rows,
        "min": [32.5] * rows,
    })


def _season_avg_df():
    return pd.DataFrame({
        "player_id": [1], "player_name": ["LeBron James"],
        "season": [2023], "pts": [28.0], "reb": [8.0], "ast": [7.0],
        "min": [35.0],
    })


@pytest.fixture(autouse=True)
def reset_fetch_functions():
    """Restore FETCH_FUNCTIONS after each test."""
    original = dict(fetcher_mod.FETCH_FUNCTIONS)
    yield
    fetcher_mod.FETCH_FUNCTIONS.update(original)


@pytest.fixture
def api_key(monkeypatch):
    monkeypatch.setenv("BALL_IS_LIFE", "test_key_123")


def test_multi_type_run_returns_all_requested_types(api_key, tmp_path):
    """run() with multiple types returns dict keyed by all requested types."""
    fetcher_mod.FETCH_FUNCTIONS["game_logs"] = lambda **kw: _game_logs_df()
    fetcher_mod.FETCH_FUNCTIONS["season_averages"] = lambda **kw: _season_avg_df()

    from pipeline.run import run
    result = run(
        ["game_logs", "season_averages"],
        season=2023, player="LeBron James", output_dir=tmp_path,
    )
    assert "game_logs" in result
    assert "season_averages" in result


def test_each_type_has_status_key(api_key, tmp_path):
    """Each type result dict has 'status' key."""
    fetcher_mod.FETCH_FUNCTIONS["game_logs"] = lambda **kw: _game_logs_df()
    fetcher_mod.FETCH_FUNCTIONS["season_averages"] = lambda **kw: _season_avg_df()

    from pipeline.run import run
    result = run(
        ["game_logs", "season_averages"],
        season=2023, player="LeBron James", output_dir=tmp_path,
    )
    for dt in ["game_logs", "season_averages"]:
        assert "status" in result[dt]


def test_one_type_failure_does_not_stop_others(api_key, tmp_path):
    """Failure in one data type does not prevent others from completing."""
    fetcher_mod.FETCH_FUNCTIONS["game_logs"] = lambda **kw: (_ for _ in ()).throw(RuntimeError("fail"))
    fetcher_mod.FETCH_FUNCTIONS["season_averages"] = lambda **kw: _season_avg_df()

    from pipeline.run import run
    result = run(
        ["game_logs", "season_averages"],
        season=2023, player="LeBron James", output_dir=tmp_path,
    )
    assert result["game_logs"]["status"] == "failed"
    assert result["season_averages"]["status"] == "success"


def test_failed_type_has_error_message(api_key, tmp_path):
    """Failed type includes 'error' key with message."""
    fetcher_mod.FETCH_FUNCTIONS["game_logs"] = lambda **kw: (_ for _ in ()).throw(RuntimeError("API down"))

    from pipeline.run import run
    result = run(["game_logs"], season=2023, player="LeBron James", output_dir=tmp_path)
    assert result["game_logs"]["status"] == "failed"
    assert "error" in result["game_logs"]


def test_all_types_succeed_in_multi_type_run(api_key, tmp_path):
    """All types produce status='success' when all succeed."""
    fetcher_mod.FETCH_FUNCTIONS["game_logs"] = lambda **kw: _game_logs_df()
    fetcher_mod.FETCH_FUNCTIONS["season_averages"] = lambda **kw: _season_avg_df()

    from pipeline.run import run
    result = run(
        ["game_logs", "season_averages"],
        season=2023, player="LeBron James", output_dir=tmp_path,
    )
    for dt in ["game_logs", "season_averages"]:
        assert result[dt]["status"] == "success"
