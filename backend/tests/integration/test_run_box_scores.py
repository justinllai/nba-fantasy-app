"""
Integration tests for run() with box_scores data type.
Updated for new 8-param run() signature (T055).
"""
import os
import pytest
import pandas as pd


def _mock_df(rows=5):
    return pd.DataFrame({
        "player_id": list(range(rows)),
        "player_name": ["LeBron James"] * rows,
        "game_id": list(range(rows)),
        "team_id": [1] * rows,
        "team": ["LAL"] * rows,
        "date": [f"2023-01-{str(i+1).zfill(2)}" for i in range(rows)],
        "season": [2023] * rows,
        "min": [32.5] * rows,
        "pts": [28] * rows,
        "reb": [8] * rows,
        "ast": [7] * rows,
        "stl": [1] * rows,
        "blk": [1] * rows,
    })


@pytest.fixture
def run_env(tmp_path, monkeypatch):
    monkeypatch.setenv("BALL_IS_LIFE", "test_key_123")
    import pipeline.fetcher as fetcher_mod
    mock_df = _mock_df()
    orig = fetcher_mod.FETCH_FUNCTIONS["box_scores"]
    fetcher_mod.FETCH_FUNCTIONS["box_scores"] = lambda **kw: mock_df
    yield tmp_path
    fetcher_mod.FETCH_FUNCTIONS["box_scores"] = orig


def test_team_season_path_produces_output(run_env):
    """Team+season path produces clean parquet + .sidecar.json."""
    tmp_path = run_env
    from pipeline.run import run
    result = run(["box_scores"], season=2023, team="LAL", output_dir=tmp_path)
    assert "box_scores" in result
    assert result["box_scores"]["status"] == "success"
    assert len(list((tmp_path / "clean").glob("*.parquet"))) >= 1
    assert len(list((tmp_path / "clean").glob("*.sidecar.json"))) >= 1


def test_result_has_status_key(run_env):
    """Result has 'status' key."""
    from pipeline.run import run
    result = run(["box_scores"], season=2023, team="LAL", output_dir=run_env)
    assert "status" in result["box_scores"]


def test_team_path_filename_contains_team(run_env):
    """Filename contains normalized team name."""
    from pipeline.run import run
    result = run(["box_scores"], season=2023, team="LAL", output_dir=run_env)
    assert "lal" in result["box_scores"]["file_path"].lower()


def test_player_path_produces_output(run_env):
    """Player path works for box_scores."""
    from pipeline.run import run
    result = run(["box_scores"], season=2023, player="LeBron James", output_dir=run_env)
    assert result["box_scores"]["status"] == "success"
