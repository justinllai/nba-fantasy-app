"""
Integration tests for run() with game_scores data type.
Updated for new 8-param run() signature.
"""
import json
import os
import pytest
import pandas as pd


def _mock_df(rows=5):
    return pd.DataFrame({
        "game_id": list(range(rows)),
        "team_id": [1] * rows,
        "team": ["LAL"] * rows,
        "home_team": ["LAL"] * rows,
        "visitor_team": ["BOS"] * rows,
        "home_team_score": [110] * rows,
        "visitor_team_score": [105] * rows,
        "date": [f"2023-01-{str(i+1).zfill(2)}" for i in range(rows)],
        "season": [2023] * rows,
        "status": ["Final"] * rows,
    })


@pytest.fixture
def run_env(tmp_path, monkeypatch):
    monkeypatch.setenv("BALL_IS_LIFE", "test_key_123")
    import pipeline.fetcher as fetcher_mod
    mock_df = _mock_df()
    orig = fetcher_mod.FETCH_FUNCTIONS["game_scores"]
    fetcher_mod.FETCH_FUNCTIONS["game_scores"] = lambda **kw: mock_df
    yield tmp_path
    fetcher_mod.FETCH_FUNCTIONS["game_scores"] = orig


def test_clean_parquet_and_sidecar_written(run_env):
    from pipeline.run import run
    result = run(["game_scores"], season=2023, team="LAL", output_dir=run_env)
    assert result["game_scores"]["status"] == "success"
    assert len(list((run_env / "clean").glob("*.parquet"))) >= 1
    assert len(list((run_env / "clean").glob("*.sidecar.json"))) >= 1


def test_result_has_status_and_metrics(run_env):
    from pipeline.run import run
    result = run(["game_scores"], season=2023, team="LAL", output_dir=run_env)
    r = result["game_scores"]
    for key in ["status", "rows_before", "rows_after", "file_path"]:
        assert key in r


def test_sidecar_has_correct_fields(run_env):
    from pipeline.run import run
    result = run(["game_scores"], season=2023, team="LAL", output_dir=run_env)
    sidecar_file = next((run_env / "clean").glob("*.sidecar.json"))
    with open(sidecar_file) as f:
        sc = json.load(f)
    assert sc["data_type"] == "game_scores"
    assert "schema_drift" in sc
    assert "thresholds_applied" in sc
