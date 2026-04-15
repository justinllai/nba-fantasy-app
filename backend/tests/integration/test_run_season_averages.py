"""
Integration tests for run() with season_averages data type.
Updated for new 8-param run() signature (T058).
"""
import json
import os
import pytest
import pandas as pd


def _mock_df(rows=1):
    return pd.DataFrame({
        "player_id": [1] * rows,
        "player_name": ["Stephen Curry"] * rows,
        "season": [2023] * rows,
        "pts": [30.1] * rows,
        "reb": [5.3] * rows,
        "ast": [6.4] * rows,
        "stl": [1.3] * rows,
        "blk": [0.4] * rows,
        "min": [34.2] * rows,
        "games_played": [55] * rows,
        "fg_pct": [0.48] * rows,
        "fg3_pct": [0.43] * rows,
        "ft_pct": [0.92] * rows,
        "oreb": [0.3] * rows,
        "dreb": [5.0] * rows,
    })


@pytest.fixture
def run_env(tmp_path, monkeypatch):
    monkeypatch.setenv("BALL_IS_LIFE", "test_key_123")
    import pipeline.fetcher as fetcher_mod
    mock_df = _mock_df()
    orig = fetcher_mod.FETCH_FUNCTIONS["season_averages"]
    fetcher_mod.FETCH_FUNCTIONS["season_averages"] = lambda **kw: mock_df
    yield tmp_path
    fetcher_mod.FETCH_FUNCTIONS["season_averages"] = orig


def test_clean_parquet_and_sidecar_written(run_env):
    from pipeline.run import run
    result = run(["season_averages"], season=2023, player="Stephen Curry", output_dir=run_env)
    assert result["season_averages"]["status"] == "success"
    assert len(list((run_env / "clean").glob("*.parquet"))) >= 1
    assert len(list((run_env / "clean").glob("*.sidecar.json"))) >= 1


def test_dedup_skipped_true_in_sidecar(run_env):
    """dedup_skipped=true in sidecar for season_averages (no dedup keys)."""
    from pipeline.run import run
    run(["season_averages"], season=2023, player="Stephen Curry", output_dir=run_env)
    sidecar_file = next((run_env / "clean").glob("*.sidecar.json"))
    with open(sidecar_file) as f:
        sc = json.load(f)
    assert sc["dedup_skipped"] is True
    assert sc["dedup_reason"] is not None


def test_result_has_all_metric_keys(run_env):
    from pipeline.run import run
    result = run(["season_averages"], season=2023, player="Stephen Curry", output_dir=run_env)
    r = result["season_averages"]
    for key in ["status", "rows_before", "rows_after", "file_path"]:
        assert key in r


def test_single_row_succeeds_no_min_row_error(run_env):
    """1 row is valid for season_averages (no 10-row minimum)."""
    from pipeline.run import run
    result = run(["season_averages"], season=2023, player="Stephen Curry", output_dir=run_env)
    assert result["season_averages"]["status"] == "success"


def test_all_api_fields_captured(run_env):
    """All fetched fields present in clean parquet (not just hardcoded subset)."""
    from pipeline.run import run
    result = run(["season_averages"], season=2023, player="Stephen Curry", output_dir=run_env)
    df = pd.read_parquet(result["season_averages"]["file_path"])
    for col in ["fg_pct", "fg3_pct", "oreb", "dreb"]:
        assert col in df.columns
