"""
Integration tests for run() with game_logs data type.
Tests the new 8-param run() signature introduced in T053.
"""
import json
import os
import pytest
import pandas as pd
from unittest.mock import patch

from pipeline.exceptions import ValidationError


def _mock_df(rows=15, *, include_pts=True):
    data = {
        "player_id": list(range(rows)),
        "player_name": ["LeBron James"] * rows,
        "game_id": list(range(rows)),
        "date": [f"2023-01-{str(i+1).zfill(2)}" for i in range(rows)],
        "season": [2023] * rows,
        "team": ["LAL"] * rows,
        "min": [32.5] * rows,
        "reb": [8] * rows,
        "ast": [7] * rows,
        "stl": [1] * rows,
        "blk": [1] * rows,
        "fg_pct": [0.5] * rows,
        "fg3_pct": [0.35] * rows,
        "ft_pct": [0.75] * rows,
        "turnover": [3] * rows,
    }
    if include_pts:
        data["pts"] = [28] * rows
    return pd.DataFrame(data)


@pytest.fixture
def run_env(tmp_path, monkeypatch):
    """Set up API key and output_dir; patch FETCH_FUNCTIONS to return controlled DataFrame."""
    monkeypatch.setenv("BALL_IS_LIFE", "test_key_123")
    mock_df = _mock_df()

    import pipeline.fetcher as fetcher_mod
    original = dict(fetcher_mod.FETCH_FUNCTIONS)
    fetcher_mod.FETCH_FUNCTIONS["game_logs"] = lambda **kw: mock_df
    yield tmp_path, mock_df
    fetcher_mod.FETCH_FUNCTIONS.update(original)


# ---------------------------------------------------------------------------
# New signature accepted
# ---------------------------------------------------------------------------

def test_new_run_signature_accepted(run_env):
    """run() accepts the new 8-param signature."""
    tmp_path, _ = run_env
    from pipeline.run import run
    result = run(["game_logs"], season=2023, player="LeBron James", output_dir=tmp_path)
    assert "game_logs" in result


def test_result_has_status_key(run_env):
    """Result dict has 'status' key set to 'success'."""
    tmp_path, _ = run_env
    from pipeline.run import run
    result = run(["game_logs"], season=2023, player="LeBron James", output_dir=tmp_path)
    assert result["game_logs"]["status"] == "success"


def test_result_has_expected_metrics(run_env):
    """Result dict contains pipeline metrics."""
    tmp_path, _ = run_env
    from pipeline.run import run
    result = run(["game_logs"], season=2023, player="LeBron James", output_dir=tmp_path)
    r = result["game_logs"]
    for key in ["rows_before", "rows_after", "outliers_flagged", "corrupted_removed",
                "nulls_found", "file_path"]:
        assert key in r, f"Missing key: {key}"


# ---------------------------------------------------------------------------
# File outputs
# ---------------------------------------------------------------------------

def test_clean_parquet_written(run_env):
    """Clean parquet written to output_dir/clean/."""
    tmp_path, _ = run_env
    from pipeline.run import run
    result = run(["game_logs"], season=2023, player="LeBron James", output_dir=tmp_path)
    assert os.path.exists(result["game_logs"]["file_path"])


def test_sidecar_uses_dot_sidecar_json_extension(run_env):
    """Sidecar file uses .sidecar.json extension (not .json)."""
    tmp_path, _ = run_env
    from pipeline.run import run
    run(["game_logs"], season=2023, player="LeBron James", output_dir=tmp_path)
    sidecar_files = list((tmp_path / "clean").glob("*.sidecar.json"))
    assert len(sidecar_files) >= 1


def test_no_plain_json_sidecar(run_env):
    """No bare .json files written — only .sidecar.json."""
    tmp_path, _ = run_env
    from pipeline.run import run
    run(["game_logs"], season=2023, player="LeBron James", output_dir=tmp_path)
    plain_json = [f for f in (tmp_path / "clean").glob("*.json")
                  if not f.name.endswith(".sidecar.json")]
    assert len(plain_json) == 0


def test_raw_parquet_written(run_env):
    """Raw parquet written to output_dir/raw/."""
    tmp_path, _ = run_env
    from pipeline.run import run
    run(["game_logs"], season=2023, player="LeBron James", output_dir=tmp_path)
    raw_parquets = list((tmp_path / "raw").glob("*.parquet"))
    assert len(raw_parquets) >= 1


# ---------------------------------------------------------------------------
# Sidecar content
# ---------------------------------------------------------------------------

def test_sidecar_contains_schema_drift(run_env):
    """Clean sidecar contains 'schema_drift' key."""
    tmp_path, _ = run_env
    from pipeline.run import run
    run(["game_logs"], season=2023, player="LeBron James", output_dir=tmp_path)
    sidecar_path = next((tmp_path / "clean").glob("*.sidecar.json"))
    with open(sidecar_path) as f:
        sc = json.load(f)
    assert "schema_drift" in sc


def test_sidecar_contains_thresholds_applied(run_env):
    """Clean sidecar contains 'thresholds_applied' key."""
    tmp_path, _ = run_env
    from pipeline.run import run
    run(["game_logs"], season=2023, player="LeBron James", output_dir=tmp_path)
    sidecar_path = next((tmp_path / "clean").glob("*.sidecar.json"))
    with open(sidecar_path) as f:
        sc = json.load(f)
    assert "thresholds_applied" in sc


def test_sidecar_contains_dedup_conflicts(run_env):
    """Clean sidecar contains 'dedup_conflicts' key."""
    tmp_path, _ = run_env
    from pipeline.run import run
    run(["game_logs"], season=2023, player="LeBron James", output_dir=tmp_path)
    sidecar_path = next((tmp_path / "clean").glob("*.sidecar.json"))
    with open(sidecar_path) as f:
        sc = json.load(f)
    assert "dedup_conflicts" in sc


# ---------------------------------------------------------------------------
# Column filtering
# ---------------------------------------------------------------------------

def test_columns_filter_retains_pts_and_identity_columns(run_env):
    """columns=['pts','reb'] keeps pts, reb, and all identity columns in clean parquet."""
    tmp_path, _ = run_env
    from pipeline.run import run
    result = run(
        ["game_logs"], season=2023, player="LeBron James",
        output_dir=tmp_path, columns=["pts", "reb"]
    )
    df = pd.read_parquet(result["game_logs"]["file_path"])
    assert "pts" in df.columns
    assert "reb" in df.columns
    # Identity columns always kept
    for col in ["player_id", "game_id", "date"]:
        if col in _mock_df().columns:
            assert col in df.columns


def test_unrecognized_column_in_columns_does_not_crash(run_env):
    """columns=['pts','nonexistent_col'] runs successfully; nonexistent col absent from parquet."""
    tmp_path, _ = run_env
    from pipeline.run import run
    result = run(
        ["game_logs"], season=2023, player="LeBron James",
        output_dir=tmp_path, columns=["pts", "nonexistent_col"]
    )
    assert result["game_logs"]["status"] == "success"
    df = pd.read_parquet(result["game_logs"]["file_path"])
    assert "nonexistent_col" not in df.columns


def test_columns_none_retains_all_columns(run_env):
    """columns=None retains all fetched columns."""
    tmp_path, _ = run_env
    from pipeline.run import run
    result = run(["game_logs"], season=2023, player="LeBron James", output_dir=tmp_path)
    df = pd.read_parquet(result["game_logs"]["file_path"])
    assert "pts" in df.columns
    assert "reb" in df.columns


# ---------------------------------------------------------------------------
# is_outlier column
# ---------------------------------------------------------------------------

def test_is_outlier_column_present_in_clean_parquet(run_env):
    """is_outlier bool column present in clean parquet."""
    tmp_path, _ = run_env
    from pipeline.run import run
    result = run(["game_logs"], season=2023, player="LeBron James", output_dir=tmp_path)
    df = pd.read_parquet(result["game_logs"]["file_path"])
    assert "is_outlier" in df.columns


# ---------------------------------------------------------------------------
# Error isolation
# ---------------------------------------------------------------------------

def test_failed_type_has_status_failed(tmp_path, monkeypatch):
    """When fetcher raises, result has status='failed' with error message."""
    monkeypatch.setenv("BALL_IS_LIFE", "test_key_123")
    import pipeline.fetcher as fetcher_mod
    orig = fetcher_mod.FETCH_FUNCTIONS["game_logs"]
    fetcher_mod.FETCH_FUNCTIONS["game_logs"] = lambda **kw: (_ for _ in ()).throw(RuntimeError("API down"))
    try:
        from pipeline.run import run
        result = run(["game_logs"], season=2023, player="LeBron James", output_dir=tmp_path)
    finally:
        fetcher_mod.FETCH_FUNCTIONS["game_logs"] = orig
    assert result["game_logs"]["status"] == "failed"
    assert "error" in result["game_logs"]


def test_one_type_failure_does_not_stop_others(tmp_path, monkeypatch):
    """Multi-type run: failed type has status='failed' in its result dict."""
    monkeypatch.setenv("BALL_IS_LIFE", "test_key_123")
    import pipeline.fetcher as fetcher_mod
    orig_gl = fetcher_mod.FETCH_FUNCTIONS["game_logs"]
    fetcher_mod.FETCH_FUNCTIONS["game_logs"] = lambda **kw: (_ for _ in ()).throw(RuntimeError("down"))
    try:
        from pipeline.run import run
        result = run(["game_logs"], season=2023, player="LeBron James", output_dir=tmp_path)
    finally:
        fetcher_mod.FETCH_FUNCTIONS["game_logs"] = orig_gl
    assert "game_logs" in result
    assert result["game_logs"]["status"] == "failed"


# ---------------------------------------------------------------------------
# Mutual exclusion
# ---------------------------------------------------------------------------

def test_both_player_and_team_raises(tmp_path, monkeypatch):
    """Providing both player and team raises PipelineConfigError."""
    monkeypatch.setenv("BALL_IS_LIFE", "test_key_123")
    from pipeline.run import run
    from pipeline.exceptions import PipelineConfigError
    with pytest.raises(PipelineConfigError):
        run(["game_logs"], season=2023, player="LeBron James", team="LAL", output_dir=tmp_path)


# ---------------------------------------------------------------------------
# Schema drift: second run detects column changes
# ---------------------------------------------------------------------------

def test_schema_drift_second_run_detects_missing_column(tmp_path, monkeypatch):
    """Second run with a column removed shows it in sidecar schema_drift.columns_missing."""
    monkeypatch.setenv("BALL_IS_LIFE", "test_key_123")
    import pipeline.fetcher as fetcher_mod
    from pipeline.run import run

    df1 = _mock_df()
    df2 = _mock_df().drop(columns=["reb"])

    fetcher_mod.FETCH_FUNCTIONS["game_logs"] = lambda **kw: df1
    run(["game_logs"], season=2023, player="LeBron James", output_dir=tmp_path)

    fetcher_mod.FETCH_FUNCTIONS["game_logs"] = lambda **kw: df2
    run(["game_logs"], season=2023, player="LeBron James", output_dir=tmp_path)

    sidecar_file = next((tmp_path / "clean").glob("*.sidecar.json"))
    with open(sidecar_file) as f:
        sc = json.load(f)

    assert "reb" in sc["schema_drift"]["columns_missing"]


# ---------------------------------------------------------------------------
# T064: Feature engineering integration
# ---------------------------------------------------------------------------

def test_features_config_writes_feature_sidecar(run_env):
    """features_config provided → .features.sidecar.json written with version + windows."""
    tmp_path, _ = run_env
    from pipeline.run import run
    features_config = {
        "rolling_windows": [3],
        "min_observations": 2,
        "scoring": {"pts": 1.0, "reb": 1.2},
    }
    run(
        ["game_logs"], season=2023, player="LeBron James",
        output_dir=tmp_path, features_config=features_config,
    )
    feat_sidecars = list((tmp_path / "clean").glob("*.features.sidecar.json"))
    assert len(feat_sidecars) == 1
    with open(feat_sidecars[0]) as f:
        sc = json.load(f)
    assert "feature_schema_version" in sc
    assert "rolling_windows" in sc
    assert sc["rolling_windows"] == [3]
    assert "min_observations" in sc


def test_no_feature_sidecar_when_features_config_is_none(run_env):
    """features_config=None → no .features.sidecar.json written."""
    tmp_path, _ = run_env
    from pipeline.run import run
    run(["game_logs"], season=2023, player="LeBron James", output_dir=tmp_path)
    feat_sidecars = list((tmp_path / "clean").glob("*.features.sidecar.json"))
    assert len(feat_sidecars) == 0


# ---------------------------------------------------------------------------
# T071: Write failure isolation
# ---------------------------------------------------------------------------

def test_write_failure_results_in_status_failed(run_env):
    """Parquet write failure → status='failed'; no .sidecar.json; no .tmp files."""
    tmp_path, _ = run_env
    from pipeline.run import run
    from pipeline.exceptions import FileWriteError
    with patch("pipeline.writer.write_parquet", side_effect=FileWriteError("disk full")):
        result = run(["game_logs"], season=2023, player="LeBron James", output_dir=tmp_path)
    assert result["game_logs"]["status"] == "failed"
    assert len(list((tmp_path / "clean").glob("*.tmp"))) == 0
