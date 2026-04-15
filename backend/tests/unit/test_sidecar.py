"""Unit tests for pipeline/sidecar.py"""
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import pandas as pd
from pipeline.sidecar import build_base_sidecar, build_raw_sidecar, build_feature_sidecar
from pipeline.config import IMPOSSIBLE_VALUE_THRESHOLDS


def _make_df():
    return pd.DataFrame({
        "game_id": [1, 2],
        "player_name": ["LeBron James", "LeBron James"],
        "pts": [28, 30],
        "is_outlier": [False, True],
    })


def test_build_raw_sidecar_has_required_keys():
    df = _make_df()
    sidecar = build_raw_sidecar("game_logs", "LeBron James", None, 2023, df)
    assert sidecar["data_type"] == "game_logs"
    assert sidecar["player"] == "LeBron James"
    assert sidecar["team"] is None
    assert sidecar["season"] == 2023
    assert sidecar["rows"] == 2
    assert "game_id" in sidecar["columns"]
    assert "written_at" in sidecar


def test_build_base_sidecar_has_all_required_keys():
    df = _make_df()
    cleaning_stats = {
        "rows_before": 10, "rows_after": 8,
        "nulls_found": 2, "outliers_flagged": 1, "corrupted_removed": 1,
    }
    dedup_meta = {"dedup_skipped": False, "dedup_reason": None, "dedup_conflicts": 0}
    drift_meta = {"first_run": True, "columns_added": [], "columns_missing": [], "identity_columns_missing": []}

    sidecar = build_base_sidecar(
        "game_logs", "LeBron James", None, 2023,
        cleaning_stats, dedup_meta, drift_meta, IMPOSSIBLE_VALUE_THRESHOLDS, df
    )

    assert sidecar["rows_before"] == 10
    assert sidecar["rows_after"] == 8
    assert sidecar["nulls_found"] == 2
    assert sidecar["outliers_flagged"] == 1
    assert sidecar["corrupted_removed"] == 1
    assert sidecar["dedup_skipped"] is False
    assert sidecar["dedup_conflicts"] == 0
    assert "thresholds_applied" in sidecar
    assert sidecar["thresholds_applied"] == IMPOSSIBLE_VALUE_THRESHOLDS
    assert "schema_drift" in sidecar
    assert sidecar["schema_drift"]["first_run"] is True
    assert "written_at" in sidecar


def test_build_base_sidecar_thresholds_applied_matches_input():
    df = _make_df()
    custom_thresholds = {"upper": {"pts": 50}, "lower": {"pts": 0}}
    sidecar = build_base_sidecar(
        "game_logs", None, "LAL", 2023,
        {}, {}, {}, custom_thresholds, df
    )
    assert sidecar["thresholds_applied"] == custom_thresholds


def test_build_feature_sidecar_has_version():
    sidecar = build_feature_sidecar(
        feature_names=["rolling_pts_5"],
        rolling_windows=[5],
        min_observations=5,
        scoring_config={"pts": 1.0},
        version="abc123def456",
        success=True,
    )
    assert sidecar["feature_schema_version"] == "abc123def456"
    assert sidecar["success"] is True
    assert sidecar["rolling_windows"] == [5]
    assert sidecar["min_observations"] == 5
    assert "generated_at" in sidecar
    assert sidecar["labels_generated"] == []


def test_build_feature_sidecar_with_labels():
    sidecar = build_feature_sidecar(
        feature_names=["rolling_pts_5"],
        rolling_windows=[5],
        min_observations=5,
        scoring_config=None,
        version="v1",
        success=True,
        end_of_series_rows=3,
        labels_generated=["next_game_pts", "pts_20_plus"],
    )
    assert sidecar["end_of_series_rows"] == 3
    assert "next_game_pts" in sidecar["labels_generated"]
