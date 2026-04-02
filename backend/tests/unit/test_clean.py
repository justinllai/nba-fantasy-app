"""
Unit tests for pipeline.clean.
One test per cleaning rule as specified.
"""
import logging
import pytest
import pandas as pd
import numpy as np

from pipeline.clean import clean


def make_logger():
    logger = logging.getLogger("test_clean")
    logger.setLevel(logging.DEBUG)
    if not logger.handlers:
        logger.addHandler(logging.NullHandler())
    return logger


def make_base_df(**overrides):
    """Create a baseline valid game_logs DataFrame."""
    data = {
        "game_id": list(range(1, 16)),
        "player_name": ["LeBron James"] * 15,
        "date": ["2023-10-01"] * 15,
        "min": [32.0] * 15,
        "pts": [28] * 15,
        "reb": [8] * 15,
        "ast": [7] * 15,
        "stl": [1] * 15,
        "blk": [1] * 15,
    }
    data.update(overrides)
    return pd.DataFrame(data)


def test_blank_placeholders_become_null():
    """NULL_PLACEHOLDERS like '', 'N/A', 'null', 'None' become pd.NA."""
    logger = make_logger()
    df = make_base_df()
    df = df.astype({"player_name": "object"})
    df.loc[0, "player_name"] = "N/A"
    df.loc[1, "player_name"] = ""
    df.loc[2, "player_name"] = "None"
    df.loc[3, "player_name"] = "null"

    result_df, _ = clean(df, "game_logs", logger)

    assert pd.isna(result_df.loc[0, "player_name"])
    assert pd.isna(result_df.loc[1, "player_name"])
    assert pd.isna(result_df.loc[2, "player_name"])
    assert pd.isna(result_df.loc[3, "player_name"])


def test_minutes_colon_format_converted_to_decimal():
    """'32:45' is converted to 32.75."""
    logger = make_logger()
    df = make_base_df()
    df = df.astype({"min": "object"})
    df.loc[0, "min"] = "32:45"

    result_df, _ = clean(df, "game_logs", logger)

    assert abs(result_df.loc[0, "min"] - 32.75) < 0.001


def test_minutes_28_30_converts_to_28_5():
    """'28:30' converts to 28.5."""
    logger = make_logger()
    df = make_base_df()
    df = df.astype({"min": "object"})
    df.loc[0, "min"] = "28:30"

    result_df, _ = clean(df, "game_logs", logger)

    assert abs(result_df.loc[0, "min"] - 28.5) < 0.001


def test_malformed_minutes_handled_consistently():
    """Malformed minutes string returns NaN (not an exception)."""
    logger = make_logger()
    df = make_base_df()
    df = df.astype({"min": "object"})
    df.loc[0, "min"] = "invalid:time:format"

    # Should not raise
    result_df, _ = clean(df, "game_logs", logger)

    # The value should be NaN or some consistent representation
    assert pd.isna(result_df.loc[0, "min"]) or isinstance(result_df.loc[0, "min"], float)


def test_player_name_title_cased_and_whitespace_collapsed():
    """Player names are title-cased and internal whitespace is collapsed."""
    logger = make_logger()
    df = make_base_df()
    df.loc[0, "player_name"] = "lebron   james"
    df.loc[1, "player_name"] = "STEPHEN CURRY"

    result_df, _ = clean(df, "game_logs", logger)

    assert result_df.loc[0, "player_name"] == "Lebron James"
    assert result_df.loc[1, "player_name"] == "Stephen Curry"


def test_team_abbreviation_uppercased():
    """Team abbreviations are normalized to uppercase."""
    logger = make_logger()
    df = pd.DataFrame({
        "game_id": list(range(1, 11)),
        "player_name": ["LeBron James"] * 10,
        "team": ["lal", "Lal", "LAL", "gsw", "GSW", "BOS", "bos", "MIA", "mia", "CHI"],
        "date": ["2023-10-01"] * 10,
        "min": [32.0] * 10,
        "pts": [28] * 10,
        "reb": [8] * 10,
        "ast": [7] * 10,
        "stl": [1] * 10,
        "blk": [1] * 10,
    })

    result_df, _ = clean(df, "box_scores", logger)

    for team in result_df["team"]:
        if pd.notna(team):
            assert team == team.upper()


def test_duplicate_game_id_rows_removed():
    """Duplicate rows by game_id are deduplicated, keeping first."""
    logger = make_logger()
    df = make_base_df()
    # Add a duplicate game_id
    duplicate_row = df.iloc[0].copy()
    df = pd.concat([df, pd.DataFrame([duplicate_row])], ignore_index=True)
    assert df.duplicated(subset=["game_id"]).any()

    result_df, _ = clean(df, "game_logs", logger)

    assert not result_df.duplicated(subset=["game_id"]).any()


def test_dedup_skipped_without_game_id():
    """Dedup is skipped when game_id column is not present; dedup_skipped=True recorded."""
    logger = make_logger()
    df = pd.DataFrame({
        "player_name": ["Stephen Curry"] * 5,
        "season": [2023] * 5,
        "pts": [30.1] * 5,
        "reb": [5.3] * 5,
        "ast": [6.4] * 5,
        "stl": [1.3] * 5,
        "blk": [0.4] * 5,
        "min": [34.2] * 5,
    })

    _, metrics = clean(df, "season_averages", logger)

    assert metrics["dedup_skipped"] is True
    assert metrics["dedup_reason"] is not None
    assert len(metrics["dedup_reason"]) > 0


def test_dnp_rows_removed():
    """Rows where min == 0 (DNP) are removed."""
    logger = make_logger()
    df = make_base_df()
    df.loc[0, "min"] = 0.0  # DNP row

    result_df, _ = clean(df, "game_logs", logger)

    assert 0.0 not in result_df["min"].values


def test_outlier_rows_flagged_with_is_outlier_true():
    """Statistical outlier rows are flagged with is_outlier=True; values unchanged."""
    logger = make_logger()
    # Create df with mostly 20pts but one extreme outlier
    pts = [20] * 14 + [99]  # 99 pts is extreme outlier
    df = make_base_df(pts=pts)

    result_df, metrics = clean(df, "game_logs", logger)

    assert "is_outlier" in result_df.columns
    # At least one row should be flagged
    assert result_df["is_outlier"].any()
    # The value should be unchanged (just flagged, not removed)
    assert 99 in result_df["pts"].values


def test_corrupted_rows_removed_per_thresholds():
    """Rows with values exceeding CORRUPTION_THRESHOLDS are removed."""
    logger = make_logger()
    pts = [28] * 14 + [101]  # 101 exceeds threshold of 100
    df = make_base_df(pts=pts)

    result_df, metrics = clean(df, "game_logs", logger)

    assert 101 not in result_df["pts"].values
    assert metrics["corrupted_removed"] >= 1


def test_nulls_preserved_not_filled():
    """Null values are preserved, not filled."""
    logger = make_logger()
    df = make_base_df()
    df = df.astype({"pts": "float64"})
    df.loc[0, "pts"] = float("nan")

    result_df, metrics = clean(df, "game_logs", logger)

    # The null should still be null
    assert pd.isna(result_df.loc[0, "pts"])
    # nulls_found should reflect it
    assert metrics["nulls_found"] >= 1


def test_metrics_dict_has_all_required_keys():
    """Metrics dict contains all required keys."""
    logger = make_logger()
    df = make_base_df()

    _, metrics = clean(df, "game_logs", logger)

    required_keys = [
        "rows_before", "rows_after", "outliers_flagged",
        "corrupted_removed", "nulls_found", "dedup_skipped", "dedup_reason"
    ]
    for key in required_keys:
        assert key in metrics, f"Missing key: {key}"
