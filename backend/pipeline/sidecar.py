"""
Sidecar builders for the NBA stats data pipeline.
Produces metadata dicts written alongside each parquet file.
"""
from datetime import datetime

import pandas as pd


def build_base_sidecar(
    data_type: str,
    player: str | None,
    team: str | None,
    season,
    cleaning_stats: dict,
    dedup_meta: dict,
    drift_meta: dict,
    thresholds: dict,
    df: pd.DataFrame,
) -> dict:
    """
    Build the base sidecar dict for a clean parquet file.

    Args:
        data_type: One of the supported data types
        player: Player name/id or None
        team: Team abbreviation/id or None
        season: Season year
        cleaning_stats: Metrics from cleaner.clean()
        dedup_meta: Metadata from deduplicator.deduplicate()
        drift_meta: Metadata from schema_drift.check_drift()
        thresholds: The active IMPOSSIBLE_VALUE_THRESHOLDS dict applied
        df: Cleaned DataFrame

    Returns:
        Dict matching the BaseSidecar schema
    """
    return {
        "data_type": data_type,
        "player": player,
        "team": team,
        "season": season,
        "rows_before": cleaning_stats.get("rows_before", 0),
        "rows_after": cleaning_stats.get("rows_after", len(df)),
        "columns": list(df.columns),
        "nulls_found": cleaning_stats.get("nulls_found", 0),
        "outliers_flagged": cleaning_stats.get("outliers_flagged", 0),
        "corrupted_removed": cleaning_stats.get("corrupted_removed", 0),
        "dedup_skipped": dedup_meta.get("dedup_skipped", False),
        "dedup_reason": dedup_meta.get("dedup_reason", None),
        "dedup_conflicts": dedup_meta.get("dedup_conflicts", 0),
        "thresholds_applied": thresholds,
        "schema_drift": drift_meta,
        "written_at": datetime.now().isoformat(timespec="seconds"),
    }


def build_raw_sidecar(
    data_type: str,
    player: str | None,
    team: str | None,
    season,
    df: pd.DataFrame,
) -> dict:
    """
    Build the sidecar dict for a raw parquet file.

    Returns:
        Dict matching the RawSidecar schema
    """
    return {
        "data_type": data_type,
        "player": player,
        "team": team,
        "season": season,
        "rows": len(df),
        "columns": list(df.columns),
        "written_at": datetime.now().isoformat(timespec="seconds"),
    }


def build_feature_sidecar(
    feature_names: list[str],
    rolling_windows: list[int],
    min_observations: int,
    scoring_config: dict | None,
    version: str,
    success: bool,
    end_of_series_rows: int = 0,
    labels_generated: list[str] | None = None,
) -> dict:
    """
    Build the feature sidecar dict.
    Written alongside the features parquet as {name}.features.sidecar.json.

    Returns:
        Dict matching the FeatureSidecar schema
    """
    return {
        "feature_names": feature_names,
        "rolling_windows": rolling_windows,
        "min_observations": min_observations,
        "scoring_config": scoring_config,
        "feature_schema_version": version,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "success": success,
        "end_of_series_rows": end_of_series_rows,
        "labels_generated": labels_generated or [],
    }
