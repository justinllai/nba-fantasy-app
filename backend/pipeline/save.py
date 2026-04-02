"""
File writing utilities for the NBA stats data pipeline.
Implements temp-then-replace atomic write strategy with JSON sidecars.
"""
import json
import logging
import os
from datetime import datetime

import pandas as pd

from pipeline.exceptions import FileWriteError


def write_with_sidecar(
    df: pd.DataFrame,
    parquet_path: str,
    sidecar_dict: dict,
    logger: logging.Logger = None,
) -> None:
    """
    Write a DataFrame to parquet and a sidecar dict to JSON atomically.

    Strategy:
    1. Write parquet to {parquet_path}.tmp
    2. Write sidecar to {parquet_path}.sidecar.tmp
    3. Atomically rename both to final paths

    On any exception:
    - Log ERROR with path
    - Delete both .tmp files if they exist
    - Re-raise FileWriteError

    Args:
        df: DataFrame to write
        parquet_path: Target path for the parquet file
        sidecar_dict: Dict to write as JSON sidecar
        logger: Optional logger for error reporting
    """
    parquet_tmp = parquet_path + ".tmp"
    sidecar_path = _sidecar_path(parquet_path)
    sidecar_tmp = sidecar_path + ".tmp"

    try:
        # Ensure parent directory exists
        os.makedirs(os.path.dirname(parquet_path), exist_ok=True)

        # Step 1: Write parquet to temp
        df.to_parquet(parquet_tmp, engine="pyarrow", compression="snappy", index=False)

        # Step 2: Write sidecar to temp
        with open(sidecar_tmp, "w", encoding="utf-8") as f:
            json.dump(sidecar_dict, f, indent=2, default=str)

        # Step 3: Atomic rename both
        os.replace(parquet_tmp, parquet_path)
        os.replace(sidecar_tmp, sidecar_path)

    except Exception as e:
        if logger:
            logger.error(f"File write failed for {parquet_path}: {e}")

        # Clean up temp files
        for tmp_file in [parquet_tmp, sidecar_tmp]:
            if os.path.exists(tmp_file):
                try:
                    os.remove(tmp_file)
                except OSError:
                    pass

        raise FileWriteError(f"Failed to write {parquet_path}: {e}") from e


def _sidecar_path(parquet_path: str) -> str:
    """Convert a parquet path to its corresponding JSON sidecar path."""
    # Replace .parquet extension with .json
    if parquet_path.endswith(".parquet"):
        return parquet_path[:-len(".parquet")] + ".json"
    return parquet_path + ".json"


def build_raw_sidecar(
    data_type: str,
    player_or_team: str,
    season,
    df: pd.DataFrame,
) -> dict:
    """
    Build the sidecar dict for a raw parquet file.

    Args:
        data_type: One of the supported data types
        player_or_team: Player name, team abbreviation, or game_id
        season: Season year
        df: Raw DataFrame after ingest

    Returns:
        Dict matching the RawSidecar schema
    """
    return {
        "data_type": data_type,
        "player_or_team": player_or_team,
        "season": season,
        "rows": len(df),
        "columns": list(df.columns),
        "written_at": datetime.now().isoformat(timespec="seconds"),
    }


def build_clean_sidecar(
    data_type: str,
    player_or_team: str,
    season,
    metrics_dict: dict,
    df: pd.DataFrame,
) -> dict:
    """
    Build the sidecar dict for a clean parquet file.

    Args:
        data_type: One of the supported data types
        player_or_team: Player name, team abbreviation, or game_id
        season: Season year
        metrics_dict: Cleaning metrics from clean()
        df: Cleaned DataFrame

    Returns:
        Dict matching the CleanSidecar schema
    """
    return {
        "data_type": data_type,
        "player_or_team": player_or_team,
        "season": season,
        "rows_before": metrics_dict.get("rows_before", 0),
        "rows_after": metrics_dict.get("rows_after", len(df)),
        "columns": list(df.columns),
        "nulls_found": metrics_dict.get("nulls_found", 0),
        "outliers_flagged": metrics_dict.get("outliers_flagged", 0),
        "corrupted_removed": metrics_dict.get("corrupted_removed", 0),
        "dedup_skipped": metrics_dict.get("dedup_skipped", False),
        "dedup_reason": metrics_dict.get("dedup_reason", None),
        "written_at": datetime.now().isoformat(timespec="seconds"),
    }
