"""
Schema drift detection for the NBA stats data pipeline.
Compares current DataFrame columns against a per-data-type baseline.
On first run, writes the baseline. On subsequent runs, detects changes.
"""
import json
import logging
import os
from datetime import datetime
from pathlib import Path

import pandas as pd

from pipeline.config import IDENTITY_COLUMNS


def check_drift(
    df: pd.DataFrame,
    data_type: str,
    baseline_dir: str | Path,
    logger: logging.Logger = None,
) -> dict:
    """
    Compare current columns against the stored schema baseline.

    First run: writes baseline JSON, returns drift dict with first_run=True.
    Subsequent runs: compares columns and reports added/missing.

    Logs WARNING if any IDENTITY_COLUMNS are missing from the current DataFrame.

    Args:
        df: Current DataFrame (after ingestion)
        data_type: One of the supported data types
        baseline_dir: Directory for baseline JSON files
        logger: Optional logger

    Returns:
        Dict with schema drift metadata:
        - first_run: bool
        - columns_added: list[str]
        - columns_missing: list[str]
        - identity_columns_missing: list[str]
    """
    baseline_dir = Path(baseline_dir)
    baseline_path = baseline_dir / f"{data_type}.json"
    current_columns = set(df.columns)

    # Check identity columns regardless of baseline
    identity_missing = [c for c in IDENTITY_COLUMNS if c not in current_columns]
    if identity_missing and logger:
        logger.warning(
            f"[{data_type}] schema drift: required identity columns missing: {identity_missing}"
        )

    if not baseline_path.exists():
        # First run — write baseline
        os.makedirs(baseline_dir, exist_ok=True)
        baseline = {
            "data_type": data_type,
            "columns": sorted(current_columns),
            "written_at": datetime.now().isoformat(timespec="seconds"),
        }
        with open(baseline_path, "w", encoding="utf-8") as f:
            json.dump(baseline, f, indent=2)

        if logger:
            logger.info(f"[{data_type}] schema baseline written to {baseline_path}")

        return {
            "first_run": True,
            "columns_added": [],
            "columns_missing": [],
            "identity_columns_missing": identity_missing,
        }

    # Subsequent run — compare against baseline
    with open(baseline_path, encoding="utf-8") as f:
        baseline = json.load(f)

    baseline_columns = set(baseline.get("columns", []))
    columns_added = sorted(current_columns - baseline_columns)
    columns_missing = sorted(baseline_columns - current_columns)

    if columns_added and logger:
        logger.info(f"[{data_type}] schema drift: new columns observed: {columns_added}")
    if columns_missing and logger:
        logger.warning(f"[{data_type}] schema drift: columns missing since baseline: {columns_missing}")

    return {
        "first_run": False,
        "columns_added": columns_added,
        "columns_missing": columns_missing,
        "identity_columns_missing": identity_missing,
    }
