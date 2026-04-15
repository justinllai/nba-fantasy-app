"""
Atomic file writing utilities for the NBA stats data pipeline.
Implements temp-then-replace strategy for both parquet and sidecar files.
Sidecar extension: .sidecar.json (base) and .features.sidecar.json (features).
"""
import json
import logging
import os
from pathlib import Path

import pandas as pd

from pipeline.exceptions import FileWriteError


def write_parquet(
    df: pd.DataFrame,
    path: str | Path,
    logger: logging.Logger = None,
) -> None:
    """
    Write a DataFrame to a parquet file atomically (write .tmp, then rename).

    On failure:
    - Log ERROR with the file path
    - Delete the .tmp file if it exists
    - Raise FileWriteError

    Args:
        df: DataFrame to write
        path: Target path for the parquet file
        logger: Optional logger
    """
    path = str(path)
    tmp_path = path + ".tmp"

    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_parquet(tmp_path, engine="pyarrow", compression="snappy", index=False)
        os.replace(tmp_path, path)
    except Exception as e:
        if logger:
            logger.error(f"Parquet write failed for {path}: {e}")
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
        raise FileWriteError(f"Failed to write parquet {path}: {e}") from e


def write_sidecar(
    data: dict,
    path: str | Path,
    logger: logging.Logger = None,
) -> None:
    """
    Write a dict to a JSON sidecar file atomically.
    Path should end in .sidecar.json or .features.sidecar.json.

    On failure:
    - Log ERROR with the file path
    - Delete the .tmp file if it exists
    - Raise FileWriteError

    Args:
        data: Dict to serialize as JSON
        path: Target path for the sidecar file
        logger: Optional logger
    """
    path = str(path)
    tmp_path = path + ".tmp"

    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)
        os.replace(tmp_path, path)
    except Exception as e:
        if logger:
            logger.error(f"Sidecar write failed for {path}: {e}")
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
        raise FileWriteError(f"Failed to write sidecar {path}: {e}") from e


def sidecar_path(parquet_path: str | Path) -> str:
    """Return the base sidecar path for a given parquet path (.sidecar.json)."""
    p = str(parquet_path)
    if p.endswith(".parquet"):
        return p[: -len(".parquet")] + ".sidecar.json"
    return p + ".sidecar.json"


def feature_sidecar_path(parquet_path: str | Path) -> str:
    """Return the feature sidecar path for a given parquet path (.features.sidecar.json)."""
    p = str(parquet_path)
    if p.endswith(".parquet"):
        return p[: -len(".parquet")] + ".features.sidecar.json"
    return p + ".features.sidecar.json"
