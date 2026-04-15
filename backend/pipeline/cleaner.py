"""
Data cleaning module for the NBA stats data pipeline.
Handles string normalization, null standardization, minute conversion,
impossible-value removal, column filtering, and time-series sorting.

Outlier flagging and deduplication are handled by dedicated modules
(outlier.py and deduplicator.py) and are NOT performed here.
"""
import logging
import re

import pandas as pd

from pipeline.config import (
    IDENTITY_COLUMNS,
    IMPOSSIBLE_VALUE_THRESHOLDS,
    NULL_PLACEHOLDERS,
    TIME_SERIES_TYPES,
)


def clean(
    df: pd.DataFrame,
    data_type: str,
    columns: list[str] | None = None,
    thresholds: dict | None = None,
    logger: logging.Logger = None,
) -> tuple[pd.DataFrame, dict]:
    """
    Clean a raw DataFrame and return the cleaned version with metrics.

    Cleaning steps:
    1. Remove non-printable chars from string columns
    2. Normalize NULL_PLACEHOLDERS to pd.NA
    3. Standardize player names: title case + collapse whitespace
    4. Standardize dates to YYYY-MM-DD
    5. Normalize team abbreviations to uppercase
    6. Convert 'min' from MM:SS to decimal float
    7. Remove rows that violate impossible-value thresholds
    8. Remove DNP rows (min == 0)
    9. Apply column filter (retain identity columns regardless)
    10. Sort time-series data by date ASC, game_id ASC

    Args:
        df: Raw DataFrame to clean
        data_type: One of the supported data types
        columns: Optional list of stat columns to retain (identity columns always kept)
        thresholds: Optional dict with 'upper' and 'lower' sub-dicts; defaults to config values
        logger: Optional logger instance

    Returns:
        Tuple of (cleaned_df, metrics_dict) where metrics_dict contains:
        - rows_before: int
        - rows_after: int
        - corrupted_removed: int
        - nulls_found: int
        - dnp_removed: int
        - thresholds_applied: dict
    """
    if thresholds is None:
        thresholds = IMPOSSIBLE_VALUE_THRESHOLDS

    df = df.copy()
    rows_before = len(df)

    # Step 1: Remove non-printable characters from string columns
    df = _remove_non_printable(df)

    # Step 2: Normalize NULL_PLACEHOLDERS to pd.NA
    df = _normalize_nulls(df)

    # Step 3: Standardize player names
    if "player_name" in df.columns:
        df["player_name"] = df["player_name"].apply(_normalize_player_name)

    # Step 4: Standardize dates to YYYY-MM-DD
    if "date" in df.columns:
        df["date"] = df["date"].apply(_normalize_date)

    # Step 5: Normalize team abbreviations to uppercase
    for team_col in ["team", "home_team", "visitor_team"]:
        if team_col in df.columns:
            df[team_col] = df[team_col].apply(
                lambda x: x.upper().strip() if pd.notna(x) and isinstance(x, str) else x
            )

    # Step 6: Convert 'min' from MM:SS to decimal float
    if "min" in df.columns:
        df["min"] = df["min"].apply(_convert_minutes)

    # Step 7: Remove rows that violate impossible-value thresholds
    corrupted_mask = pd.Series([False] * len(df), index=df.index)
    upper_bounds = thresholds.get("upper", {})
    lower_bounds = thresholds.get("lower", {})

    for col, limit in upper_bounds.items():
        if col in df.columns:
            mask = df[col].notna() & (df[col] > limit)
            if mask.any() and logger:
                logger.warning(
                    f"[{data_type}] removing {mask.sum()} corrupted rows: {col} > {limit}"
                )
            corrupted_mask = corrupted_mask | mask

    for col, limit in lower_bounds.items():
        if col in df.columns:
            # DNP (min==0) is handled separately in step 8 — skip here
            if col == "min":
                continue
            mask = df[col].notna() & (df[col] < limit)
            if mask.any() and logger:
                logger.warning(
                    f"[{data_type}] removing {mask.sum()} corrupted rows: {col} < {limit}"
                )
            corrupted_mask = corrupted_mask | mask

    corrupted_removed = int(corrupted_mask.sum())
    df = df[~corrupted_mask].reset_index(drop=True)

    # Step 8: Remove DNP rows (min == 0)
    dnp_removed = 0
    if "min" in df.columns:
        dnp_mask = df["min"].notna() & (df["min"] == 0.0)
        dnp_removed = int(dnp_mask.sum())
        if dnp_removed > 0 and logger:
            logger.info(f"[{data_type}] removed {dnp_removed} DNP rows (min == 0)")
        df = df[~dnp_mask].reset_index(drop=True)

    # Step 9: Apply column filter — always retain identity columns
    if columns is not None:
        keep = set(IDENTITY_COLUMNS) | set(columns)
        cols_to_keep = [c for c in df.columns if c in keep]
        df = df[cols_to_keep]

    # Step 10: Sort time-series data chronologically
    if data_type in TIME_SERIES_TYPES:
        sort_cols = [c for c in ["date", "game_id"] if c in df.columns]
        if sort_cols:
            df = df.sort_values(sort_cols, ascending=True).reset_index(drop=True)

    rows_after = len(df)
    nulls_found = int(df.isnull().sum().sum())

    metrics = {
        "rows_before": rows_before,
        "rows_after": rows_after,
        "corrupted_removed": corrupted_removed,
        "nulls_found": nulls_found,
        "dnp_removed": dnp_removed,
        "thresholds_applied": thresholds,
    }

    return df, metrics


def _remove_non_printable(df: pd.DataFrame) -> pd.DataFrame:
    """Remove non-printable characters from all string columns."""
    def _clean_str(s):
        if not isinstance(s, str):
            return s
        return "".join(c for c in s if c.isprintable())

    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].apply(_clean_str)
    return df


def _normalize_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Replace NULL_PLACEHOLDERS with pd.NA in all string-typed columns."""
    for col in df.columns:
        dtype_str = str(df[col].dtype)
        if df[col].dtype == object or "str" in dtype_str or "string" in dtype_str:
            df[col] = df[col].apply(
                lambda x: pd.NA if isinstance(x, str) and x.strip() in NULL_PLACEHOLDERS else x
            )
    return df


def _normalize_player_name(name) -> str:
    """Title case + collapse whitespace for player names."""
    if not isinstance(name, str) or pd.isna(name):
        return name
    return re.sub(r"\s+", " ", name.strip()).title()


def _normalize_date(date_val) -> str:
    """Standardize date to YYYY-MM-DD format."""
    if date_val is None:
        return date_val
    try:
        if pd.isna(date_val):
            return date_val
    except (TypeError, ValueError):
        pass
    try:
        return pd.Timestamp(date_val).strftime("%Y-%m-%d")
    except Exception:
        return date_val


def _convert_minutes(val) -> float:
    """
    Convert minutes from MM:SS string to decimal float.
    E.g., "32:45" -> 32.75, "28:30" -> 28.5
    Also handles already-numeric values.
    """
    if val is None:
        return val
    try:
        if pd.isna(val):
            return val
    except (TypeError, ValueError):
        pass

    if isinstance(val, (int, float)):
        return float(val)

    if isinstance(val, str):
        val = val.strip()
        if ":" in val:
            parts = val.split(":")
            try:
                minutes = int(parts[0])
                seconds = int(parts[1]) if len(parts) > 1 else 0
                return float(minutes) + float(seconds) / 60.0
            except (ValueError, IndexError):
                return float("nan")
        try:
            return float(val)
        except ValueError:
            return float("nan")

    return float(val)
