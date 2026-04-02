"""
Data cleaning module for the NBA stats data pipeline.
"""
import logging
import re
import unicodedata

import numpy as np
import pandas as pd

from pipeline.constants import CORRUPTION_THRESHOLDS, NULL_PLACEHOLDERS


def clean(
    df: pd.DataFrame,
    data_type: str,
    logger: logging.Logger,
) -> tuple[pd.DataFrame, dict]:
    """
    Clean a raw DataFrame and return the cleaned version with metrics.

    Cleaning steps:
    1. Remove non-printable chars from string columns
    2. Standardize player names: title case + collapse whitespace
    3. Standardize dates to YYYY-MM-DD
    4. Normalize team abbreviations to uppercase
    5. Convert 'min' from MM:SS to decimal float
    6. Normalize NULL_PLACEHOLDERS to pd.NA
    7. Deduplicate by game_id if present, else log + record dedup_skipped
    8. Remove DNP rows (min == 0)
    9. Flag outliers with 1.5x IQR Tukey fence -> is_outlier bool column
    10. Remove corrupted rows per CORRUPTION_THRESHOLDS

    Args:
        df: Raw DataFrame to clean
        data_type: One of the supported data types
        logger: Logger instance

    Returns:
        Tuple of (cleaned_df, metrics_dict)
    """
    df = df.copy()
    rows_before = len(df)
    dedup_skipped = False
    dedup_reason = None
    corrupted_removed = 0

    # Step 1: Remove non-printable characters from string columns
    df = _remove_non_printable(df)

    # Step 6: Normalize NULL_PLACEHOLDERS to pd.NA (before other string operations)
    df = _normalize_nulls(df)

    # Step 2: Standardize player names
    if "player_name" in df.columns:
        df["player_name"] = df["player_name"].apply(_normalize_player_name)

    # Step 3: Standardize dates to YYYY-MM-DD
    if "date" in df.columns:
        df["date"] = df["date"].apply(_normalize_date)

    # Step 4: Normalize team abbreviations to uppercase
    if "team" in df.columns:
        df["team"] = df["team"].apply(
            lambda x: x.upper().strip() if pd.notna(x) and isinstance(x, str) else x
        )
    for team_col in ["home_team", "visitor_team"]:
        if team_col in df.columns:
            df[team_col] = df[team_col].apply(
                lambda x: x.upper().strip() if pd.notna(x) and isinstance(x, str) else x
            )

    # Step 5: Convert 'min' from MM:SS to decimal float
    if "min" in df.columns:
        df["min"] = df["min"].apply(_convert_minutes)

    # Step 7: Deduplicate by game_id if present
    if "game_id" in df.columns:
        df = df.drop_duplicates(subset=["game_id"], keep="first")
        logger.info(f"deduplication by game_id: {rows_before - len(df)} duplicates removed")
    else:
        dedup_skipped = True
        dedup_reason = f"game_id column not present in {data_type} dataset"
        logger.info(f"dedup skipped: {dedup_reason}")

    # Step 8: Remove DNP rows (min == 0)
    if "min" in df.columns:
        before_dnp = len(df)
        df = df[~((df["min"].notna()) & (df["min"] == 0.0))]
        dnp_removed = before_dnp - len(df)
        if dnp_removed > 0:
            logger.info(f"removed {dnp_removed} DNP rows (min == 0)")

    # Step 9: Flag outliers with 1.5x IQR Tukey fence
    df = _flag_outliers(df, logger)

    # Step 10: Remove corrupted rows per CORRUPTION_THRESHOLDS
    corrupted_mask = pd.Series([False] * len(df), index=df.index)
    for col, threshold in CORRUPTION_THRESHOLDS.items():
        if col in df.columns:
            col_mask = df[col].notna() & (df[col] > threshold)
            if col_mask.any():
                bad_count = col_mask.sum()
                logger.info(
                    f"removing {bad_count} corrupted rows: {col} > {threshold}"
                )
                corrupted_mask = corrupted_mask | col_mask

    corrupted_removed = int(corrupted_mask.sum())
    df = df[~corrupted_mask]

    rows_after = len(df)
    nulls_found = int(df.isnull().sum().sum())
    outliers_flagged = int(df["is_outlier"].sum()) if "is_outlier" in df.columns else 0

    metrics = {
        "rows_before": rows_before,
        "rows_after": rows_after,
        "outliers_flagged": outliers_flagged,
        "corrupted_removed": corrupted_removed,
        "nulls_found": nulls_found,
        "dedup_skipped": dedup_skipped,
        "dedup_reason": dedup_reason,
    }

    return df, metrics


def _remove_non_printable(df: pd.DataFrame) -> pd.DataFrame:
    """Remove non-printable characters from all string columns."""
    def clean_string(s):
        if not isinstance(s, str):
            return s
        return "".join(c for c in s if c.isprintable())

    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].apply(clean_string)
    return df


def _normalize_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Replace NULL_PLACEHOLDERS with pd.NA in all columns."""
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
    # Collapse multiple whitespace and title case
    normalized = re.sub(r"\s+", " ", name.strip()).title()
    return normalized


def _normalize_date(date_val) -> str:
    """Standardize date to YYYY-MM-DD format."""
    if pd.isna(date_val) or date_val is None:
        return date_val

    if not isinstance(date_val, str):
        # Already a datetime-like object
        try:
            return pd.Timestamp(date_val).strftime("%Y-%m-%d")
        except Exception:
            return date_val

    # Try to parse various formats
    date_str = str(date_val).strip()
    try:
        return pd.Timestamp(date_str).strftime("%Y-%m-%d")
    except Exception:
        return date_val


def _convert_minutes(val) -> float:
    """
    Convert minutes from MM:SS string to decimal float.
    E.g., "32:45" -> 32.75, "28:30" -> 28.5
    Also handles already-numeric values.
    """
    if pd.isna(val) or val is None:
        return val

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
        else:
            try:
                return float(val)
            except ValueError:
                return float("nan")

    return float(val)


def _flag_outliers(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Flag statistical outliers using 1.5x IQR Tukey fence.
    Adds 'is_outlier' boolean column; does not remove rows.
    """
    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    # Exclude game_id and season from outlier detection
    exclude_cols = {"game_id", "season"}
    stat_cols = [c for c in numeric_cols if c not in exclude_cols]

    if not stat_cols:
        df["is_outlier"] = False
        return df

    outlier_mask = pd.Series([False] * len(df), index=df.index)
    for col in stat_cols:
        series = df[col].dropna()
        if len(series) < 4:
            continue
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        col_outlier = df[col].notna() & ((df[col] < lower) | (df[col] > upper))
        outlier_mask = outlier_mask | col_outlier

    df["is_outlier"] = outlier_mask
    outlier_count = int(outlier_mask.sum())
    if outlier_count > 0:
        logger.info(f"flagged {outlier_count} outlier rows (1.5x IQR Tukey fence)")

    return df
