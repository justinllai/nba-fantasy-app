"""
Outlier detection for the NBA stats data pipeline.
Uses Tukey fence (1.5× IQR) to flag statistical outliers.
Flagged rows are NEVER removed — only marked with is_outlier=True.
"""
import logging

import pandas as pd


def flag_outliers(
    df: pd.DataFrame,
    logger: logging.Logger = None,
) -> tuple[pd.DataFrame, int]:
    """
    Flag statistical outliers using the 1.5× IQR Tukey fence.

    Adds an 'is_outlier' boolean column set to True for any row where
    at least one numeric stat column falls outside:
        Q1 - 1.5 * IQR  or  Q3 + 1.5 * IQR

    Excludes: game_id, season, player_id, team_id (non-stat identifiers).
    Rows are NEVER modified or removed — only flagged.

    Args:
        df: Input DataFrame
        logger: Optional logger

    Returns:
        Tuple of (df_with_is_outlier_column, count_flagged)
    """
    exclude_cols = {"game_id", "season", "player_id", "team_id"}
    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    stat_cols = [c for c in numeric_cols if c not in exclude_cols]

    if not stat_cols:
        df = df.copy()
        df["is_outlier"] = False
        return df, 0

    df = df.copy()
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
    count = int(outlier_mask.sum())

    if count > 0 and logger:
        logger.info(f"flagged {count} outlier rows (1.5× IQR Tukey fence)")

    return df, count
