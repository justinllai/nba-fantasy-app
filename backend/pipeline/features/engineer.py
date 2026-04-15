"""
Feature engineering for the NBA stats data pipeline.
Anti-leakage rolling features: always .shift(1) before .rolling().
"""
import logging

import pandas as pd


def engineer(
    df: pd.DataFrame,
    features_config: dict,
    logger: logging.Logger = None,
) -> tuple[pd.DataFrame, list[str]]:
    """
    Generate features from a cleaned game-log DataFrame.

    Rolling features use .shift(1) before .rolling() to prevent data leakage:
    game N's rolling window covers games N-min_obs..N-1, never game N itself.

    Features generated:
    - rolling_{stat}_{window}: rolling mean for each stat and window
    - rolling_std_{stat}_{window}: rolling std dev
    - {stat}_delta: last game minus rolling mean (trend indicator)
    - fantasy_pts: weighted sum from scoring config
    - is_outlier column is excluded from feature inputs

    Args:
        df: Cleaned DataFrame (must have player_id, date columns)
        features_config: Dict with keys:
            - rolling_windows: list[int]
            - min_observations: int
            - scoring: dict[str, float]  (stat → weight for fantasy_pts)
        logger: Optional logger

    Returns:
        Tuple of (df_with_features, list_of_feature_column_names)
    """
    df = df.copy()
    rolling_windows = features_config.get("rolling_windows", [])
    min_obs = features_config.get("min_observations", 1)
    scoring = features_config.get("scoring", {})

    # Determine stat columns: numeric, not identity/meta columns, not is_outlier
    exclude = {"game_id", "season", "player_id", "team_id", "is_outlier"}
    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    stat_cols = [c for c in numeric_cols if c not in exclude]

    feature_names = []

    # Sort by player_id + date for correct temporal ordering
    sort_cols = [c for c in ["player_id", "date", "game_id"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols).reset_index(drop=True)

    for window in rolling_windows:
        for stat in stat_cols:
            if stat not in df.columns:
                continue

            rolling_col = f"rolling_{stat}_{window}"
            std_col = f"rolling_std_{stat}_{window}"
            delta_col = f"{stat}_delta"

            if "player_id" in df.columns:
                # Group by player, shift(1) then rolling to prevent leakage
                shifted = df.groupby("player_id")[stat].shift(1)
                rolling_mean = shifted.groupby(df["player_id"]).transform(
                    lambda s: s.rolling(window, min_periods=min_obs).mean()
                )
                rolling_std = shifted.groupby(df["player_id"]).transform(
                    lambda s: s.rolling(window, min_periods=min_obs).std()
                )
            else:
                shifted = df[stat].shift(1)
                rolling_mean = shifted.rolling(window, min_periods=min_obs).mean()
                rolling_std = shifted.rolling(window, min_periods=min_obs).std()

            df[rolling_col] = rolling_mean
            feature_names.append(rolling_col)

            df[std_col] = rolling_std
            feature_names.append(std_col)

            # Delta: current game stat minus rolling mean (trend)
            if rolling_col in df.columns:
                delta_col_name = f"{stat}_delta_{window}"
                df[delta_col_name] = df[stat] - df[rolling_col]
                feature_names.append(delta_col_name)

    # Fantasy points (computed from raw stats, not rolling — no leakage concern)
    if scoring:
        fantasy = pd.Series([0.0] * len(df), index=df.index)
        for stat, weight in scoring.items():
            if stat in df.columns:
                fantasy = fantasy + df[stat].fillna(0) * weight
        df["fantasy_pts"] = fantasy
        feature_names.append("fantasy_pts")

    if logger:
        logger.info(f"Feature engineering complete: {len(feature_names)} features generated")

    return df, feature_names
