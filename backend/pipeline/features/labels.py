"""
Label generation for the NBA stats data pipeline.
Labels are computed using .shift(-1) on sorted time series.
The last game per player has null labels and is_end_of_series=True.
"""
import logging

import pandas as pd


def generate_labels(
    df: pd.DataFrame,
    labels_config: dict,
    logger: logging.Logger = None,
) -> tuple[pd.DataFrame, dict]:
    """
    Generate prediction labels from a time-series DataFrame.

    For each target stat in labels_config["targets"]:
      next_game_{target} = df.groupby("player_id")[target].shift(-1)

    The last game per player has a null label and is marked is_end_of_series=True.
    Sorting is applied internally — input order does not need to be chronological.

    Args:
        df: Cleaned or feature-engineered DataFrame
        labels_config: Dict with keys:
            - targets: list[str]  (stat columns to generate labels for)
        logger: Optional logger

    Returns:
        Tuple of (df_with_labels, label_meta) where label_meta contains:
        - end_of_series_count: int
        - labels_generated: list[str]
    """
    df = df.copy()
    targets = labels_config.get("targets", [])
    labels_generated = []

    # Sort chronologically before generating labels
    sort_cols = [c for c in ["player_id", "date", "game_id"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols).reset_index(drop=True)

    for target in targets:
        if target not in df.columns:
            if logger:
                logger.warning(f"Label target '{target}' not found in DataFrame — skipped")
            continue

        label_col = f"next_game_{target}"

        if "player_id" in df.columns:
            df[label_col] = df.groupby("player_id")[target].shift(-1)
        else:
            df[label_col] = df[target].shift(-1)

        labels_generated.append(label_col)

    # Mark end-of-series rows: rows where the FIRST label column is null
    # (these are the last game per player — all label cols will be null there)
    if labels_generated:
        first_label = labels_generated[0]
        df["is_end_of_series"] = df[first_label].isna()
    else:
        df["is_end_of_series"] = False

    end_of_series_count = int(df["is_end_of_series"].sum())

    if logger:
        logger.info(
            f"Label generation complete: {len(labels_generated)} labels, "
            f"{end_of_series_count} end-of-series rows"
        )

    return df, {
        "end_of_series_count": end_of_series_count,
        "labels_generated": labels_generated,
    }
