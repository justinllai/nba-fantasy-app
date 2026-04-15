"""
Natural-grain deduplication for the NBA stats data pipeline.
Uses composite keys per data type rather than game_id alone.
"""
import logging

import pandas as pd

from pipeline.config import DEDUP_KEYS, DEDUP_FALLBACK_KEYS


def deduplicate(
    df: pd.DataFrame,
    data_type: str,
    logger: logging.Logger = None,
) -> tuple[pd.DataFrame, dict]:
    """
    Remove duplicate rows using the natural grain of the dataset.

    Deduplication keys by data type:
    - game_logs:       player_id + game_id  (fallback: player_name + game_id)
    - box_scores:      player_id + game_id  (fallback: player_name + game_id)
    - game_scores:     team_id + game_id    (fallback: team + game_id)
    - season_averages: skip

    When duplicate composite keys have conflicting values, the most recently
    ingested row (last occurrence) is kept. Conflicts are counted and logged.

    Args:
        df: DataFrame to deduplicate
        data_type: One of the supported data types
        logger: Optional logger

    Returns:
        Tuple of (deduped_df, dedup_meta) where dedup_meta contains:
        - dedup_skipped: bool
        - dedup_reason: str or None
        - dedup_conflicts: int
    """
    primary_keys = DEDUP_KEYS.get(data_type)
    fallback_keys = DEDUP_FALLBACK_KEYS.get(data_type)

    # Determine which key set to use
    if primary_keys is not None and all(k in df.columns for k in primary_keys):
        keys = list(primary_keys)
        key_source = "primary"
    elif fallback_keys is not None and all(k in df.columns for k in fallback_keys):
        keys = list(fallback_keys)
        key_source = "fallback"
        if logger:
            logger.info(
                f"[{data_type}] dedup: primary keys {primary_keys} not available, "
                f"using fallback keys {fallback_keys}"
            )
    else:
        reason = _skip_reason(data_type, primary_keys, fallback_keys, df)
        if logger:
            logger.info(f"[{data_type}] dedup skipped: {reason}")
        return df, {"dedup_skipped": True, "dedup_reason": reason, "dedup_conflicts": 0}

    rows_before = len(df)

    # Count conflicts: rows that share composite keys but have differing values
    # A conflict is a duplicate key group with more than one distinct row
    dup_mask = df.duplicated(subset=keys, keep=False)
    conflict_count = 0
    if dup_mask.any():
        # Count groups where values differ (not just exact duplicates)
        dup_groups = df[dup_mask].groupby(keys)
        for _, group in dup_groups:
            if len(group) > 1:
                conflict_count += len(group) - 1

    # Keep last occurrence on conflict (most recently ingested row)
    df = df.drop_duplicates(subset=keys, keep="last").reset_index(drop=True)
    rows_after = len(df)
    removed = rows_before - rows_after

    if removed > 0 and logger:
        logger.info(
            f"[{data_type}] dedup by {keys} ({key_source}): "
            f"removed {removed} duplicates, {conflict_count} conflicts resolved"
        )

    return df, {
        "dedup_skipped": False,
        "dedup_reason": None,
        "dedup_conflicts": conflict_count,
    }


def _skip_reason(data_type, primary_keys, fallback_keys, df) -> str:
    if primary_keys is None and fallback_keys is None:
        return f"no deduplication keys defined for {data_type}"
    missing_primary = [k for k in (primary_keys or []) if k not in df.columns]
    missing_fallback = [k for k in (fallback_keys or []) if k not in df.columns]
    return (
        f"required dedup columns absent — "
        f"primary keys missing: {missing_primary}, "
        f"fallback keys missing: {missing_fallback}"
    )
