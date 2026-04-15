"""
Validation module for the NBA stats data pipeline.
Distinguishes required structural fields (halt on missing) from
optional stat fields (warn but continue).

Required structural fields per data type are defined in config.REQUIRED_STRUCTURAL.
Optional stat fields are any numeric columns not in REQUIRED_STRUCTURAL.
"""
import logging

import pandas as pd

from pipeline.config import REQUIRED_STRUCTURAL
from pipeline.exceptions import ValidationError


def validate(
    df: pd.DataFrame,
    data_type: str,
    logger: logging.Logger = None,
) -> dict:
    """
    Validate a DataFrame against schema requirements for the given data type.

    Two-tier validation:
    - Required structural fields: missing any halts this data type (raises ValidationError)
    - Optional stat fields: missing columns are logged as WARNING, never halt

    Additionally validates:
    - game_logs/box_scores: must have player_id OR player_name (one or both)
    - game_scores: must have team_id OR team (one or both)
    - season_averages: must have player_id OR player_name (one or both)

    Args:
        df: DataFrame to validate
        data_type: One of the supported data types
        logger: Optional logger

    Returns:
        Dict with validation metadata:
        - passed: bool
        - structural_failures: list[str]
        - optional_warnings: list[str]

    Raises:
        ValidationError: If any structural field is missing
    """
    structural_failures = []
    optional_warnings = []

    required = REQUIRED_STRUCTURAL.get(data_type, [])
    for col in required:
        if col not in df.columns:
            structural_failures.append(f"required structural field missing: '{col}'")

    # Player/team identity check
    if data_type in ("game_logs", "box_scores", "season_averages"):
        if "player_id" not in df.columns and "player_name" not in df.columns:
            structural_failures.append(
                "missing both player_id and player_name — at least one is required"
            )
    elif data_type == "game_scores":
        if "team_id" not in df.columns and "team" not in df.columns:
            structural_failures.append(
                "missing both team_id and team — at least one is required"
            )

    if structural_failures:
        for msg in structural_failures:
            if logger:
                logger.error(f"[{data_type}] validation: {msg}")
        raise ValidationError(
            f"Validation failed for {data_type} — structural fields missing: "
            + "; ".join(structural_failures)
        )

    if logger:
        logger.info(f"[{data_type}] structural validation passed")

    # Warn on expected-but-absent optional stat columns
    _common_stats = ["pts", "reb", "ast", "stl", "blk", "min", "fg_pct", "fg3_pct", "ft_pct"]
    if data_type in ("game_logs", "box_scores"):
        for col in _common_stats:
            if col not in df.columns:
                msg = f"optional stat column absent: '{col}'"
                optional_warnings.append(msg)
                if logger:
                    logger.warning(f"[{data_type}] {msg}")
    elif data_type == "season_averages":
        for col in _common_stats + ["games_played"]:
            if col not in df.columns:
                msg = f"optional stat column absent: '{col}'"
                optional_warnings.append(msg)
                if logger:
                    logger.warning(f"[{data_type}] {msg}")

    return {
        "passed": True,
        "structural_failures": [],
        "optional_warnings": optional_warnings,
    }
