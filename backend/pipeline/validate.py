"""
Validation module for the NBA stats data pipeline.
Checks required columns, dtypes, and row counts before cleaning.
"""
import logging

import numpy as np
import pandas as pd

from pipeline.constants import EXPECTED_DTYPES, REQUIRED_COLUMNS
from pipeline.exceptions import ValidationError
from pipeline.logging_utils import log_validation_block


def validate(df: pd.DataFrame, data_type: str, logger: logging.Logger) -> None:
    """
    Validate a DataFrame against schema requirements for the given data type.

    Checks:
    1. All required columns are present
    2. Dtypes match expected or are safely castable (float64 -> int64 when whole numbers)
    3. For game_logs only: row count >= 10

    Collects all failures and raises a single ValidationError if any found.
    If validation passes, logs INFO "validation passed".

    Args:
        df: DataFrame to validate
        data_type: One of the supported data types
        logger: Logger instance for reporting

    Raises:
        ValidationError: If any validation checks fail
    """
    failures = []

    # Check 1: Required columns present
    required = REQUIRED_COLUMNS.get(data_type, [])
    for col in required:
        if col not in df.columns:
            failures.append(f"missing required column: '{col}'")

    # Check 2: Dtype validation (only for columns that are present)
    expected_dtypes = EXPECTED_DTYPES.get(data_type, {})
    for col, expected_type in expected_dtypes.items():
        if col not in df.columns:
            # Already caught in required columns check if applicable
            continue

        series = df[col]
        actual_dtype = str(series.dtype)

        if expected_type == "int":
            # Accept int types directly
            if pd.api.types.is_integer_dtype(series):
                pass  # OK
            elif pd.api.types.is_float_dtype(series):
                # Safe cast: check all non-null values are whole numbers
                non_null = series.dropna()
                if len(non_null) > 0 and not non_null.mod(1).eq(0).all():
                    failures.append(
                        f"column '{col}' has dtype '{actual_dtype}' but expected int; "
                        f"contains non-whole-number values"
                    )
                else:
                    logger.info(
                        f"column '{col}' dtype is float64 but all values are whole numbers — "
                        f"safe cast to int accepted"
                    )
            else:
                failures.append(
                    f"column '{col}' has dtype '{actual_dtype}' but expected int"
                )

        elif expected_type == "float":
            if pd.api.types.is_float_dtype(series) or pd.api.types.is_integer_dtype(series):
                pass  # int is safely castable to float
            else:
                failures.append(
                    f"column '{col}' has dtype '{actual_dtype}' but expected float"
                )

    # Check 3: Minimum row count for game_logs
    if data_type == "game_logs" and len(df) < 10:
        failures.append(
            f"game_logs requires at least 10 rows, but got {len(df)}"
        )

    # Report results
    if failures:
        log_validation_block(logger, failures)
        raise ValidationError(
            f"Validation failed for {data_type}: " + "; ".join(failures)
        )

    logger.info(f"validation passed for {data_type}")
