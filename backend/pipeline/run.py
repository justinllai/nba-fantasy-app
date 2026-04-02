"""
Orchestration entry point for the NBA stats data pipeline.
"""
import os
from datetime import datetime
from pathlib import Path


from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

import pipeline.constants as _constants
from pipeline.constants import SUPPORTED_DATA_TYPES
from pipeline.exceptions import (
    MissingAPIKeyError,
    UnsupportedDataTypeError,
)
from pipeline.logging_utils import create_logger
from pipeline.save import (
    build_clean_sidecar,
    build_raw_sidecar,
    write_with_sidecar,
)

load_dotenv()


def _normalize_subject(player_or_team: str) -> str:
    """Normalize player_or_team for use in filenames."""
    return player_or_team.replace(" ", "_").lower()


def _make_parquet_path(base_dir: str, data_type: str, player_or_team: str, season) -> str:
    """Build a parquet file path."""
    subject = _normalize_subject(str(player_or_team))
    filename = f"{data_type}_{subject}_{season}.parquet"
    return os.path.join(base_dir, filename)


def run(
    data_types: list,
    player_or_team: str,
    season,
) -> dict:
    """
    Run the NBA stats data pipeline for the specified data types.

    Args:
        data_types: Non-empty list of data type strings to process
        player_or_team: Player name, team abbreviation, or game_id
        season: Season year (e.g., 2023)

    Returns:
        Dict keyed by data type with success metrics or {"error": str} on failure

    Raises:
        ValueError: If data_types is empty or not a list
        UnsupportedDataTypeError: If any data type is not supported
        MissingAPIKeyError: If BALL_IS_LIFE env var is not set
    """
    # --- Input validation (raises, does not return error dicts) ---

    # Check API key first
    load_dotenv()
    api_key = os.environ.get("BALL_IS_LIFE")
    if not api_key:
        raise MissingAPIKeyError()

    # Validate data_types
    if not isinstance(data_types, list):
        raise ValueError("data_types must be a list")
    if len(data_types) == 0:
        raise ValueError("data_types must not be empty")

    for dt in data_types:
        if dt not in SUPPORTED_DATA_TYPES:
            raise UnsupportedDataTypeError(dt)

    # --- Pipeline setup ---
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    logger = create_logger(run_id)

    # Ensure directories exist (read from constants module at call time so tests can monkeypatch)
    os.makedirs(_constants.RAW_DIR, exist_ok=True)
    os.makedirs(_constants.CLEAN_DIR, exist_ok=True)
    os.makedirs(_constants.LOGS_DIR, exist_ok=True)

    logger.info(
        f"Pipeline run {run_id} started: data_types={data_types}, "
        f"player_or_team='{player_or_team}', season={season}"
    )

    results = {}

    for data_type in data_types:
        logger.info(f"Processing data type: {data_type}")
        try:
            results[data_type] = _process_data_type(
                data_type, player_or_team, season, run_id, logger
            )
        except Exception as e:
            logger.error(f"Failed to process {data_type}: {e}")
            results[data_type] = {"error": str(e)}

    logger.info(f"Pipeline run {run_id} complete: {list(results.keys())}")

    _cleanup_old_logs(_constants.LOGS_DIR, keep=100)

    return results


def _cleanup_old_logs(logs_dir: str, keep: int = 100) -> None:
    """Delete oldest log files when count exceeds keep limit."""
    logs = sorted(Path(logs_dir).glob("*.log"), key=lambda p: p.stat().st_mtime)
    for old_log in logs[:-keep]:
        old_log.unlink()


def _process_data_type(
    data_type: str,
    player_or_team: str,
    season,
    run_id: str,
    logger,
) -> dict:
    """
    Process a single data type through the full pipeline:
    ingest -> save_raw -> validate -> clean -> save_clean

    Returns success metrics dict.
    Raises any exception encountered (caller wraps in error dict).
    """
    from pipeline.clean import clean
    from pipeline.validate import validate

    # Import ingest functions lazily to avoid circular imports
    if data_type == "game_logs":
        from pipeline.ingest import ingest_game_logs as ingest_fn
        ingest = lambda: ingest_fn(player_or_team, int(season), logger)
    elif data_type == "box_scores":
        from pipeline.ingest import ingest_box_scores as ingest_fn
        ingest = lambda: ingest_fn(player_or_team, int(season), logger)
    elif data_type == "game_scores":
        from pipeline.ingest import ingest_game_scores as ingest_fn
        ingest = lambda: ingest_fn(player_or_team, int(season), logger)
    elif data_type == "season_averages":
        from pipeline.ingest import ingest_season_averages as ingest_fn
        ingest = lambda: ingest_fn(player_or_team, int(season), logger)
    else:
        raise UnsupportedDataTypeError(data_type)

    # Step 1: Ingest
    raw_df = ingest()

    # Step 2: Save raw
    raw_path = _make_parquet_path(_constants.RAW_DIR, data_type, player_or_team, season)
    raw_sidecar = build_raw_sidecar(data_type, player_or_team, season, raw_df)
    write_with_sidecar(raw_df, raw_path, raw_sidecar, logger)
    logger.info(f"Raw parquet written: {raw_path}")

    # Step 3: Validate (raises ValidationError if fails)
    validate(raw_df, data_type, logger)

    # Step 4: Clean
    clean_df, metrics = clean(raw_df, data_type, logger)

    # Step 5: Save clean
    clean_path = _make_parquet_path(_constants.CLEAN_DIR, data_type, player_or_team, season)
    clean_sidecar = build_clean_sidecar(data_type, player_or_team, season, metrics, clean_df)
    write_with_sidecar(clean_df, clean_path, clean_sidecar, logger)
    logger.info(f"Clean parquet written: {clean_path}")

    return {
        "rows_before": metrics["rows_before"],
        "rows_after": metrics["rows_after"],
        "outliers_flagged": metrics["outliers_flagged"],
        "corrupted_removed": metrics["corrupted_removed"],
        "nulls_found": metrics["nulls_found"],
        "file_path": os.path.abspath(clean_path),
    }
