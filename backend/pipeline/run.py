"""
Orchestration entry point for the NBA stats data pipeline.

run() coordinates: fetch → validate → clean → deduplicate → outlier-flag
                   → schema-drift → write parquet → write sidecar
                   → [optional] feature engineering → labels
"""
import logging
import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

import pipeline.config as _config
from pipeline.config import IMPOSSIBLE_VALUE_THRESHOLDS, SCHEMA_BASELINES_DIR, SUPPORTED_DATA_TYPES
from pipeline.exceptions import (
    APIKeyMissingError,
    MissingAPIKeyError,   # backward-compat alias — kept until all callers updated
    PipelineConfigError,
    UnsupportedDataTypeError,
    ValidationError,
)


# ---------------------------------------------------------------------------
# Logger factory (inline — avoids dependency on deprecated logging_utils.py)
# ---------------------------------------------------------------------------

def _create_logger(run_id: str, logs_dir: str) -> logging.Logger:
    logger = logging.getLogger(f"pipeline.run.{run_id}")
    logger.setLevel(logging.DEBUG)
    if logger.handlers:
        return logger

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    sh = logging.StreamHandler()
    sh.setLevel(logging.DEBUG)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    os.makedirs(logs_dir, exist_ok=True)
    fh = logging.FileHandler(os.path.join(logs_dir, f"run_{run_id}.log"), encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def _subject_label(player, team) -> str:
    """Return a filesystem-safe label from player or team."""
    val = player if player is not None else team
    return str(val).replace(" ", "_").lower()


def _parquet_path(base_dir: str, data_type: str, player, team, season) -> str:
    label = _subject_label(player, team)
    return os.path.join(base_dir, f"{data_type}_{label}_{season}.parquet")


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run(
    data_types: list[str],
    season: int,
    player: str | int | None = None,
    team: str | int | None = None,
    columns: list[str] | None = None,
    output_dir: str | Path = "data/",
    features_config: dict | None = None,
    labels_config: dict | None = None,
) -> dict[str, dict]:
    """
    Run the NBA stats data pipeline for the specified data types.

    Args:
        data_types: Non-empty list of data type strings to process
        season: Season year (e.g., 2023)
        player: Player name or player_id. Mutually exclusive with team.
        team: Team abbreviation/name or team_id. Mutually exclusive with player.
        columns: Optional list of stat columns to retain (identity columns always kept).
                 Unrecognized column names are logged as WARNING and ignored.
        output_dir: Root directory for output; raw/ and clean/ subdirs created automatically
        features_config: Optional feature-engineering config dict
        labels_config: Optional label-generation config dict

    Returns:
        Dict keyed by data type:
          success → {"status": "success", "rows_before": int, "rows_after": int,
                     "outliers_flagged": int, "corrupted_removed": int, "nulls_found": int,
                     "file_path": str}
          failure → {"status": "failed", "error": str}

    Raises:
        APIKeyMissingError: If BALL_IS_LIFE env var is not set
        PipelineConfigError: If both player and team provided, or data_types invalid
    """
    # --- API key ---
    load_dotenv()
    if not os.environ.get("BALL_IS_LIFE"):
        raise APIKeyMissingError()

    # --- Arg validation ---
    if not isinstance(data_types, list) or len(data_types) == 0:
        raise PipelineConfigError("data_types must be a non-empty list")

    if player is not None and team is not None:
        raise PipelineConfigError("player and team are mutually exclusive — provide only one")

    for dt in data_types:
        if dt not in SUPPORTED_DATA_TYPES:
            raise UnsupportedDataTypeError(dt)

    # --- Directories ---
    output_dir = Path(output_dir)
    raw_dir = str(output_dir / "raw")
    clean_dir = str(output_dir / "clean")
    logs_dir = str(output_dir / "logs")

    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(clean_dir, exist_ok=True)
    os.makedirs(logs_dir, exist_ok=True)

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    logger = _create_logger(run_id, logs_dir)
    logger.info(
        f"Pipeline run {run_id} started: data_types={data_types}, "
        f"player={player!r}, team={team!r}, season={season}, columns={columns}"
    )

    # --- Warn on unrecognized columns ---
    if columns is not None:
        from pipeline.config import IDENTITY_COLUMNS
        # We can't know which stat columns are valid without fetching, so warn on
        # anything that looks like a non-stat name (non-alphabetic/underscore)
        # — actual column existence warning happens in cleaner after fetch
        pass

    results = {}
    for data_type in data_types:
        logger.info(f"Processing {data_type}")
        try:
            results[data_type] = _process(
                data_type=data_type,
                player=player,
                team=team,
                season=season,
                columns=columns,
                raw_dir=raw_dir,
                clean_dir=clean_dir,
                thresholds=IMPOSSIBLE_VALUE_THRESHOLDS,
                features_config=features_config,
                labels_config=labels_config,
                run_id=run_id,
                logger=logger,
            )
        except Exception as exc:
            logger.error(f"[{data_type}] pipeline failed: {exc}")
            results[data_type] = {"status": "failed", "error": str(exc)}

    logger.info(f"Pipeline run {run_id} complete")
    _prune_logs(logs_dir, keep=100)
    return results


# ---------------------------------------------------------------------------
# Per-data-type pipeline
# ---------------------------------------------------------------------------

def _process(
    data_type, player, team, season, columns,
    raw_dir, clean_dir, thresholds, features_config, labels_config, run_id, logger,
) -> dict:
    from pipeline.fetcher import FETCH_FUNCTIONS
    from pipeline.validator import validate
    from pipeline.cleaner import clean
    from pipeline.deduplicator import deduplicate
    from pipeline.outlier import flag_outliers
    from pipeline.schema_drift import check_drift
    from pipeline.sidecar import build_base_sidecar, build_raw_sidecar
    from pipeline.writer import write_parquet, write_sidecar, sidecar_path

    fetch_fn = FETCH_FUNCTIONS[data_type]

    # Step 1: Fetch
    raw_df = fetch_fn(player=player, team=team, season=int(season), logger=logger)

    # Step 2: Write raw parquet + sidecar
    raw_path = _parquet_path(raw_dir, data_type, player, team, season)
    write_parquet(raw_df, raw_path, logger=logger)
    raw_sc = build_raw_sidecar(data_type, player, team, season, raw_df)
    write_sidecar(raw_sc, sidecar_path(raw_path), logger=logger)
    logger.info(f"[{data_type}] raw written: {raw_path}")

    # Step 3: Validate (structural failures halt; optional field absences warn)
    validate(raw_df, data_type, logger=logger)

    # Step 4: Warn on requested columns that don't exist in df
    if columns is not None:
        from pipeline.config import IDENTITY_COLUMNS
        unknown = [c for c in columns if c not in raw_df.columns and c not in IDENTITY_COLUMNS]
        for col in unknown:
            logger.warning(f"[{data_type}] requested column '{col}' not found in data — skipped")

    # Step 5: Clean (normalize strings, dates, units; remove impossible values; filter columns; sort)
    clean_df, cleaning_stats = clean(
        raw_df, data_type, columns=columns, thresholds=thresholds, logger=logger
    )

    # Step 6: Deduplicate
    clean_df, dedup_meta = deduplicate(clean_df, data_type, logger=logger)

    # Step 7: Flag outliers
    clean_df, outliers_flagged = flag_outliers(clean_df, logger=logger)

    # Step 8: Schema drift
    baselines_dir = Path(SCHEMA_BASELINES_DIR)
    drift_meta = check_drift(raw_df, data_type, baselines_dir, logger=logger)

    # Step 9: Write clean parquet + sidecar
    clean_path = _parquet_path(clean_dir, data_type, player, team, season)
    write_parquet(clean_df, clean_path, logger=logger)

    sc_data = build_base_sidecar(
        data_type=data_type,
        player=player,
        team=team,
        season=season,
        cleaning_stats=cleaning_stats,
        dedup_meta=dedup_meta,
        drift_meta=drift_meta,
        thresholds=thresholds,
        df=clean_df,
    )
    write_sidecar(sc_data, sidecar_path(clean_path), logger=logger)
    logger.info(f"[{data_type}] clean written: {clean_path}")

    result = {
        "status": "success",
        "rows_before": cleaning_stats["rows_before"],
        "rows_after": cleaning_stats["rows_after"],
        "outliers_flagged": outliers_flagged,
        "corrupted_removed": cleaning_stats["corrupted_removed"],
        "nulls_found": cleaning_stats["nulls_found"],
        "file_path": os.path.abspath(clean_path),
    }

    # Step 10: Optional feature engineering + labels
    # Failure here does NOT fail the base result — only logged as ERROR
    if features_config is not None:
        try:
            result = _run_features(
                clean_df=clean_df,
                clean_path=clean_path,
                data_type=data_type,
                features_config=features_config,
                labels_config=labels_config,
                result=result,
                logger=logger,
            )
        except Exception as exc:
            logger.error(f"[{data_type}] feature engineering failed (base result preserved): {exc}")

    return result


def _run_features(
    clean_df, clean_path, data_type, features_config, labels_config, result, logger
) -> dict:
    from pipeline.features.engineer import engineer
    from pipeline.features.labels import generate_labels
    from pipeline.features.versioning import get_feature_version
    from pipeline.sidecar import build_feature_sidecar
    from pipeline.writer import write_parquet, write_sidecar, feature_sidecar_path

    feat_df, feature_names = engineer(clean_df, features_config, logger=logger)

    label_meta = {"end_of_series_count": 0, "labels_generated": []}
    if labels_config is not None:
        feat_df, label_meta = generate_labels(feat_df, labels_config, logger=logger)

    # Write features parquet (replaces clean parquet — same path, features included)
    features_parquet = clean_path  # overwrite clean parquet with feature-enriched version
    write_parquet(feat_df, features_parquet, logger=logger)

    version = get_feature_version(features_config)

    rolling_windows = features_config.get("rolling_windows", [])
    min_obs = features_config.get("min_observations", 1)
    scoring = features_config.get("scoring")

    feat_sc = build_feature_sidecar(
        feature_names=feature_names,
        rolling_windows=rolling_windows,
        min_observations=min_obs,
        scoring_config=scoring,
        version=version,
        success=True,
        end_of_series_rows=label_meta["end_of_series_count"],
        labels_generated=label_meta["labels_generated"],
    )
    write_sidecar(feat_sc, feature_sidecar_path(clean_path), logger=logger)
    logger.info(f"[{data_type}] features written: {len(feature_names)} features, version={version}")

    result["feature_schema_version"] = version
    result["features_count"] = len(feature_names)
    return result


def _prune_logs(logs_dir: str, keep: int = 100) -> None:
    logs = sorted(Path(logs_dir).glob("*.log"), key=lambda p: p.stat().st_mtime)
    for old in logs[:-keep]:
        old.unlink()
