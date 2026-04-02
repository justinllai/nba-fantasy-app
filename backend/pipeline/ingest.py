"""
Data ingestion module for the NBA stats data pipeline.
Fetches data from BallDontLie API and returns raw DataFrames.
"""
import logging
import time
from typing import Optional

import pandas as pd

from api import get_player_id, get_game_logs, load
from pipeline.constants import NULL_PLACEHOLDERS, UNUSED_COLUMNS
from pipeline.exceptions import IngestionError, EntityLookupError


def _parse_minutes(value) -> float | None:
    """Convert API minutes string ('32:15' or '32') to a float."""
    if value is None:
        return None
    s = str(value).strip()
    if ":" in s:
        parts = s.split(":")
        try:
            return float(parts[0]) + float(parts[1]) / 60
        except (ValueError, IndexError):
            return None
    try:
        return float(s)
    except ValueError:
        return None


def _normalize_null_placeholders(df: pd.DataFrame) -> pd.DataFrame:
    """Replace NULL_PLACEHOLDERS with pd.NA."""
    for col in df.columns:
        if df[col].dtype == object or str(df[col].dtype) == "string":
            df[col] = df[col].apply(
                lambda x: pd.NA if isinstance(x, str) and x.strip() in NULL_PLACEHOLDERS else x
            )
    return df


def _downcast_numerics(df: pd.DataFrame) -> pd.DataFrame:
    """Downcast numeric columns to reduce memory usage."""
    for col in df.select_dtypes(include=["float64"]).columns:
        df[col] = pd.to_numeric(df[col], downcast="float")
    for col in df.select_dtypes(include=["int64"]).columns:
        df[col] = pd.to_numeric(df[col], downcast="integer")
    return df


def _drop_unused(df: pd.DataFrame, data_type: str) -> pd.DataFrame:
    """Drop columns in UNUSED_COLUMNS[data_type] if they exist."""
    cols_to_drop = [c for c in UNUSED_COLUMNS.get(data_type, []) if c in df.columns]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)
    return df


def _retry_with_backoff(fn, max_retries=3, chunk_label="chunk", logger=None):
    """
    Execute fn with exponential backoff retries.
    Delays: 1s, 2s, 4s between attempts.

    Args:
        fn: Callable to execute
        max_retries: Maximum number of retries (default 3)
        chunk_label: Label for error logging
        logger: Logger instance

    Returns:
        Result of fn()

    Raises:
        IngestionError: If all retries fail
    """
    last_error = None
    for attempt in range(max_retries):
        try:
            return fn()
        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                delay = 2 ** attempt  # 1s, 2s, 4s
                if logger:
                    logger.warning(
                        f"Attempt {attempt + 1}/{max_retries} failed for {chunk_label}: {e}. "
                        f"Retrying in {delay}s..."
                    )
                time.sleep(delay)
            else:
                if logger:
                    logger.error(
                        f"All {max_retries} attempts failed for {chunk_label}: {last_error}"
                    )

    raise IngestionError(
        f"Ingestion failed for {chunk_label} after {max_retries} attempts: {last_error}"
    )


def _flatten_game_log(record: dict) -> dict:
    """Flatten a game_logs API record into a flat dict."""
    game = record.get("game", {})
    player = record.get("player", {})
    team = record.get("team", {})

    first = player.get("first_name", "")
    last = player.get("last_name", "")
    player_name = f"{first} {last}".strip() if (first or last) else player.get("name", "")

    return {
        "game_id": game.get("id"),
        "player_id": player.get("id"),
        "player_name": player_name,
        "team": team.get("abbreviation"),
        "date": game.get("date"),
        "min": _parse_minutes(record.get("min")),
        "pts": record.get("pts"),
        "reb": record.get("reb"),
        "ast": record.get("ast"),
        "stl": record.get("stl"),
        "blk": record.get("blk"),
        "fg_pct": record.get("fg_pct"),
        "fg3_pct": record.get("fg3_pct"),
        "ft_pct": record.get("ft_pct"),
        "turnover": record.get("turnover"),
    }


def ingest_game_logs(
    player_or_team: str,
    season: int,
    logger: logging.Logger,
) -> pd.DataFrame:
    """
    Fetch game logs for a player and season.

    Args:
        player_or_team: Player name
        season: Season year
        logger: Logger instance

    Returns:
        Raw DataFrame after column drops, null normalization, and downcasting

    Raises:
        IngestionError: If API calls fail after retries
        EntityLookupError: If player not found
    """
    logger.info(f"Ingesting game_logs for '{player_or_team}' season {season}")

    def _get_player():
        return get_player_id(player_or_team)

    player_id = _retry_with_backoff(
        _get_player, max_retries=3, chunk_label=f"player_id lookup for '{player_or_team}'",
        logger=logger
    )

    def _fetch_logs():
        return get_game_logs(player_id, season)

    records = _retry_with_backoff(
        _fetch_logs, max_retries=3, chunk_label=f"game_logs for player_id={player_id}",
        logger=logger
    )

    if not records:
        logger.warning(f"No game logs returned for '{player_or_team}' season {season}")
        records = []

    # Flatten records
    flat_records = [_flatten_game_log(r) for r in records]

    if not flat_records:
        df = pd.DataFrame(columns=[
            "game_id", "player_id", "player_name", "team", "date",
            "min", "pts", "reb", "ast", "stl", "blk",
            "fg_pct", "fg3_pct", "ft_pct", "turnover"
        ])
    else:
        df = pd.DataFrame(flat_records)

    # Drop unused columns
    df = _drop_unused(df, "game_logs")

    # Normalize null placeholders
    df = _normalize_null_placeholders(df)

    # Downcast numerics
    df = _downcast_numerics(df)

    logger.info(f"game_logs ingestion complete: {len(df)} rows")
    return df


def _flatten_box_score(record: dict) -> dict:
    """Flatten a box_scores API record into a flat dict."""
    game = record.get("game", {})
    player = record.get("player", {})
    team = record.get("team", {})

    first = player.get("first_name", "")
    last = player.get("last_name", "")
    player_name = f"{first} {last}".strip() if (first or last) else player.get("name", "")

    return {
        "game_id": game.get("id"),
        "player_name": player_name,
        "team": team.get("abbreviation"),
        "date": game.get("date"),
        "min": _parse_minutes(record.get("min")),
        "pts": record.get("pts"),
        "reb": record.get("reb"),
        "ast": record.get("ast"),
        "stl": record.get("stl"),
        "blk": record.get("blk"),
        "fg_pct": record.get("fg_pct"),
        "fg3_pct": record.get("fg3_pct"),
        "ft_pct": record.get("ft_pct"),
        "turnover": record.get("turnover"),
    }


def ingest_box_scores(
    player_or_team: str,
    season: int,
    logger: logging.Logger,
) -> pd.DataFrame:
    """
    Fetch box scores for a team+season or specific game_id.

    game_id detection: int(player_or_team) cast — if succeeds, treat as game_id.

    Args:
        player_or_team: Team abbreviation or game_id string
        season: Season year
        logger: Logger instance

    Returns:
        Raw DataFrame after column drops, null normalization, and downcasting

    Raises:
        IngestionError: If API calls fail after retries
        EntityLookupError: If team not found
    """
    # Detect whether it's a game_id or team abbreviation
    try:
        game_id = int(player_or_team)
        is_game_id = True
    except ValueError:
        is_game_id = False

    if is_game_id:
        logger.info(f"Ingesting box_scores for game_id={game_id}")

        def _fetch():
            return load("/stats", {"game_ids[]": game_id})

        records = _retry_with_backoff(
            _fetch, max_retries=3, chunk_label=f"box_scores for game_id={game_id}",
            logger=logger
        )
    else:
        # Team + season lookup
        logger.info(f"Ingesting box_scores for team='{player_or_team}' season {season}")

        # Look up team ID first
        def _get_team():
            teams = load("/teams", {"search": player_or_team})
            if not teams:
                raise EntityLookupError(player_or_team, f"Team not found: '{player_or_team}'")
            # Find exact abbreviation match or first result
            for team in teams:
                if team.get("abbreviation", "").upper() == player_or_team.upper():
                    return team["id"]
            return teams[0]["id"]

        team_id = _retry_with_backoff(
            _get_team, max_retries=3, chunk_label=f"team_id lookup for '{player_or_team}'",
            logger=logger
        )

        def _fetch():
            return load("/stats", {"team_ids[]": team_id, "seasons[]": season})

        records = _retry_with_backoff(
            _fetch, max_retries=3, chunk_label=f"box_scores for team_id={team_id}",
            logger=logger
        )

    if not records:
        logger.warning(f"No box score records returned for '{player_or_team}'")
        records = []

    flat_records = [_flatten_box_score(r) for r in records]

    if not flat_records:
        df = pd.DataFrame(columns=[
            "game_id", "player_name", "team", "date",
            "min", "pts", "reb", "ast", "stl", "blk",
            "fg_pct", "fg3_pct", "ft_pct", "turnover"
        ])
    else:
        df = pd.DataFrame(flat_records)

    df = _drop_unused(df, "box_scores")
    df = _normalize_null_placeholders(df)
    df = _downcast_numerics(df)

    logger.info(f"box_scores ingestion complete: {len(df)} rows")
    return df


def _flatten_game_score(record: dict) -> dict:
    """Flatten a game_scores API record into a flat dict."""
    home_team = record.get("home_team", {})
    visitor_team = record.get("visitor_team", {})

    return {
        "game_id": record.get("id"),
        "home_team": home_team.get("abbreviation"),
        "visitor_team": visitor_team.get("abbreviation"),
        "home_team_score": record.get("home_team_score"),
        "visitor_team_score": record.get("visitor_team_score"),
        "date": record.get("date"),
        "status": record.get("status"),
        "period": record.get("period"),
        "postseason": record.get("postseason"),
        "season": record.get("season"),
    }


def ingest_game_scores(
    player_or_team: str,
    season: int,
    logger: logging.Logger,
) -> pd.DataFrame:
    """
    Fetch game scores for a team and season.

    Args:
        player_or_team: Team abbreviation
        season: Season year
        logger: Logger instance

    Returns:
        Raw DataFrame after column drops, null normalization, and downcasting

    Raises:
        IngestionError: If API calls fail after retries
        EntityLookupError: If team not found
    """
    logger.info(f"Ingesting game_scores for team='{player_or_team}' season {season}")

    def _get_team():
        teams = load("/teams", {"search": player_or_team})
        if not teams:
            raise EntityLookupError(player_or_team, f"Team not found: '{player_or_team}'")
        for team in teams:
            if team.get("abbreviation", "").upper() == player_or_team.upper():
                return team["id"]
        return teams[0]["id"]

    team_id = _retry_with_backoff(
        _get_team, max_retries=3, chunk_label=f"team_id lookup for '{player_or_team}'",
        logger=logger
    )

    def _fetch():
        return load("/games", {"team_ids[]": team_id, "seasons[]": season})

    records = _retry_with_backoff(
        _fetch, max_retries=3, chunk_label=f"game_scores for team_id={team_id}",
        logger=logger
    )

    if not records:
        logger.warning(f"No game score records returned for '{player_or_team}'")
        records = []

    flat_records = [_flatten_game_score(r) for r in records]

    if not flat_records:
        df = pd.DataFrame(columns=[
            "game_id", "home_team", "visitor_team",
            "home_team_score", "visitor_team_score",
            "date", "status", "period", "postseason", "season"
        ])
    else:
        df = pd.DataFrame(flat_records)

    df = _drop_unused(df, "game_scores")
    df = _normalize_null_placeholders(df)
    df = _downcast_numerics(df)

    logger.info(f"game_scores ingestion complete: {len(df)} rows")
    return df


def _flatten_season_average(record: dict) -> dict:
    """Flatten a season_averages API record into a flat dict."""
    player = record.get("player", {})

    first = player.get("first_name", "")
    last = player.get("last_name", "")
    player_name = f"{first} {last}".strip() if (first or last) else player.get("name", "")

    return {
        "player_id": player.get("id"),
        "player_name": player_name,
        "season": record.get("season"),
        "pts": record.get("pts"),
        "reb": record.get("reb"),
        "ast": record.get("ast"),
        "stl": record.get("stl"),
        "blk": record.get("blk"),
        "min": _parse_minutes(record.get("min")),
        "games_played": record.get("games_played"),
        "fg_pct": record.get("fg_pct"),
        "fg3_pct": record.get("fg3_pct"),
        "ft_pct": record.get("ft_pct"),
        "turnover": record.get("turnover"),
    }


def ingest_season_averages(
    player_or_team: str,
    season: int,
    logger: logging.Logger,
) -> pd.DataFrame:
    """
    Fetch season averages for a player and season.

    Args:
        player_or_team: Player name
        season: Season year
        logger: Logger instance

    Returns:
        Raw DataFrame after column drops, null normalization, and downcasting

    Raises:
        IngestionError: If API calls fail after retries
        EntityLookupError: If player not found
    """
    logger.info(f"Ingesting season_averages for '{player_or_team}' season {season}")

    def _get_player():
        return get_player_id(player_or_team)

    player_id = _retry_with_backoff(
        _get_player, max_retries=3, chunk_label=f"player_id lookup for '{player_or_team}'",
        logger=logger
    )

    def _fetch():
        return load("/season_averages", {"player_id": player_id, "season": season})

    records = _retry_with_backoff(
        _fetch, max_retries=3, chunk_label=f"season_averages for player_id={player_id}",
        logger=logger
    )

    if not records:
        logger.warning(f"No season averages returned for '{player_or_team}' season {season}")
        records = []

    flat_records = [_flatten_season_average(r) for r in records]

    if not flat_records:
        df = pd.DataFrame(columns=[
            "player_id", "player_name", "season",
            "pts", "reb", "ast", "stl", "blk", "min",
            "games_played", "fg_pct", "fg3_pct", "ft_pct", "turnover"
        ])
    else:
        df = pd.DataFrame(flat_records)

    df = _drop_unused(df, "season_averages")
    df = _normalize_null_placeholders(df)
    df = _downcast_numerics(df)

    logger.info(f"season_averages ingestion complete: {len(df)} rows")
    return df
