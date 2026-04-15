"""
Data fetching module for the NBA stats data pipeline.
Fetches data from BallDontLie API and returns raw DataFrames with ALL API fields.

Key differences from ingest.py:
- Separate `player` and `team` params (mutually exclusive)
- ALL API fields retained — no column dropping
- Position field captured from player records
- `player` param accepts name (str) or player_id (int)
- `team` param accepts abbreviation/name (str) or team_id (int)
"""
import logging
import time

import pandas as pd

from api import get_player_id, get_game_logs, load
from pipeline.exceptions import IngestionError, EntityLookupError


# ---------------------------------------------------------------------------
# Retry helper
# ---------------------------------------------------------------------------

def _retry(fn, max_retries=3, label="request", logger=None):
    """Execute fn with exponential backoff (1s, 2s, 4s). Raises IngestionError on failure."""
    last_error = None
    for attempt in range(max_retries):
        try:
            return fn()
        except Exception as exc:
            last_error = exc
            if attempt < max_retries - 1:
                delay = 2 ** attempt
                if logger:
                    logger.warning(
                        f"Attempt {attempt + 1}/{max_retries} failed for {label}: {exc}. "
                        f"Retrying in {delay}s..."
                    )
                time.sleep(delay)
            else:
                if logger:
                    logger.error(f"All {max_retries} attempts failed for {label}: {exc}")
    raise IngestionError(f"Fetch failed for {label} after {max_retries} attempts: {last_error}")


# ---------------------------------------------------------------------------
# Entity resolution helpers
# ---------------------------------------------------------------------------

def _resolve_player_id(player: str | int, logger=None) -> int:
    """Return player_id from name or passthrough if already int."""
    if isinstance(player, int):
        return player
    return _retry(
        lambda: get_player_id(player),
        label=f"player_id lookup for '{player}'",
        logger=logger,
    )


def _resolve_team_id(team: str | int, logger=None) -> int:
    """Return team_id from abbreviation/name or passthrough if already int."""
    if isinstance(team, int):
        return team

    def _lookup():
        teams = load("/teams", {"search": team})
        if not teams:
            raise EntityLookupError(team, f"Team not found: '{team}'")
        for t in teams:
            if t.get("abbreviation", "").upper() == team.upper():
                return t["id"]
        return teams[0]["id"]

    return _retry(_lookup, label=f"team_id lookup for '{team}'", logger=logger)


def _fetch_player_position(player_id: int, logger=None) -> str | None:
    """
    Fetch position for a player_id via /players endpoint.
    Returns None if not available or request fails.
    """
    try:
        players = load("/players", {"player_ids[]": player_id})
        if players:
            return players[0].get("position")
    except Exception as exc:
        if logger:
            logger.debug(f"Could not fetch position for player_id={player_id}: {exc}")
    return None


# ---------------------------------------------------------------------------
# Record flatteners — capture ALL available API fields
# ---------------------------------------------------------------------------

def _flatten_stat_record(record: dict, position: str | None = None) -> dict:
    """
    Flatten a /stats API record (used by game_logs and box_scores).
    Captures all available fields from player, game, team sub-objects.
    """
    game = record.get("game") or {}
    player = record.get("player") or {}
    team = record.get("team") or {}

    first = player.get("first_name") or ""
    last = player.get("last_name") or ""
    player_name = f"{first} {last}".strip() if (first or last) else player.get("name", "")

    # Position: prefer record-level, then player object, then injected arg
    pos = record.get("position") or player.get("position") or position

    return {
        # Identity
        "game_id": game.get("id"),
        "player_id": player.get("id"),
        "player_name": player_name,
        "team_id": team.get("id"),
        "team": team.get("abbreviation"),
        "date": game.get("date"),
        "season": game.get("season"),
        # Player attributes
        "position": pos,
        # Game context
        "home_team_id": game.get("home_team_id"),
        "visitor_team_id": game.get("visitor_team_id"),
        "home_team_score": game.get("home_team_score"),
        "visitor_team_score": game.get("visitor_team_score"),
        "postseason": game.get("postseason"),
        "status": game.get("status"),
        # Per-game stats
        "min": record.get("min"),
        "fgm": record.get("fgm"),
        "fga": record.get("fga"),
        "fg_pct": record.get("fg_pct"),
        "fg3m": record.get("fg3m"),
        "fg3a": record.get("fg3a"),
        "fg3_pct": record.get("fg3_pct"),
        "ftm": record.get("ftm"),
        "fta": record.get("fta"),
        "ft_pct": record.get("ft_pct"),
        "oreb": record.get("oreb"),
        "dreb": record.get("dreb"),
        "reb": record.get("reb"),
        "ast": record.get("ast"),
        "stl": record.get("stl"),
        "blk": record.get("blk"),
        "turnover": record.get("turnover"),
        "pf": record.get("pf"),
        "pts": record.get("pts"),
    }


def _flatten_game_score_record(record: dict) -> dict:
    """Flatten a /games API record. Captures all available fields."""
    home = record.get("home_team") or {}
    visitor = record.get("visitor_team") or {}

    return {
        "game_id": record.get("id"),
        "date": record.get("date"),
        "season": record.get("season"),
        "status": record.get("status"),
        "period": record.get("period"),
        "time": record.get("time"),
        "postseason": record.get("postseason"),
        "home_team_id": home.get("id"),
        "home_team": home.get("abbreviation"),
        "home_team_name": home.get("full_name"),
        "home_team_city": home.get("city"),
        "home_team_conference": home.get("conference"),
        "home_team_division": home.get("division"),
        "home_team_score": record.get("home_team_score"),
        "visitor_team_id": visitor.get("id"),
        "visitor_team": visitor.get("abbreviation"),
        "visitor_team_name": visitor.get("full_name"),
        "visitor_team_city": visitor.get("city"),
        "visitor_team_conference": visitor.get("conference"),
        "visitor_team_division": visitor.get("division"),
        "visitor_team_score": record.get("visitor_team_score"),
    }


def _flatten_season_average_record(record: dict) -> dict:
    """Flatten a /season_averages API record. Captures all available fields."""
    player = record.get("player") or {}

    first = player.get("first_name") or ""
    last = player.get("last_name") or ""
    player_name = f"{first} {last}".strip() if (first or last) else player.get("name", "")

    pos = record.get("position") or player.get("position")

    return {
        "player_id": player.get("id"),
        "player_name": player_name,
        "position": pos,
        "season": record.get("season"),
        "games_played": record.get("games_played"),
        "min": record.get("min"),
        "fgm": record.get("fgm"),
        "fga": record.get("fga"),
        "fg_pct": record.get("fg_pct"),
        "fg3m": record.get("fg3m"),
        "fg3a": record.get("fg3a"),
        "fg3_pct": record.get("fg3_pct"),
        "ftm": record.get("ftm"),
        "fta": record.get("fta"),
        "ft_pct": record.get("ft_pct"),
        "oreb": record.get("oreb"),
        "dreb": record.get("dreb"),
        "reb": record.get("reb"),
        "ast": record.get("ast"),
        "stl": record.get("stl"),
        "blk": record.get("blk"),
        "turnover": record.get("turnover"),
        "pf": record.get("pf"),
        "pts": record.get("pts"),
    }


def _records_to_df(flat_records: list[dict], empty_cols: list[str]) -> pd.DataFrame:
    """Convert flat records list to DataFrame; return empty df with schema if no records."""
    if not flat_records:
        return pd.DataFrame(columns=empty_cols)
    return pd.DataFrame(flat_records)


# ---------------------------------------------------------------------------
# Public fetch functions
# ---------------------------------------------------------------------------

def fetch_game_logs(
    player: str | int | None = None,
    team: str | int | None = None,
    season: int = None,
    logger: logging.Logger = None,
) -> pd.DataFrame:
    """
    Fetch game logs from /stats endpoint.

    Args:
        player: Player name or player_id. Mutually exclusive with team.
        team: Team abbreviation/name or team_id. Mutually exclusive with player.
        season: Season year
        logger: Optional logger

    Returns:
        Raw DataFrame with ALL API fields retained

    Raises:
        IngestionError: If player/team not provided or API fails
        EntityLookupError: If player or team not found
    """
    if player is None and team is None:
        raise IngestionError("fetch_game_logs requires either player or team")

    position = None

    if player is not None:
        player_id = _resolve_player_id(player, logger=logger)
        if logger:
            logger.info(f"Fetching game_logs for player_id={player_id} season={season}")

        position = _fetch_player_position(player_id, logger=logger)

        records = _retry(
            lambda: get_game_logs(player_id, season),
            label=f"game_logs player_id={player_id}",
            logger=logger,
        )
    else:
        team_id = _resolve_team_id(team, logger=logger)
        if logger:
            logger.info(f"Fetching game_logs for team_id={team_id} season={season}")

        records = _retry(
            lambda: load("/stats", {"team_ids[]": team_id, "seasons[]": season}),
            label=f"game_logs team_id={team_id}",
            logger=logger,
        )

    records = records or []
    flat = [_flatten_stat_record(r, position=position) for r in records]
    df = _records_to_df(flat, list(_flatten_stat_record({}).keys()))

    if logger:
        logger.info(f"game_logs fetch complete: {len(df)} rows")
    return df


def fetch_box_scores(
    player: str | int | None = None,
    team: str | int | None = None,
    season: int = None,
    logger: logging.Logger = None,
) -> pd.DataFrame:
    """
    Fetch box scores from /stats endpoint.

    Args:
        player: Player name or player_id. Mutually exclusive with team.
        team: Team abbreviation/name or team_id. Mutually exclusive with player.
        season: Season year
        logger: Optional logger

    Returns:
        Raw DataFrame with ALL API fields retained
    """
    if player is None and team is None:
        raise IngestionError("fetch_box_scores requires either player or team")

    position = None

    if player is not None:
        player_id = _resolve_player_id(player, logger=logger)
        if logger:
            logger.info(f"Fetching box_scores for player_id={player_id} season={season}")
        position = _fetch_player_position(player_id, logger=logger)
        records = _retry(
            lambda: load("/stats", {"player_ids[]": player_id, "seasons[]": season}),
            label=f"box_scores player_id={player_id}",
            logger=logger,
        )
    else:
        team_id = _resolve_team_id(team, logger=logger)
        if logger:
            logger.info(f"Fetching box_scores for team_id={team_id} season={season}")
        records = _retry(
            lambda: load("/stats", {"team_ids[]": team_id, "seasons[]": season}),
            label=f"box_scores team_id={team_id}",
            logger=logger,
        )

    records = records or []
    flat = [_flatten_stat_record(r, position=position) for r in records]
    df = _records_to_df(flat, list(_flatten_stat_record({}).keys()))

    if logger:
        logger.info(f"box_scores fetch complete: {len(df)} rows")
    return df


def fetch_game_scores(
    player: str | int | None = None,
    team: str | int | None = None,
    season: int = None,
    logger: logging.Logger = None,
) -> pd.DataFrame:
    """
    Fetch game scores from /games endpoint.

    Args:
        player: Ignored for game_scores; included for consistent signature
        team: Team abbreviation/name or team_id
        season: Season year
        logger: Optional logger

    Returns:
        Raw DataFrame with ALL API fields retained
    """
    if team is None:
        raise IngestionError("fetch_game_scores requires a team")

    team_id = _resolve_team_id(team, logger=logger)
    if logger:
        logger.info(f"Fetching game_scores for team_id={team_id} season={season}")

    records = _retry(
        lambda: load("/games", {"team_ids[]": team_id, "seasons[]": season}),
        label=f"game_scores team_id={team_id}",
        logger=logger,
    )

    records = records or []
    flat = [_flatten_game_score_record(r) for r in records]
    df = _records_to_df(flat, list(_flatten_game_score_record({}).keys()))

    if logger:
        logger.info(f"game_scores fetch complete: {len(df)} rows")
    return df


def fetch_season_averages(
    player: str | int | None = None,
    team: str | int | None = None,
    season: int = None,
    logger: logging.Logger = None,
) -> pd.DataFrame:
    """
    Fetch season averages from /season_averages endpoint.

    Args:
        player: Player name or player_id (required)
        team: Ignored for season_averages; included for consistent signature
        season: Season year
        logger: Optional logger

    Returns:
        Raw DataFrame with ALL API fields retained
    """
    if player is None:
        raise IngestionError("fetch_season_averages requires a player")

    player_id = _resolve_player_id(player, logger=logger)
    if logger:
        logger.info(f"Fetching season_averages for player_id={player_id} season={season}")

    records = _retry(
        lambda: load("/season_averages", {"player_id": player_id, "season": season}),
        label=f"season_averages player_id={player_id}",
        logger=logger,
    )

    records = records or []
    flat = [_flatten_season_average_record(r) for r in records]
    df = _records_to_df(flat, list(_flatten_season_average_record({}).keys()))

    if logger:
        logger.info(f"season_averages fetch complete: {len(df)} rows")
    return df


# ---------------------------------------------------------------------------
# Routing table — maps data_type to fetch function
# ---------------------------------------------------------------------------

FETCH_FUNCTIONS = {
    "game_logs": fetch_game_logs,
    "box_scores": fetch_box_scores,
    "game_scores": fetch_game_scores,
    "season_averages": fetch_season_averages,
}
