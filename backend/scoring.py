from pydantic import BaseModel
from typing import Optional

ESPN_WEIGHTS = {
    "pts": 1.0,
    "reb": 1.0,
    "ast": 2.0,
    "stl": 4.0,
    "blk": 4.0,
    "turnover": -2.0,
    "fgm": 2.0,
    "fga": -1.0,
    "ftm": 1.0,
    "fta": -1.0,
    "fg3m": 1.0
}

SIGNAL_WEIGHTS = {
    "replacement_value": 0.33,
    "minutes_trend": 0.33,
    "sustainability": 0.34
}

SIGNAL_RANGES = {
    "replacement_value": {"min": 0, "max": 50},
    "minutes_trend": {"min": -10, "max": 10},
    "sustainability": {"min": 0, "max": 1},
}

RECENT_GAMES_WINDOW = 5


class PlayerStats(BaseModel):
    player_id: int
    name: str
    team: str
    position: str
    games_played: int
    mins_per_game: float
    pts: float
    reb: float
    ast: float
    stl: float
    blk: float
    turnover: float
    fgm: float
    fga: float
    ftm: float
    fta: float
    fg3m: float
    fg_pct: float
    ft_pct: float

    recent_minutes: Optional[list] = None
    injured_starter_replacement: Optional[bool] = None


def calculate_fppg(player: PlayerStats) -> float:
    total = 0.0
    total += player.pts * ESPN_WEIGHTS["pts"]
    total += player.reb * ESPN_WEIGHTS["reb"]
    total += player.ast * ESPN_WEIGHTS["ast"]
    total += player.stl * ESPN_WEIGHTS["stl"]
    total += player.blk * ESPN_WEIGHTS["blk"]
    total += player.turnover * ESPN_WEIGHTS["turnover"]
    total += player.fgm * ESPN_WEIGHTS["fgm"]
    total += player.fga * ESPN_WEIGHTS["fga"]
    total += player.ftm * ESPN_WEIGHTS["ftm"]
    total += player.fta * ESPN_WEIGHTS["fta"]
    total += player.fg3m * ESPN_WEIGHTS["fg3m"]
    return total


def normalize(value: float, min_val: float, max_val: float) -> float:
    if max_val == min_val:
        return 0.0
    return (value - min_val) / (max_val - min_val) * 100


def calculate_replacement_value(player: PlayerStats) -> float:
    score = 0.0

    if player.injured_starter_replacement:
        score += 50.0

    if player.recent_minutes:
        avg_recent = sum(player.recent_minutes) / len(player.recent_minutes)
        minutes_boost = avg_recent - player.mins_per_game
        if minutes_boost > 0:
            score += minutes_boost * 5

    return score


def calculate_minutes_trend(player: PlayerStats) -> float:
    if not player.recent_minutes:
        return 0.0

    mid = len(player.recent_minutes) // 2
    first_half = player.recent_minutes[:mid]
    second_half = player.recent_minutes[mid:]

    avg_first = sum(first_half) / len(first_half)
    avg_second = sum(second_half) / len(second_half)

    return avg_second - avg_first


def calculate_sustainability(player: PlayerStats) -> float:
    if player.ft_pct is not None:
        ft_pct = player.ft_pct
    else:
        ft_pct = player.fg_pct

    sustainability_score = (player.fg_pct + ft_pct) / 2
    return sustainability_score


def calculate_pickup_score(player: PlayerStats) -> float:
    replacement_raw = calculate_replacement_value(player)
    minutes_raw = calculate_minutes_trend(player)
    sustainability_raw = calculate_sustainability(player)

    replacement_normalized = normalize(
        replacement_raw,
        SIGNAL_RANGES["replacement_value"]["min"],
        SIGNAL_RANGES["replacement_value"]["max"]
    )
    minutes_normalized = normalize(
        minutes_raw,
        SIGNAL_RANGES["minutes_trend"]["min"],
        SIGNAL_RANGES["minutes_trend"]["max"]
    )
    sustainability_normalized = normalize(
        sustainability_raw,
        SIGNAL_RANGES["sustainability"]["min"],
        SIGNAL_RANGES["sustainability"]["max"]
    )

    final_score = (
        replacement_normalized * SIGNAL_WEIGHTS["replacement_value"]
        + minutes_normalized * SIGNAL_WEIGHTS["minutes_trend"]
        + sustainability_normalized * SIGNAL_WEIGHTS["sustainability"]
    )
    return final_score


def adapt_pipeline_row(row) -> PlayerStats:
    return PlayerStats(
        player_id=row["player_id"],
        name=row["player_name"],
        team=row.get("team", "UNKNOWN"),
        position=row.get("position", "UNKNOWN"),
        games_played=row["games_played"],
        mins_per_game=row["min"],
        pts=row["pts"],
        reb=row["reb"],
        ast=row["ast"],
        stl=row["stl"],
        blk=row["blk"],
        turnover=row["turnover"],
        fgm=row["fgm"],
        fga=row["fga"],
        ftm=row["ftm"],
        fta=row["fta"],
        fg3m=row.get("fg3m", 0.0),
        fg_pct=row["fg_pct"],
        ft_pct=row["ft_pct"],
    )