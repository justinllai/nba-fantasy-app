from pydantic import BaseModel
from typing import Optional

ESPN_WEIGHTS = {
    "points": 1.0,
    "rebounds": 1.0,
    "assists": 2.0,
    "steals": 4.0,
    "blocks": 4.0,
    "turnovers": -2.0,
    "fg_made": 2.0,
    "fg_attempt": -1.0,
    "ft_made": 1.0,
    "ft_attempt": -1.0,
    "three_made": 1.0
}

SIGNAL_WEIGHTS = {
    "replacement_value": 0.33,
    "minutes_trend": 0.33,
    "sustainability": 0.34
}

SIGNAL_RANGES = {
    "replacement_value": {"min": 0, "max": 50},
    "minutes_trend":     {"min": -10, "max": 10},
    "sustainability":    {"min": 0, "max": 1},
}

RECENT_GAMES_WINDOW = 5

class PlayerStats(BaseModel):
    player_id: int
    name: str
    team: str
    position: str
    games_played: int
    mins_per_game:float
    points: float
    rebounds: float
    assists: float
    steals: float
    blocks: float
    turnovers: float
    fg_made: float
    fg_attempt: float
    ft_made: float
    ft_attempt: float
    three_made: float

    recent_minutes: Optional[list] = None
    injured_starter_replacement: Optional[bool] = None


def calculate_fppg(player: PlayerStats) -> float:
    total = 0.0
    total += player.points * ESPN_WEIGHTS["points"]
    total += player.rebounds * ESPN_WEIGHTS["rebounds"]
    total += player.assists * ESPN_WEIGHTS["assists"]
    total += player.steals * ESPN_WEIGHTS["steals"]
    total += player.blocks * ESPN_WEIGHTS["blocks"]
    total += player.turnovers * ESPN_WEIGHTS["turnovers"]
    total += player.fg_made * ESPN_WEIGHTS["fg_made"]
    total += player.fg_attempt * ESPN_WEIGHTS["fg_attempt"]
    total += player.ft_made * ESPN_WEIGHTS["ft_made"]
    total += player.ft_attempt * ESPN_WEIGHTS["ft_attempt"]
    total += player.three_made * ESPN_WEIGHTS["three_made"]
    
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
    
    first_half = player.recent_minutes[:2]
    second_half = player.recent_minutes[2:]
    
    avg_first = sum(first_half) / len(first_half)
    avg_second = sum(second_half) / len(second_half)
    
    return avg_second - avg_first

def calculate_sustainability(player: PlayerStats) -> float:
    if player.ft_attempt > 0:
        ft_pct = player.ft_made / player.ft_attempt
    else:
        ft_pct = player.fg_made / player.fg_attempt

    sustainability_score = (player.fg_made / player.fg_attempt + ft_pct) / 2
    return sustainability_score

def calculate_pickup_score(player: PlayerStats) -> float:
    replacement_raw = calculate_replacement_value(player)
    minutes_raw = calculate_minutes_trend(player)
    sustainability_raw = calculate_sustainability(player)

    replacement_normalized = normalize(replacement_raw, SIGNAL_RANGES["replacement_value"]["min"], SIGNAL_RANGES["replacement_value"]["max"])
    minutes_normalized = normalize(minutes_raw, SIGNAL_RANGES["minutes_trend"]["min"], SIGNAL_RANGES["minutes_trend"]["max"])
    sustainability_normalized = normalize(sustainability_raw, SIGNAL_RANGES["sustainability"]["min"], SIGNAL_RANGES["sustainability"]["max"])

    final_score = (replacement_normalized * SIGNAL_WEIGHTS["replacement_value"]) + (minutes_normalized * SIGNAL_WEIGHTS["minutes_trend"]) + (sustainability_normalized * SIGNAL_WEIGHTS["sustainability"])
    return final_score