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
    "replacement": 0.33,
    "minutes_trend": 0.33,
    "sustainability": 0.34
}

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