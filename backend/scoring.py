from pydantic import BaseModel
from typing import Optional

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
