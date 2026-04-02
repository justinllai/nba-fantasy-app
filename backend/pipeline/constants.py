"""
Constants for the NBA stats data pipeline.
"""

SUPPORTED_DATA_TYPES = ["game_logs", "box_scores", "game_scores", "season_averages"]

REQUIRED_COLUMNS = {
    "game_logs": ["game_id", "player_name", "date", "min", "pts", "reb", "ast", "stl", "blk"],
    "box_scores": ["game_id", "player_name", "team", "date", "min", "pts", "reb", "ast", "stl", "blk"],
    "game_scores": ["game_id", "home_team", "visitor_team", "home_team_score", "visitor_team_score", "date", "status"],
    "season_averages": ["player_name", "season", "pts", "reb", "ast", "stl", "blk", "min"],
}

EXPECTED_DTYPES = {
    "game_logs": {
        "game_id": "int",
        "pts": "int",
        "reb": "int",
        "ast": "int",
        "stl": "int",
        "blk": "int",
        "min": "float",
    },
    "box_scores": {
        "game_id": "int",
        "pts": "int",
        "reb": "int",
        "ast": "int",
        "stl": "int",
        "blk": "int",
        "min": "float",
    },
    "game_scores": {
        "game_id": "int",
        "home_team_score": "int",
        "visitor_team_score": "int",
    },
    "season_averages": {
        "pts": "float",
        "reb": "float",
        "ast": "float",
        "stl": "float",
        "blk": "float",
        "min": "float",
    },
}

UNUSED_COLUMNS = {
    "game_logs": ["player_id", "fg3_pct", "fg_pct", "ft_pct", "turnover"],
    "box_scores": ["fg3_pct", "fg_pct", "ft_pct", "turnover"],
    "game_scores": ["period", "postseason", "season"],
    "season_averages": ["games_played", "fg3_pct", "fg_pct", "ft_pct", "turnover"],
}

CORRUPTION_THRESHOLDS = {
    "pts": 100,
    "min": 60,
    "reb": 50,
    "ast": 30,
    "stl": 15,
    "blk": 15,
}

NULL_PLACEHOLDERS = ["", " ", "N/A", "null", "None"]

RAW_DIR = "backend/data/raw"
CLEAN_DIR = "backend/data/clean"
LOGS_DIR = "backend/logs"
