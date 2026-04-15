"""
Configuration constants for the NBA stats data pipeline.
Single source of truth for all thresholds, column lists, and routing tables.
"""

SUPPORTED_DATA_TYPES = ["game_logs", "box_scores", "game_scores", "season_averages"]

# Identity and modeling keys — always retained regardless of columns filter
IDENTITY_COLUMNS = [
    "player_id",
    "player_name",
    "team_id",
    "team",
    "game_id",
    "date",
    "season",
    "opponent",
]

# Required structural fields per data type — missing any halts that data type
REQUIRED_STRUCTURAL = {
    "game_logs": ["date", "game_id"],        # + player_name or player_id
    "box_scores": ["date", "game_id"],       # + player_name or player_id
    "game_scores": ["date", "game_id"],      # + team or team_id
    "season_averages": ["season"],           # + player_name or player_id
}

# Natural-grain deduplication composite keys — primary preferred, fallback used when primary absent
DEDUP_KEYS = {
    "game_logs": ("player_id", "game_id"),
    "box_scores": ("player_id", "game_id"),
    "game_scores": ("team_id", "game_id"),
    "season_averages": None,                 # skip unless duplicate subject-season rows found
}

DEDUP_FALLBACK_KEYS = {
    "game_logs": ("player_name", "game_id"),
    "box_scores": ("player_name", "game_id"),
    "game_scores": ("team", "game_id"),
    "season_averages": None,
}

# Data types that are time series — sorted by date ASC, game_id ASC before write
TIME_SERIES_TYPES = ["game_logs", "box_scores", "game_scores"]

# Configurable impossible-value thresholds
# upper: value must not exceed this
# lower: value must not fall below this (negative check)
IMPOSSIBLE_VALUE_THRESHOLDS = {
    "upper": {
        "pts": 100,
        "min": 60,
        "reb": 50,
        "ast": 30,
        "stl": 15,
        "blk": 15,
        "games_played": 82,
    },
    "lower": {
        "pts": 0,
        "reb": 0,
        "ast": 0,
        "stl": 0,
        "blk": 0,
        "min": 0,
        "games_played": 0,
    },
}

# Game context fields — preserved or derived when source data permits
CONTEXT_FIELDS = [
    "home_or_away",
    "opponent",
    "won_game",
    "team_score",
    "opponent_score",
    "point_diff",
    "days_rest",
    "back_to_back",
]

# Null placeholder strings — replaced with pd.NA during cleaning
NULL_PLACEHOLDERS = ["", " ", "N/A", "null", "None", "NA", "n/a"]

# Default output directories (relative to project root)
RAW_DIR = "backend/data/raw"
CLEAN_DIR = "backend/data/clean"
LOGS_DIR = "backend/logs"
SCHEMA_BASELINES_DIR = "backend/data/schema_baselines"
