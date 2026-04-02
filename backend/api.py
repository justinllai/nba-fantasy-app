"""
BallDontLie API helpers for the NBA stats data pipeline.
"""
import os
from pathlib import Path

import requests
from dotenv import load_dotenv

from pipeline.exceptions import EntityLookupError, IngestionError

load_dotenv(Path(__file__).parent / ".env")

BASE_URL = "https://api.balldontlie.io/v1"


def _get_headers() -> dict:
    api_key = os.environ.get("BALL_IS_LIFE")
    if not api_key:
        raise IngestionError("BALL_IS_LIFE not set in environment")
    return {"Authorization": api_key}


def load(endpoint: str, params: dict = None) -> list[dict]:
    """Fetch all pages from a BallDontLie API endpoint. Returns list of records."""
    headers = _get_headers()
    url = BASE_URL + endpoint
    all_records = []
    cursor = None

    while True:
        request_params = dict(params or {})
        request_params["per_page"] = 100
        if cursor is not None:
            request_params["cursor"] = cursor

        response = requests.get(url, headers=headers, params=request_params)
        response.raise_for_status()
        data = response.json()

        records = data.get("data", [])
        all_records.extend(records)

        meta = data.get("meta", {})
        cursor = meta.get("next_cursor")
        if cursor is None:
            break

    return all_records


def get_player_id(player_name: str) -> int:
    """Look up player ID by name. Raises EntityLookupError if not found."""
    headers = _get_headers()
    url = BASE_URL + "/players"

    # Split name and search by last name (API search matches partial first or last name)
    parts = player_name.strip().split()
    search_term = parts[-1] if len(parts) > 1 else player_name

    response = requests.get(url, headers=headers, params={"search": search_term, "per_page": 25})
    response.raise_for_status()
    data = response.json()

    players = data.get("data", [])
    if not players:
        raise EntityLookupError(player_name, f"Player not found: '{player_name}'")

    # Try exact full name match
    search_lower = player_name.strip().lower()
    for player in players:
        first = player.get("first_name", "")
        last = player.get("last_name", "")
        full_name = f"{first} {last}".strip().lower()
        if full_name == search_lower:
            return player["id"]

    # Fall back to first result
    return players[0]["id"]


def get_game_logs(player_id: int, season: int) -> list[dict]:
    """Fetch game logs for a player/season. Returns list of game log dicts."""
    return load("/stats", {"player_ids[]": player_id, "seasons[]": season})
