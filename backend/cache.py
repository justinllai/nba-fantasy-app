from datetime import datetime, timedelta

_cache = {}

def get_cached_players():
    if not _cache:
        return None
    elif (datetime.now() - _cache["timestamp"]) > timedelta(hours=1):
        return None
    else:
        return _cache["data"]
    
def set_cached_players(players):
    _cache["data"] = players
    _cache["timestamp"] = datetime.now()