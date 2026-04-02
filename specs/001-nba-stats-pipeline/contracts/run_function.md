# Contract: `run()` Function

**Module**: `pipeline.run`  
**Public via**: `pipeline/__init__.py`

---

## Signature

```python
def run(
    data_types: list[str],
    player_or_team: str,
    season: str | int,
) -> dict[str, dict]:
```

---

## Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `data_types` | `list[str]` | Yes | One or more data types to process. Must be a non-empty list. |
| `player_or_team` | `str` | Yes | Player name (for `game_logs`, `season_averages`), team abbreviation, or game_id string (for `box_scores`). |
| `season` | `str` or `int` | Yes | Season year as used by BallDontLie API (e.g., `2023` for 2023-24 season). |

### Supported values for `data_types` (Phase 1)

| Value | Description |
|-------|-------------|
| `"game_logs"` | Per-game player stats for the given player and season |
| `"box_scores"` | Per-game stats for a team+season or a specific game_id |
| `"season_averages"` | Per-season averages for the given player |

---

## Return Value

A `dict` keyed by each requested data type. Each value is either a **success dict** or a **failure dict**.

### Success dict

```python
{
    "rows_before": int,       # Row count entering clean step
    "rows_after": int,        # Row count in clean output
    "outliers_flagged": int,  # Rows with is_outlier == True
    "corrupted_removed": int, # Rows removed for impossible stat values
    "nulls_found": int,       # Total null cells across all columns
    "file_path": str,         # Absolute path to clean parquet file
}
```

### Failure dict

```python
{
    "error": str,  # Human-readable description of what failed and why
}
```

### Example

```python
run(["game_logs", "box_scores"], "LeBron James", 2023)

# Returns:
{
    "game_logs": {
        "rows_before": 82,
        "rows_after": 78,
        "outliers_flagged": 3,
        "corrupted_removed": 1,
        "nulls_found": 5,
        "file_path": "/abs/path/data/clean/game_logs_lebron_james_2023.parquet"
    },
    "box_scores": {
        "error": "ValidationError: missing required columns: ['min', 'pts']"
    }
}
```

---

## Exceptions (raised before any processing)

These exceptions are raised (not returned) — they indicate a misconfiguration that prevents any data type from running:

| Exception | Trigger |
|-----------|---------|
| `MissingAPIKeyError` | `BALL_IS_LIFE` not found in environment |
| `UnsupportedDataTypeError` | Any entry in `data_types` is not in `SUPPORTED_DATA_TYPES` |
| `ValueError` | `data_types` is empty or not a list |

---

## Guarantees

1. Each data type in `data_types` is processed independently — one failure does not halt others.
2. Raw parquet is written before validation begins. Clean parquet is written only after successful validation and cleaning.
3. No file is partially written. Temp-then-replace strategy ensures the last known good file is never overwritten by a failed write.
4. No sidecar is written unless its paired parquet write succeeded.
5. A per-run log file is created at `logs/run_{YYYYMMDD}_{HHMMSS}.log` for every invocation.
