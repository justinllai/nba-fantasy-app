# Contract: `run()` Function

**Module**: `pipeline.run`  
**Public via**: `pipeline/__init__.py`  
**Spec Version**: 2026-04-14

---

## Signature

```python
def run(
    data_types: list[str],
    season: int,
    player: str | int | None = None,
    team: str | int | None = None,
    columns: list[str] | None = None,
    output_dir: str | Path = "data/",
    features_config: dict | None = None,
    labels_config: dict | None = None,
) -> dict[str, dict]:
```

---

## Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `data_types` | `list[str]` | Yes | One or more data types to process. Must be a non-empty list. |
| `season` | `int` | Yes | Season year as used by BallDontLie API (e.g., `2023` for 2023-24). |
| `player` | `str \| int \| None` | One of player/team | Player name for `game_logs` / `season_averages`. Mutually exclusive with `team`. |
| `team` | `str \| int \| None` | One of player/team | Team abbreviation for `box_scores` / `game_scores`. Mutually exclusive with `player`. |
| `columns` | `list[str] \| None` | No | Stat columns to retain in clean output. Identity columns always kept. If `None`, all API columns kept. |
| `output_dir` | `str \| Path` | No | Root output directory. Resolves `raw/`, `clean/`, `schema_baselines/` from it. Defaults to `"data/"`. |
| `features_config` | `dict \| None` | No | Feature engineering config. If `None`, feature layer is skipped entirely. |
| `labels_config` | `dict \| None` | No | Label generation config. If `None`, label layer is skipped entirely. |

### Mutual exclusivity rule
If both `player` and `team` are provided, `PipelineConfigError` is raised immediately before any I/O.

### Supported `data_types`

| Value | Requires | Description |
|-------|----------|-------------|
| `"game_logs"` | `player` | Per-game player stats for the given player and season |
| `"box_scores"` | `team` | Per-game stats for a team+season |
| `"game_scores"` | `team` | Game results (scores, date, status) for a team+season |
| `"season_averages"` | `player` | Per-season averages for the given player |

### Identity columns (always retained regardless of `columns` arg)
`player_id`, `player_name`, `team_id`, `team`, `game_id`, `date`, `season`, `opponent`

---

## Return Value

A `dict` keyed by each requested data type. Each value is either a **success dict** or a **failure dict**.

### Success dict

```python
{
    "status": "success",
    "rows_before": int,        # Row count entering clean step
    "rows_after": int,         # Row count in clean output
    "outliers_flagged": int,   # Rows with is_outlier == True
    "corrupted_removed": int,  # Rows removed for impossible stat values
    "nulls_found": int,        # Total null cells across all columns
    "file_path": str,          # Absolute path to clean parquet file
}
```

### Failure dict

```python
{
    "status": "failed",
    "error": str,  # Human-readable description of what failed and why
}
```

### Example

```python
result = run(
    data_types=["game_logs", "season_averages"],
    season=2023,
    player="LeBron James",
    columns=["pts", "reb", "ast"],
)

# Returns:
{
    "game_logs": {
        "status": "success",
        "rows_before": 82,
        "rows_after": 79,
        "outliers_flagged": 3,
        "corrupted_removed": 0,
        "nulls_found": 5,
        "file_path": "/abs/path/data/clean/game_logs_lebron_james_2023.parquet"
    },
    "season_averages": {
        "status": "failed",
        "error": "ValidationError: missing required column: 'season'"
    }
}
```

---

## File Outputs

Three distinct sidecar types — never merged:

| File | When Written |
|------|-------------|
| `{name}.parquet` | Every successful run |
| `{name}.sidecar.json` | After parquet is confirmed written (FR-025) |
| `{name}.features.sidecar.json` | Only when `features_config` is provided and features succeed |

---

## Exceptions (raised before any processing)

| Exception | Trigger |
|-----------|---------|
| `PipelineConfigError` | `data_types` empty, not a list, or both `player` and `team` provided |
| `APIKeyMissingError` | `BALL_IS_LIFE` not found in environment |
| `UnsupportedDataTypeError` | Any entry in `data_types` not in `SUPPORTED_DATA_TYPES` |
| `FeatureVersionMismatchError` | Stored model version does not match current feature schema version |

---

## Guarantees

1. Each data type in `data_types` is processed independently — one failure does not halt others.
2. Raw parquet is written before validation begins. Clean parquet is written only after successful validation and cleaning.
3. No file is partially written. Temp-then-replace strategy ensures the last known good file is never overwritten by a failed write.
4. No sidecar is written unless its paired parquet write succeeded.
5. Identity columns (`player_id`, `player_name`, `team_id`, `team`, `game_id`, `date`, `season`, `opponent`) are always retained in the clean output regardless of the `columns` argument.
6. Unrecognized column names in `columns` produce a WARNING and are silently ignored — no exception raised.
7. Features and labels are only generated after the base clean parquet is confirmed written.
8. All rolling feature computations use `.shift(1)` before `.rolling()` — no current-row data contaminates any window.
