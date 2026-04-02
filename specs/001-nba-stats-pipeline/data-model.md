# Data Model: NBA Stats Data Pipeline

**Branch**: `001-nba-stats-pipeline` | **Date**: 2026-03-31

---

## Entities

### 1. PipelineRun

Represents one invocation of `run()`. Not persisted to disk — exists in memory for the duration of the call.

| Field | Type | Description |
|-------|------|-------------|
| `data_types` | `list[str]` | Requested data types (e.g., `["game_logs", "box_scores"]`) |
| `player_or_team` | `str` | Player name or team abbreviation or game_id string |
| `season` | `str` or `int` | Season identifier (e.g., `2023` for 2023-24) |
| `run_id` | `str` | Timestamp-based ID used for the log filename: `YYYYMMDD_HHMMSS` |
| `results` | `dict[str, dict]` | Per-data-type summary dicts (keyed by data type) |

**Validation rules**:
- `data_types` must be a non-empty list
- Each entry in `data_types` must be in `SUPPORTED_DATA_TYPES`
- `BALL_IS_LIFE` must be present in the environment before any processing

---

### 2. RawDataset

A DataFrame produced by ingestion for one data type. Held in memory; persisted as a raw parquet + sidecar.

| Field | Type | Description |
|-------|------|-------------|
| `data_type` | `str` | One of the supported data types |
| `player_or_team` | `str` | Lookup subject as passed to `run()` |
| `season` | `str` | Season string |
| `df` | `pd.DataFrame` | The raw DataFrame after column drops and downcasting |
| `rows` | `int` | Row count at time of raw write |
| `columns` | `list[str]` | Column names at time of raw write |
| `written_at` | `str` | ISO-8601 timestamp of write |

**Validation rules** (applied before clean step):
- All columns in `REQUIRED_COLUMNS[data_type]` must be present
- Dtypes must match `EXPECTED_DTYPES[data_type]` or be safely castable (see research.md)
- For `game_logs` only: row count ≥ 10

---

### 3. CleanDataset

A DataFrame produced by the clean step. Persisted as a clean parquet + sidecar.

| Field | Type | Description |
|-------|------|-------------|
| `df` | `pd.DataFrame` | Cleaned DataFrame with `is_outlier` column added |
| `rows_before` | `int` | Row count entering the clean step |
| `rows_after` | `int` | Row count after cleaning |
| `outliers_flagged` | `int` | Rows where `is_outlier == True` |
| `corrupted_removed` | `int` | Rows removed for exceeding impossible-stat thresholds |
| `nulls_found` | `int` | Total null cell count across all columns |
| `dedup_skipped` | `bool` | `True` if deduplication was skipped (missing `game_id`) |
| `dedup_reason` | `str` or `null` | Human-readable reason dedup was skipped, or `null` |

---

### 4. GameLogRecord

One row in a `game_logs` or `box_scores` dataset. Represents a single player's stats in a single game.

| Column | Dtype (target) | Description |
|--------|---------------|-------------|
| `game_id` | `int32` | Unique game identifier (used for dedup) |
| `player_id` | `int32` | Player identifier |
| `player_name` | `str` | Title-cased player name |
| `team` | `str` | Uppercase team abbreviation (e.g., `"LAL"`) |
| `date` | `str` | ISO date string `YYYY-MM-DD` |
| `min` | `float32` | Minutes played as decimal (e.g., `32.75`) |
| `pts` | `int8` | Points scored |
| `reb` | `int8` | Total rebounds |
| `ast` | `int8` | Assists |
| `stl` | `int8` | Steals |
| `blk` | `int8` | Blocks |
| `is_outlier` | `bool` | `True` if row is a statistical outlier (Tukey fence, 1.5× IQR) |

**Constraints**:
- DNP rows (`min == 0`) removed during cleaning
- Duplicate rows by `game_id` removed (if `game_id` present)
- Corrupted rows removed: `pts > 100`, `min > 60`, `reb > 50`, `ast > 30`, `stl > 15`, `blk > 15`
- `is_outlier` is added during clean step — not present in raw dataset

---

### 5. SeasonAverageRecord

One row in a `season_averages` dataset. Represents a player's per-game averages for a full season.

| Column | Dtype (target) | Description |
|--------|---------------|-------------|
| `player_id` | `int32` | Player identifier |
| `player_name` | `str` | Title-cased player name |
| `season` | `int16` | Season year (e.g., `2023`) |
| `pts` | `float32` | Points per game |
| `reb` | `float32` | Rebounds per game |
| `ast` | `float32` | Assists per game |
| `stl` | `float32` | Steals per game |
| `blk` | `float32` | Blocks per game |
| `min` | `float32` | Minutes per game |
| `is_outlier` | `bool` | Outlier flag (added during clean step) |

**Constraints**:
- No minimum row count requirement
- No `game_id` column → dedup skipped, `dedup_skipped: true` in sidecar

---

### 6. RawSidecar

JSON file written alongside each raw parquet.

```json
{
  "data_type": "game_logs",
  "player_or_team": "LeBron James",
  "season": "2023",
  "rows": 82,
  "columns": ["game_id", "player_name", "date", "min", "pts", "reb", "ast", "stl", "blk"],
  "written_at": "2026-03-31T14:22:05"
}
```

---

### 7. CleanSidecar

JSON file written alongside each clean parquet. Superset of RawSidecar fields.

```json
{
  "data_type": "game_logs",
  "player_or_team": "LeBron James",
  "season": "2023",
  "rows_before": 82,
  "rows_after": 78,
  "columns": ["game_id", "player_name", "date", "min", "pts", "reb", "ast", "stl", "blk", "is_outlier"],
  "nulls_found": 5,
  "outliers_flagged": 3,
  "corrupted_removed": 1,
  "dedup_skipped": false,
  "dedup_reason": null,
  "written_at": "2026-03-31T14:22:07"
}
```

---

## State Transitions

```
API Response
    │
    ▼ ingest()
RawDataset (df after column drops + downcast + null normalization)
    │
    ├─► save_raw() → data/raw/{filename}.parquet + .json
    │
    ▼ validate()
RawDataset (unchanged df)  ← ValidationError halts this data type
    │
    ▼ clean()
CleanDataset (df + metrics)
    │
    ▼ save_clean() → data/clean/{filename}.parquet + .json
    │
    ▼
Summary dict entry for this data type
```

---

## File Naming Convention

| Output | Path |
|--------|------|
| Raw parquet | `data/raw/{data_type}_{player_or_team}_{season}.parquet` |
| Raw sidecar | `data/raw/{data_type}_{player_or_team}_{season}.json` |
| Clean parquet | `data/clean/{data_type}_{player_or_team}_{season}.parquet` |
| Clean sidecar | `data/clean/{data_type}_{player_or_team}_{season}.json` |
| Run log | `logs/run_{YYYYMMDD}_{HHMMSS}.log` |
| Temp (in-flight) | `{target_path}.tmp` — deleted after atomic replace |

**Normalization**: `player_or_team` in filenames has spaces replaced with underscores and is lowercased (e.g., `"LeBron James"` → `"lebron_james"`).
