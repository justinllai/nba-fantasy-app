# Data Model: NBA Stats Data Pipeline

**Branch**: `001-nba-stats-pipeline` | **Updated**: 2026-04-14

---

## Entities

### 1. PipelineRun

Represents one invocation of `run()`. Not persisted to disk — exists in memory for the duration of the call.

| Field | Type | Description |
|-------|------|-------------|
| `data_types` | `list[str]` | Requested data types |
| `player` | `str \| int \| None` | Player name or ID (mutually exclusive with `team`) |
| `team` | `str \| int \| None` | Team abbreviation or ID (mutually exclusive with `player`) |
| `season` | `int` | Season year |
| `columns` | `list[str] \| None` | Caller-specified stat columns to retain; `None` = keep all |
| `output_dir` | `Path` | Root output directory |
| `features_config` | `dict \| None` | Feature engineering config; `None` = skip |
| `labels_config` | `dict \| None` | Label generation config; `None` = skip |
| `run_id` | `str` | Timestamp-based ID: `YYYYMMDD_HHMMSS` |
| `results` | `dict[str, dict]` | Per-data-type summary dicts keyed by data type |

**Validation rules**:
- `data_types` must be a non-empty list
- `player` and `team` are mutually exclusive — providing both raises `PipelineConfigError`
- `BALL_IS_LIFE` must be present in environment before any processing

---

### 2. RawDataset

DataFrame produced by ingestion. Persisted as raw parquet + `.sidecar.json`.

| Field | Type | Description |
|-------|------|-------------|
| `data_type` | `str` | One of the four supported types |
| `player` | `str \| None` | Player subject |
| `team` | `str \| None` | Team subject |
| `season` | `int` | Season year |
| `df` | `pd.DataFrame` | Raw DataFrame after ingestion |
| `rows` | `int` | Row count at time of write |
| `columns` | `list[str]` | All column names captured from API |
| `written_at` | `str` | ISO-8601 timestamp |

---

### 3. CleanDataset

DataFrame produced by the clean step. Persisted as clean parquet + `.sidecar.json`.

| Field | Type | Description |
|-------|------|-------------|
| `df` | `pd.DataFrame` | Cleaned DataFrame with `is_outlier` column added |
| `rows_before` | `int` | Row count entering clean step |
| `rows_after` | `int` | Row count after cleaning |
| `outliers_flagged` | `int` | Rows where `is_outlier == True` |
| `corrupted_removed` | `int` | Rows removed for impossible stat values |
| `nulls_found` | `int` | Total null cell count across all columns |
| `dedup_skipped` | `bool` | `True` if deduplication was skipped |
| `dedup_reason` | `str \| None` | Reason dedup was skipped, or `null` |
| `dedup_conflicts` | `int` | Count of rows kept due to conflict resolution |
| `thresholds_applied` | `dict` | The active impossible-value thresholds used |

---

### 4. GameLogRecord

One row in a `game_logs` or `box_scores` dataset. One player's stats in one game.

| Column | Dtype | Description |
|--------|-------|-------------|
| `game_id` | `int32` | Unique game identifier |
| `player_id` | `int32` | Player identifier |
| `player_name` | `str` | Title-cased player name |
| `team` | `str` | Uppercase team abbreviation |
| `date` | `str` | `YYYY-MM-DD` |
| `min` | `float32` | Minutes played as decimal |
| `pts` | `int8` | Points |
| `reb` | `int8` | Total rebounds |
| `ast` | `int8` | Assists |
| `stl` | `int8` | Steals |
| `blk` | `int8` | Blocks |
| `is_outlier` | `bool` | `True` if any stat outside Tukey fence |
| *(all other API fields)* | varies | Captured but not required |

**Constraints**:
- DNP rows (`min == 0`) removed during cleaning
- Natural-grain dedup: `player_id + game_id` (fallback: `player_name + game_id`)
- Corruption thresholds: `pts > 100`, `min > 60`, `reb > 50`, `ast > 30`, `stl/blk > 15`
- Negative stats flagged as corrupted

---

### 5. SeasonAverageRecord

One row in a `season_averages` dataset.

| Column | Dtype | Description |
|--------|-------|-------------|
| `player_id` | `int32` | Player identifier |
| `player_name` | `str` | Title-cased player name |
| `season` | `int16` | Season year |
| `pts` | `float32` | Points per game |
| `reb` | `float32` | Rebounds per game |
| `ast` | `float32` | Assists per game |
| `stl` | `float32` | Steals per game |
| `blk` | `float32` | Blocks per game |
| `min` | `float32` | Minutes per game |
| `is_outlier` | `bool` | Outlier flag |

**Constraints**:
- No `game_id` → dedup skipped, `dedup_skipped: true` in sidecar
- No minimum row count requirement

---

### 6. FeatureRecord

One row in a derived features dataset (optional layer). Extends CleanDataset columns.

| Column | Dtype | Description |
|--------|-------|-------------|
| `rolling_pts_N` | `float32` | Rolling avg points over prior N games |
| `rolling_reb_N` | `float32` | Rolling avg rebounds over prior N games |
| `rolling_ast_N` | `float32` | Rolling avg assists over prior N games |
| `pts_std_N` | `float32` | Rolling std dev of points |
| `pts_delta` | `float32` | Last game pts minus rolling avg |
| `fantasy_pts` | `float32` | Fantasy points under scoring config |
| `hot_streak` | `bool` | Player above rolling avg in last 3 games |
| `home_or_away` | `str` | `"H"` or `"A"` |
| `days_rest` | `int8` | Days since last game |
| `back_to_back` | `bool` | `True` if 0 days rest |

**Leakage rule**: All rolling columns computed using `.shift(1)` before `.rolling()`. Rows with fewer prior observations than `min_observations` are `null`.

---

### 7. LabelRecord

Labels appended to FeatureRecord for supervised learning. One label per target.

| Column | Dtype | Description |
|--------|-------|-------------|
| `next_game_pts` | `float32` | Points in next game (`null` for last game) |
| `next_game_reb` | `float32` | Rebounds in next game |
| `next_game_ast` | `float32` | Assists in next game |
| `next_game_fantasy_pts` | `float32` | Fantasy points in next game |
| `pts_20_plus` | `bool` | `True` if next game pts ≥ 20 |
| `is_end_of_series` | `bool` | `True` if no next game exists (label = `null`) |

**End-of-series rule**: Last game per player has no next game → all labels `null`, `is_end_of_series: True`. These rows are excluded from training splits by default.

---

### 8. BaseSidecar

JSON file written alongside each parquet (`{name}.sidecar.json`).

```json
{
  "data_type": "game_logs",
  "player": "LeBron James",
  "team": null,
  "season": 2023,
  "rows_before": 82,
  "rows_after": 79,
  "columns": ["game_id", "player_id", "player_name", "team", "date", "min", "pts", "reb", "ast", "stl", "blk", "is_outlier"],
  "nulls_found": 5,
  "outliers_flagged": 3,
  "corrupted_removed": 0,
  "dedup_skipped": false,
  "dedup_reason": null,
  "dedup_conflicts": 0,
  "thresholds_applied": {
    "upper": {"pts": 100, "min": 60, "reb": 50, "ast": 30, "stl": 15, "blk": 15},
    "lower": {"pts": 0, "reb": 0, "ast": 0, "stl": 0, "blk": 0, "min": 0}
  },
  "schema_drift": {
    "first_run": false,
    "columns_added": [],
    "columns_missing": [],
    "identity_columns_missing": []
  },
  "written_at": "2026-04-14T10:22:07"
}
```

---

### 9. FeatureSidecar

JSON file written alongside feature parquet (`{name}.features.sidecar.json`). Only written when `features_config` is provided.

```json
{
  "feature_names": ["rolling_pts_5", "rolling_reb_5", "pts_delta", "fantasy_pts"],
  "rolling_windows": [5, 10],
  "min_observations": 5,
  "scoring_config": {"pts": 1.0, "reb": 1.2, "ast": 1.5, "stl": 3.0, "blk": 3.0},
  "feature_schema_version": "a3f9c2e10b41",
  "generated_at": "2026-04-14T10:22:15",
  "success": true,
  "end_of_series_rows": 1,
  "labels_generated": ["next_game_pts", "pts_20_plus"]
}
```

---

## State Transitions

```
API Response
    │
    ▼ fetch()
RawDataset (all API fields captured)
    │
    ├─► write_parquet(raw) → write_sidecar(raw.sidecar.json)
    │
    ▼ validate()       ← ValidationError halts this data type
RawDataset (unchanged)
    │
    ▼ deduplicate()    ← natural-grain composite keys; conflict resolution logged
    │
    ▼ clean()          ← 10 transforms + column filter applied
    │
    ▼ flag_outliers()  ← is_outlier column added
    │
    ▼ check_drift()    ← baseline written or compared
    │
    ▼ write_parquet(clean) → write_sidecar(clean.sidecar.json)
    │
    ├─► [Optional] engineer() → write_parquet(features) → write_sidecar(features.sidecar.json)
    │
    ├─► [Optional] generate_labels() → labels appended → sidecar updated
    │
    ▼
Summary dict entry for this data type
```

---

## File Naming Convention

| Output | Path |
|--------|------|
| Raw parquet | `{output_dir}/raw/{data_type}_{subject}_{season}.parquet` |
| Raw sidecar | `{output_dir}/raw/{data_type}_{subject}_{season}.sidecar.json` |
| Clean parquet | `{output_dir}/clean/{data_type}_{subject}_{season}.parquet` |
| Clean sidecar | `{output_dir}/clean/{data_type}_{subject}_{season}.sidecar.json` |
| Feature sidecar | `{output_dir}/clean/{data_type}_{subject}_{season}.features.sidecar.json` |
| Schema baseline | `{output_dir}/schema_baselines/{data_type}.json` |
| Run log | `logs/pipeline.log` |
| Temp (in-flight) | `{target_path}.tmp` — deleted after atomic replace |

**Normalization**: `subject` in filenames = player name or team abbreviation with spaces → underscores, lowercased.
