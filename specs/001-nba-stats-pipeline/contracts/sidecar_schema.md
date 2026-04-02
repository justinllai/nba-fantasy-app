# Contract: JSON Sidecar Schema

Each parquet file written by the pipeline has exactly one JSON sidecar written alongside it. Raw and clean sidecars have different schemas.

---

## Raw Sidecar Schema

Written to `data/raw/{data_type}_{player_or_team}_{season}.json` immediately after the raw parquet write succeeds.

```json
{
  "data_type":      "string — one of: game_logs, box_scores, season_averages",
  "player_or_team": "string — as passed to run()",
  "season":         "string or integer — as passed to run()",
  "rows":           "integer — row count in raw parquet",
  "columns":        ["string", "..."],
  "written_at":     "string — ISO-8601 datetime, e.g. 2026-03-31T14:22:05"
}
```

### Example

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

## Clean Sidecar Schema

Written to `data/clean/{data_type}_{player_or_team}_{season}.json` immediately after the clean parquet write succeeds.

```json
{
  "data_type":         "string",
  "player_or_team":    "string",
  "season":            "string or integer",
  "rows_before":       "integer — row count entering clean step",
  "rows_after":        "integer — row count in clean parquet",
  "columns":           ["string", "..."],
  "nulls_found":       "integer — total null cells across all columns",
  "outliers_flagged":  "integer — rows where is_outlier == true",
  "corrupted_removed": "integer — rows removed for impossible stat values",
  "dedup_skipped":     "boolean — true if game_id column was absent",
  "dedup_reason":      "string or null — reason dedup was skipped, null if dedup ran",
  "written_at":        "string — ISO-8601 datetime"
}
```

### Example (dedup ran normally)

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

### Example (dedup skipped)

```json
{
  "data_type": "season_averages",
  "player_or_team": "LeBron James",
  "season": "2023",
  "rows_before": 1,
  "rows_after": 1,
  "columns": ["player_id", "player_name", "season", "pts", "reb", "ast", "stl", "blk", "min", "is_outlier"],
  "nulls_found": 0,
  "outliers_flagged": 0,
  "corrupted_removed": 0,
  "dedup_skipped": true,
  "dedup_reason": "game_id column not present in season_averages dataset",
  "written_at": "2026-03-31T14:22:10"
}
```

---

## Guarantees

1. A sidecar is **never** written unless its paired parquet write completed successfully.
2. If the parquet write fails, any partial sidecar file is deleted.
3. Sidecars are overwritten atomically (temp-then-replace) along with their parquets.
4. There is **exactly one** sidecar per parquet — never zero, never two.
