# Quickstart: NBA Stats Data Pipeline

## Prerequisites

1. Python 3.11+
2. `BALL_IS_LIFE` API key in `backend/.env`
3. Dependencies installed: `pip install -r backend/requirements.txt`

---

## Basic Usage

```python
from pipeline import run

# Fetch game logs for one player
result = run(
    data_types=["game_logs"],
    season=2023,
    player="LeBron James",
)

print(result["game_logs"]["status"])       # "success"
print(result["game_logs"]["rows_after"])   # e.g., 78
print(result["game_logs"]["file_path"])    # data/clean/game_logs_lebron_james_2023.parquet
```

---

## Multiple Data Types in One Call

```python
result = run(
    data_types=["game_logs", "season_averages"],
    season=2023,
    player="Stephen Curry",
)

for dtype, summary in result.items():
    if summary["status"] == "failed":
        print(f"{dtype} FAILED: {summary['error']}")
    else:
        print(f"{dtype}: {summary['rows_after']} rows → {summary['file_path']}")
```

---

## Box Scores — By Team

```python
result = run(
    data_types=["box_scores"],
    season=2023,
    team="LAL",    # Team abbreviation or name
)
```

## Game Scores — By Team

```python
result = run(
    data_types=["game_scores"],
    season=2023,
    team="LAL",
)
```

---

## Selecting Specific Columns

```python
# Get only pts and reb — identity columns (player_id, game_id, date, etc.) always retained
result = run(
    data_types=["game_logs"],
    season=2023,
    player="LeBron James",
    columns=["pts", "reb", "ast"],
)
```

Unrecognized column names are logged as WARNING and silently skipped — the run never fails because of a bad column name.

---

## Custom Output Directory

```python
result = run(
    data_types=["game_logs"],
    season=2023,
    player="LeBron James",
    output_dir="my_data/",   # creates my_data/raw/, my_data/clean/, my_data/logs/
)
```

---

## Feature Engineering

```python
features_config = {
    "rolling_windows": [3, 5, 10],    # game rolling windows
    "min_observations": 3,            # null below this many prior games
    "scoring": {                      # weights for fantasy_pts composite
        "pts": 1.0,
        "reb": 1.2,
        "ast": 1.5,
        "stl": 3.0,
        "blk": 3.0,
    },
}

result = run(
    data_types=["game_logs"],
    season=2023,
    player="LeBron James",
    features_config=features_config,
)
# Clean parquet now includes rolling_pts_3, rolling_pts_5, fantasy_pts, etc.
# result["game_logs"]["feature_schema_version"] holds the deterministic version hash
```

---

## Label Generation

```python
labels_config = {"targets": ["pts", "reb"]}

result = run(
    data_types=["game_logs"],
    season=2023,
    player="LeBron James",
    features_config=features_config,
    labels_config=labels_config,
)
# Clean parquet now includes next_game_pts, next_game_reb, is_end_of_series
```

---

## Output Files

After a successful run the following files are written:

```
data/raw/game_logs_lebron_james_2023.parquet
data/raw/game_logs_lebron_james_2023.sidecar.json
data/clean/game_logs_lebron_james_2023.parquet
data/clean/game_logs_lebron_james_2023.sidecar.json
data/clean/game_logs_lebron_james_2023.features.sidecar.json   # only if features_config provided
data/logs/run_20260415_142205_123456.log
data/schema_baselines/game_logs.json                            # written on first run
```

---

## Reading Output

```python
import pandas as pd
import json

df = pd.read_parquet("data/clean/game_logs_lebron_james_2023.parquet")

with open("data/clean/game_logs_lebron_james_2023.sidecar.json") as f:
    meta = json.load(f)

print(meta["rows_after"], meta["dedup_conflicts"])
print(meta["schema_drift"])          # drift info vs. first-run baseline
print(meta["thresholds_applied"])    # impossible-value thresholds that were active

# Outlier-flagged rows are never removed — only marked
print(df[df["is_outlier"]].head())
```

---

## Error Handling

```python
from pipeline.exceptions import APIKeyMissingError, UnsupportedDataTypeError, PipelineConfigError

try:
    result = run(["game_logs"], season=2023, player="LeBron James")
except APIKeyMissingError:
    print("Set BALL_IS_LIFE in backend/.env")
except UnsupportedDataTypeError as e:
    print(f"Bad data type: {e}")
except PipelineConfigError as e:
    print(f"Config error: {e}")   # e.g., both player and team provided

# Check per-type failures without exceptions:
if result["game_logs"]["status"] == "failed":
    print("game_logs failed:", result["game_logs"]["error"])
```

---

## Running Tests

```bash
cd backend
pytest tests/unit/
pytest tests/integration/
pytest tests/file_output/
```
