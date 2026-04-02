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
    player_or_team="LeBron James",
    season=2023,
)

print(result["game_logs"]["rows_after"])   # e.g., 78
print(result["game_logs"]["file_path"])    # data/clean/game_logs_lebron_james_2023.parquet
```

---

## Multiple Data Types in One Call

```python
result = run(
    data_types=["game_logs", "season_averages"],
    player_or_team="Stephen Curry",
    season=2023,
)

for dtype, summary in result.items():
    if "error" in summary:
        print(f"{dtype} FAILED: {summary['error']}")
    else:
        print(f"{dtype}: {summary['rows_after']} rows → {summary['file_path']}")
```

---

## Box Scores — Team + Season

```python
result = run(
    data_types=["box_scores"],
    player_or_team="LAL",    # Team abbreviation
    season=2023,
)
```

## Box Scores — Specific Game

```python
result = run(
    data_types=["box_scores"],
    player_or_team="18370647",   # game_id as a string of digits
    season=2023,
)
```

*Detection rule*: if `player_or_team` is parseable as an integer, it is treated as a `game_id`.

---

## Output Files

After a successful run the following files are written (and old files replaced):

```
backend/data/raw/game_logs_lebron_james_2023.parquet
backend/data/raw/game_logs_lebron_james_2023.json
backend/data/clean/game_logs_lebron_james_2023.parquet
backend/data/clean/game_logs_lebron_james_2023.json
backend/logs/run_20260331_142205.log
```

---

## Reading Output

```python
import pandas as pd
import json

df = pd.read_parquet("backend/data/clean/game_logs_lebron_james_2023.parquet")

with open("backend/data/clean/game_logs_lebron_james_2023.json") as f:
    meta = json.load(f)

print(meta["rows_after"], meta["outliers_flagged"])
print(df[df["is_outlier"]].head())
```

---

## Error Handling

```python
from pipeline.exceptions import MissingAPIKeyError, UnsupportedDataTypeError

try:
    result = run(["game_logs"], "LeBron James", 2023)
except MissingAPIKeyError:
    print("Set BALL_IS_LIFE in backend/.env")
except UnsupportedDataTypeError as e:
    print(f"Bad data type: {e}")

# Check per-type failures without exceptions:
if "error" in result.get("game_logs", {}):
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
