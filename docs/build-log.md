# NBA Fantasy App — Build Log

## Session 1 — 03/16/26 Justin
**What we built:** Ran speckit/spec & speckit/plan to generate full technical blueprint before writing any code.

**Why:** Never code without a plan. Speckit reads your feature description and generates a plan automatically.

**Key decisions:**
- In-memory cache over PostgreSQL — goal is to prove signal engine works, not manage a database yet
- nba_api (free, no API key) over paid APIs like SportRadar — same data source, no auth complexity
- 2 bulk API calls over 500 individual calls — rate limiting would block us instantly

---

## Session 2 — 03/18/26 Justin
**What we built:** Folder structure, requirements.txt, main.py, .gitignore, GitHub repo connected & pushed.

**Key decisions:**
- FastAPI over Django (too heavy) and Flask (too bare)
- Uvicorn as the server — FastAPI is the car, uvicorn is the engine
- Pinned versions in requirements.txt so everyone runs the exact same code

---

## Session 3 — 03/23/26 Justin
**What we built:** .env file, README.md, Phase 1 complete.

---

## Session 4 — 03/31/26 Justin
**What we built:** T006 — PlayerStats data model in scoring.py. Set up docs/ folder.

## Session 5 — 04/01/26 Justin
**What we built:** T007 ESPN scoring constants, 
T008 FPPG calculator function.

---

## Session 6 — 04/02/26 Justin
**What we built:** T009 normalize function, 
T010 replacement value signal, T011 minutes 
trend signal.

**Key decisions:**
- normalize() guards against division by zero
  with a guard clause
- RECENT_GAMES_WINDOW = 5 as a named constant
  so changing window size only requires one update
- Minutes trend splits list in half and compares
  averages — more robust than first vs last game
- Used len() over hardcoded numbers so function
  adapts if RECENT_GAMES_WINDOW changes



**Key decisions:**
- Pydantic BaseModel over plain dict or @dataclass — auto type validation, built for FastAPI
- One flat class over multiple classes — one object travels through whole app, simpler for V1
- Optional fields for recent_minutes and injured_starter_replacement — real world data is incomplete

## Session 7 — 04/06/26 Justin
**Starting at:** T012 — Sustainability signal
**What we built:** 
- T012 — calculate_sustainability function with FG%/FT% averaging and zero free throw edge case handled
- T013 — calculate_pickup_score, Signal Engine fully complete
**Next:** T014 — cache.py in-memory cache layer

## Session 8 — 04/17/26 Justin
**Starting at:** T014 — Cache layer
**What we built:**
- T014 — cache.py with get_cached_players and set_cached_players (1hr expiry)
- Updated PlayerStats to add fg_pct and ft_pct fields
- Simplified calculate_sustainability to use percentages directly
- Added adapt_pipeline_row for season_averages data

**Key decisions:**
- Cache expiry = 1 hour (balance freshness vs API rate limits)
- Adapter pattern at pipeline→scoring boundary (data contract)
- Build for season_averages first (game_logs for trend signals later)
- Scoped project to V1 waiver wire recommender (V3 = full fantasy platform)

**Next:** T015 — Wire cache into main.py










## Sesion 8 -04/07/26 Carson
  # Pipeline Data Log — `run()`

**File:** `backend/pipeline/run.py`

---

## What It Does

The `run()` function is the main entry point for the data pipeline. Given a list of data types, a player or team, and a season, it fetches, validates, cleans, and saves NBA data from the BallDontLie API.

It runs **5 steps for every data type** passed in — always in this order:

| Step | Name | What happens |
|---|---|---|
| 1 | **Ingest** | Fetches data from the BallDontLie API and loads it into a raw DataFrame |
| 2 | **Save Raw** | Writes the raw DataFrame to `backend/data/raw/*.parquet` with a `.json` sidecar |
| 3 | **Validate** | Checks required columns, data types, and row count (minimum 10 rows for `game_logs`) |
| 4 | **Clean** | Removes nulls, (flags does not delete)outliers, and duplicates — flags corrupted rows |
| 5 | **Save Clean** | Writes the cleaned DataFrame to `backend/data/clean/*.parquet` with a `.json` sidecar |

If validation fails at step 3, the pipeline raises immediately and skips steps 4 and 5.

---

## How to Use

```python
from backend.pipeline.run import run

results = run(
    data_types=["game_logs", "season_averages"],
    player_or_team="LeBron James",
    season=2024
)
```

---

## Parameters

| Param | Type | What to pass |
|---|---|---|
| `data_types` | `list[str]` | One or more of: `"game_logs"`, `"box_scores"`, `"game_scores"`, `"season_averages"` |
| `player_or_team` | `str` | Player name, team abbreviation, or game ID |
| `season` | `int` | Season year — e.g. `2024` |

---

## Output

Returns a dict keyed by data type on success:

```python
{
  "game_logs": {
    "rows_before": 82,        # row count before cleaning
    "rows_after": 80,         # row count after cleaning
    "outliers_flagged": 1,    # rows flagged as outliers
    "corrupted_removed": 1,   # rows removed as corrupted
    "nulls_found": 3,         # null values detected
    "file_path": "/abs/path/to/clean/game_logs_lebron_james_2024.parquet"
  }
}
```

Returns `{"error": "..."}` if any step fails.

---

## Error Handling

- **API retries** — failed requests retry up to 3× with exponential backoff: 1s → 2s → 4s
- **Validation failures** — raise immediately; clean and save steps are skipped for that data type
- **Environment** — requires `BALL_IS_LIFE` env var set in `backend/.env`

---

## File Outputs

| Type | Location | Format |
|---|---|---|
| Raw data | `backend/data/raw/` | `.parquet` + `.json` sidecar |
| Clean data | `backend/data/clean/` | `.parquet` + `.json` sidecar |

Filenames follow the pattern: `{data_type}_{player_or_team}_{season}.parquet`
Example: `game_logs_lebron_james_2024.parquet`






## Sesion 9 -04/015/26 Carson
  # Pipeline Data Log — `run()`

**File:** `backend/pipeline/run.py`


## Tech Stack
- **Backend:** Python / FastAPI
- **Frontend:** React
- **Data:** BallDontLie API (requires API key)

## Running Locally

### Backend
```bash
cd backend
pip install -r requirements.txt
uvicorn main:app --reload
```

### Frontend
```bash
cd frontend
npm install
npm run dev
```

## Architecture
The backend is split into focused modules:

- `main.py`: starts the server
- `scoring.py`: calculates the three signals
- `cache.py`: stores results in memory
- `routers/`: handles API endpoints
- `pipeline/`: fetches, cleans, and processes all NBA stats data

**All data fetching and processing must go through the pipeline.**

---

## NBA Stats Pipeline

### What Was Built
The pipeline (`backend/pipeline/`) is a full data processing system that pulls NBA stats from the BallDontLie API, cleans and validates them, and writes structured parquet files for downstream use.

It was built in three phases:

**Phase 1 — Original build**  
Basic ingestion, validation, cleaning, and file saving across all four data types:
- game logs
- box scores
- game scores
- season averages

**Phase 2 — Architecture refactor**  
Complete rewrite into a cleaner module structure with a new `run()` signature. Key improvements:
- Separate `player` / `team` args (mutually exclusive)
- Natural-grain composite deduplication (`player_id + game_id`)
- Configurable impossible-value thresholds (upper and lower bounds)
- Two-tier validation: structural failures halt; missing optional stats only warn
- Schema drift tracking: baselines written on first run, diffs detected on subsequent runs
- All API fields captured with no hardcoded column drops
- Atomic file writes (`.tmp` then rename) for both parquet and sidecar JSON
- Sidecar metadata files renamed from `.json` to `.sidecar.json`

**Phase 3 — Feature engineering layer**  
Optional layer activated by passing `features_config` to `run()`:
- Leakage-safe rolling features (`.shift(1)` before `.rolling()`)
- Rolling mean, standard deviation, and delta (trend) per stat per window
- Fantasy points computed from a scoring weight config
- Next-game prediction labels via `.shift(-1)`
- SHA-256 feature schema versioning to detect training/config mismatches

### Pipeline Modules

| File | What it does |
|---|---|
| `run.py` | Single public entry point — orchestrates all steps |
| `config.py` | All constants: thresholds, column lists, dedup keys |
| `fetcher.py` | Pulls raw data from BallDontLie API with retry logic |
| `validator.py` | Two-tier validation (halt vs. warn) |
| `cleaner.py` | 10-step cleaning: nulls, dates, units, bad rows, sorting |
| `deduplicator.py` | Composite key deduplication per data type |
| `outlier.py` | Tukey fence outlier flagging (marks rows, never removes) |
| `schema_drift.py` | Detects column changes against a stored baseline |
| `sidecar.py` | Builds metadata JSON written alongside each parquet |
| `writer.py` | Atomic parquet + sidecar file writes |
| `features/engineer.py` | Rolling features (leakage-safe) |
| `features/labels.py` | Next-game prediction labels |
| `features/versioning.py` | SHA-256 feature schema versioning |
| `exceptions.py` | All typed exceptions |

## How to Use

### Setup
Create `backend/.env` with your API key:

```env
BALL_IS_LIFE=your_api_key_here
```

### Basic Usage
```python
from pipeline.run import run

# Player game logs
results = run(
    data_types=["game_logs"],
    season=2023,
    player="LeBron James",
)

# Team box scores
results = run(
    data_types=["box_scores"],
    season=2023,
    team="LAL",
)

# Season averages
results = run(
    data_types=["season_averages"],
    season=2023,
    player="Stephen Curry",
)

# Multiple data types in one call
results = run(
    data_types=["game_logs", "season_averages"],
    season=2023,
    player="LeBron James",
)
```

### Filter to Specific Columns
Identity columns (`player_id`, `player_name`, `team`, `game_id`, `date`, `season`) are always retained regardless.

```python
results = run(
    data_types=["game_logs"],
    season=2023,
    player="LeBron James",
    columns=["pts", "reb", "ast"],
)
```

### With Feature Engineering
```python
results = run(
    data_types=["game_logs"],
    season=2023,
    player="LeBron James",
    features_config={
        "rolling_windows": [5, 10],
        "min_observations": 5,
        "scoring": {
            "pts": 1.0,
            "reb": 1.2,
            "ast": 1.5,
            "stl": 3.0,
            "blk": 3.0,
        },
    },
    labels_config={
        "targets": ["pts", "reb", "ast"],
    },
)
```

### Return Value
```python
{
    "game_logs": {
        "status": "success",
        "rows_before": 82,
        "rows_after": 79,
        "outliers_flagged": 3,
        "corrupted_removed": 1,
        "nulls_found": 0,
        "file_path": "/absolute/path/to/data/clean/game_logs_lebron_james_2023.parquet",
        "feature_schema_version": "a1b2c3d4e5f6",
        "features_count": 24,
    }
}
```

On failure, a data type returns:

```python
{"status": "failed", "error": "..."}
```

One failure does not stop other data types.

### Output Files
For each run, these files are written per data type:
- `data/raw/{type}_{subject}_{season}.parquet` — raw API data
- `data/clean/{type}_{subject}_{season}.parquet` — cleaned data
- `data/clean/{type}_{subject}_{season}.sidecar.json` — run metadata (row counts, drift, thresholds applied)
- `data/clean/{type}_{subject}_{season}.features.sidecar.json` — feature metadata (if `features_config` is provided)

### Running Tests
```bash
cd backend
pytest tests/unit/
pytest tests/integration/
```
