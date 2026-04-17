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


# feat: NBA Stats Data Pipeline — `001-nba-stats-pipeline`

## Tech Stack
- Backend: Python / FastAPI
- Frontend: React
- Data: BallDontLie API (requires API key)

## Running locally

### Backend
```bash
cd backend
pip install -r requirements.txt
uvicorn main:app --reload
