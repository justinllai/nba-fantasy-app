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

## Overview

Implements the full NBA stats data pipeline under `backend/pipeline/`. This is the foundational data layer for fantasy scoring decisions and downstream modeling. The pipeline fetches raw NBA stats from the BallDontLie API, validates and cleans them, writes compressed parquet outputs with JSON metadata sidecars, and optionally generates leakage-safe features and prediction labels.

Single public entry point: `run()`. Everything else is internal.

---

## What Was Added

### `backend/pipeline/`

**`run.py`** — Orchestrator and only public entry point. Executes the full pipeline sequence per data type: fetch → write raw → validate → clean → deduplicate → flag outliers → schema drift → write clean → (optional) features → (optional) labels. Per-data-type failure isolation means one bad data type never kills the rest. Feature engineering failures are also isolated — the base clean result is always preserved.

**`config.py`** — Single source of truth for all constants: supported data types, identity columns, required structural fields per data type, natural-grain dedup composite keys, configurable impossible-value thresholds (upper and lower bounds), null placeholder strings, and default directory paths.

**`exceptions.py`** — All typed exceptions used across the pipeline: `PipelineConfigError`, `APIKeyMissingError`, `UnsupportedDataTypeError`, `ValidationError`, `IngestionError`, `FileWriteError`, `EntityLookupError`, `FeatureVersionMismatchError`.

**`fetcher.py`** — Pulls raw data from the BallDontLie API. Resolves player/team names to IDs, handles chunked ingestion, implements 3-attempt exponential backoff on failures. Flattens nested API records (player, game, team sub-objects) into flat DataFrames. Routes data types to fetch functions via a `FETCH_FUNCTIONS` table.

**`validator.py`** — Two-tier validation. Missing required structural fields (`date`, `game_id`, `player_name` or `player_id`, `team` or `team_id`) raise `ValidationError` and halt that data type. Missing optional stat columns log a WARNING and allow processing to continue. All failures are collected before reporting — never halts on the first.

**`cleaner.py`** — 10-step cleaning pipeline applied in a fixed order:
1. Strip non-printable characters from string columns
2. Normalize null placeholder strings to `NaN`
3. Title-case player names, strip whitespace
4. Standardize dates to `YYYY-MM-DD`
5. Uppercase team abbreviations
6. Convert `MM:SS` minutes strings to decimal float
7. Remove impossible-value rows per configurable thresholds
8. Remove DNP rows (`min == 0`)
9. Apply column filter — always retains identity columns regardless of `columns` arg
10. Sort time-series types chronologically by `date` then `game_id`

**`deduplicator.py`** — Removes duplicates using natural-grain composite keys per data type (`player_id + game_id` for game logs and box scores, `team_id + game_id` for game scores). Falls back to name-based keys when ID columns are absent. On value conflicts, keeps the most recently ingested row and logs the conflict. `season_averages` skips deduplication. Records `dedup_skipped` and `dedup_reason` in the sidecar when dedup cannot run.

**`outlier.py`** — Flags statistical outliers using the 1.5× IQR Tukey fence per numeric column. Adds an `is_outlier` boolean column set to `True` when any column falls outside the fence. Rows are never removed or modified — only marked.

**`schema_drift.py`** — On first run for a data type, writes a JSON column baseline to `data/schema_baselines/`. On subsequent runs, compares current columns against the baseline. Logs added or missing columns. Logs at WARNING when a required identity column is missing. First-run sidecar notes that no drift comparison was possible.

**`sidecar.py`** — Three sidecar builders:
- `build_raw_sidecar` — row count and column list for raw files
- `build_base_sidecar` — full cleaning stats, dedup metadata, drift metadata, threshold config used
- `build_feature_sidecar` — feature names, rolling windows, min observation thresholds, scoring config, version hash, label metadata

**`writer.py`** — Atomic file writes via write-to-`.tmp`-then-rename. Prevents partial files on failed writes. Writes snappy-compressed parquet via `pyarrow` and JSON sidecars. Sidecar is always written after parquet is confirmed on disk — never before. Provides `sidecar_path()` and `feature_sidecar_path()` helpers for consistent naming.

---

### `backend/pipeline/features/`

**`engineer.py`** — Generates leakage-safe rolling features. Uses `.shift(1)` before `.rolling()` so game N's window never includes game N's own stats. Produces `rolling_{stat}_{window}`, `rolling_std_{stat}_{window}`, `{stat}_delta_{window}`, and optionally `fantasy_pts` from a configurable scoring weight dict. Rows below the configured minimum observations threshold are set to `null`, never estimated from partial data.

**`labels.py`** — Generates next-game prediction labels using `.shift(-1)` per player on a sorted time series. Produces `next_game_{target}` columns. The last game per player always receives a `null` label and is marked `is_end_of_series=True` in the sidecar. These rows are excluded from supervised training by default.

**`versioning.py`** — Produces a deterministic 12-character SHA-256 hash from `features_config`. Same config always produces the same version. `assert_version_compatible()` raises `FeatureVersionMismatchError` when a stored model's feature version does not match the current config.

---

## Output Files Per Run

| File | Location | Written When |
|------|----------|-------------|
| `{type}_{subject}_{season}.parquet` | `data/raw/` | After successful fetch |
| `{type}_{subject}_{season}.sidecar.json` | `data/raw/` | After raw parquet confirmed written |
| `{type}_{subject}_{season}.parquet` | `data/clean/` | After successful clean |
| `{type}_{subject}_{season}.sidecar.json` | `data/clean/` | After clean parquet confirmed written |
| `{type}_{subject}_{season}.features.sidecar.json` | `data/clean/` | Only when features are generated |
| `{type}.json` | `data/schema_baselines/` | First run per data type only |
| `pipeline.log` | `logs/` | Continuously during run |

---

## Key Design Decisions

- **Sidecar is the success signal** — if a sidecar is absent, that run is considered incomplete regardless of whether a parquet file exists
- **Atomic writes** — all files are written to `.tmp` first and renamed on success; failed writes leave no partial files on disk
- **Leakage safety enforced at write time** — rolling features always use `.shift(1)` before `.rolling()`; this is not optional
- **Configurable thresholds** — impossible-value bounds live in `config.py` and are recorded in every sidecar so every run is reproducible
- **Per-data-type isolation** — a failure in one data type is caught, logged at ERROR, and skipped; all other data types continue
- **Idempotent** — running with the same arguments twice produces identical outputs; filenames are deterministic and overwrite previous results
- **Sequential API calls only** — no concurrency in v1; rate limit safety is handled by exponential backoff in `fetcher.py`

---

## `run()` Signature

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
) -> dict[str, dict]
```

Returns a dict keyed by data type:

```python
{
    "game_logs": {
        "status": "success",
        "rows_before": 82,
        "rows_after": 79,
        "outliers_flagged": 3,
        "corrupted_removed": 0,
        "nulls_found": 12,
        "file_path": "data/clean/game_logs_lebron_james_2023.parquet"
    }
}
```

---

## Dependencies

No new dependencies added. Uses only what was already specified:
- `pandas`
- `pyarrow`
- `requests`
- `python-dotenv`
- Python standard library

---

## Testing

- Unit tests for each module in `backend/pipeline/`
- Integration test: full run for a single player season end-to-end
- Failure isolation tests: one data type fails, others complete
- Idempotency test: clean step run twice produces identical output
- Leakage test: rolling features verified to use only prior rows
- Atomic write test: simulated parquet failure leaves no files and no sidecar
