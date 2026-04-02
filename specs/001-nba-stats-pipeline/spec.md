# Feature Specification: NBA Stats Data Pipeline

**Feature Branch**: `001-nba-stats-pipeline`  
**Created**: 2026-03-31  
**Status**: Draft  

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Fetch and Store Player Game Logs (Priority: P1)

A data analyst or developer runs the pipeline to pull a specific player's game logs for a given season. The pipeline fetches the data in chunks, validates it, cleans it, flags outliers, and saves a clean compressed file to disk with a metadata sidecar.

**Why this priority**: Game logs are the primary data type driving fantasy scoring decisions. This path exercises every step of the pipeline end-to-end and delivers the core value of the feature.

**Independent Test**: Can be fully tested by calling `run(data_type="game_logs", player_or_team="LeBron James", season="2023")` and verifying a `.parquet` file and `.json` sidecar appear in `data/clean/`.

**Acceptance Scenarios**:

1. **Given** a valid player name and season, **When** `run()` is called, **Then** a `.parquet` file appears in `data/clean/` and a JSON sidecar is created alongside it.
2. **Given** the raw data contains DNP rows (minutes = 0) and duplicate game entries, **When** the clean step runs, **Then** those rows are removed and the clean file has fewer rows than the raw file.
3. **Given** a player with outlier scoring games, **When** outlier detection runs, **Then** an `is_outlier` boolean column is present in the output and flagged rows are kept exactly as-is — no values are modified or removed.

---

### User Story 2 - Fetch and Store Box Scores (Priority: P2)

A developer runs the pipeline for box score data for a team or game. The same `run()` interface is used, and the result is a clean parquet file in `data/clean/` with a JSON sidecar.

**Why this priority**: Box scores provide game-level context needed for lineup and waiver decisions — more actionable than season averages and needed sooner.

**Independent Test**: Can be tested by calling `run(data_type="box_scores", player_or_team="LAL", season="2023")` and verifying output files are created in `data/clean/`.

**Acceptance Scenarios**:

1. **Given** a valid team identifier and season, **When** `run(data_type="box_scores", ...)` is called, **Then** a clean parquet file and its JSON sidecar are written to `data/clean/`.
2. **Given** a box score dataset with corrupted rows (e.g., `pts > 100`), **When** the clean step runs, **Then** those rows are removed and each removal is logged with the column name and value.

---

### User Story 3 - Validate Data Before Processing (Priority: P3)

A developer runs the pipeline on a dataset that is too small or malformed. The pipeline detects the problem early, logs a clear warning to the terminal and to `logs/pipeline.log`, and halts that data type without affecting others.

**Why this priority**: Silent failures produce incorrect waiver wire recommendations. Early loud failures preserve trust.

**Independent Test**: Can be tested by mocking the API to return fewer than 10 rows and verifying a warning is logged and no output file is written to `data/clean/`.

**Acceptance Scenarios**:

1. **Given** the fetched dataset has fewer than 10 rows, **When** validation runs, **Then** a WARNING-level log message naming the row count is emitted and no file is written.
2. **Given** the fetched dataset is missing an expected column (e.g., `pts`), **When** validation runs, **Then** a WARNING naming the missing column is logged and processing stops for that data type.
3. **Given** a fully valid dataset, **When** validation runs, **Then** no warnings are logged and the pipeline proceeds to cleaning.

---

### User Story 4 - Retrieve Season Averages (Priority: P4)

A developer runs the pipeline for season averages using the same `run()` interface.

**Why this priority**: Season averages provide context but are the least time-sensitive data type for fantasy decisions.

**Independent Test**: Can be tested by calling `run(data_type="season_averages", ...)` and verifying output files are created.

**Acceptance Scenarios**:

1. **Given** a valid player and season, **When** `run(data_type="season_averages", ...)` is called, **Then** a clean parquet file and its JSON sidecar are written to `data/clean/`.

---

### Edge Cases

- What happens when the API key is missing from the environment at startup?
- What happens when the API returns an empty response for a valid player/season query?
- How does the pipeline handle a player name that returns no matching ID from the API?
- What happens when `data/raw/`, `data/clean/`, or `logs/` directories do not exist at runtime?
- What happens when a numeric column contains all-null values — can it still be downcast?
- What happens if the parquet write fails mid-run — is the sidecar still written?
- What happens when one data type fails — do the remaining data types still run?
- What happens when the API fails mid-stream during chunked ingestion (timeout or rate limit on chunk 3 of 8)?

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The pipeline MUST expose a single `run(data_type, player_or_team, season)` function as its only public entry point. If the `BALL_IS_LIFE` environment variable is absent at the time `run()` is called, the pipeline MUST raise a typed exception immediately — before any API call or file I/O is attempted.
- **FR-002**: The pipeline MUST support four data types: `game_logs`, `box_scores`, `game_scores`, and `season_averages`.
- **FR-003**: Data MUST be fetched in chunks rather than loading the entire dataset into memory at once.
- **FR-004**: Unused columns MUST be dropped immediately after ingestion, before any further processing step.
- **FR-005**: All numeric columns MUST be downcast to the smallest appropriate numeric dtype after ingestion.
- **FR-006**: Intermediate DataFrames MUST be deleted between pipeline steps and garbage collection triggered after each deletion.
- **FR-007**: Duplicate rows MUST be removed by `game_id` during the clean step. If `game_id` is not present in the dataset, deduplication MUST be skipped. The pipeline MUST log an INFO message stating deduplication was skipped and the reason, MUST record `dedup_skipped: true` and `dedup_reason` in the JSON sidecar, and MUST continue to the next step without halting.
- **FR-008**: Rows where minutes played equals 0 (DNP rows) MUST be removed during the clean step.
- **FR-009**: Missing numeric values MUST remain null. The pipeline does not fill, interpolate, or engineer missing values. Nulls are logged and reported in the sidecar but preserved in the output.
- **FR-010**: All player name values MUST be standardized to title case with no leading, trailing, or extra internal whitespace.
- **FR-011**: All date values MUST be standardized to `YYYY-MM-DD` format.
- **FR-012**: Minutes MUST be converted from `"MM:SS"` string format to decimal float (e.g., `"32:45"` → `32.75`).
- **FR-013**: Non-printable characters MUST be removed from all string columns.
- **FR-014**: Team name abbreviations MUST be normalized to uppercase.
- **FR-015**: Real outliers (statistically unusual but physically possible values) MUST be flagged with `is_outlier = True` and kept exactly as-is. Outliers are defined as values falling outside Q1 − 1.5×IQR or Q3 + 1.5×IQR (Tukey fence) per numeric column. No values are modified, capped, or removed during outlier handling.
- **FR-016**: Corrupted values (physically impossible stats) MUST be flagged, logged with the row and column name, and removed. Impossible thresholds are: `pts > 100`, `min > 60`, `reb > 50`, `ast > 30`, `stl > 15`, `blk > 15`.
- **FR-017**: Validation MUST confirm all expected columns are present, data types are correct, and the dataset contains at least 10 rows before the clean step begins. Validation MUST collect all failures before reporting — it MUST NOT halt on the first error. All failures MUST be logged together in a single WARNING block, then the pipeline halts that data type.
- **FR-018**: Each validation failure MUST produce a clearly worded WARNING-level log message that names the specific column or condition that failed. All failure messages for a single validation run MUST be emitted together in one log block.
- **FR-019**: For each unique combination of `data_type`, `player_or_team`, and `season`, the pipeline MUST overwrite the existing raw and clean parquet files and their sidecars. No historical versions are retained. Filename format: `{data_type}_{player_or_team}_{season}.parquet` for both raw and clean outputs. All temporary intermediate files MUST be deleted at the end of every successful run.
- **FR-020**: Each parquet file MUST have exactly one JSON sidecar written alongside it. The sidecar contains row count, column list, and cleaning stats (nulls found, outliers flagged, corrupted rows removed, `dedup_skipped`, and `dedup_reason` when deduplication was skipped).
- **FR-021**: The pipeline MUST import and reuse `load()`, `get_player_id()`, and `get_game_logs()` from the existing `api.py` and MUST NOT reimplement their logic.
- **FR-022**: All log messages MUST be written simultaneously to the terminal and to `logs/pipeline.log`. The log file MUST be created automatically if it does not exist. Log format: `YYYY-MM-DD HH:MM:SS | LEVEL | message`. Log levels used: `INFO`, `WARNING`, `ERROR`.
- **FR-023**: `run()` MUST return a summary dict with exactly these keys: `rows_before` (int), `rows_after` (int), `outliers_flagged` (int), `corrupted_removed` (int), `nulls_found` (int), `file_path` (str).
- **FR-024**: Each data type MUST run independently. A failure in one data type MUST be logged as a WARNING and skipped. It MUST NOT halt processing of other data types.
- **FR-025**: If the parquet write fails at any point the sidecar MUST NOT be written. The error MUST be logged at ERROR level with the file path that failed. Any partial files written during the failed attempt MUST be deleted before the pipeline exits that step.
- **FR-026**: If the API returns an error or times out during chunked ingestion, the pipeline MUST fail fast — log at ERROR level, delete any partially written raw file, and not write a sidecar. No partial datasets proceed to the clean step.

### Key Entities

- **Pipeline Run**: A single execution of `run()` for one data type, subject, and season. Produces one raw parquet with its sidecar, and one clean parquet with its sidecar.
- **Game Log Record**: One row of player game data — player name, date, team, minutes played, and per-game stats.
- **Outlier Flag**: A boolean column (`is_outlier`) set to `True` for rows where any stat column falls outside Q1 − 1.5×IQR or Q3 + 1.5×IQR (Tukey fence). The row is kept untouched.
- **Corrupted Row**: A row containing a physically impossible stat value (per FR-016 thresholds). Removed from the clean output and logged.
- **Metadata Sidecar**: One JSON file written alongside each parquet. Contains row count, column list, and cleaning stats (nulls found, outliers flagged, corrupted rows removed). Includes `dedup_skipped: true` and `dedup_reason` when deduplication was skipped due to a missing `game_id` column.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A full pipeline run for a single player's season of game logs completes without exceeding 200 MB of peak memory usage.
- **SC-002**: A full season of game logs (82 games) is processed end-to-end in under 30 seconds on a standard laptop.
- **SC-003**: All cleaning transformations are idempotent — running the clean step twice on the same raw data produces identical output.
- **SC-004**: Every parquet file written to disk has exactly one JSON sidecar alongside it.
- **SC-005**: Every validation failure produces a log message that names the specific column or condition that caused it. No generic error messages.
- **SC-006**: `run()` returns a populated summary dict for every successful run and raises a clearly typed exception for every failure — no silent `None` returns.
- **SC-007**: A failure in one data type does not prevent other data types from completing their runs.
- **SC-008**: If a parquet write fails no sidecar is written and no partial files remain on disk after the pipeline exits.

## Clarifications

### Session 2026-03-31

- Q: What happens when the API fails mid-stream during chunked ingestion (timeout or 429 rate limit)? → A: Fail fast — log at ERROR level, delete any partial raw file, no sidecar written. No partial datasets proceed to the clean step.
- Q: What IQR multiplier threshold defines an outlier for FR-015? → A: 1.5× IQR (standard Tukey fence).
- Q: What should happen when `game_id` is absent from the dataset during deduplication (FR-007)? → A: Skip deduplication, log INFO with the reason, record `dedup_skipped: true` and `dedup_reason` in the JSON sidecar, and continue to the next step without halting.
- Q: What should happen when `BALL_IS_LIFE` is missing from the environment at startup? → A: Raise a typed exception immediately before any API call or file I/O is attempted.
- Q: When validation finds multiple failures (wrong dtype AND missing column), does it halt on the first or collect all? → A: Collect all validation failures, log them all together in a single block, then halt that data type.

## Assumptions

- The existing `api.py` file is located in the `backend/` directory and its `load()`, `get_player_id()`, and `get_game_logs()` functions have stable, documented signatures that will not change.
- The BallDontLie API key is stored in `backend/.env` under the key `BALL_IS_LIFE`.
- `data/raw/`, `data/clean/`, and `logs/` directories will be created by the pipeline on first run if they do not already exist.
- The `pipeline/` package will live inside the `backend/` directory alongside `api.py`.
- The `season` argument follows the integer format used by the BallDontLie API (e.g., `2023` for the 2023-24 season).
- The set of "unused columns" to drop after ingestion is defined per data type as a constant inside `ingest.py` and is not user-configurable.
- Only the five listed libraries are permitted: `requests`, `pandas`, `pyarrow`, `python-dotenv`, and the Python standard library.
