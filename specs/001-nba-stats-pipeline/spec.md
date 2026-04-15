# Feature Specification: NBA Stats Data Pipeline

**Feature Branch**: `001-nba-stats-pipeline`  
**Created**: 2026-03-31  
**Status**: Draft  

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Fetch and Store Player Game Logs (Priority: P1)

A data analyst or developer runs the pipeline to pull a specific player's game logs for a given season. The pipeline fetches the data in chunks, validates it, cleans it, flags outliers, and saves a clean compressed file to disk with a metadata sidecar.

**Why this priority**: Game logs are the primary data type driving fantasy scoring decisions. This path exercises every step of the pipeline end-to-end and delivers the core value of the feature.

**Independent Test**: Can be fully tested by calling `run(data_types=["game_logs"], player_or_team="LeBron James", season=2023)` and verifying a `.parquet` file and `.json` sidecar appear in `data/clean/` with all available stat columns present.

**Acceptance Scenarios**:

1. **Given** a valid player name and season with no `columns` argument, **When** `run()` is called, **Then** a `.parquet` file appears in `data/clean/` containing all stat columns the API returned, and a JSON sidecar is created alongside it.
2. **Given** a valid player name and season with `columns=["pts", "reb", "ast"]`, **When** `run()` is called, **Then** the clean parquet contains only those three stat columns plus identity columns (`player_name`, `team`, `date`, `game_id`).
3. **Given** the raw data contains DNP rows (minutes = 0) and duplicate game entries, **When** the clean step runs, **Then** those rows are removed and the clean file has fewer rows than the raw file.
4. **Given** a player with outlier scoring games, **When** outlier detection runs, **Then** an `is_outlier` boolean column is present in the output and flagged rows are kept exactly as-is — no values are modified or removed.

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

- **FR-001**: The pipeline MUST expose a single `run(data_types, player_or_team, season, columns=None)` function as its only public entry point. If the `BALL_IS_LIFE` environment variable is absent at the time `run()` is called, the pipeline MUST raise a typed exception immediately — before any API call or file I/O is attempted.
- **FR-002**: The pipeline MUST support four data types: `game_logs`, `box_scores`, `game_scores`, and `season_averages`.
- **FR-003**: Data MUST be fetched in chunks rather than loading the entire dataset into memory at once.
- **FR-004**: The ingest functions MUST capture all stat fields returned by the API for the given data type — no fields are hardcoded for exclusion at the flatten stage. If the caller passes a `columns` argument to `run()`, only those columns plus all required identity and modeling keys MUST be retained in the clean output whenever present. Required keys that are always retained: `player_name`, `player_id`, `team`, `team_id`, `date`, `game_id`, `season`, `opponent`. If `columns` is `None`, all available stat columns are kept.
- **FR-005**: All numeric columns MUST be downcast to the smallest appropriate numeric dtype after ingestion.
- **FR-006**: Intermediate DataFrames MUST be deleted between pipeline steps and garbage collection triggered after each deletion.
- **FR-007**: Duplicate rows MUST be removed according to the natural grain of the data type — `game_id` alone is not sufficient when multiple valid rows may exist per game. Minimum deduplication keys by data type: `game_logs` uses `player_id + game_id` when `player_id` is present, otherwise `player_name + game_id`; `box_scores` uses `player_id + game_id` when player-level rows are returned; `game_scores` uses `team_id + game_id` when `team_id` is present, otherwise `team + game_id`; `season_averages` skips deduplication unless duplicate subject-season rows are present. When duplicate grain keys exist with differing values, the pipeline MUST retain the most recently ingested row and log the conflict in the sidecar. If minimum deduplication keys are unavailable, deduplication MAY be skipped but the sidecar MUST record the exact reason.
- **FR-008**: Rows where minutes played equals 0 (DNP rows) MUST be removed during the clean step.
- **FR-009**: Missing numeric values MUST remain null. The pipeline does not fill, interpolate, or engineer missing values. Nulls are logged and reported in the sidecar but preserved in the output.
- **FR-010**: All player name values MUST be standardized to title case with no leading, trailing, or extra internal whitespace.
- **FR-011**: All date values MUST be standardized to `YYYY-MM-DD` format.
- **FR-012**: Minutes MUST be converted from `"MM:SS"` string format to decimal float (e.g., `"32:45"` → `32.75`).
- **FR-013**: Non-printable characters MUST be removed from all string columns.
- **FR-014**: Team name abbreviations MUST be normalized to uppercase.
- **FR-015**: Real outliers (statistically unusual but physically possible values) MUST be flagged with `is_outlier = True` and kept exactly as-is. Outliers are defined as values falling outside Q1 − 1.5×IQR or Q3 + 1.5×IQR (Tukey fence) per numeric column. No values are modified, capped, or removed during outlier handling.
- **FR-016**: Corrupted values (physically impossible stats) MUST be flagged, logged with the row and column name, and removed. Impossible-value thresholds MUST be configurable in code rather than fixed permanently. The active threshold set MUST be logged and recorded in the sidecar. Default upper-bound thresholds: `pts > 100`, `min > 60`, `reb > 50`, `ast > 30`, `stl > 15`, `blk > 15`, `games_played > 82`. Lower-bound rule: any stat that cannot be negative MUST be flagged if it falls below zero.
- **FR-017**: Validation MUST distinguish between required structural fields and optional statistical fields. Required structural fields (`date`, `game_id`, `player_name` or `player_id`, `team` or `team_id` where applicable) MUST be present — missing required structural fields MUST halt the affected data type. Missing optional statistical fields MUST produce a WARNING but MUST NOT halt processing if the dataset is otherwise valid. Validation MUST collect all failures before reporting and emit them together in a single WARNING block.
- **FR-018**: Each validation failure MUST produce a clearly worded WARNING-level log message that names the specific column or condition that failed. All failure messages for a single validation run MUST be emitted together in one log block.
- **FR-019**: For each unique combination of `data_type`, `player_or_team`, and `season`, the pipeline MUST overwrite the existing raw and clean parquet files and their sidecars. No historical versions are retained. Filename format: `{data_type}_{player_or_team}_{season}.parquet` for both raw and clean outputs. All temporary intermediate files MUST be deleted at the end of every successful run.
- **FR-020**: Each parquet file MUST have exactly one JSON sidecar written alongside it. The sidecar contains row count, column list, and cleaning stats (nulls found, outliers flagged, corrupted rows removed, `dedup_skipped`, and `dedup_reason` when deduplication was skipped).
- **FR-021**: The pipeline MUST import and reuse `load()`, `get_player_id()`, and `get_game_logs()` from the existing `api.py` and MUST NOT reimplement their logic.
- **FR-022**: All log messages MUST be written simultaneously to the terminal and to `logs/pipeline.log`. The log file MUST be created automatically if it does not exist. Log format: `YYYY-MM-DD HH:MM:SS | LEVEL | message`. Log levels used: `INFO`, `WARNING`, `ERROR`.
- **FR-023**: `run()` MUST return a summary dict with exactly these keys: `rows_before` (int), `rows_after` (int), `outliers_flagged` (int), `corrupted_removed` (int), `nulls_found` (int), `file_path` (str).
- **FR-024**: Each data type MUST run independently. A failure in one data type MUST be logged as a WARNING and skipped. It MUST NOT halt processing of other data types.
- **FR-025**: If the parquet write fails at any point the sidecar MUST NOT be written. The error MUST be logged at ERROR level with the file path that failed. Any partial files written during the failed attempt MUST be deleted before the pipeline exits that step.
- **FR-026**: If the API returns an error or times out during chunked ingestion, the pipeline MUST fail fast — log at ERROR level, delete any partially written raw file, and not write a sidecar. No partial datasets proceed to the clean step.
- **FR-027**: For all time-series outputs (`game_logs`, `box_scores`, `game_scores`), rows MUST be sorted in ascending chronological order by `date` and then `game_id` before writing the clean dataset. `season_averages` is exempt as it contains no per-game rows.
- **FR-028**: The pipeline MUST preserve a stable set of modeling and join keys whenever present in the source data: `player_id`, `player_name`, `team_id`, `team`, `game_id`, `date`, `season`, `opponent`. These fields MUST be retained regardless of the `columns` argument when they exist in the source payload.
- **FR-029**: Deduplication MUST occur at the natural grain of the dataset rather than by `game_id` alone. See FR-007 for full deduplication keys by data type. If minimum keys are unavailable, deduplication MAY be skipped but the sidecar MUST record the exact reason.
- **FR-030**: Any derived feature, indicator, label, or rolling statistic MUST be calculated using only information available prior to the current row. The pipeline MUST NOT use future games when computing moving averages, rolling windows, trend indicators, streak indicators, labels for predictive modeling, or opponent-strength features. Features that are direct transformations of the prediction target MUST be excluded from the feature set or clearly flagged as potentially leaky.
- **FR-031**: The pipeline MAY include an optional derived-features layer after cleaning. When enabled, it MAY generate: rolling averages over prior N games, rolling standard deviation/volatility, trend direction flags, hot/cold streak indicators, last-game delta from rolling average, fantasy points under a configurable scoring system, recent minutes trend, recent usage trend, home/away splits, and opponent-strength proxies. Derived features MUST be written only after the base clean dataset passes validation and cleaning. Rolling features MUST require a configurable minimum number of observations — rows below the minimum window size MUST be marked `null`. The minimum observation threshold MUST be logged and recorded in the sidecar.
- **FR-032**: For time-series outputs, the pipeline SHOULD preserve or derive game-context fields whenever source data permits: `home_or_away`, `opponent`, `won_game`, `team_score`, `opponent_score`, `point_diff`, `days_rest`, `back_to_back`. If a context field cannot be derived because required source fields are absent, the pipeline MUST leave it `null` rather than infer or guess.
- **FR-033**: The pipeline MUST record schema drift in the JSON sidecar for each run. Schema drift metadata MUST include: columns added since the previous successful run for the same `data_type`, columns missing since the previous run, required identity columns missing from the current run, and optional columns newly observed from the API. Schema drift MUST be logged at WARNING level if any required identity field is missing. On the first run for a given data type the pipeline MUST write a schema baseline file — drift detection begins on the second run and the sidecar for a first run MUST note that no drift comparison was possible.
- **FR-034**: Validation MUST distinguish between required structural fields and optional statistical fields. Required structural fields include `date`, `game_id`, `player_name` or `player_id`, `team` or `team_id` where applicable. Missing required structural fields MUST halt the affected data type. Missing optional statistical fields MUST produce a WARNING but MUST NOT halt processing if the dataset is otherwise valid.
- **FR-035**: Impossible-value thresholds MUST be configurable in code or config rather than fixed permanently. Default upper-bound thresholds: `pts > 100`, `min > 60`, `reb > 50`, `ast > 30`, `stl > 15`, `blk > 15`. Any stat that cannot be negative MUST be flagged if it falls below zero. The pipeline MUST log which threshold set was applied and record it in the sidecar.
- **FR-036**: If derived features are generated, the clean sidecar MUST additionally include: derived feature names created, rolling windows used, minimum observation thresholds applied, fantasy scoring configuration used, feature generation timestamp, feature generation success/failure summary, and a feature schema version identifier.
- **FR-037**: The pipeline MAY support optional supervised-learning label creation. Supported labels MAY include next-game points, rebounds, assists, fantasy points, and threshold outcomes such as `pts_20_plus`. All labels MUST be aligned without leakage and generated only from chronologically ordered data. Rows at the end of the time series where no future observation exists MUST have their labels set to `null` and flagged in the sidecar.
- **FR-038**: Any data split for model training and evaluation MUST be performed on a chronological boundary, not a random sample. Random train/test splits are prohibited on time-series data. The split boundary date or game index MUST be logged and recorded in the sidecar. Data in the training window MUST NOT appear in the evaluation window.
- **FR-039**: The feature sidecar MUST include a feature schema version identifier. Any change to feature definitions, rolling windows, or scoring configurations MUST increment the version. Models trained on a prior version MUST NOT consume features from a later version without explicit acknowledgment.

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
- **SC-009**: All time-series outputs used for modeling are leakage-safe — recomputing any derived feature for a row uses only prior rows and never future rows.
- **SC-010**: For each supported data type, deduplication preserves the dataset's natural grain and removes only truly duplicated rows. Conflicting duplicate rows are logged.
- **SC-011**: Schema drift is recorded in every sidecar, and newly added or missing columns are surfaced in logs. First-run baseline files are written automatically.
- **SC-012**: A model-ready feature run produces a feature sidecar listing every derived feature, rolling window, minimum observation threshold, and scoring configuration used.
- **SC-013**: Required structural columns missing from a dataset halt processing; optional statistical columns missing produce warnings without unnecessary failure.
- **SC-014**: All train/test splits are performed chronologically. No game or player row appears in both the training and evaluation sets.
- **SC-015**: Feature sidecar version identifiers are present on every feature run. Version mismatches between stored models and new feature outputs are surfaced as errors.
- **SC-016**: End-of-series rows with no future label are set to null and flagged in the sidecar. They are excluded from supervised training by default.
- **SC-017**: Rolling features computed from fewer observations than the configured minimum are null, not estimated from partial data.

## Clarifications

### Session 2026-03-31

- Q: What happens when the API fails mid-stream during chunked ingestion (timeout or 429 rate limit)? → A: Fail fast — log at ERROR level, delete any partial raw file, no sidecar written. No partial datasets proceed to the clean step.
- Q: What IQR multiplier threshold defines an outlier for FR-015? → A: 1.5× IQR (standard Tukey fence).
- Q: What should happen when `game_id` is absent from the dataset during deduplication (FR-007)? → A: Skip deduplication, log INFO with the reason, record `dedup_skipped: true` and `dedup_reason` in the JSON sidecar, and continue to the next step without halting.
- Q: What should happen when `BALL_IS_LIFE` is missing from the environment at startup? → A: Raise a typed exception immediately before any API call or file I/O is attempted.
- Q: When validation finds multiple failures (wrong dtype AND missing column), does it halt on the first or collect all? → A: Collect all validation failures, log them all together in a single block, then halt that data type.

### Session 2026-04-09

- Q: Should the pipeline hardcode which stat columns to keep, or let the caller decide? → A: The ingest layer MUST capture all stat fields the API returns. The caller MAY pass a `columns` list to `run()` to filter the output to specific stats. If `columns` is omitted, all available stat columns are kept. Identity columns (`player_name`, `team`, `date`, `game_id`) are always preserved regardless of the `columns` argument.
- Q: If the caller passes a `columns` value that includes a column the API did not return, what happens? → A: The pipeline MUST log a WARNING naming the unrecognized column(s) and silently ignore them — it MUST NOT raise an exception.

### Session 2026-04-14

- Q: Should deduplication key be `game_id` alone or a composite key? → A: Natural grain per data type — `player_id + game_id` for game_logs/box_scores, `team_id + game_id` for game_scores. When duplicate keys conflict on values, keep the most recently ingested row and log the conflict.
- Q: Should impossible-value thresholds be hardcoded? → A: No — thresholds must be configurable. Lower-bound checks (negative values) are also added. Active threshold set must be logged and stored in the sidecar.
- Q: Should validation halt on any missing column? → A: No — required structural fields halt processing; missing optional stat fields produce a WARNING only.
- Q: Are derived features and labels in scope? → A: Both are optional layers (FR-031, FR-037). When enabled, they must be leakage-safe, use configurable rolling windows, and produce a versioned feature sidecar.
- Q: What identity columns must always be retained regardless of the `columns` filter? → A: `player_id`, `player_name`, `team_id`, `team`, `game_id`, `date`, `season`, `opponent` — whenever present in the source payload.

## Assumptions

- The existing `api.py` file is located in the `backend/` directory and its `load()`, `get_player_id()`, and `get_game_logs()` functions have stable, documented signatures that will not change.
- The BallDontLie API key is stored in `backend/.env` under the key `BALL_IS_LIFE`.
- `data/raw/`, `data/clean/`, and `logs/` directories will be created by the pipeline on first run if they do not already exist.
- The `pipeline/` package will live inside the `backend/` directory alongside `api.py`.
- The `season` argument follows the integer format used by the BallDontLie API (e.g., `2023` for the 2023-24 season).
- The set of available columns is determined entirely by what the BallDontLie API returns — no columns are hardcoded for exclusion. The caller controls column selection via the optional `columns` argument to `run()`.
- Only the five listed libraries are permitted: `requests`, `pandas`, `pyarrow`, `python-dotenv`, and the Python standard library.
