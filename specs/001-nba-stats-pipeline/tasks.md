# Tasks: NBA Stats Data Pipeline

**Input**: Design documents from `/specs/001-nba-stats-pipeline/`  
**Prerequisites**: plan.md ✓, spec.md ✓, research.md ✓, data-model.md ✓, contracts/ ✓, quickstart.md ✓

**Tests**: Included — explicitly requested in the technical plan. TDD order is mandatory (constitution Principle III).

**Organization**: Tasks grouped by user story to enable independent implementation and testing.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies on incomplete tasks)
- **[Story]**: Which user story this task belongs to (US1–US4)
- Exact file paths included in every task description

## Path Conventions

All paths are relative to the repository root. Pipeline code lives under `backend/`.

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Create the complete directory and dependency scaffold before any module is written.

- [X] T001 Create directory structure: `backend/pipeline/`, `backend/data/raw/`, `backend/data/clean/`, `backend/logs/`, `backend/tests/unit/`, `backend/tests/integration/`, `backend/tests/file_output/`
- [X] T002 Add `pyarrow` to `backend/requirements.txt` if not already present; create `backend/requirements-dev.txt` with `pytest>=7.0`
- [X] T003 [P] Create empty `backend/pipeline/__init__.py` with a `run` stub: `from pipeline.run import run  # noqa: F401`
- [X] T004 [P] Create empty `__init__.py` files in `backend/tests/`, `backend/tests/unit/`, `backend/tests/integration/`, `backend/tests/file_output/`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: All shared modules and their unit tests. TDD order: write failing test → implement → confirm green. No user story can begin until this phase is complete.

**⚠️ CRITICAL**: Complete this phase fully before any user story phase begins.

- [X] T005 Write `backend/pipeline/constants.py` — define: `SUPPORTED_DATA_TYPES = ["game_logs", "box_scores", "game_scores", "season_averages"]`; `REQUIRED_COLUMNS` dict keyed by data type (including `game_scores` entry); `EXPECTED_DTYPES` dict (including `game_scores` entry); `UNUSED_COLUMNS` dict (including `game_scores` entry); `CORRUPTION_THRESHOLDS = {"pts": 100, "min": 60, "reb": 50, "ast": 30, "stl": 15, "blk": 15}`; `NULL_PLACEHOLDERS = ["", " ", "N/A", "null", "None"]`; `RAW_DIR = "backend/data/raw"`; `CLEAN_DIR = "backend/data/clean"`; `LOGS_DIR = "backend/logs"`
- [X] T006 [P] Write `backend/pipeline/exceptions.py` — define: `MissingAPIKeyError`, `UnsupportedDataTypeError`, `ValidationError`, `IngestionError`, `FileWriteError`, `EntityLookupError` — all subclasses of `Exception` with descriptive default messages
- [X] T007 Write `backend/pipeline/logging_utils.py` — implement `create_logger(run_id: str) -> logging.Logger` that attaches a `StreamHandler` (terminal) and a `FileHandler` (`backend/logs/run_{run_id}.log`, auto-created); format: `%(asctime)s | %(levelname)s | %(message)s` with `datefmt="%Y-%m-%d %H:%M:%S"`; add `log_validation_block(logger, failures: list[str])` helper that emits all failures in one WARNING call
- [X] T008 [P] Write failing unit tests for `logging_utils` in `backend/tests/unit/test_logging_utils.py` — verify: log file created at correct path; message appears in both handlers; `log_validation_block` emits one WARNING with all failures joined; verify format string contains expected timestamp pattern
- [X] T009 Write `backend/pipeline/save.py` — implement `write_with_sidecar(df, parquet_path, sidecar_dict)`: writes to `{parquet_path}.tmp`, then writes `{parquet_path}.sidecar.tmp` as JSON, then atomically renames both to final paths; on any exception: log ERROR with path, delete both `.tmp` files if they exist, re-raise `FileWriteError`; implement `build_raw_sidecar(data_type, player_or_team, season, df)` and `build_clean_sidecar(data_type, player_or_team, season, metrics_dict, df)` per `contracts/sidecar_schema.md`
- [X] T010 [P] Write failing unit tests for `save.py` in `backend/tests/unit/test_save.py` — verify: temp file replaced by final file on success; no final file created on simulated write failure; no sidecar created when parquet write fails; both `.tmp` files deleted after failure; idempotent on re-run (overwrites)
- [X] T011 Write `backend/pipeline/validate.py` — implement `validate(df, data_type: str, logger) -> None`: check all `REQUIRED_COLUMNS[data_type]` present; check dtypes match `EXPECTED_DTYPES[data_type]` or are safely castable (float64→int64 only when all non-null values are whole numbers); for `game_logs` only: check `len(df) >= 10`; collect all failures into a list; if any failures: call `log_validation_block(logger, failures)` then raise `ValidationError` with all failure messages; if no failures: log INFO "validation passed"
- [X] T012 [P] Write failing unit tests for `validate.py` in `backend/tests/unit/test_validate.py` — verify: missing column raises `ValidationError` naming the column; wrong dtype raises `ValidationError`; fewer than 10 rows raises for `game_logs` but not for `box_scores`; multiple failures collected into one `ValidationError`; valid DataFrame passes silently
- [X] T013 Write `backend/pipeline/clean.py` — implement `clean(df, data_type: str, logger) -> tuple[pd.DataFrame, dict]`: (1) remove non-printable chars from all string columns; (2) standardize player names: title case + collapse whitespace; (3) standardize dates to `YYYY-MM-DD`; (4) normalize team abbreviations to uppercase; (5) convert `min` from `"MM:SS"` to decimal float; (6) normalize `NULL_PLACEHOLDERS` to `pd.NA`; (7) deduplicate by `game_id` if present, else log INFO + record `dedup_skipped=True, dedup_reason=...`; (8) remove DNP rows where `min == 0`; (9) flag outliers with 1.5× IQR Tukey fence → `is_outlier` bool column; (10) remove corrupted rows per `CORRUPTION_THRESHOLDS`, log each removal; return cleaned df + metrics dict with keys: `rows_before`, `rows_after`, `outliers_flagged`, `corrupted_removed`, `nulls_found`, `dedup_skipped`, `dedup_reason`
- [X] T014 [P] Write failing unit tests for `clean.py` in `backend/tests/unit/test_clean.py` — one test per cleaning rule: blank placeholders → null; `"32:45"` → `32.75`; malformed minutes handled consistently; player name title-cased + whitespace collapsed; team abbreviation uppercased; duplicate `game_id` rows removed; dedup skipped without `game_id` + `dedup_skipped=True` recorded; DNP rows removed; outlier rows flagged with `is_outlier=True` + values unchanged; corrupted rows removed per thresholds; nulls preserved (not filled)

**Checkpoint**: Foundation complete — all foundational modules implemented and unit tests green. User story phases may now begin.

---

## Phase 3: User Story 1 — Fetch and Store Player Game Logs (Priority: P1) 🎯 MVP

**Goal**: `run(["game_logs"], "LeBron James", 2023)` fetches, validates, cleans, and writes a clean parquet + sidecar to `data/clean/`. Returns a summary dict.

**Independent Test**: Call `run(["game_logs"], "LeBron James", 2023)`, verify: parquet exists at `backend/data/clean/game_logs_lebron_james_2023.parquet`; JSON sidecar exists alongside it; summary dict has all required keys; `is_outlier` column present in parquet.

### Tests for User Story 1 ⚠️ Write first — verify they FAIL before implementing T016–T018

- [X] T015 [P] [US1] Write failing unit tests for `run()` input validation in `backend/tests/unit/test_input_validation.py` — verify: empty `data_types` list raises `ValueError`; unsupported data type raises `UnsupportedDataTypeError`; missing `BALL_IS_LIFE` env var raises `MissingAPIKeyError` before any file or API action; valid inputs do not raise
- [X] T016 [P] [US1] Write failing integration test for game_logs full path in `backend/tests/integration/test_run_game_logs.py` — mock API helpers; verify: summary dict returned with correct keys; `data/clean/` parquet + sidecar written; `data/raw/` parquet + sidecar written; failed run leaves old files intact (overwrite safety)

### Implementation for User Story 1

- [X] T017 [US1] Implement game_logs ingestion in `backend/pipeline/ingest.py` — import `get_player_id`, `get_game_logs` from `api.py`; implement `ingest_game_logs(player_or_team, season, logger) -> pd.DataFrame`: resolve player ID via `get_player_id()`; fetch in chunks via `get_game_logs()` pagination; combine into one DataFrame; drop `UNUSED_COLUMNS["game_logs"]`; normalize `NULL_PLACEHOLDERS`; downcast numeric columns; return raw df; on API error/timeout: retry the failed chunk up to 3 times with exponential backoff (1s, 2s, 4s delays between attempts); if all 3 retries fail: log ERROR naming the chunk number and error, delete any partially written raw file, raise `IngestionError`
- [X] T018 [US1] Implement `run()` in `backend/pipeline/run.py` — `run(data_types, player_or_team, season)`: check `BALL_IS_LIFE` present, else raise `MissingAPIKeyError`; validate `data_types` is non-empty list of supported types; generate `run_id = datetime.now().strftime("%Y%m%d_%H%M%S")`; create logger; ensure `data/raw/`, `data/clean/`, `logs/` exist; for each data type: call `ingest → save_raw → validate → clean → save_clean`; wrap in try/except; on failure: add `{"error": str(e)}` to results dict; on success: add metrics dict; return a dictionary keyed by data type — e.g. `{"game_logs": {...}, "box_scores": {...}}` — where each value is either a metrics dict on success or `{"error": str(e)}` on failure
- [X] T019 [US1] Update `backend/pipeline/__init__.py` — replace stub with `from pipeline.run import run`; confirm module importable as `from pipeline import run`

**Checkpoint**: US1 complete — `run(["game_logs"], "LeBron James", 2023)` works end-to-end with mocked API. Integration tests green.

---

## Phase 4: User Story 2 — Fetch and Store Box Scores (Priority: P2)

**Goal**: `run(["box_scores"], "LAL", 2023)` and `run(["box_scores"], "18370647", 2023)` both produce clean parquet + sidecar. `run(["game_logs", "box_scores"], ...)` returns keyed results for both.

**Independent Test**: Call both box_scores lookup styles; verify each produces a clean parquet + sidecar. Call a multi-type run where one type fails; verify the other type's output is still written.

### Tests for User Story 2 ⚠️ Write first — verify they FAIL before implementing T022–T023

- [X] T020 [P] [US2] Write failing unit tests for box_scores detection logic in `backend/tests/unit/test_ingest.py` — verify: `"18370647"` (int-castable) routed to game_id path; `"LAL"` routed to team+season path; edge cases: `"123abc"` treated as team string
- [X] T021 [P] [US2] Write failing integration tests in `backend/tests/integration/test_run_box_scores.py` — verify: team+season path produces output; game_id path produces output; both include correct filenames
- [X] T022 [P] [US2] Write failing integration test for multi-type run in `backend/tests/integration/test_run_multi_type.py` — verify: `run(["game_logs", "box_scores", "game_scores"], ...)` returns dict with keys for all requested data types; one-type-failure-does-not-stop-others: mock one type to fail, verify other types' output files still written and result dict contains entries for all requested types (success dict or `{"error": ...}` for each)

### Implementation for User Story 2

- [X] T023 [US2] Add box_scores ingestion path in `backend/pipeline/ingest.py` — implement `ingest_box_scores(player_or_team, season, logger) -> pd.DataFrame`: detect `game_id` via `int(player_or_team)` cast; game_id path: fetch `/stats?game_ids[]={id}`; team path: fetch `/stats?team_ids[]={team_id}&seasons[]={season}` with chunked pagination; apply same drop/normalize/downcast logic as game_logs; on API error/timeout: retry the failed chunk up to 3 times with exponential backoff (1s, 2s, 4s delays between attempts); if all 3 retries fail: log ERROR naming the chunk number and error, delete any partially written raw file, raise `IngestionError`; on lookup failure: raise `EntityLookupError`

**Checkpoint**: US2 complete — both box_scores lookup paths work; multi-type run returns keyed results; one failure does not stop others.

---

## Phase 5: User Story 3 — Validate Data Before Processing (Priority: P3)

**Goal**: Validation failures (missing columns, wrong dtypes, <10 rows for game_logs) emit a grouped WARNING block and halt only that data type. Other requested data types continue.

**Independent Test**: Mock API to return <10 rows for game_logs; verify WARNING logged, no clean file written for that type, and `result["game_logs"]["error"]` is present.

### Tests for User Story 3 ⚠️ Write first — verify they FAIL before T028

- [X] T024 [P] [US3] Write failing integration test for validation failure scenarios in `backend/tests/integration/test_run_game_logs.py` — verify: <10 rows emits WARNING + no clean output written; missing `pts` column emits WARNING naming the column; valid dataset emits no WARNING and clean output is written
- [X] T025 [P] [US3] Write failing integration test: multi-type run where one type fails validation in `backend/tests/integration/test_run_multi_type.py` — one type gets <10 rows (fails); other type succeeds; result dict has `{"error": ...}` for failed type and success dict for passing type

### Implementation for User Story 3

- [X] T026 [US3] Wire validation failure result into `run()` in `backend/pipeline/run.py` — confirm `ValidationError` is caught per-type, added as `{"error": str(e)}` in results, and does NOT raise at the `run()` level; log grouped WARNING block via `log_validation_block`; verify no clean file is written after validation failure (clean file write only reachable after `validate()` returns without raising)

**Checkpoint**: US3 complete — validation failures are logged, scoped to one data type, and the result dict accurately reflects per-type outcomes.

---

## Phase 6: User Story 3b — Fetch and Store Game Scores (Priority: P3)

**Goal**: `run(["game_scores"], "LAL", 2023)` produces a clean parquet + sidecar in `data/clean/`.

**Independent Test**: Call `run(["game_scores"], "LAL", 2023)` with mocked API; verify parquet + sidecar written; summary dict returned with all required keys.

### Tests for game_scores ⚠️ Write first — verify they FAIL before implementing T028b

- [X] T028a [P] [US3b] Write failing integration test for game_scores in `backend/tests/integration/test_run_game_scores.py` — verify: clean parquet + sidecar written; summary dict returned with all required keys; corrupted rows (e.g. impossible scores) removed and logged

### Implementation for game_scores

- [X] T028b [US3b] Add game_scores ingestion path in `backend/pipeline/ingest.py` — implement `ingest_game_scores(player_or_team, season, logger) -> pd.DataFrame`: resolve team ID via team name or abbreviation; fetch `/games?team_ids[]={team_id}&seasons[]={season}` with chunked pagination; extract home and visitor scores, team names, date, status; apply drop/normalize/downcast; on API error/timeout: retry the failed chunk up to 3 times with exponential backoff (1s, 2s, 4s delays between attempts); if all 3 retries fail: log ERROR naming the chunk number and error, delete any partially written raw file, raise `IngestionError`; on lookup failure: raise `EntityLookupError`

**Checkpoint**: game_scores complete — `run(["game_scores"], "LAL", 2023)` produces correct output.

---

## Phase 7: User Story 4 — Retrieve Season Averages (Priority: P4)

**Goal**: `run(["season_averages"], "Stephen Curry", 2023)` produces a clean parquet + sidecar. dedup is skipped (no `game_id`) and `dedup_skipped: true` appears in sidecar.

**Independent Test**: Call `run(["season_averages"], "Stephen Curry", 2023)` with mocked API; verify parquet + sidecar written; sidecar has `dedup_skipped: true`.

### Tests for User Story 4 ⚠️ Write first — verify they FAIL before T029

- [X] T027 [P] [US4] Write failing integration test for season_averages in `backend/tests/integration/test_run_season_averages.py` — verify: clean parquet + sidecar written; `dedup_skipped: true` in sidecar; summary dict returned with all required keys; no minimum-row validation failure (10-row rule does not apply)

### Implementation for User Story 4

- [X] T028 [US4] Add season_averages ingestion path in `backend/pipeline/ingest.py` — implement `ingest_season_averages(player_or_team, season, logger) -> pd.DataFrame`: resolve player ID via `get_player_id()`; fetch `/season_averages?player_id={id}&season={season}`; wrap in chunked pattern (single page expected but treat consistently); apply drop/normalize/downcast; on lookup failure: raise `EntityLookupError`

**Checkpoint**: US4 complete — all four data types produce correct outputs.

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: File output safety tests, overwrite behavior, quickstart validation.

- [X] T029 [P] Write file output tests in `backend/tests/file_output/test_file_outputs.py` — assert: raw parquet + sidecar both written after successful ingest; clean parquet + sidecar both written after successful clean; exactly one sidecar per parquet (never zero, never two); filenames match `{data_type}_{player_or_team}_{season}.parquet` convention with normalized subject; run log file `logs/run_*.log` created per execution; no `.tmp` files remain after successful run; no `.tmp` files remain after simulated write failure; clean output NOT written when validation fails; last known good parquet survives a failed overwrite attempt
- [X] T030 [P] Write overwrite safety integration test in `backend/tests/integration/test_run_game_logs.py` — pre-seed `data/clean/` with a known good parquet; simulate write failure mid-run; assert pre-seeded file still intact; then run successfully and assert file updated
- [X] T031 Run quickstart.md validation — execute all code examples in `specs/001-nba-stats-pipeline/quickstart.md` against mocked API; confirm all examples produce expected output without errors

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (Setup)**: No dependencies — start immediately
- **Phase 2 (Foundational)**: Depends on Phase 1 — BLOCKS all user story phases
- **Phase 3 (US1)**: Depends on Phase 2 complete
- **Phase 4 (US2)**: Depends on Phase 3 complete (shares `ingest.py` and `run.py`)
- **Phase 5 (US3)**: Depends on Phase 3 (wires into existing `run.py` and `validate.py`)
- **Phase 6 (game_scores)**: Depends on Phase 2 complete — can run in parallel with US1, US2, US4
- **Phase 7 (US4)**: Depends on Phase 3 complete (adds to `ingest.py`)
- **Phase 8 (Polish)**: Depends on Phases 3–7 all complete

### User Story Dependencies

- **US1 (P1)**: Can start after Phase 2 — no dependency on other stories
- **US2 (P2)**: Depends on US1 (`run.py` must exist; adds to `ingest.py`)
- **US3 (P3)**: Depends on US1 (wires into `run.py` error path — already partially implemented in T018)
- **US4 (P4)**: Depends on US1 (`run.py` must exist; adds to `ingest.py`)

### TDD Order Within Each Phase

1. Write test(s) marked ⚠️
2. Confirm tests FAIL (red)
3. Implement module
4. Confirm tests PASS (green)
5. Refactor if needed — tests stay green

### Parallel Opportunities

Within Phase 2:
- T006 (exceptions.py) and T008 (logging tests) can run in parallel with T005 (constants.py)
- T010 (save tests) and T012 (validate tests) and T014 (clean tests) can run in parallel once their respective implementations are done

Within each US phase:
- All `[P]`-marked test tasks can be written in parallel before implementation begins

---

## Parallel Example: Phase 2

```
# Write all Phase 2 tests in parallel once T005 constants are done:
Task T008: test_logging_utils.py
Task T010: test_save.py
Task T012: test_validate.py
Task T014: test_clean.py

# Then implement modules in order:
T007 → T009 → T011 → T013
(logging_utils → save → validate → clean)
```

## Parallel Example: User Story 1

```
# Write tests in parallel:
Task T015: test_input_validation.py
Task T016: test_run_game_logs.py

# Then implement in order:
T017 (ingest.py game_logs) → T018 (run.py) → T019 (__init__.py)
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (all modules + unit tests green)
3. Complete Phase 3: US1 (game_logs end-to-end)
4. **STOP and VALIDATE**: `run(["game_logs"], "LeBron James", 2023)` produces correct output
5. Demo or validate before proceeding

### Incremental Delivery

1. Setup + Foundational → core infrastructure ready
2. US1 → game_logs pipeline working (MVP)
3. US2 → box_scores added (team+season and game_id)
4. US3 → validation failure behavior verified
5. game_scores → game score data type added (parallel with US2/US4)
6. US4 → season_averages added
7. Polish → file output safety + overwrite tests

---

## Notes

- `[P]` tasks touch different files and have no incomplete dependencies — safe to parallelize
- `[Story]` label maps each task to a user story for traceability
- Constitution Principle III (TDD) is non-negotiable: every ⚠️ test task MUST be red before its implementation task begins
- Each user story phase is independently completable and deliverable
- Commit after each task or logical group; commit message format: `T014: implement game_logs ingestion in ingest.py`
