# Tasks: NBA Stats Data Pipeline

**Input**: Design documents from `/specs/001-nba-stats-pipeline/`  
**Prerequisites**: plan.md ✓, spec.md ✓, research.md ✓, data-model.md ✓, contracts/ ✓, quickstart.md ✓

**Tests**: Included — TDD is mandatory per constitution Principle III.

**Organization**: Tasks grouped by user story to enable independent implementation and testing.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no incomplete dependencies)
- **[Story]**: Which user story this task belongs to (US1–US4)
- Exact file paths in every task description

## Path Conventions

All paths relative to repository root. Pipeline code lives under `backend/`.

---

## ✅ Phases 1–8: Original Implementation (Complete)

> Tasks T001–T031 completed in the original build. All four data types operational with the original architecture (`ingest.py`, `validate.py`, `clean.py`, `save.py`, `constants.py`, `exceptions.py`, `logging_utils.py`).

---

## Phase 9: Spec Update — Refactored Architecture (2026-04-14)

**Purpose**: Implement all changes from the 2026-04-14 spec update. This phase refactors the existing pipeline into the new module structure and wires the updated `run()` signature.

**⚠️ CRITICAL**: Complete the foundational refactor tasks (T032–T042) fully before beginning user story phases.

**Spec changes driving this phase**:
- New `run()` signature: separate `player`/`team` args, `columns`, `output_dir`, `features_config`, `labels_config`
- Natural-grain composite deduplication (replaces `game_id`-only dedup)
- Configurable impossible-value thresholds (upper + lower bounds)
- Required vs. optional validation distinction (halt vs. warn)
- Schema drift tracking with first-run baseline
- Capture all API fields (no hardcoded UNUSED_COLUMNS drop)
- Column filter applied after cleaning (identity columns always kept)
- Sidecar renamed: `.json` → `.sidecar.json`; feature sidecar: `.features.sidecar.json`

---

### Phase 9A: Foundational Refactor (Blocks all story phases)

- [X] T032 Replace `backend/pipeline/constants.py` with `backend/pipeline/config.py` — define: `IDENTITY_COLUMNS = ["player_id","player_name","team_id","team","game_id","date","season","opponent"]`; `REQUIRED_STRUCTURAL` dict per data type; `DEDUP_KEYS` and `DEDUP_FALLBACK_KEYS` dicts; `TIME_SERIES_TYPES = ["game_logs","box_scores","game_scores"]`; `IMPOSSIBLE_VALUE_THRESHOLDS = {"upper":{...},"lower":{...}}`; `CONTEXT_FIELDS`; `SUPPORTED_DATA_TYPES`; remove `UNUSED_COLUMNS` entirely
- [X] T033 [P] Write failing unit tests for `config.py` in `backend/tests/unit/test_config.py` — verify all constants present, `IDENTITY_COLUMNS` has all 8 keys, `DEDUP_KEYS` covers all 4 data types, `IMPOSSIBLE_VALUE_THRESHOLDS` has both `upper` and `lower` dicts with correct values
- [X] T034 Expand `backend/pipeline/exceptions.py` — add `PipelineConfigError(Exception)`, `APIKeyMissingError(PipelineConfigError)`, `APIFetchError(Exception)`, `FeatureVersionMismatchError(Exception)`; preserve existing `ValidationError`, `IngestionError`, `FileWriteError`, `EntityLookupError`; update default messages
- [X] T035 [P] Write failing unit tests for updated `exceptions.py` in `backend/tests/unit/test_exceptions.py` — verify all 9 exception classes importable, raise correctly, carry expected default messages, and follow correct inheritance chain
- [X] T036 Create `backend/pipeline/sidecar.py` — implement `build_base_sidecar(df, cleaning_stats, dedup_meta, drift_meta, thresholds) -> dict` per updated `data-model.md` BaseSidecar schema (includes `schema_drift`, `thresholds_applied`, `dedup_conflicts`); implement `build_feature_sidecar(feature_names, rolling_windows, min_observations, scoring_config, version, success) -> dict` per FeatureSidecar schema; delete sidecar builders from `backend/pipeline/save.py`
- [X] T037 [P] Write failing unit tests for `sidecar.py` in `backend/tests/unit/test_sidecar.py` — verify base sidecar contains all required keys; feature sidecar contains `feature_schema_version`; `thresholds_applied` dict matches what was passed in; `schema_drift` key present with correct shape
- [X] T038 Create `backend/pipeline/writer.py` from `backend/pipeline/save.py` — rename file; update sidecar filenames from `.json` to `.sidecar.json`; update feature sidecar path to `.features.sidecar.json`; import sidecar builders from `sidecar.py` (not locally defined); preserve atomic write logic
- [X] T039 [P] Write failing unit tests for `writer.py` in `backend/tests/unit/test_writer.py` — verify `.sidecar.json` extension used (not `.json`); sidecar not written if parquet write fails; no `.tmp` files remain after failure; atomic rename occurs

**Checkpoint A**: Config, exceptions, sidecar, and writer modules updated. Unit tests green.

- [X] T040 Create `backend/pipeline/deduplicator.py` — implement `deduplicate(df, data_type) -> tuple[pd.DataFrame, dict]`: look up primary keys from `config.DEDUP_KEYS`; if primary key columns all present, use them; else fall back to `config.DEDUP_FALLBACK_KEYS`; if neither set present, return `(df, {"dedup_skipped":True,"dedup_reason":"...","dedup_conflicts":0})`; when duplicates found with differing values on composite key, keep most-recently-ingested row (last occurrence), log conflict count; return `(deduped_df, dedup_meta)` where meta contains `dedup_skipped`, `dedup_reason`, `dedup_conflicts`
- [X] T041 [P] Write failing unit tests for `deduplicator.py` in `backend/tests/unit/test_deduplicator.py` — verify: `game_logs` uses `player_id+game_id`; falls back to `player_name+game_id` when `player_id` absent; exact duplicates removed; conflicting duplicates keep last row and log conflict; `season_averages` skips dedup; skipped dedup returns correct metadata dict
- [X] T042 Create `backend/pipeline/outlier.py` — extract `_flag_outliers` from `backend/pipeline/clean.py`; implement as standalone `flag_outliers(df) -> tuple[pd.DataFrame, int]` returning `(df_with_is_outlier, count)`; remove outlier logic from `clean.py`
- [X] T043 [P] Write failing unit tests for `outlier.py` in `backend/tests/unit/test_outlier.py` — verify `is_outlier` column added; flagged rows have original values unchanged; count matches `df["is_outlier"].sum()`; column present even when no outliers found

**Checkpoint B**: Deduplicator and outlier modules complete. Unit tests green.

- [X] T044 Create `backend/pipeline/schema_drift.py` — implement `check_drift(df, data_type, baseline_dir) -> dict`: compute baseline path as `{baseline_dir}/{data_type}.json`; if file does not exist: write baseline (`{"columns": list(df.columns), "written_at": ...}`), return `{"first_run":True,"columns_added":[],"columns_missing":[],"identity_columns_missing":[]}`; if file exists: compare current columns to baseline; detect added, missing; check `IDENTITY_COLUMNS` present in current df; log WARNING if any identity column missing; return drift dict
- [X] T045 [P] Write failing unit tests for `schema_drift.py` in `backend/tests/unit/test_schema_drift.py` — verify: first run writes baseline JSON; first-run return dict has `first_run:True`; second run with same columns returns empty diffs; second run with removed column reports it in `columns_missing`; missing identity column triggers WARNING log
- [X] T046 Rewrite `backend/pipeline/clean.py` as `backend/pipeline/cleaner.py` — update `clean(df, data_type, columns, thresholds)` signature; remove hardcoded `CORRUPTION_THRESHOLDS` reference — accept `thresholds` param instead; remove outlier step (now in `outlier.py`); remove dedup step (now in `deduplicator.py`); apply configurable thresholds for both upper and lower bounds; add sort step (step 10): for `TIME_SERIES_TYPES`, sort by `date` ASC then `game_id` ASC; add column filter step (step 11): keep `IDENTITY_COLUMNS` + caller-specified `columns` (warn on unrecognized column names, silently skip); preserve steps 1–9 from original `clean.py`
- [X] T047 [P] Write failing unit tests for `cleaner.py` in `backend/tests/unit/test_cleaner.py` — verify: configurable thresholds used (not hardcoded); lower-bound negatives flagged and removed; time-series output sorted by date then game_id; unrecognized column in `columns` arg logs WARNING and is ignored; identity columns always present in output regardless of `columns` arg; idempotent on double-run
- [X] T048 Create `backend/pipeline/fetcher.py` from `backend/pipeline/ingest.py` — rename file; update all four ingest functions to capture ALL fields from API response (remove hardcoded field selection in flatten functions — use `{k:v for k,v in record.items()}` pattern or include all documented fields); remove `_drop_unused` function entirely; split `player_or_team` param into `player` and `team` params per new signature; add `position` field capture from `/players` endpoint in player lookup; preserve retry logic
- [X] T049 [P] Write failing unit tests for `fetcher.py` in `backend/tests/unit/test_fetcher.py` — verify: all API fields captured in output DataFrame (not just hardcoded subset); `position` field present for player-based data types; `player` and `team` args route correctly; `APIKeyMissingError` raised before any network call when env var absent; `APIFetchError` raised and no partial file on mid-stream failure
- [X] T050 Rewrite `backend/pipeline/validator.py` from `backend/pipeline/validate.py` — update `validate(df, data_type, logger) -> tuple[bool, list[str]]`; distinguish required structural fields (from `config.REQUIRED_STRUCTURAL`) from optional stat fields; missing required → append to `required_failures`; missing optional → append to `optional_warnings`; emit required failures as ERROR, optional warnings as WARNING; return `(False, required_failures)` if any required failures; return `(True, optional_warnings)` if only optional issues; remove `game_logs`-specific 10-row hardcoded check — make min_rows configurable parameter defaulting to 10 for game_logs only
- [X] T051 [P] Write failing unit tests for `validator.py` in `backend/tests/unit/test_validator.py` — verify: missing structural field returns `False`; missing optional stat field returns `True` with warning; multiple required failures all collected before returning; valid df returns `True` with empty list; min-rows check only applies to game_logs; return type is always `tuple[bool, list[str]]`

**Checkpoint C**: All refactored modules implemented with passing unit tests.

---

### Phase 9B: Updated run() and User Story Wiring

**US1 — Game Logs**

**Independent Test**: `run(data_types=["game_logs"], season=2023, player="LeBron James")` produces `data/clean/game_logs_lebron_james_2023.parquet` and `data/clean/game_logs_lebron_james_2023.sidecar.json`. Sidecar contains `schema_drift`, `thresholds_applied`, `dedup_conflicts`. Summary dict has `status: "success"`.

- [X] T052 [P] [US1] Write failing integration test for updated game_logs run in `backend/tests/integration/test_run_game_logs.py` — verify: new `run()` signature accepted; `.sidecar.json` extension used; summary dict has `status` key; sidecar contains `schema_drift` and `thresholds_applied`; `columns=["pts","reb"]` retains identity columns in parquet; unrecognized column name in `columns` logs WARNING and doesn't crash
- [X] T053 [US1] Rewrite `backend/pipeline/run.py` — update `run()` to new 8-param signature; raise `PipelineConfigError` if both `player` and `team` provided; pass `player`/`team` separately to `fetcher.py`; pass `thresholds` from `config.IMPOSSIBLE_VALUE_THRESHOLDS` to `cleaner.py`; call `deduplicator.deduplicate()` as separate step; call `outlier.flag_outliers()` as separate step; call `schema_drift.check_drift()` and include drift meta in sidecar; apply `columns` filter via `cleaner.clean()`; use `writer.py` (not `save.py`); update return dict to include `"status"` key (`"success"` or `"failed"`); update `output_dir` resolution to `{output_dir}/raw/` and `{output_dir}/clean/`; keep per-type error isolation
- [X] T054 [US1] Update `backend/pipeline/__init__.py` — expose only `run` from `pipeline.run`; add `__all__ = ["run"]`

**Checkpoint D**: `run(data_types=["game_logs"], season=2023, player="LeBron James")` works end-to-end with new signature. Integration tests green.

**US2 — Box Scores**

**Independent Test**: `run(data_types=["box_scores"], season=2023, team="LAL")` and `run(data_types=["box_scores"], season=2023, team="18370647")` both produce output with `.sidecar.json`.

- [X] T055 [P] [US2] Write failing integration test for updated box_scores run in `backend/tests/integration/test_run_box_scores.py` — verify both team+season and game_id paths work with new `team=` arg; `.sidecar.json` extension present; summary has `status` key
- [X] T056 [US2] Update box_scores path in `backend/pipeline/fetcher.py` to accept `team` arg (not `player_or_team`); verify game_id detection still works via `int(team)` cast

**Checkpoint E**: Box scores working with updated signature.

**US3 — Validation**

**Independent Test**: Mock API returning 5 rows for game_logs; verify `result["game_logs"]["status"] == "failed"` and no clean file written. Mock API missing optional `fg_pct` column; verify run succeeds with warning.

- [X] T057 [P] [US3] Write failing integration tests for new validation behavior in `backend/tests/integration/test_run_game_logs.py` — verify: missing required structural field → `status:"failed"` + no clean file; missing optional stat field → `status:"success"` + warning logged; multiple required failures all appear in result error string

**US4 — Season Averages**

**Independent Test**: `run(data_types=["season_averages"], season=2023, player="Stephen Curry")` produces output with `dedup_skipped:true` in `.sidecar.json`.

- [X] T058 [P] [US4] Write failing integration test for updated season_averages run in `backend/tests/integration/test_run_season_averages.py` — verify `player=` arg accepted; `dedup_skipped:true` in sidecar; all API fields captured (not just original hardcoded subset)

**Multi-type and cross-cutting**

- [X] T059 [P] Write failing integration test for multi-type run with new signature in `backend/tests/integration/test_run_multi_type.py` — verify: `run(data_types=["game_logs","season_averages"], season=2023, player="LeBron James")` returns dict keyed by both types; one type failure doesn't stop others; both result dicts have `status` key
- [X] T060 [P] Write failing file output tests for new sidecar naming in `backend/tests/file_output/test_file_outputs.py` — verify `.sidecar.json` files written (not `.json`); feature sidecar uses `.features.sidecar.json`; `schema_baselines/` directory created automatically; baseline JSON written on first run

**Checkpoint F**: All four data types operational with new signature. All integration tests green.

---

## Phase 10: Optional Feature Engineering Layer

**Purpose**: Implement FR-031–039 (features, labels, versioning). Only runs when `features_config` / `labels_config` is provided to `run()`.

**Independent Test**: `run(data_types=["game_logs"], season=2023, player="LeBron James", features_config={"rolling_windows":[5,10],"min_observations":5,"scoring":{"pts":1.0,"reb":1.2,"ast":1.5,"stl":3.0,"blk":3.0}})` produces a `.features.sidecar.json` alongside the clean parquet. Rolling features use only prior rows (`.shift(1)` before `.rolling()`). Rows with fewer than `min_observations` prior games are `null`.

- [X] T061 [P] Write failing unit tests for `features/versioning.py` in `backend/tests/unit/test_versioning.py` — verify: same config always produces same version string; changing any config key produces different version; version string is 12 hex chars; `FeatureVersionMismatchError` raised when stored version differs from current
- [X] T062 [P] Write failing unit tests for `features/engineer.py` in `backend/tests/unit/test_engineer.py` — verify: rolling features use `.shift(1)` before `.rolling()` (assert current row's stat NOT in its own rolling window); rows with fewer prior games than `min_observations` are `null`; `is_outlier` column not used as input feature; `fantasy_pts` computed correctly from scoring config
- [X] T063 [P] Write failing unit tests for `features/labels.py` in `backend/tests/unit/test_labels.py` — verify: `next_game_pts` is `null` for last game per player; `is_end_of_series` is `True` for last game; labels computed from `.shift(-1)` on sorted time series; label for game N is stat from game N+1 (not N)
- [X] T064 [P] Write failing integration test for features run in `backend/tests/integration/test_run_game_logs.py` — verify: `features_config` provided → `.features.sidecar.json` written; feature sidecar contains `feature_schema_version`, `rolling_windows`, `min_observations`; no feature sidecar written when `features_config=None`
- [X] T065 Implement `backend/pipeline/features/versioning.py` — `get_feature_version(features_config: dict) -> str`: serialize config with `json.dumps(features_config, sort_keys=True)`, SHA-256 hash, return first 12 hex chars; implement `assert_version_compatible(stored_version, current_version)` that raises `FeatureVersionMismatchError` if they differ
- [X] T066 Implement `backend/pipeline/features/engineer.py` — `engineer(df, features_config) -> tuple[pd.DataFrame, list[str]]`: for each rolling window N in `features_config["rolling_windows"]`: compute `rolling_{stat}_N = df.groupby("player_id")[stat].shift(1).rolling(N, min_periods=min_obs).mean()` for each stat; compute rolling std dev; compute `{stat}_delta` (last game minus rolling mean); compute `fantasy_pts` from scoring config; compute `hot_streak` (above rolling mean last 3 games); derive `days_rest` from date diff; derive `back_to_back` from `days_rest == 0`; return `(df_with_features, list_of_feature_names)`
- [X] T067 Implement `backend/pipeline/features/labels.py` — `generate_labels(df, labels_config) -> tuple[pd.DataFrame, dict]`: for each target in `labels_config["targets"]`: compute `next_game_{target} = df.groupby("player_id")[target].shift(-1)` on sorted time series; compute threshold labels (e.g. `pts_20_plus = next_game_pts >= 20`); mark `is_end_of_series = True` for rows where `next_game_pts` is null; return `(df_with_labels, label_meta)` where meta contains `end_of_series_count` and list of label names generated
- [X] T068 Extend `backend/pipeline/run.py` — add optional features + labels layers after clean parquet confirmed written: if `features_config` provided, call `engineer()` → write features parquet → call `generate_labels()` if `labels_config` provided → build feature sidecar via `sidecar.build_feature_sidecar()` → write `.features.sidecar.json`; include `feature_schema_version` in feature sidecar; entire features block wrapped in try/except — failure logs ERROR but does NOT fail the base data type result

**Checkpoint G**: Feature engineering layer works end-to-end. Feature sidecar written with version identifier. Labels null for end-of-series rows.

---

## Phase 11: Polish & Cross-Cutting Concerns

**Purpose**: Edge case coverage, contract updates, cleanup of deprecated files.

- [X] T069 [P] Update `specs/001-nba-stats-pipeline/contracts/sidecar_schema.md` — replace existing raw/clean sidecar schemas with new BaseSidecar schema (including `schema_drift`, `thresholds_applied`, `dedup_conflicts`) and FeatureSidecar schema (including `feature_schema_version`, `end_of_series_rows`); update filename convention from `.json` to `.sidecar.json`
- [X] T070 [P] Write integration test: `columns` with unrecognized name in `backend/tests/integration/test_run_game_logs.py` — verify WARNING logged naming the bad column; run succeeds; identity columns present in parquet; unrecognized column absent without error
- [X] T071 [P] Write integration test: parquet write failure mocked in `backend/tests/integration/test_run_game_logs.py` — verify no `.sidecar.json` written; no `.tmp` files remain; result dict has `status:"failed"`; existing pre-seeded clean file untouched
- [X] T072 [P] Write integration test: API mid-stream chunk failure in `backend/tests/integration/test_run_game_logs.py` — mock chunk 3 of 8 to raise; verify no raw parquet written; no `.sidecar.json` written; result dict has `status:"failed"` with error message
- [X] T073 [P] Write integration test: schema drift second run in `backend/tests/integration/test_run_game_logs.py` — first run writes baseline; mock second run with one column removed; verify sidecar `schema_drift.columns_missing` contains the removed column; WARNING logged
- [X] T074 Delete deprecated files: remove `backend/pipeline/ingest.py`, `backend/pipeline/validate.py`, `backend/pipeline/clean.py`, `backend/pipeline/save.py`, `backend/pipeline/constants.py`, `backend/pipeline/logging_utils.py` — update any imports in existing test files that reference these old module names
- [X] T075 [P] Update `specs/001-nba-stats-pipeline/quickstart.md` — replace all `run(data_types, player_or_team, season)` call examples with new `run(data_types, season, player=..., team=...)` signature; add column filter example; add features_config example
- [X] T076 Run full test suite from `backend/` — `pytest tests/` — all tests green; no import errors from deleted modules

**Checkpoint H**: All tests green. No deprecated files. Contracts updated. Quickstart valid.

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 9A (Foundational Refactor)**: Depends on completed original implementation — can start immediately
- **Phase 9B (US Wiring)**: Depends on Phase 9A complete — all new modules must exist before `run.py` rewrite
- **Phase 10 (Features Layer)**: Depends on Phase 9B complete — features run after base pipeline confirmed working
- **Phase 11 (Polish)**: Depends on Phases 9A–10 complete

### Within Phase 9A

```
T032 (config.py) → T033 [P] (config tests)
T034 (exceptions.py) → T035 [P] (exception tests)
T036 (sidecar.py) → T037 [P] (sidecar tests)
T038 (writer.py) → T039 [P] (writer tests)

Once T032–T039 done:
T040 (deduplicator.py) → T041 [P] (dedup tests)
T042 (outlier.py) → T043 [P] (outlier tests)
T044 (schema_drift.py) → T045 [P] (drift tests)
T046 (cleaner.py) → T047 [P] (cleaner tests)
T048 (fetcher.py) → T049 [P] (fetcher tests)
T050 (validator.py) → T051 [P] (validator tests)
```

### Parallel Opportunities

```
# Phase 9A — can parallelize these groups after their prerequisite:
Group A (no deps): T032, T034
Group B (after T032): T033, T036, T038
Group C (after T036,T038): T037, T039, T040, T042, T044, T046, T048, T050

# Phase 10 — all test tasks [P] can be written in parallel:
T061, T062, T063, T064 (write all failing tests before any implementation)

# Phase 11 — all [P] tasks can run in parallel:
T069, T070, T071, T072, T073, T075
```

---

## Implementation Strategy

### Immediate Next Step (Recommended)

1. Complete Phase 9A foundational refactor (T032–T051) — all new modules + passing unit tests
2. Rewrite `run.py` (T053) — wire new signature through all new modules
3. Run existing integration tests — expect failures; fix broken imports from renamed modules
4. Phase 11 cleanup (T074) — delete deprecated files last, after all tests green

### Feature Layer (After Base Pipeline Stable)

1. T061–T064: Write all failing feature tests in parallel
2. T065–T067: Implement versioning, engineer, labels
3. T068: Wire into run.py
4. Confirm T064 integration test green

---

## Notes

- `[P]` tasks touch different files with no incomplete dependencies — safe to parallelize
- `[Story]` label maps task to user story for traceability
- Constitution Principle III (TDD) is non-negotiable — unit tests must be written and red before implementation
- Delete old modules (T074) only after all tests pass — avoids broken-import cascades
- Commit after each logical group: `T046: implement cleaner.py with configurable thresholds`
- Each checkpoint is a valid stopping and validation point
