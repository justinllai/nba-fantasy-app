# Implementation Plan: NBA Stats Data Pipeline

**Branch**: `001-nba-stats-pipeline` | **Date**: 2026-04-14 | **Spec**: [spec.md](spec.md)  
**Input**: Feature specification from `/specs/001-nba-stats-pipeline/spec.md`

## Summary

Build a modular, memory-efficient NBA stats data pipeline under `backend/pipeline/` that ingests from the BallDontLie API, validates, cleans, deduplicates, detects schema drift, optionally engineers features, and writes parquet outputs with JSON sidecars atomically. The pipeline now supports configurable thresholds, natural-grain deduplication, an optional derived-feature layer, optional label generation, and feature versioning. The public entry point is `run()` returning a dict keyed by data type.

## Technical Context

**Language/Version**: Python 3.11+  
**Primary Dependencies**: `requests`, `pandas`, `pyarrow`, `python-dotenv` (standard library only — no additional packages)  
**Storage**: snappy-compressed parquet files (`data/raw/`, `data/clean/`), JSON sidecars (`.sidecar.json`), feature sidecars (`.features.sidecar.json`), schema baseline JSONs (`data/schema_baselines/`), per-run log files under `logs/`  
**Testing**: pytest — unit, integration, and file output tests in `backend/tests/`  
**Target Platform**: macOS / Linux developer machine  
**Project Type**: CLI pipeline / importable library  
**Performance Goals**: Full season of game logs (82 games) processed end-to-end in under 30 seconds on a standard laptop  
**Constraints**: Peak memory ≤ 200 MB per run; exactly 5 permitted libraries; no new external dependencies  
**Scale/Scope**: Single player or team per run; four data types; chunked ingestion; optional feature and label layers

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-checked after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. User-First Design | ✓ PASS | Developer-facing pipeline. `run()` remains the single entry point. Typed exceptions with clear messages. Separate `player`/`team` args remove ambiguity. |
| II. Data Integrity | ✓ PASS | Natural-grain dedup (not just `game_id`). Configurable thresholds logged to sidecar. Schema drift tracked per run. Anti-leakage enforced on all derived features. Conflict resolution logged. |
| III. Test-First Development | ✓ PASS | Unit tests required per module before integration. TDD order: exceptions → config → module → tests → implementation. |
| IV. Performance & Responsiveness | ✓ PASS | SC-001 (≤200 MB peak), SC-002 (82 games < 30s). `del` + `gc.collect()` between steps. Features layer is optional and runs after base clean is confirmed written. |
| V. Simplicity & YAGNI | ⚠ JUSTIFIED | Feature engineering, label generation, and versioning add significant scope beyond the core pipeline. Each is opt-in via `features_config` / `labels_config` — base pipeline runs identically when omitted. Complexity is justified by the prediction workflow requirements in FR-030–039. See Complexity Tracking. |

**Gate result: PASS (with justified complexity) — proceed to Phase 0.**

## Project Structure

### Documentation (this feature)

```text
specs/001-nba-stats-pipeline/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/
│   ├── run_function.md  # Public function contract
│   └── sidecar_schema.md  # JSON sidecar schema
└── tasks.md             # Phase 2 output (/speckit.tasks)
```

### Source Code

```text
backend/
├── api.py                        # Existing — do not modify
├── .env                          # BALL_IS_LIFE key
├── pipeline/
│   ├── __init__.py               # Exposes run() only
│   ├── run.py                    # Single public entry point + logging setup
│   ├── config.py                 # Thresholds, identity columns, dedup keys, constants
│   ├── fetcher.py                # Chunked ingestion, wraps api.py
│   ├── validator.py              # Required structural + optional field validation
│   ├── cleaner.py                # All cleaning transforms (10 steps)
│   ├── deduplicator.py           # Natural-grain deduplication
│   ├── outlier.py                # Tukey fence flagging
│   ├── schema_drift.py           # Baseline write + drift detection
│   ├── sidecar.py                # Sidecar builders (base + feature)
│   ├── writer.py                 # Atomic parquet + sidecar writes
│   ├── features/
│   │   ├── __init__.py
│   │   ├── engineer.py           # Rolling features, streaks, splits
│   │   ├── labels.py             # Next-game label generation
│   │   └── versioning.py        # Feature schema version tracking
│   └── exceptions.py            # All typed exceptions
├── data/
│   ├── raw/                      # Created at runtime if missing
│   ├── clean/                    # Created at runtime if missing
│   └── schema_baselines/         # One JSON per data_type (FR-033)
├── logs/                         # Created at runtime if missing
└── tests/
    ├── unit/
    │   ├── test_exceptions.py
    │   ├── test_config.py
    │   ├── test_fetcher.py
    │   ├── test_validator.py
    │   ├── test_cleaner.py
    │   ├── test_deduplicator.py
    │   ├── test_outlier.py
    │   ├── test_schema_drift.py
    │   ├── test_sidecar.py
    │   └── test_writer.py
    ├── integration/
    │   ├── test_run_game_logs.py
    │   ├── test_run_box_scores.py
    │   ├── test_run_season_averages.py
    │   ├── test_run_game_scores.py
    │   └── test_run_multi_type.py
    └── file_output/
        └── test_file_outputs.py
```

**Structure Decision**: Single backend package. Pipeline split into focused single-responsibility modules. Optional `features/` subpackage runs only when configured. All tests co-located under `backend/tests/`.

## Complexity Tracking

| Addition | Why Needed | Simpler Alternative Rejected Because |
|----------|------------|--------------------------------------|
| `features/engineer.py` | FR-031 requires rolling averages, streaks, and fantasy points for prediction workflows | Hardcoding rolling logic in `cleaner.py` violates single-responsibility; leakage rule requires `.shift(1)` discipline that deserves its own module |
| `features/labels.py` | FR-037 requires next-game label generation with end-of-series null handling | Labels are structurally different from features (shift -1 vs +1); merging into `engineer.py` would make leakage auditing harder |
| `features/versioning.py` | FR-039 — models must not silently consume stale feature schemas | A simple version string in the sidecar is insufficient; deterministic hash of config is the only way to catch config drift without a separate registry |
| `schema_drift.py` | FR-033 — detect when API adds or drops columns across runs | Adding drift detection to `validator.py` would conflate two concerns: structural validation (halt) vs. drift observation (warn) |
| `deduplicator.py` | FR-007 revised — composite natural-grain keys vary per data type | Embedding dedup logic in `cleaner.py` makes the key-selection logic untestable in isolation |
| Separate `player` / `team` args | Pre-implementation decision 0.1 — `player_or_team` is ambiguous for modeling keys | A single string arg conflates two semantically distinct concepts; composite key dedup and identity column retention require knowing which is which |

---

## Phase 0: Research

See [research.md](research.md) for all resolved decisions.

## Phase 1: Design

See [data-model.md](data-model.md), [contracts/](contracts/), and [quickstart.md](quickstart.md).

---

## Build Order

Build and test each module in this exact order. Do not proceed to the next step until the current module has passing unit tests.

### Step 1 — `exceptions.py`
No dependencies. Define all typed exceptions.

```python
class PipelineConfigError(Exception): ...    # bad args, missing env var
class APIKeyMissingError(PipelineConfigError): ...
class APIFetchError(Exception): ...          # timeout, 429, mid-stream failure
class ValidationError(Exception): ...        # missing required structural field
class ParquetWriteError(Exception): ...      # write failed
class FeatureVersionMismatchError(Exception): ...  # model/feature version conflict
```

**Done when**: All classes importable and raise correctly.

---

### Step 2 — `config.py`
No dependencies.

```python
IDENTITY_COLUMNS = ["player_id", "player_name", "team_id", "team", "game_id", "date", "season", "opponent"]
REQUIRED_STRUCTURAL = { "game_logs": [...], "box_scores": [...], ... }
DEDUP_KEYS = { "game_logs": ("player_id", "game_id"), ... }
DEDUP_FALLBACK_KEYS = { "game_logs": ("player_name", "game_id"), ... }
TIME_SERIES_TYPES = ["game_logs", "box_scores", "game_scores"]
IMPOSSIBLE_VALUE_THRESHOLDS = { "upper": {...}, "lower": {...} }
CONTEXT_FIELDS = ["home_or_away", "opponent", "won_game", ...]
```

**Done when**: All constants importable and match spec values.

---

### Step 3 — Logging (inside `run.py`)
Dual-sink logger: terminal + `logs/pipeline.log`. Creates `logs/` if missing.
Format: `YYYY-MM-DD HH:MM:SS | LEVEL | message`.

**Done when**: Single log call appears in both terminal and log file.

---

### Step 4 — `fetcher.py`
Wraps `api.py`. Raises `APIKeyMissingError` before any I/O if env var missing. Raises `APIFetchError` on mid-stream failure and leaves no partial files.

```python
def fetch(data_type, player, team, season, output_dir) -> pd.DataFrame: ...
```

**Done when**: Fetches real data; raises correctly on missing key and mid-stream failure.

---

### Step 5 — `writer.py`
Atomic write: parquet first (`.tmp` → rename), sidecar second. On failure: delete partial, do NOT write sidecar, log ERROR.

```python
def write_parquet(df, path) -> None: ...
def write_sidecar(data, path) -> None: ...
```

**Done when**: Atomic write works; simulated failure leaves no files.

---

### Step 6 — `validator.py`
Separates required structural fields (halt on missing) from optional stat fields (warn, continue). Collects all failures before reporting.

```python
def validate(df, data_type, min_rows=10) -> tuple[bool, list[str]]: ...
```

**Done when**: Missing required field → `False`; missing optional → `True` + warning; all failures logged together.

---

### Step 7 — `deduplicator.py`
Natural-grain composite keys per data type. Falls back to secondary keys. Conflicting values → keep most-recent, log conflict.

```python
def deduplicate(df, data_type) -> tuple[pd.DataFrame, dict]: ...
```

**Done when**: Correct keys used per type; conflict resolution works; skipped dedup returns correct metadata.

---

### Step 8 — `cleaner.py`
Ten transforms in exact order:
1. Downcast numerics
2. Strip non-printable chars
3. Title-case player names
4. Uppercase team abbreviations
5. Standardize dates → `YYYY-MM-DD`
6. Convert `min` MM:SS → decimal float
7. Remove DNP rows (`min == 0`)
8. Remove corrupted rows (configurable thresholds)
9. Log nulls, preserve them
10. Sort time-series by `date` ASC, `game_id` ASC
11. Apply column filter (identity columns always kept)

```python
def clean(df, data_type, columns, thresholds) -> tuple[pd.DataFrame, dict]: ...
```

**Done when**: Double-run produces identical output (idempotency).

---

### Step 9 — `outlier.py`
Tukey fence (1.5× IQR). Adds `is_outlier` bool column. Never removes rows.

```python
def flag_outliers(df) -> tuple[pd.DataFrame, int]: ...
```

**Done when**: Column present; flagged rows unmodified; count accurate.

---

### Step 10 — `schema_drift.py`
First run: write baseline JSON to `data/schema_baselines/{data_type}.json`.
Subsequent runs: compare columns, log WARNING on missing identity fields.

```python
def check_drift(df, data_type, baseline_dir) -> dict: ...
```

**Done when**: Baseline written on first run; missing column detected on second run.

---

### Step 11 — `sidecar.py`
Builds base sidecar dict and feature sidecar dict.

```python
def build_base_sidecar(df, cleaning_stats, dedup_meta, drift_meta, thresholds) -> dict: ...
def build_feature_sidecar(feature_names, rolling_windows, min_observations, scoring_config, version, success) -> dict: ...
```

---

### Step 12 — `run.py` (Integration)

Per data type flow:
```
1. Validate args → PipelineConfigError
2. Check BALL_IS_LIFE → APIKeyMissingError
3. Create output dirs
4. fetch() → raw DataFrame
5. write_parquet(raw) → write_sidecar(raw)
6. validate() → halt data type on required field failure
7. deduplicate()
8. clean()
9. flag_outliers()
10. check_drift()
11. write_parquet(clean) → write_sidecar(clean)  [sidecar written LAST]
12. [Optional] engineer features → write features parquet → write feature sidecar
13. [Optional] generate labels → write labels parquet → update feature sidecar
14. Append to result dict
```

**Done when**: Failure in one type doesn't stop others; full run produces correct parquet + sidecar.

---

### Step 13 — `features/engineer.py` (Optional)
Only runs when `features_config` is provided. Leakage rule: ALL rolling ops use `.shift(1)` before `.rolling()`.

```python
# Correct
df["rolling_pts_5"] = df.groupby("player_id")["pts"].shift(1).rolling(5, min_periods=min_obs).mean()
```

Rows below `min_observations` → `null`, not computed.

**Done when**: SC-009 — recomputing any feature uses only prior rows.

---

### Step 14 — `features/labels.py` (Optional)
Only runs when `labels_config` is provided. Uses `.shift(-1)` on sorted time series. Last game per player → label is `null`, flagged in sidecar.

**Done when**: SC-016 — end-of-series rows are null and flagged.

---

### Step 15 — `features/versioning.py`
Deterministic SHA-256 hash of `features_config`. Any config change → different version. Mismatch with stored model → `FeatureVersionMismatchError`.

```python
def get_feature_version(features_config: dict) -> str: ...
```

**Done when**: SC-015 — version present in every feature sidecar; mismatch raises correctly.

---

## Test Plan Summary

### Unit Tests (one file per module)

| Module | Key Assertions |
|--------|---------------|
| `exceptions.py` | All raise correctly |
| `config.py` | All constants present and correct |
| `fetcher.py` | Returns DataFrame; raises on missing key; no partial file on failure |
| `validator.py` | Required missing → False; optional missing → True + warning |
| `deduplicator.py` | Correct keys per type; conflict keeps most-recent; skip metadata correct |
| `cleaner.py` | All transforms in order; idempotent; DNP removed; corrupted removed |
| `outlier.py` | Column present; rows unmodified; count accurate |
| `schema_drift.py` | Baseline written on first run; drift detected on second |
| `writer.py` | Parquet before sidecar; no files on failure |

### Integration Tests

| Test | Validates |
|------|-----------|
| Full run — LeBron James, 2023, game_logs | SC-001, SC-002, SC-004 |
| Missing env var | FR-001 |
| Two types, one fails | SC-007 |
| `columns=["pts","reb"]` | FR-004 — identity columns always present |
| Clean step twice | SC-003 — idempotent |
| <10 rows mocked | FR-017 |
| Parquet write fails (mocked) | FR-025 |
| API chunk failure (mocked) | FR-026 |
| Features run | SC-009, SC-017 |
| Labels run | SC-016 |

## Constraints (Non-Negotiable)

| Constraint | Rule |
|------------|------|
| Libraries | `requests`, `pandas`, `pyarrow`, `python-dotenv`, stdlib only |
| API logic | Reuse `load()`, `get_player_id()`, `get_game_logs()` — do not reimplement |
| Memory | `del` intermediate DataFrames + `gc.collect()` between steps |
| Idempotency | Same inputs → identical outputs; overwrites previous |
| Concurrency | All API calls sequential — threading out of scope for v1 |
| Sidecar order | Sidecar written after parquet confirmed on disk |
| Feature splits | Chronological only — random splits prohibited |
