# Implementation Plan: NBA Stats Data Pipeline

**Branch**: `001-nba-stats-pipeline` | **Date**: 2026-03-31 | **Spec**: [spec.md](spec.md)  
**Input**: Feature specification from `/specs/001-nba-stats-pipeline/spec.md`

## Summary

Build a modular, memory-efficient NBA stats data pipeline under `backend/pipeline/` that accepts a list of data types, fetches from the BallDontLie API via existing `api.py` helpers, validates, cleans, and writes both raw and clean parquet outputs with JSON sidecars using a safe temp-then-replace write strategy. Phase 1 supports `game_logs`, `box_scores`, and `season_averages`. The public entry point is `run(data_types, player_or_team, season)` returning a dict keyed by data type.

## Technical Context

**Language/Version**: Python 3.11+  
**Primary Dependencies**: `requests`, `pandas`, `pyarrow`, `python-dotenv` (standard library only — no additional packages)  
**Storage**: snappy-compressed parquet files (`data/raw/`, `data/clean/`), JSON sidecars alongside each parquet, per-run log files under `logs/`  
**Testing**: pytest — unit, integration, and file output tests in `backend/tests/`  
**Target Platform**: macOS / Linux developer machine  
**Project Type**: CLI pipeline / importable library  
**Performance Goals**: Full season of game logs (82 games) processed end-to-end in under 30 seconds on a standard laptop  
**Constraints**: Peak memory ≤ 200 MB per run; exactly 5 permitted libraries; no new external dependencies  
**Scale/Scope**: Single player or team per run; three data types in phase 1; chunked ingestion to avoid full-dataset loads

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-checked after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. User-First Design | ✓ PASS | Developer-facing pipeline. `run()` is the single entry point. Errors are typed and descriptive. Grouped validation failures prevent fix-one-run-discover-next cycling. |
| II. Data Integrity | ✓ PASS | Single authoritative source (BallDontLie API). Corrupted rows removed with logged justification. Nulls preserved rather than imputed. Raw parquet written before clean for full lineage. |
| III. Test-First Development | ✓ PASS | Testing is Phase 1 work (not deferred). Unit, integration, and file output tests specified before implementation begins. TDD order: constants → exceptions → tests → implementation. |
| IV. Performance & Responsiveness | ✓ PASS | SC-001 (≤200 MB peak), SC-002 (82 games < 30s). Chunked ingest, immediate column drops, numeric downcasting, and `del` + `gc.collect()` between steps enforce this. No UI thread blocking (pipeline has no UI). |
| V. Simplicity & YAGNI | ✓ PASS | `game_scores` deferred. Each module has one responsibility. No abstract base classes or plugin architecture introduced until needed. Hardcoded thresholds over configurable ones for now. |

**Gate result: PASS — proceed to Phase 0.**

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
├── api.py                       # Existing — reused, not modified
├── .env                         # BALL_IS_LIFE key
├── pipeline/
│   ├── __init__.py              # Exports run()
│   ├── run.py                   # Orchestration entry point
│   ├── ingest.py                # Chunked fetch + initial shaping
│   ├── validate.py              # Pre-clean schema/type/row validation
│   ├── clean.py                 # Cleaning, outlier flagging, corruption removal
│   ├── save.py                  # Temp-then-replace parquet + sidecar writes
│   ├── logging_utils.py         # Per-run dual-sink logger
│   ├── constants.py             # Columns, dtypes, thresholds, paths
│   └── exceptions.py            # Typed exception classes
├── data/
│   ├── raw/                     # Raw parquet + sidecars
│   └── clean/                   # Clean parquet + sidecars
├── logs/                        # Per-run log files
└── tests/
    ├── unit/
    │   ├── test_input_validation.py
    │   ├── test_ingest.py
    │   ├── test_validate.py
    │   └── test_clean.py
    ├── integration/
    │   ├── test_run_game_logs.py
    │   ├── test_run_multi_type.py
    │   └── test_run_box_scores.py
    └── file_output/
        └── test_file_outputs.py
```

**Structure Decision**: Single backend package. All pipeline code lives under `backend/pipeline/`. Tests are co-located under `backend/tests/` organized by test type (unit / integration / file_output).

## Complexity Tracking

No constitution violations. No complexity justification required.

---

## Phase 0: Research

See [research.md](research.md) for all resolved decisions.

## Phase 1: Design

See [data-model.md](data-model.md), [contracts/](contracts/), and [quickstart.md](quickstart.md).
