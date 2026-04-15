# nba-fantasy-app Development Guidelines

Auto-generated from all feature plans. Last updated: 2026-04-14

## Active Technologies

- Python 3.11+ + `requests`, `pandas`, `pyarrow`, `python-dotenv` (standard library only — no additional packages) (001-nba-stats-pipeline)

## Project Structure

```text
backend/
├── api.py                        # BallDontLie API client — do not modify
├── pipeline/
│   ├── run.py                    # Single public entry point
│   ├── config.py                 # Thresholds, identity columns, dedup keys
│   ├── fetcher.py                # Chunked ingestion
│   ├── validator.py              # Required structural + optional field validation
│   ├── cleaner.py                # 10-step cleaning pipeline
│   ├── deduplicator.py           # Natural-grain composite deduplication
│   ├── outlier.py                # Tukey fence flagging
│   ├── schema_drift.py           # Baseline write + drift detection
│   ├── sidecar.py                # Sidecar builders
│   ├── writer.py                 # Atomic parquet + sidecar writes
│   ├── features/
│   │   ├── engineer.py           # Rolling features (leakage-safe)
│   │   ├── labels.py             # Next-game label generation
│   │   └── versioning.py        # SHA-256 feature schema versioning
│   └── exceptions.py
├── data/
│   ├── raw/
│   ├── clean/
│   └── schema_baselines/
├── logs/
└── tests/
    ├── unit/
    ├── integration/
    └── file_output/
```

## Commands

```bash
cd backend
pytest tests/unit/
pytest tests/integration/
pytest tests/file_output/
ruff check .
```

## Code Style

Python 3.11+: Follow standard conventions

## Recent Changes

- 001-nba-stats-pipeline (2026-04-14): Expanded pipeline architecture — added `config.py`, `fetcher.py`, `validator.py`, `cleaner.py`, `deduplicator.py`, `outlier.py`, `schema_drift.py`, `sidecar.py`, `writer.py`, and `features/` subpackage. New `run()` signature with separate `player`/`team` args, `columns`, `output_dir`, `features_config`, `labels_config`. Natural-grain deduplication, configurable thresholds, schema drift tracking, optional feature engineering and label generation with leakage-safe rolling operations and SHA-256 feature versioning.

<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->
