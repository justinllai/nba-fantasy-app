# Research: NBA Stats Data Pipeline

**Branch**: `001-nba-stats-pipeline` | **Date**: 2026-03-31

All four open points from the technical plan are resolved below. No external research agents required — decisions are deterministic from context, API behavior, and Python best practices.

---

## Open Point 1: game_id vs. team code detection for box_scores

**Decision**: Attempt `int(player_or_team)`. If it succeeds without raising `ValueError`, treat the input as a `game_id` (integer lookup). If it raises, treat it as a team abbreviation string.

**Rationale**: BallDontLie API game IDs are always integers (e.g., `1234567`). Team abbreviations are always alphabetic strings (e.g., `"LAL"`, `"BOS"`). These two sets are disjoint — an integer string is never a valid team code, and a team code is never parseable as an integer. The `int()` cast is zero-dependency, instant, and produces no false positives.

**Alternatives considered**:
- Regex for 2-3 uppercase letters: Brittle for edge cases like `"OKC"` vs `"GSW"`. Requires maintaining an allowed-values list.
- Separate `game_id` parameter: Would break the shared `player_or_team` public interface already locked in the spec.
- Length heuristic (>3 chars = game_id): Unreliable; some future team codes or IDs could overlap.

---

## Open Point 2: Failed data types in the result dict

**Decision**: Include failed data types in the returned dict with an `"error"` key instead of omitting them.

Example:
```python
{
    "game_logs": {"rows_before": 82, "rows_after": 78, ...},
    "box_scores": {"error": "ValidationError: missing columns: ['min', 'pts']"}
}
```

**Rationale**: Silently omitting a data type makes it impossible for the caller to distinguish "type not requested" from "type failed". Including an `"error"` entry makes the failure visible, log-searchable, and testable without requiring exception handling at the call site for partial failures. Aligns with FR-024 (fail one, continue others) and SC-006 (no silent `None` returns).

**Alternatives considered**:
- Raise exception for any failure: Breaks FR-024's independence guarantee.
- Omit failed types: Caller can't distinguish absent from failed. Debugging requires reading log files.
- Separate `errors` dict alongside main result: More complex interface with no additional information value.

---

## Open Point 3: Raw sidecar content

**Decision**: Raw sidecars contain **schema and row count only** — no cleaning stats, no outlier counts, no corrupted counts (those haven't happened yet).

Raw sidecar fields:
```json
{
  "data_type": "game_logs",
  "player_or_team": "LeBron James",
  "season": "2023",
  "rows": 82,
  "columns": ["game_id", "player_name", "date", "min", "pts", "reb", "ast", ...],
  "written_at": "2026-03-31T14:22:05"
}
```

Clean sidecar fields (superset):
```json
{
  "data_type": "game_logs",
  "player_or_team": "LeBron James",
  "season": "2023",
  "rows_before": 82,
  "rows_after": 78,
  "columns": [...],
  "nulls_found": 5,
  "outliers_flagged": 3,
  "corrupted_removed": 1,
  "dedup_skipped": false,
  "dedup_reason": null,
  "written_at": "2026-03-31T14:22:07"
}
```

**Rationale**: Raw sidecars are written before validation and cleaning — they cannot contain cleaning stats that don't exist yet. Including only schema/row count is accurate and still useful for debugging and lineage tracing.

**Alternatives considered**:
- Same schema for both: Impossible without fabricating cleaning stats for the raw file.
- No raw sidecar at all: Loses lineage — can't verify raw row count after a clean run.

---

## Open Point 4: dtype coercion strategy during validation

**Decision**: Allow **safe casting** before declaring a validation failure. Specifically: if a column's actual dtype is `float64` and the expected dtype is `int64`, check whether all non-null values are whole numbers before failing. If so, accept the column and note the coercion in the log at INFO level.

**Rationale**: The BallDontLie API returns all numeric fields as JSON numbers which pandas parses as `float64` by default (e.g., `"pts": 28.0` instead of `28`). Strict dtype checking would produce false positives on every valid API response. Safe casting (checking `series.dropna().mod(1).eq(0).all()`) detects actual type errors (e.g., `"pts": "twenty-eight"`) while accepting the normal float64→int64 case.

**Alternatives considered**:
- Strict dtype matching: Fails on every valid API response. Unusable in practice.
- No dtype validation at all: Removes a useful correctness check.
- Auto-coerce without checking: Could silently corrupt fractional values (e.g., `min` as `32.75` should stay float).

---

## Dependency Confirmations

All libraries confirmed available in `backend/requirements.txt`:

| Library | Version | Use |
|---------|---------|-----|
| `pandas` | (existing) | DataFrames, cleaning, downcasting |
| `pyarrow` | (existing) | Parquet read/write with snappy compression |
| `requests` | (existing, via `api.py`) | HTTP chunked fetches |
| `python-dotenv` | (existing) | Load `BALL_IS_LIFE` from `.env` |
| `pytest` | (to be added to dev deps) | All tests |

`pyarrow` must be added to `requirements.txt` if not already present. `pytest` should be added as a dev dependency (e.g., `requirements-dev.txt`).

---

## BallDontLie API Notes

- Base URL: `https://api.balldontlie.io/v1`
- Auth: `Authorization: {BALL_IS_LIFE}` header
- Pagination: `page` and `per_page` query params; response includes `meta.next_cursor` or `meta.total_pages`
- Chunked ingestion: iterate pages until `meta.next_cursor` is null or page count exhausted
- Rate limits: free tier has limits (exact count not published); fail-fast on 429 responses per FR-026
- Relevant endpoints:
  - Game logs: `/stats` with `player_ids[]` and `seasons[]`
  - Season averages: `/season_averages` with `player_id` and `season`
  - Box scores (team+season): `/stats` with `team_ids[]` and `seasons[]`
  - Box scores (game_id): `/stats` with `game_ids[]`
