# Research: NBA Stats Data Pipeline

**Branch**: `001-nba-stats-pipeline` | **Updated**: 2026-04-14

---

## Session 2026-03-31 — Original Open Points

### Open Point 1: game_id vs. team code detection for box_scores

**Decision**: Attempt `int(player_or_team)`. If it succeeds without raising `ValueError`, treat as `game_id`. If it raises, treat as team abbreviation.

**Rationale**: BallDontLie game IDs are always integers. Team abbreviations are always alphabetic. These sets are disjoint — no false positives possible.

**Alternatives considered**:
- Regex for 2–3 uppercase letters: Brittle; requires maintaining an allowed-values list.
- Separate `game_id` parameter: Would break the shared public interface.
- Length heuristic: Unreliable; future IDs or codes could overlap.

---

### Open Point 2: Failed data types in the result dict

**Decision**: Include failed data types in the returned dict with `"status": "failed"` and an `"error"` key instead of omitting them.

**Rationale**: Silently omitting makes "not requested" indistinguishable from "failed". Aligns with FR-024 and SC-006 (no silent `None` returns).

---

### Open Point 3: Raw sidecar content

**Decision**: Raw sidecars contain schema and row count only. Clean sidecars are a superset with cleaning stats added.

**Rationale**: Raw sidecars are written before validation and cleaning — they cannot contain stats that don't exist yet.

---

### Open Point 4: dtype coercion during validation

**Decision**: Allow safe casting — if actual dtype is `float64` and expected is `int64`, check all non-null values are whole numbers before failing.

**Rationale**: BallDontLie API returns all numeric fields as JSON numbers which pandas parses as `float64`. Strict matching would produce false positives on every valid response.

---

## Session 2026-04-14 — New Open Points

### Open Point 5: `player_or_team` argument ambiguity

**Decision**: Split into two separate `player` and `team` arguments. If both are provided, raise `PipelineConfigError` immediately.

**Rationale**: A single string conflates two semantically distinct concepts. Natural-grain deduplication and identity column retention require knowing whether the subject is a player or a team. Separate args make this unambiguous at the call site.

**Alternatives considered**:
- Keep `player_or_team`, infer from data type: Fragile — `box_scores` can accept either depending on game_id vs. team lookup. Breaks composite key dedup.
- Type enum: Adds a new public type; overkill for two states.

---

### Open Point 6: Deduplication — composite keys vs. `game_id` alone

**Decision**: Use natural-grain composite keys per data type. When duplicate composite keys exist with differing values, keep the most recently ingested row and log the conflict.

**Rationale**: A single `game_id` is not sufficient for `game_logs` or `box_scores` — multiple valid player rows exist per game. `game_id` alone would incorrectly collapse different players' stats.

**Deduplication keys by data type**:
- `game_logs`: `player_id + game_id` → fallback `player_name + game_id`
- `box_scores`: `player_id + game_id`
- `game_scores`: `team_id + game_id` → fallback `team + game_id`
- `season_averages`: skip unless duplicate subject-season rows detected

---

### Open Point 7: Impossible-value threshold configurability

**Decision**: Thresholds live in `config.py` as a dict with `"upper"` and `"lower"` keys. The active threshold dict is passed into `cleaner.py` — never hardcoded in cleaning logic. The active thresholds are logged and recorded in the sidecar.

**Rationale**: FR-016 explicitly requires configurability. Recording the active thresholds in the sidecar allows downstream consumers to know exactly what rules produced the clean output.

**Default config**:
```python
IMPOSSIBLE_VALUE_THRESHOLDS = {
    "upper": {"pts": 100, "min": 60, "reb": 50, "ast": 30, "stl": 15, "blk": 15, "games_played": 82},
    "lower": {"pts": 0, "reb": 0, "ast": 0, "stl": 0, "blk": 0, "min": 0, "games_played": 0},
}
```

---

### Open Point 8: Anti-leakage enforcement for rolling features

**Decision**: All rolling operations MUST use `.shift(1)` before `.rolling()`. This is a code-level constraint, not just a convention — it MUST be enforced in `features/engineer.py` and verified in tests.

```python
# Correct — shift before rolling
df["rolling_pts_5"] = (
    df.groupby("player_id")["pts"]
    .shift(1)
    .rolling(window=5, min_periods=min_obs)
    .mean()
)
```

**Rationale**: FR-030 prohibits using future games in any derived feature or label. Without `.shift(1)`, the current row's value is included in the rolling window, contaminating every feature with the target it is trying to predict.

---

### Open Point 9: Feature versioning strategy

**Decision**: Deterministic SHA-256 hash of the serialized `features_config` dict (sorted keys). Any change to rolling windows, scoring config, or feature list produces a different version string automatically.

```python
def get_feature_version(features_config: dict) -> str:
    import hashlib, json
    return hashlib.sha256(json.dumps(features_config, sort_keys=True).encode()).hexdigest()[:12]
```

**Rationale**: A hand-maintained version number will drift. A deterministic hash guarantees that version changes are detected automatically without developer discipline.

**Alternatives considered**:
- Semantic version string: Requires manual increment; easy to forget.
- Timestamp: Non-deterministic; same config produces different versions on re-run.
- Full config hash (64 chars): Unnecessarily verbose for a sidecar field.

---

### Open Point 10: Schema drift baseline location

**Decision**: Baseline files written to `data/schema_baselines/{data_type}.json`. Created automatically on first run. Drift comparison begins on second run.

**Rationale**: Storing baselines in `data/` keeps them alongside the pipeline outputs they describe. They are not source artifacts (don't belong in `specs/`) and are not logs (don't belong in `logs/`).

---

### Open Point 11: Sidecar file naming

**Decision**: Three distinct file naming patterns — never merged into one file:

| File | Extension | Written |
|------|-----------|---------|
| Data parquet | `.parquet` | Every successful run |
| Base metadata | `.sidecar.json` | After parquet confirmed on disk |
| Feature metadata | `.features.sidecar.json` | Only when features generated |

**Rationale**: Merging base and feature sidecars into one file would require re-reading and re-writing it after feature generation, creating a race-condition window between the two writes. Separate files allow atomic independent writes.

---

## Dependency Confirmations

All libraries confirmed available in `backend/requirements.txt`:

| Library | Use |
|---------|-----|
| `pandas` | DataFrames, cleaning, downcasting, rolling operations |
| `pyarrow` | Parquet read/write with snappy compression |
| `requests` | HTTP chunked fetches (via `api.py`) |
| `python-dotenv` | Load `BALL_IS_LIFE` from `.env` |
| `pytest` | All tests (dev dependency) |

No new dependencies introduced. `hashlib` and `json` used for feature versioning are stdlib.

---

## BallDontLie API Notes

- Base URL: `https://api.balldontlie.io/v1`
- Auth: `Authorization: {BALL_IS_LIFE}` header
- Pagination: cursor-based via `meta.next_cursor`; 100 records per page
- Rate limits: free tier limits not published; fail-fast on 429 (FR-026)
- Relevant endpoints:
  - Game logs: `/stats` with `player_ids[]` and `seasons[]`
  - Season averages: `/season_averages` with `player_id` and `season`
  - Box scores (team+season): `/stats` with `team_ids[]` and `seasons[]`
  - Game scores: `/games` with `team_ids[]` and `seasons[]`
  - Player lookup: `/players` with `search` — returns `id` and `position`
  - Team lookup: `/teams` with `search` — returns `id` and `abbreviation`
