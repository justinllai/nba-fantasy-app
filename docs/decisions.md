# NBA Fantasy App — Architecture Decisions

This document captures the **big** decisions and the reasoning behind them. Small implementation choices belong in the build log; this file is for choices that shape the whole project.

---

## Decision Format

Each entry follows this pattern:
- **Context** — What problem are we solving?
- **Options Considered** — What did we evaluate?
- **Decision** — What we chose
- **Rationale** — Why we chose it
- **Consequences** — What this commits us to

---

## ADR-001: In-Memory Cache for V1 (Not PostgreSQL)

**Context:** The app needs to store NBA player data so we don't hit the API on every request. Two main options: a real database or a simple in-memory dict.

**Options Considered:**
- PostgreSQL — full database, persists between server restarts, scales to many users
- Redis — dedicated in-memory store, fast, also persists with config
- Plain Python dict — simple, fast, lost on server restart

**Decision:** Plain Python dict with timestamp-based expiry (1 hour).

**Rationale:** V1's goal is to prove the scoring engine works, not to manage data infrastructure. Adding a database means installing it, learning SQL queries, writing migrations, and handling connection pooling — all before we even know if our signals produce useful results. A dict gets us to a working API the fastest.

**Consequences:**
- Data is lost when the server restarts (acceptable for V1; just refetch from the pipeline)
- Doesn't scale beyond a single server (V1 has one user — the developer)
- Will need to migrate to Redis or Postgres for V2/V3 when we have real users

---

## ADR-002: BallDontLie API (Not Paid Sports APIs)

**Context:** The app needs NBA player stats. Multiple data providers exist, ranging from free to expensive.

**Options Considered:**
- BallDontLie — free, basic auth, slower rate limits
- SportRadar — premium, real-time, expensive (~$500+/month)
- ESPN unofficial scraping — free but fragile and against TOS

**Decision:** BallDontLie API.

**Rationale:** Same data domain (NBA stats) without auth complexity or cost. V1 doesn't need real-time game data — historical season averages are enough to test the signal engine. Paying for SportRadar before we know the project works is premature optimization.

**Consequences:**
- Limited to season averages and game logs (no real-time/live data)
- Subject to BallDontLie's rate limits (handled by bulk fetching)
- Will reevaluate for V3 if real-time data becomes a requirement

---

## ADR-003: Cache-Aside Pattern for Cache Management

**Context:** When should the pipeline run to populate the cache?

**Options Considered:**
- **Option A — Startup only:** Pipeline runs once when server boots. Problem: cache expires after 1hr, no refresh mechanism.
- **Option B — Lazy only:** Fetch only when cache is empty. Problem: first user of the day waits 5–10 seconds.
- **Option C — Scheduled background job:** What ESPN/Yahoo do. Problem: overkill for V1 — adds APScheduler complexity.

**Decision:** Cache-aside pattern (A + B combined). Warm the cache on startup; refresh lazily when it expires.

**Rationale:** Covers both first-user UX (fast startup response) and stale data (refresh on demand). Industry-standard pattern, well-understood in interviews. Avoids the operational complexity of background schedulers until we actually need that scale.

**Consequences:**
- One user gets the slow refresh hit when the cache expires (acceptable for V1)
- Need to implement both startup warmup AND refresh-on-miss logic
- Migration path to Option C later is straightforward — same refresh function called by a scheduler instead of a request

---

## ADR-004: Equal Signal Weights (33/33/34) as V1 Baseline

**Context:** The pickup score combines three signals — replacement value, minutes trend, sustainability. They need to be weighted somehow.

**Options Considered:**
- Educated guess weights (e.g., 50/30/20 favoring replacement value)
- Equal weights (33/33/34)
- Tune empirically against historical data

**Decision:** Equal weights, 33/33/34, defined in `SIGNAL_WEIGHTS` constant.

**Rationale:** Without real performance data, any educated guess is just bias dressed up as analysis. Equal weights give every signal an equal chance to prove itself. The constant is a single source of truth — easy to tune later when we have data.

**Consequences:**
- V1 scores treat all signals as equally important (probably not optimal)
- V2 should include analysis of which signals actually predicted good pickups
- Easy to adjust — change one constant, rerun the engine

---

## ADR-005: Single Flat `PlayerStats` Class (Not Multiple Classes)

**Context:** Player data has multiple categories — identity (name, team, position), raw stats (pts, reb, ast), and computed signals. Should they be one class or several?

**Options Considered:**
- Multiple classes — `Identity`, `RawStats`, `SignalData` — each function takes 3 objects
- Single flat class — all fields in one `PlayerStats`

**Decision:** Single flat `PlayerStats(BaseModel)` class.

**Rationale:** Splitting would force every function to accept and pass three objects, with bookkeeping to keep them in sync. For V1's scope and a single developer, that's complexity without benefit. The class is around 20 fields — readable at a glance.

**Consequences:**
- One large class (acceptable while readable)
- Refactor to nested classes if it grows past ~50 fields
- All functions take a single `PlayerStats` parameter — uniform interface

---

## ADR-006: Adapter Pattern for Pipeline-to-Scoring Boundary

**Context:**  pipeline produces DataFrame rows with one set of column names. The scoring engine expects `PlayerStats` objects with potentially different field names. How do we bridge them?

**Options Considered:**
- rename pipeline columns to match `PlayerStats`
- Make `PlayerStats` field names match pipeline columns
- Adapter function — `adapt_pipeline_row()` — translates between the two

**Decision:** Adapter function in `scoring.py`.

**Rationale:** Each side keeps its natural naming. The adapter is one well-named function — clear, testable, easy to update if either side's naming changes. This is the standard solution to data contract mismatches between two independently developed systems.

**Consequences:**
- One extra function to maintain
- If pipeline columns change, only the adapter needs updating (not all of scoring.py)
- Discovered later that pipeline drops some required columns — adapter pattern made the issue easier to localize and discuss (see Session 9 notes)

---

## ADR-007: Field Naming Convention — API-style (`pts`, `reb`) Not English (`points`, `rebounds`)

**Context:** Should `PlayerStats` use NBA API field names (`pts`, `fgm`) or full English (`points`, `field_goals_made`)?

**Options Considered:**
- Full English — readable to non-basketball people, but requires translation in adapter
- API-style — matches pipeline output directly, less translation work

**Decision:** API-style (`pts`, `reb`, `ast`, `fgm`, `fga`, etc.).

**Rationale:** Reduces translation work in the adapter. The audience for this code is fantasy basketball people who already know the abbreviations. Trading a small readability cost for a meaningful reduction in cross-system mapping complexity.

**Consequences:**
- Code less readable to non-NBA audience (acceptable — the project is NBA-specific)
- Adapter function is mostly a 1-to-1 copy now, with only a few real renames
- Future contributors need basic NBA stat literacy

---

## How to Use This File

When you make a new big decision (one that affects multiple files or the project's direction), add an ADR. Number them sequentially. Don't edit old ADRs — if a decision changes, write a new ADR that "supersedes" the old one and reference it.

This file is your project's memory. Six months from now you (or anyone reading) can answer "why did we do X?" without rederiving everything.