<!--
SYNC IMPACT REPORT
==================
Version change: (unversioned template) → 1.0.0
Modified principles: N/A (initial population from template)
Added sections:
  - Core Principles (I–V)
  - Technology Stack & Architecture
  - Development Workflow
  - Governance
Templates requiring updates:
  - .specify/templates/plan-template.md ✅ no changes required; "Constitution Check"
    gate is generic and aligns with these principles
  - .specify/templates/spec-template.md ✅ no changes required; mandatory sections
    (User Scenarios, Requirements, Success Criteria) align with Principle III
  - .specify/templates/tasks-template.md ✅ no changes required; phase structure and
    test-first ordering align with Principles III and IV
  - .specify/templates/constitution-template.md ✅ read-only reference; no update needed
  - .claude/commands/*.md ✅ all commands are generic; no agent-specific conflicts found
Deferred TODOs:
  - TODO(RATIFICATION_DATE): Set to 2026-03-14 (project creation date, confirmed
    from filesystem timestamps). Update if an earlier decision record exists.
-->

# NBA Fantasy App Constitution

## Core Principles

### I. User-First Design

Every feature MUST be evaluated against user value before implementation begins.
The app serves fantasy basketball players — decisions about UI, data presentation,
and workflows MUST prioritize clarity, speed of interaction, and meaningful insight
over technical elegance.

- Screens MUST be navigable without documentation.
- Core fantasy actions (draft, set lineup, view scores) MUST complete in ≤3 taps/clicks.
- Error messages MUST explain what went wrong AND what the user can do next.

**Rationale**: Fantasy sports apps live or die by daily engagement. Friction in lineup
management or score-checking directly causes user churn.

### II. Data Integrity

All NBA player stats, game results, and fantasy scores displayed to users MUST
originate from a single authoritative data source (official NBA API or a licensed
stats provider). Manual overrides are prohibited without an explicit audit trail.

- Fantasy scoring calculations MUST be deterministic and reproducible from raw
  game data alone.
- Data pipelines MUST validate schema on ingestion and reject malformed records.
- Stale data (>15 minutes old during live games) MUST be surfaced visibly to the
  user — the UI MUST NOT silently display outdated scores.

**Rationale**: Incorrect scoring destroys trust irreparably. Users make lineup decisions
based on stats; bad data = bad decisions = lost users.

### III. Test-First Development (NON-NEGOTIABLE)

TDD is mandatory for all business logic, scoring calculations, and API contracts:

1. Write the test.
2. Confirm the test fails (red).
3. Implement the minimum code to pass (green).
4. Refactor while tests remain green.

- Scoring engine logic MUST have unit test coverage before any UI work begins.
- API contracts MUST have contract tests before client-side consumption.
- The Red-Green-Refactor cycle MUST NOT be skipped under deadline pressure.

**Rationale**: Fantasy scoring bugs discovered in production are high-visibility failures.
The cost of a test-first discipline is far lower than the cost of a disputed score.

### IV. Performance & Responsiveness

The app MUST meet these baseline performance targets:

- Lineup changes MUST be persisted and confirmed to the user within 2 seconds.
- Score refresh during live games MUST complete within 5 seconds of trigger.
- Initial app load (cold start) MUST render usable UI within 3 seconds on a
  mid-range device / standard broadband connection.
- No blocking network call MUST execute on the UI thread.

**Rationale**: Fantasy sports are time-sensitive. A slow lineup submission before tip-off
or a delayed score update during a close game is a product failure, not just a
performance metric.

### V. Simplicity & Incremental Delivery

Start with the simplest implementation that satisfies the user story. YAGNI
(You Aren't Gonna Need It) applies strictly.

- Features MUST ship as independently testable increments — each increment MUST
  deliver standalone value.
- Abstractions, patterns, and frameworks MUST be introduced only when a concrete,
  present need is demonstrated — not in anticipation of future requirements.
- Complexity added beyond the minimum MUST be documented in the plan's Complexity
  Tracking table with explicit justification.

**Rationale**: An NBA fantasy app has a well-understood domain. Over-engineering early
wastes time that should go toward core game mechanics and live-score reliability.

## Technology Stack & Architecture

Technology choices MUST be made per-feature during `/speckit.plan` and documented
in the plan's Technical Context section. Until a choice is locked in a plan, the
following defaults apply:

- **Backend**: Python 3.11+ with FastAPI (REST); PostgreSQL for persistent storage.
- **Frontend/Mobile**: React (web) or Swift/SwiftUI (iOS-first if mobile), decided
  at project kickoff.
- **Testing**: pytest (backend), Jest/React Testing Library (frontend), XCTest (iOS).
- **Data Ingestion**: NBA Stats API or a licensed provider (TBD at kickoff). All
  ingestion workers MUST be idempotent.
- **Deployment**: Containerized (Docker); CI via GitHub Actions.

Any deviation from these defaults MUST be recorded in the relevant plan's Complexity
Tracking table.

## Development Workflow

- All work begins with a feature spec (`/speckit.specify`) and an implementation
  plan (`/speckit.plan`) before any code is written.
- The Constitution Check gate in `plan-template.md` MUST be completed — all five
  principles MUST be explicitly addressed before Phase 0 research proceeds.
- Feature branches MUST follow the naming convention `###-short-description`
  (e.g., `001-draft-board`, `002-live-scoring`).
- Every PR MUST include passing tests and a green CI build before merge.
- Commits MUST be atomic: one logical change per commit. Commit messages MUST use
  the imperative mood and reference the task ID (e.g., `T014: implement scoring engine`).
- Spec, plan, and task artifacts in `specs/` MUST be updated to reflect any scope
  changes discovered during implementation — artifacts MUST NOT become stale.

## Governance

This constitution supersedes all other development practices for the NBA Fantasy App
project. Amendments require:

1. A written proposal describing the change and its rationale.
2. Review and approval before the change takes effect.
3. Version increment following the semantic versioning policy below.
4. Propagation to all dependent templates (see Sync Impact Report header).

**Versioning policy**:
- **MAJOR**: Backward-incompatible governance change; principle removed or redefined
  in a way that invalidates prior decisions.
- **MINOR**: New principle or section added; materially expanded guidance.
- **PATCH**: Clarifications, wording fixes, non-semantic refinements.

All PRs and design reviews MUST verify compliance with the five Core Principles.
Complexity MUST be justified in writing. Runtime development guidance lives in
this constitution and the per-feature plan artifacts under `specs/`.

**Version**: 1.0.0 | **Ratified**: 2026-03-14 | **Last Amended**: 2026-03-14
