# NBA Fantasy App — Build Log

## Session 1 — 03/16/26 Justin
**What we built:** Ran speckit/spec & speckit/plan to generate full technical blueprint before writing any code.

**Why:** Never code without a plan. Speckit reads your feature description and generates a plan automatically.

**Key decisions:**
- In-memory cache over PostgreSQL — goal is to prove signal engine works, not manage a database yet
- nba_api (free, no API key) over paid APIs like SportRadar — same data source, no auth complexity
- 2 bulk API calls over 500 individual calls — rate limiting would block us instantly

---

## Session 2 — 03/18/26 Justin
**What we built:** Folder structure, requirements.txt, main.py, .gitignore, GitHub repo connected & pushed.

**Key decisions:**
- FastAPI over Django (too heavy) and Flask (too bare)
- Uvicorn as the server — FastAPI is the car, uvicorn is the engine
- Pinned versions in requirements.txt so everyone runs the exact same code

---

## Session 3 — 03/23/26 Justin
**What we built:** .env file, README.md, Phase 1 complete.

---

## Session 4 — 03/31/26 Justin
**What we built:** T006 — PlayerStats data model in scoring.py. Set up docs/ folder.

**Key decisions:**
- Pydantic BaseModel over plain dict or @dataclass — auto type validation, built for FastAPI
- One flat class over multiple classes — one object travels through whole app, simpler for V1
- Optional fields for recent_minutes and injured_starter_replacement — real world data is incomplete