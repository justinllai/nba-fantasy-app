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

## Session 5 — 04/01/26 Justin
**What we built:** T007 ESPN scoring constants, 
T008 FPPG calculator function.

---

## Session 6 — 04/02/26 Justin
**What we built:** T009 normalize function, 
T010 replacement value signal, T011 minutes 
trend signal.

**Key decisions:**
- normalize() guards against division by zero
  with a guard clause
- RECENT_GAMES_WINDOW = 5 as a named constant
  so changing window size only requires one update
- Minutes trend splits list in half and compares
  averages — more robust than first vs last game
- Used len() over hardcoded numbers so function
  adapts if RECENT_GAMES_WINDOW changes



**Key decisions:**
- Pydantic BaseModel over plain dict or @dataclass — auto type validation, built for FastAPI
- One flat class over multiple classes — one object travels through whole app, simpler for V1
- Optional fields for recent_minutes and injured_starter_replacement — real world data is incomplete

## Session 7 — 04/06/26 (Justin)
**Starting at:** T012 — Sustainability signal
**What we built:** T012 — calculate_sustainability function with FG%/FT% 
averaging and zero free throw edge case handled
**Next:** T013 — Combine all signals into final pickup score