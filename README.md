# NBA Fantasy Waiver Wire App

A waiver wire recommendation tool for NBA fantasy points leagues.
Ranks available players using three signals: Replacement Value,
Minutes Trend, and Sustainability Score.

## Tech Stack
- Backend: Python / FastAPI
- Frontend: React
- Data: nba_api (free, no key required)

## Running locally

### Backend
cd backend
pip install -r requirements.txt
uvicorn main:app --reload

### Frontend
cd frontend
npm install
npm run dev

## Architecture
The backend is split into focused modules:
- main.py: starts the server
- scoring.py: calculates the three signals
- cache.py: stores results in memory
- routers/: handles API endpoints