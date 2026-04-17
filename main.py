from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from koleo.api.client import KoleoAPI
from fastapi import Query
from pydantic import BaseModel, Field

from seat_finder import (
    SeatFinderError,
    find_seats,
    find_seats_by_train,
    get_train_stops,
    search_journeys,
)


STATIC_DIR = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.koleo = KoleoAPI()
    app.state.station_cache = {}
    try:
        yield
    finally:
        await app.state.koleo.close()


app = FastAPI(title="Seat Scout", lifespan=lifespan)


class SearchRequest(BaseModel):
    from_: str = Field(alias="from", min_length=1)
    to: str = Field(min_length=1)
    date: str  # YYYY-MM-DD
    time: str  # HH:MM

    class Config:
        populate_by_name = True


class SeatsRequest(BaseModel):
    train_id: int
    from_station_id: int
    to_station_id: int
    date: str  # YYYY-MM-DD


def _parse_date(date: str) -> datetime:
    try:
        return datetime.strptime(date, "%Y-%m-%d")
    except ValueError as e:
        raise SeatFinderError("Invalid date format.") from e


def _parse_dt(date: str, time: str) -> datetime:
    try:
        return datetime.strptime(f"{date} {time}", "%Y-%m-%d %H:%M")
    except ValueError as e:
        raise SeatFinderError("Invalid date or time format.") from e


def _error(e: SeatFinderError) -> JSONResponse:
    return JSONResponse({"error": str(e)}, status_code=400)


@app.post("/api/search")
async def api_search(body: SearchRequest):
    """Legacy one-shot endpoint (still used for tests)."""
    try:
        departure = _parse_dt(body.date, body.time)
        return await find_seats(
            app.state.koleo, body.from_, body.to, departure, app.state.station_cache,
        )
    except SeatFinderError as e:
        return _error(e)


@app.get("/api/stations")
async def api_stations(q: str = ""):
    q = q.strip()
    if len(q) < 2:
        return {"stations": []}
    try:
        r = await app.state.koleo.get(
            "https://koleo.pl/ls",
            params={"q": q, "language": "en"},
        )
        raw = r.json().get("stations") or []
    except Exception:
        return {"stations": []}
    out = [
        {
            "id": s["id"],
            "name": s.get("localised_name") or s["name"],
            "slug": s["name_slug"],
        }
        for s in raw[:8]
    ]
    return {"stations": out}


# Real journeys endpoint using explicit params
@app.get("/api/journeys")
async def api_journeys(
    date: str,
    from_q: str | None = Query(default=None, alias="from"),
    to: str | None = None,
    time: str | None = None,
    number: str | None = None,
):
    try:
        base_date = _parse_date(date)
        journeys = await search_journeys(
            app.state.koleo,
            from_q=from_q,
            to_q=to,
            date=base_date,
            time_str=time,
            number=number,
            cache=app.state.station_cache,
        )
        return {"journeys": journeys}
    except SeatFinderError as e:
        return _error(e)


@app.get("/api/trains/{train_id}/stops")
async def api_train_stops(train_id: int, date: str):
    try:
        base_date = _parse_date(date)
        return await get_train_stops(app.state.koleo, train_id, base_date)
    except SeatFinderError as e:
        return _error(e)


@app.post("/api/seats")
async def api_seats(body: SeatsRequest):
    try:
        base_date = _parse_date(body.date)
        return await find_seats_by_train(
            app.state.koleo,
            body.train_id,
            body.from_station_id,
            body.to_station_id,
            base_date,
        )
    except SeatFinderError as e:
        return _error(e)


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
