"""Seat finder algorithm and supporting journey-lookup helpers.

The public surface is:

- `search_journeys(client, *, from_q, to_q, date, time_str, number, cache)`
    Flexible candidate-train search.  Requires at least one of from_q / to_q.

- `get_train_stops(client, train_id, date)`
    Full stop list for a given train on a given operating day.

- `find_seats_by_train(client, train_id, from_station_id, to_station_id, date)`
    Seat recommendations for an explicit leg of a train.

- `find_seats(client, from_q, to_q, departure_dt, station_cache)`
    Convenience one-shot: resolve stations → find matching train → recommend.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from koleo.api.client import KoleoAPI
from koleo.api.errors import errors as koleo_errors
from koleo.utils import koleo_time_to_dt


SUPPORTED_BRAND_IDS = [1, 2, 28, 29]
BRAND_NAMES = {1: "TLK", 2: "EIC", 28: "IC", 29: "EIP"}
PLACEMENT_NAMES = {1: "window", 2: "aisle", 7: "middle"}
PLACEMENT_RANK = {1: 0, 2: 1, 7: 2}  # lower = better
PLACE_TYPE_CLASS_2 = 5

COMPARTMENT_LABELS = {
    "quiet": "quiet zone",
    "women": "women-only compartment",
    "kids": "family with kids",
    "family": "family compartment",
    "bike": "bike space",
    "wheelchair": "wheelchair space",
    "guardian": "disability companion",
}

PER_SEGMENT_CONCURRENCY = 5
POLITE_DELAY_MS = 50


class SeatFinderError(Exception):
    """User-facing, human-readable error."""


@dataclass
class Stop:
    station_id: int
    station_name: str
    station_slug: str
    position: int
    arrival_dt: datetime
    departure_dt: datetime


# ---------- low-level helpers -----------------------------------------------

async def _find_station(client: KoleoAPI, query: str) -> dict[str, Any]:
    # koleo-cli 0.2.137 points find_station at api.koleo.pl/ls which 404s;
    # the working endpoint is koleo.pl/ls. Same payload shape.
    r = await client.get(
        "https://koleo.pl/ls",
        params={"q": query, "language": "en"},
    )
    stations = r.json().get("stations") or []
    if not stations:
        raise SeatFinderError(f'No station matching "{query}".')
    return stations[0]


async def _resolve_stations(
    client: KoleoAPI,
    from_query: str,
    to_query: str,
    cache: dict[str, dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    async def one(q: str) -> dict[str, Any]:
        key = q.strip().lower()
        if key in cache:
            return cache[key]
        s = await _find_station(client, q)
        cache[key] = s
        return s

    return await asyncio.gather(one(from_query), one(to_query))


async def _resolve_one(
    client: KoleoAPI,
    query: str,
    cache: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    key = query.strip().lower()
    if key in cache:
        return cache[key]
    s = await _find_station(client, query)
    cache[key] = s
    return s


def _to_dt(raw: Any, base: datetime) -> datetime:
    """koleo_time_to_dt returns datetime; strip tz for consistent arithmetic."""
    dt = koleo_time_to_dt(raw, base_date=base)
    if dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    return dt


def _display_name(train: dict[str, Any]) -> str | None:
    """Koleo's `name` is usually None; `train_full_name` is "637 ZEFIR"."""
    n = train.get("name")
    if n:
        return n
    full = (train.get("train_full_name") or "").strip()
    parts = full.split(" ", 1)
    if len(parts) == 2 and parts[1]:
        return parts[1].title()
    return None


# ---------- stop timeline construction --------------------------------------

async def _route_stops(
    client: KoleoAPI,
    train_id: int,
    from_station_id: int | None,
    to_station_id: int | None,
    base_date: datetime,
) -> list[Stop]:
    """Reconstruct a consistent, monotonically-increasing stop timeline for a
    train, then optionally slice to the requested from/to leg.
    """
    detail = await client.get_train(train_id)
    raw_stops = detail["stops"]

    def dep_or_arr(s: dict[str, Any], which: str) -> datetime | None:
        val = s.get(which)
        if val is None:
            return None
        try:
            return _to_dt(val, base_date)
        except Exception:
            return None

    last_seen: datetime | None = None
    normalized: list[Stop] = []
    for s in raw_stops:
        arr = dep_or_arr(s, "arrival")
        dep = dep_or_arr(s, "departure")
        for cand in (arr, dep):
            if cand is None:
                continue
            while last_seen and cand < last_seen - timedelta(hours=12):
                cand += timedelta(days=1)
            last_seen = max(last_seen, cand) if last_seen else cand
        if arr is None and dep is not None:
            arr = dep
        if dep is None and arr is not None:
            dep = arr
        if arr is None or dep is None:
            continue
        normalized.append(
            Stop(
                station_id=s["station_id"],
                station_name=s.get("station_display_name") or s["station_name"],
                station_slug=s["station_slug"],
                position=s["position"],
                arrival_dt=arr,
                departure_dt=dep,
            )
        )

    fixed: list[Stop] = []
    prev: datetime | None = None
    for stop in normalized:
        a, d = stop.arrival_dt, stop.departure_dt
        while prev and a < prev - timedelta(hours=12):
            a += timedelta(days=1)
        while d < a:
            d += timedelta(days=1)
        stop.arrival_dt = a
        stop.departure_dt = d
        prev = d
        fixed.append(stop)

    if from_station_id is None and to_station_id is None:
        return fixed

    try:
        start_idx = 0
        if from_station_id is not None:
            start_idx = next(i for i, s in enumerate(fixed) if s.station_id == from_station_id)
        end_idx = len(fixed) - 1
        if to_station_id is not None:
            end_idx = next(i for i, s in enumerate(fixed) if s.station_id == to_station_id)
    except StopIteration:
        raise SeatFinderError("Train does not stop at both origin and destination.")

    if end_idx <= start_idx:
        raise SeatFinderError("Destination comes before origin on this train.")

    return fixed[start_idx : end_idx + 1]


# ---------- segment resolution ----------------------------------------------

async def _segment_connection_id(
    client: KoleoAPI,
    sem: asyncio.Semaphore,
    from_slug: str,
    to_slug: str,
    depart_dt: datetime,
    train_id: int,
) -> tuple[int, int]:
    """Returns (connection_id, train_nr_for_this_segment).

    `train_nr` differs between the connection-level view (leg-specific number,
    e.g. 637 for a sub-leg) and `get_train(train_id).train_nr` (the end-to-end
    public number, e.g. 3806). `get_seats_availability` wants the leg number.
    """
    async with sem:
        await asyncio.sleep(POLITE_DELAY_MS / 1000)
        connections = await client.get_connections(
            start=from_slug,
            end=to_slug,
            brand_ids=SUPPORTED_BRAND_IDS,
            date=depart_dt - timedelta(minutes=5),
            direct=True,
        )
    for c in connections:
        for t in c["trains"]:
            if t.get("train_id") == train_id:
                return c["id"], t["train_nr"]
    raise SeatFinderError(
        f"No bookable segment {from_slug} → {to_slug} around {depart_dt:%H:%M}."
    )


async def _resolve_segments(
    client: KoleoAPI,
    sem: asyncio.Semaphore,
    stops: list[Stop],
    train_id: int,
) -> tuple[list[Stop], list[int], int]:
    """Returns (effective_stops, segment_connection_ids, leg_train_nr)."""
    tasks = [
        _segment_connection_id(
            client, sem,
            stops[i].station_slug, stops[i + 1].station_slug,
            stops[i].departure_dt, train_id,
        )
        for i in range(len(stops) - 1)
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    effective: list[Stop] = [stops[0]]
    conn_ids: list[int] = []
    leg_train_nr: int | None = None
    i = 0
    while i < len(stops) - 1:
        first_result = results[i]
        if not isinstance(first_result, Exception):
            cid, tnr = first_result
            conn_ids.append(cid)
            leg_train_nr = leg_train_nr or tnr
            effective.append(stops[i + 1])
            i += 1
            continue
        resolved = False
        for j in range(i + 2, len(stops)):
            try:
                cid, tnr = await _segment_connection_id(
                    client, sem,
                    stops[i].station_slug, stops[j].station_slug,
                    stops[i].departure_dt, train_id,
                )
            except SeatFinderError:
                continue
            conn_ids.append(cid)
            leg_train_nr = leg_train_nr or tnr
            effective.append(stops[j])
            i = j
            resolved = True
            break
        if not resolved:
            break

    if len(effective) < 2 or leg_train_nr is None:
        raise SeatFinderError(
            "Couldn't load seat availability for this train."
        )
    return effective, conn_ids, leg_train_nr


async def _fetch_segment_seats(
    client: KoleoAPI,
    sem: asyncio.Semaphore,
    connection_id: int,
    train_nr: int,
    segment_label: str,
) -> dict[str, Any]:
    async with sem:
        await asyncio.sleep(POLITE_DELAY_MS / 1000)
        try:
            return await client.get_seats_availability(
                connection_id, train_nr, PLACE_TYPE_CLASS_2
            )
        except koleo_errors.KoleoAPIException as e:
            body = ""
            try:
                body = (await e.response.text())[:200]
            except Exception:
                pass
            raise SeatFinderError(
                f"Koleo rejected the seats query for segment {segment_label} "
                f"(HTTP {e.status}). {body}".strip()
            ) from e


# ---------- ranking ----------------------------------------------------------

def _sort_key(s: str) -> tuple[int, str]:
    try:
        return (0, f"{int(s):08d}")
    except ValueError:
        return (1, s)


SOON_MAX_SEGMENTS = 2  # how many stops counts as "very soon"
SOON_MIN_FREE_MINUTES = 30  # must stay free at least this long after unlock


def _build_candidate(
    carriage: str,
    seat_nr: str,
    info: dict[str, Any],
    compartment_lookup: dict[int, dict[str, Any]],
    status: str,
    free_minutes: float,
    free_until_station: str | None,
    free_until_label: str,
    available_from_station: str | None = None,
    available_from_label: str | None = None,
    available_from_minutes: int | None = None,
) -> dict[str, Any]:
    placement_id = info.get("placement_id")
    special_id = info.get("special_compartment_type_id")
    special_icon = None
    special_label = None
    if special_id:
        comp = compartment_lookup.get(special_id)
        if comp:
            special_icon = comp.get("icon")
            special_label = (
                COMPARTMENT_LABELS.get(special_icon) or comp.get("name")
            )
    return {
        "carriage": carriage,
        "seat": seat_nr,
        "placement_id": placement_id,
        "placement": PLACEMENT_NAMES.get(placement_id, "other"),
        "status": status,
        "free_minutes": round(free_minutes),
        "free_until_station": free_until_station,
        "free_until_label": free_until_label,
        "available_from_station": available_from_station,
        "available_from_label": available_from_label,
        "available_from_minutes": available_from_minutes,
        "special_icon": special_icon,
        "special_label": special_label,
        "_rank_placement": PLACEMENT_RANK.get(placement_id, 9),
        "_carriage_sort": _sort_key(carriage),
        "_seat_sort": _sort_key(seat_nr),
    }


def _rank_and_select(
    seat_map: dict[tuple[str, str], dict[str, Any]],
    stops: list[Stop],
    compartment_lookup: dict[int, dict[str, Any]],
) -> list[dict[str, Any]]:
    total_minutes = (stops[-1].arrival_dt - stops[0].departure_dt).total_seconds() / 60
    free_now: list[dict[str, Any]] = []
    free_soon: list[dict[str, Any]] = []

    for (carriage, seat_nr), info in seat_map.items():
        states: list[str] = info["states"]
        if not states:
            continue

        first_free = next((i for i, s in enumerate(states) if s == "FREE"), None)
        if first_free is None:
            continue  # reserved or blocked on every segment

        # Find when it becomes re-reserved after it first goes FREE.
        re_reserved = next(
            (i for i, s in enumerate(states[first_free:], start=first_free) if s != "FREE"),
            len(states),
        )

        if first_free == 0:
            # FREE at boarding
            if re_reserved == len(states):
                free_minutes = total_minutes
                until_station = None
                until_label = "whole journey"
            else:
                boarding_stop = stops[re_reserved]
                free_minutes = (
                    boarding_stop.departure_dt - stops[0].departure_dt
                ).total_seconds() / 60
                until_station = boarding_stop.station_name
                until_label = (
                    f"until {boarding_stop.station_name} "
                    f"({boarding_stop.departure_dt:%H:%M})"
                )
            free_now.append(_build_candidate(
                carriage, seat_nr, info, compartment_lookup,
                status="free_now",
                free_minutes=free_minutes,
                free_until_station=until_station,
                free_until_label=until_label,
            ))
            continue

        # Not free at boarding — consider only if it opens up within 1–2 stops.
        if first_free > SOON_MAX_SEGMENTS:
            continue
        unlock_stop = stops[first_free]
        if re_reserved == len(states):
            end_dt = stops[-1].arrival_dt
        else:
            end_dt = stops[re_reserved].departure_dt
        free_minutes_after = (end_dt - unlock_stop.arrival_dt).total_seconds() / 60
        if free_minutes_after < SOON_MIN_FREE_MINUTES:
            continue  # barely opens before someone else boards

        wait_minutes = (unlock_stop.arrival_dt - stops[0].departure_dt).total_seconds() / 60
        if re_reserved == len(states):
            until_station = None
            until_label = "rest of the trip"
        else:
            boarding_stop = stops[re_reserved]
            until_station = boarding_stop.station_name
            until_label = (
                f"until {boarding_stop.station_name} "
                f"({boarding_stop.departure_dt:%H:%M})"
            )
        free_soon.append(_build_candidate(
            carriage, seat_nr, info, compartment_lookup,
            status="free_soon",
            free_minutes=free_minutes_after,
            free_until_station=until_station,
            free_until_label=until_label,
            available_from_station=unlock_stop.station_name,
            available_from_label=(
                f"free from {unlock_stop.station_name} "
                f"({unlock_stop.arrival_dt:%H:%M})"
            ),
            available_from_minutes=round(wait_minutes),
        ))

    # --- select now seats ---
    free_now.sort(
        key=lambda s: (-s["free_minutes"], s["_rank_placement"], s["_carriage_sort"], s["_seat_sort"])
    )
    now_selected = free_now[: min(10, len(free_now))]
    seen_carriages = {c["carriage"] for c in now_selected}
    if len(seen_carriages) == 1 and len(now_selected) >= 5:
        only = next(iter(seen_carriages))
        alt = next((c for c in free_now if c["carriage"] != only), None)
        if alt is not None:
            now_selected[-1] = alt

    # --- select soon seats ---
    # Prefer seats that unlock soonest; break ties by longer stay + window/aisle.
    free_soon.sort(
        key=lambda s: (
            s["available_from_minutes"],
            -s["free_minutes"],
            s["_rank_placement"],
            s["_carriage_sort"],
            s["_seat_sort"],
        )
    )
    # Dedup across carriages for variety, cap at 3 total.
    soon_selected: list[dict[str, Any]] = []
    seen_seats: set[tuple[str, str]] = set()
    carriage_counts: dict[str, int] = {}
    for c in free_soon:
        if len(soon_selected) >= 3:
            break
        key = (c["carriage"], c["seat"])
        if key in seen_seats:
            continue
        # allow at most 2 per carriage to keep some spread
        if carriage_counts.get(c["carriage"], 0) >= 2:
            continue
        soon_selected.append(c)
        seen_seats.add(key)
        carriage_counts[c["carriage"]] = carriage_counts.get(c["carriage"], 0) + 1

    # Merge and put into walking order for the UI
    combined = now_selected + soon_selected
    combined.sort(key=lambda s: (s["_carriage_sort"], s["_seat_sort"]))
    for s in combined:
        for k in ("_rank_placement", "_carriage_sort", "_seat_sort"):
            s.pop(k, None)
    return combined


# ---------- top-level operations --------------------------------------------

def _brand_ok(brand_id: int | None) -> bool:
    return brand_id in BRAND_NAMES


def _journey_from_connection(c: dict[str, Any], base_date: datetime, from_station: dict[str, Any], to_station: dict[str, Any]) -> dict[str, Any]:
    t = c["trains"][0]
    dep = _to_dt(t["departure"], base_date)
    arr = _to_dt(c["arrival"], base_date)
    if arr < dep:
        arr += timedelta(days=1)
    return {
        "train_id": t["train_id"],
        "train_nr": t["train_nr"],
        "train_full_name": t.get("train_full_name"),
        "name": _display_name(t),
        "brand": BRAND_NAMES.get(t["brand_id"], str(t["brand_id"])),
        "brand_id": t["brand_id"],
        "from": {
            "id": from_station["id"],
            "name": from_station.get("localised_name") or from_station["name"],
            "slug": from_station["name_slug"],
            "time": dep.strftime("%H:%M"),
        },
        "to": {
            "id": to_station["id"],
            "name": to_station.get("localised_name") or to_station["name"],
            "slug": to_station["name_slug"],
            "time": arr.strftime("%H:%M"),
        },
        "duration_min": round((arr - dep).total_seconds() / 60),
    }


async def _journeys_from_single_endpoint(
    client: KoleoAPI,
    station: dict[str, Any],
    date: datetime,
    mode: str,  # "departures" or "arrivals"
) -> list[dict[str, Any]]:
    """Single-endpoint search.  PKP's departures/arrivals endpoint returns one
    row per train at that station; its `stations` array holds a single entry
    pointing at the OTHER endpoint (origin or destination) with a `train_id`.
    The opposite-endpoint time is not included, so we leave it unknown — the
    user can see the full timeline once they pick the journey.
    """
    try:
        rows = await (
            client.get_departures(station["id"], date)
            if mode == "departures"
            else client.get_arrivals(station["id"], date)
        )
    except koleo_errors.KoleoAPIException:
        rows = []

    station_info = {
        "id": station["id"],
        "name": station.get("localised_name") or station.get("name"),
        "slug": station.get("name_slug"),
    }

    journeys: list[dict[str, Any]] = []
    for row in rows:
        brand_id = row.get("brand_id")
        if not _brand_ok(brand_id):
            continue
        stations = row.get("stations") or []
        if not stations:
            continue
        other = stations[0]
        train_id = other.get("train_id")
        if not train_id:
            continue

        try:
            if mode == "departures":
                here_time = _to_dt(row["departure"], date) if row.get("departure") else None
                from_info = {**station_info, "time": here_time.strftime("%H:%M") if here_time else None}
                to_info = {
                    "id": other.get("id"),
                    "name": other.get("name"),
                    "slug": other.get("slug"),
                    "time": None,
                }
            else:
                here_time = _to_dt(row["arrival"], date) if row.get("arrival") else None
                from_info = {
                    "id": other.get("id"),
                    "name": other.get("name"),
                    "slug": other.get("slug"),
                    "time": None,
                }
                to_info = {**station_info, "time": here_time.strftime("%H:%M") if here_time else None}
        except Exception:
            continue

        full_name = row.get("train_full_name") or ""
        journeys.append({
            "train_id": train_id,
            "train_nr": _parse_train_nr(full_name),
            "train_full_name": full_name,
            "name": _display_name({"train_full_name": full_name}),
            "brand": BRAND_NAMES.get(brand_id, str(brand_id)),
            "brand_id": brand_id,
            "from": from_info,
            "to": to_info,
            "duration_min": None,
        })
    return journeys


def _parse_train_nr(full_name: str | None) -> int | None:
    if not full_name:
        return None
    head = full_name.strip().split(" ", 1)[0]
    try:
        return int(head)
    except ValueError:
        return None


async def search_journeys(
    client: KoleoAPI,
    *,
    from_q: str | None,
    to_q: str | None,
    date: datetime,
    time_str: str | None = None,
    number: str | None = None,
    cache: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Flexible candidate-train search.

    At least one of `from_q` / `to_q` must be provided. `time_str` (HH:MM) narrows
    to a ±60 min window around that clock time. `number` filters by exact train
    number or substring in `train_full_name`.
    """
    cache = cache or {}
    from_q = (from_q or "").strip()
    to_q = (to_q or "").strip()
    if not from_q and not to_q:
        raise SeatFinderError("Provide at least an origin or destination station.")

    number = (number or "").strip() or None
    target_clock_dt: datetime | None = None
    if time_str:
        try:
            hh, mm = map(int, time_str.split(":"))
            target_clock_dt = date.replace(hour=hh, minute=mm, second=0, microsecond=0)
        except Exception:
            raise SeatFinderError("Niepoprawny format godziny.")

    journeys: list[dict[str, Any]] = []

    if from_q and to_q:
        from_station, to_station = await _resolve_stations(
            client, from_q, to_q, cache
        )
        query_dt = target_clock_dt - timedelta(minutes=5) if target_clock_dt else date
        connections = await client.get_connections(
            start=from_station["name_slug"],
            end=to_station["name_slug"],
            brand_ids=SUPPORTED_BRAND_IDS,
            date=query_dt,
            direct=True,
        )
        for c in connections:
            t = c["trains"][0]
            if not _brand_ok(t.get("brand_id")):
                continue
            journeys.append(_journey_from_connection(c, date, from_station, to_station))
    else:
        # Single endpoint: look up departures or arrivals at that station
        mode = "departures" if from_q else "arrivals"
        query = from_q or to_q
        station = await _resolve_one(client, query, cache)
        journeys = await _journeys_from_single_endpoint(client, station, date, mode)

    # Time filter (±60 min window)
    if target_clock_dt:
        window = timedelta(minutes=60)
        filtered: list[dict[str, Any]] = []
        for j in journeys:
            try:
                hh, mm = map(int, j["from"]["time"].split(":"))
                dep = date.replace(hour=hh, minute=mm, second=0, microsecond=0)
            except Exception:
                continue
            if abs(dep - target_clock_dt) <= window:
                filtered.append(j)
        journeys = filtered

    # Number filter
    if number:
        journeys = [
            j for j in journeys
            if str(j.get("train_nr") or "") == number
            or (number.upper() in (j.get("train_full_name") or "").upper())
            or (number.upper() in (j.get("name") or "").upper())
        ]

    # Sort by departure time
    journeys.sort(key=lambda j: j["from"].get("time") or "")
    return journeys


async def get_train_stops(
    client: KoleoAPI,
    train_id: int,
    date: datetime,
) -> dict[str, Any]:
    """Return the full stop timeline for a train on a given operating day."""
    detail = await client.get_train(train_id)
    stops = await _route_stops(client, train_id, None, None, date)
    train = detail["train"]
    brand_id = train.get("brand_id")
    return {
        "train_id": train_id,
        "train_nr": train.get("train_nr"),
        "train_full_name": train.get("train_full_name"),
        "name": _display_name(train),
        "brand": BRAND_NAMES.get(brand_id, str(brand_id or "")),
        "brand_id": brand_id,
        "stops": [
            {
                "station_id": s.station_id,
                "station_name": s.station_name,
                "station_slug": s.station_slug,
                "position": s.position,
                "arrival": s.arrival_dt.strftime("%H:%M"),
                "departure": s.departure_dt.strftime("%H:%M"),
            }
            for s in stops
        ],
    }


async def find_seats_by_train(
    client: KoleoAPI,
    train_id: int,
    from_station_id: int,
    to_station_id: int,
    date: datetime,
) -> dict[str, Any]:
    """Seat recommendations for a specific train with explicit from/to."""
    try:
        detail = await client.get_train(train_id)
        train = detail["train"]
        brand_id = train.get("brand_id")
        if not _brand_ok(brand_id):
            raise SeatFinderError(
                "This train doesn't support seat reservations (regional). "
                "Nothing to dodge — just grab any free seat."
            )
        train_nr = train.get("train_nr")

        stops = await _route_stops(
            client, train_id, from_station_id, to_station_id, date
        )
        if len(stops) < 2:
            raise SeatFinderError("Couldn't rebuild this train's route.")

        sem = asyncio.Semaphore(PER_SEGMENT_CONCURRENCY)
        stops, seg_conn_ids, leg_train_nr = await _resolve_segments(
            client, sem, stops, train_id
        )

        seats_sem = asyncio.Semaphore(PER_SEGMENT_CONCURRENCY)
        segment_responses = await asyncio.gather(*[
            _fetch_segment_seats(
                client, seats_sem, cid, leg_train_nr,
                f"{stops[i].station_name} → {stops[i + 1].station_name}",
            )
            for i, cid in enumerate(seg_conn_ids)
        ])

        compartment_lookup: dict[int, dict[str, Any]] = {}
        for resp in segment_responses:
            for comp in resp.get("special_compartment_types") or []:
                compartment_lookup.setdefault(comp["id"], comp)

        n_segments = len(segment_responses)
        seat_map: dict[tuple[str, str], dict[str, Any]] = {}
        for i, resp in enumerate(segment_responses):
            for seat in resp["seats"]:
                key = (seat["carriage_nr"], seat["seat_nr"])
                entry = seat_map.setdefault(
                    key,
                    {
                        "placement_id": seat.get("placement_id"),
                        "special_compartment_type_id": seat.get("special_compartment_type_id"),
                        "states": ["FREE"] * n_segments,
                    },
                )
                entry["states"][i] = seat["state"]

        recs = _rank_and_select(seat_map, stops, compartment_lookup)

        origin, destination = stops[0], stops[-1]
        total_duration_min = round(
            (destination.arrival_dt - origin.departure_dt).total_seconds() / 60
        )

        return {
            "train": {
                "number": str(train_nr),
                "brand": BRAND_NAMES.get(brand_id, str(brand_id)),
                "name": _display_name(train),
                "total_duration_min": total_duration_min,
            },
            "route": {
                "from": {
                    "name": origin.station_name,
                    "departure": origin.departure_dt.strftime("%H:%M"),
                },
                "to": {
                    "name": destination.station_name,
                    "arrival": destination.arrival_dt.strftime("%H:%M"),
                },
            },
            "recommendations": recs,
        }

    except SeatFinderError:
        raise
    except koleo_errors.KoleoAPIException as e:
        raise SeatFinderError(f"Koleo API error (HTTP {e.status}).") from e
    except Exception as e:
        raise SeatFinderError(f"Unexpected failure: {e}") from e


async def find_seats(
    client: KoleoAPI,
    from_query: str,
    to_query: str,
    departure_dt: datetime,
    station_cache: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Legacy one-shot flow: resolve stations, pick train within ±2 min, recommend."""
    try:
        from_station, to_station = await _resolve_stations(
            client, from_query, to_query, station_cache
        )
        connection = await _find_train_connection(
            client, from_station["name_slug"], to_station["name_slug"], departure_dt,
        )
        train = connection["trains"][0]
        return await find_seats_by_train(
            client, train["train_id"], from_station["id"], to_station["id"], departure_dt,
        )
    except SeatFinderError:
        raise
    except koleo_errors.KoleoAPIException as e:
        raise SeatFinderError(f"Koleo API error (HTTP {e.status}).") from e


async def _find_train_connection(
    client: KoleoAPI,
    from_slug: str,
    to_slug: str,
    wanted_dt: datetime,
) -> dict[str, Any]:
    query_from = wanted_dt - timedelta(minutes=5)
    connections = await client.get_connections(
        start=from_slug, end=to_slug,
        brand_ids=SUPPORTED_BRAND_IDS,
        date=query_from, direct=True,
    )
    if not connections:
        raise SeatFinderError(
            f"No direct IC/TLK/EIC/EIP train found around "
            f"{wanted_dt:%H:%M} from \"{from_slug}\"."
        )
    best = None
    best_delta = timedelta(minutes=3)
    for c in connections:
        t = c["trains"][0]
        dep = _to_dt(t["departure"], wanted_dt)
        delta = abs(dep - wanted_dt)
        if delta <= timedelta(minutes=2) and delta < best_delta:
            best_delta = delta
            best = c
    if best is None:
        nearest = min(
            connections,
            key=lambda c: abs(_to_dt(c["trains"][0]["departure"], wanted_dt) - wanted_dt),
        )
        got = _to_dt(nearest["trains"][0]["departure"], wanted_dt).strftime("%H:%M")
        raise SeatFinderError(
            f"No train found at {wanted_dt:%H:%M}. Nearest departure: {got}."
        )
    return best
