"""
NYC Open Data (Socrata) API reader for yellow taxi trip data.

Design notes:

- Pagination uses $limit/$offset, which is what the Socrata SODA API
  exposes for this dataset. This is NOT a cursor-based or keyset
  pagination scheme, so it is technically possible (though unlikely at
  our poll size) for a row to be skipped or duplicated if the
  underlying dataset changes between page requests. Worth being able
  to say this out loud in an interview rather than pretending the
  pagination is airtight.

- trip_id is NOT provided by the source dataset. We derive it
  deterministically (hash of vendor_id + pickup_datetime + fare_amount)
  rather than generating a random UUID per read. This matters: if we
  used random UUIDs, re-ingesting the same underlying rows (e.g. after
  a crash and restart) would create duplicate rows with different IDs,
  and the Day 2 uniqueness check would never catch it. A deterministic
  ID makes "duplicate row" and "duplicate ID" the same thing.
"""

import hashlib
from datetime import datetime, timezone

import requests

NYC_API_BASE_URL = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_PAGE_SIZE = 1000


def _make_trip_id(vendor_id: str, pickup_datetime: str, fare_amount: str) -> str:
    """Deterministic trip_id from stable source fields. See module docstring."""
    raw = f"{vendor_id}|{pickup_datetime}|{fare_amount}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def _map_record(record: dict, run_id: str) -> dict | None:
    """Map a raw Socrata JSON record to our internal taxi_trips schema.

    Returns None (rather than raising) for records missing fields we
    consider non-negotiable for identity purposes (vendor_id,
    pickup_datetime). We don't silently drop malformed rows without a
    trace, though -- the caller counts and reports how many were
    skipped, since that count itself is a data-quality signal worth
    surfacing rather than hiding.
    """
    vendor_id = record.get("vendor_id") or record.get("vendorid")
    pickup_dt = record.get("pickup_datetime") or record.get("tpep_pickup_datetime")

    if not vendor_id or not pickup_dt:
        return None

    fare_amount = record.get("fare_amount", "0")

    return {
        "trip_id": _make_trip_id(str(vendor_id), str(pickup_dt), str(fare_amount)),
        "vendor_id": str(vendor_id),
        "pickup_datetime": pickup_dt,
        "dropoff_datetime": record.get("dropoff_datetime") or record.get("tpep_dropoff_datetime"),
        "passenger_count": record.get("passenger_count"),
        "trip_distance": record.get("trip_distance"),
        "fare_amount": fare_amount,
        "tip_amount": record.get("tip_amount"),
        "payment_type": record.get("payment_type"),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
    }


def fetch_trips(
    run_id: str,
    limit: int = DEFAULT_PAGE_SIZE,
    max_records: int = 1000,
) -> tuple[list[dict], int]:
    """Fetch up to max_records trip rows from the NYC Open Data API.

    Returns (mapped_rows, skipped_count). Pagination stops early if a
    page comes back with fewer rows than requested (end of dataset) or
    once max_records is reached.

    A requests.Session is used (rather than bare requests.get calls)
    so TCP connections are reused across pages -- meaningful when
    polling every 60 seconds, since it avoids a fresh TLS handshake
    per page.
    """
    session = requests.Session()
    mapped_rows: list[dict] = []
    skipped = 0
    offset = 0

    while len(mapped_rows) < max_records:
        page_limit = min(limit, max_records - len(mapped_rows))
        params = {"$limit": page_limit, "$offset": offset}

        response = session.get(
            NYC_API_BASE_URL, params=params, timeout=DEFAULT_TIMEOUT_SECONDS
        )
        response.raise_for_status()
        page = response.json()

        if not page:
            break  # end of dataset

        for record in page:
            mapped = _map_record(record, run_id)
            if mapped is None:
                skipped += 1
            else:
                mapped_rows.append(mapped)

        if len(page) < page_limit:
            break  # short page => no more data

        offset += page_limit

    return mapped_rows, skipped
