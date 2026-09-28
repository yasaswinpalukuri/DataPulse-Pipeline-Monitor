"""
NYC Open Data (Socrata) reader for 2023 Yellow Taxi trips, one logical day at a time.

Design notes (each is a likely interview question):

- Date-windowed pulls, not "offset 0 every run". Each run asks for exactly one
  logical date's pickups via $where. The old reader always started at offset 0,
  so every scheduled run re-fetched the same first N rows of the dataset.

- $order=:id makes offset pagination stable. Socrata does not guarantee row
  order without $order, so $limit/$offset alone could skip or repeat rows
  across pages. :id is Socrata's internal row id, a stable total order.

- trip_id is a content hash of the FULL source record (all columns, fixed
  order). The previous hash (vendor|pickup|fare) collided on genuinely
  different trips -- same vendor, same second, flat JFK fare. Hashing every
  column means two rows share an id only if they are identical in every field.
  Corrections to a source row produce a new id; the loader's partition
  overwrite (delete the day, re-insert) is what keeps that correct.

- Numbers arrive as strings ("1.0"). We parse them here into int/float/None
  so downstream quality checks compare numbers, not strings. A value that
  won't parse becomes None rather than crashing the run -- the quality gate
  then decides whether that's acceptable.
"""

import hashlib
from datetime import date, datetime, timedelta, timezone

import requests

NYC_API_BASE_URL = "https://data.cityofnewyork.us/resource/4b4i-vvec.json"  # 2023 Yellow Taxi
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_PAGE_SIZE = 10_000

# Every source column, in a FIXED order. The hash depends on this order, so
# changing it changes every trip_id -- treat it like a schema contract.
SOURCE_COLUMNS = [
    "vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
    "trip_distance", "ratecodeid", "store_and_fwd_flag", "pulocationid", "dolocationid",
    "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee",
]

INT_FIELDS = {
    "vendor_id": "vendorid",
    "passenger_count": "passenger_count",
    "ratecode_id": "ratecodeid",
    "pu_location_id": "pulocationid",
    "do_location_id": "dolocationid",
    "payment_type": "payment_type",
}
FLOAT_FIELDS = {
    name: name
    for name in [
        "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
        "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee",
    ]
}


def make_trip_id(record: dict) -> str:
    """sha256 over every source column in SOURCE_COLUMNS order, truncated to 128 bits.

    Missing columns hash as an empty string, so a record's id doesn't depend
    on whether Socrata omitted a null key or sent it explicitly.
    """
    raw = "|".join(str(record.get(col, "")).strip() for col in SOURCE_COLUMNS)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def _to_float(value) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value) -> int | None:
    """'1.0' -> 1. Socrata serialises these integer codes as decimal strings."""
    as_float = _to_float(value)
    return int(as_float) if as_float is not None else None


def map_record(record: dict, run_id: str, ingested_at: str) -> dict | None:
    """Map one Socrata record to the raw.taxi_trips shape, or None if it has no identity.

    vendorid and tpep_pickup_datetime are the minimum for a row to mean
    anything (and pickup date drives partition overwrite), so rows missing
    either are skipped and counted by the caller rather than silently lost.
    """
    if not record.get("vendorid") or not record.get("tpep_pickup_datetime"):
        return None

    row = {
        "trip_id": make_trip_id(record),
        "pickup_datetime": record["tpep_pickup_datetime"],
        "dropoff_datetime": record.get("tpep_dropoff_datetime"),
        "store_and_fwd_flag": record.get("store_and_fwd_flag"),
        "ingested_at": ingested_at,
        "run_id": run_id,
    }
    row.update({out: _to_int(record.get(src)) for out, src in INT_FIELDS.items()})
    row.update({out: _to_float(record.get(src)) for out, src in FLOAT_FIELDS.items()})
    return row


def build_day_params(logical_date: date, limit: int, offset: int) -> dict:
    """Socrata query for one logical day, stably ordered for offset paging."""
    start = logical_date.isoformat()
    end = (logical_date + timedelta(days=1)).isoformat()
    return {
        "$where": (
            f"tpep_pickup_datetime >= '{start}T00:00:00' "
            f"AND tpep_pickup_datetime < '{end}T00:00:00'"
        ),
        "$order": ":id",
        "$limit": limit,
        "$offset": offset,
    }


def fetch_trips_for_date(
    logical_date: date,
    run_id: str,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_records: int | None = None,
) -> tuple[list[dict], int, set[str]]:
    """Fetch all trips picked up on logical_date (optionally capped).

    Returns (mapped_rows, skipped_count, observed_source_columns). The observed
    columns are the union of keys across all records -- Socrata omits a key
    when its value is null, so a single record can't tell you the schema. The
    quality gate compares this set against SOURCE_COLUMNS. max_records=None means the full day
    (~80-110k rows for a 2023 day); a cap is only for quick local testing.
    """
    session = requests.Session()  # reuse one TCP/TLS connection across pages
    ingested_at = datetime.now(timezone.utc).isoformat()
    rows: list[dict] = []
    skipped = 0
    offset = 0
    observed_columns: set[str] = set()

    while max_records is None or len(rows) + skipped < max_records:
        limit = page_size if max_records is None else min(
            page_size, max_records - len(rows) - skipped
        )
        response = session.get(
            NYC_API_BASE_URL,
            params=build_day_params(logical_date, limit, offset),
            timeout=DEFAULT_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        page = response.json()

        for record in page:
            observed_columns.update(record)
            mapped = map_record(record, run_id, ingested_at)
            if mapped is None:
                skipped += 1
            else:
                rows.append(mapped)

        if len(page) < limit:
            break  # short (or empty) page: end of this day's data
        offset += limit

    return rows, skipped, observed_columns
