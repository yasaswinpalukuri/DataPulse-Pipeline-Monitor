"""Concept tests: NYC Open Data (Socrata) ingestion by logical date."""

from datetime import date

from ingestion.nyc_api_reader import (
    SOURCE_COLUMNS,
    build_day_params,
    make_trip_id,
    map_record,
)

SAMPLE = {  # a real record from the 4b4i-vvec API (2023-01-01)
    "vendorid": "2", "tpep_pickup_datetime": "2023-01-01T00:32:10.000",
    "tpep_dropoff_datetime": "2023-01-01T00:40:36.000", "passenger_count": "1.0",
    "trip_distance": "0.97", "ratecodeid": "1.0", "store_and_fwd_flag": "N",
    "pulocationid": "161", "dolocationid": "141", "payment_type": "2",
    "fare_amount": "9.3", "extra": "1.0", "mta_tax": "0.5", "tip_amount": "0.0",
    "tolls_amount": "0.0", "improvement_surcharge": "1.0", "total_amount": "14.3",
    "congestion_surcharge": "2.5", "airport_fee": "0.0",
}


def test_trip_id_is_deterministic():
    """I learned: the same source record must always hash to the same id --
    that is what makes a uniqueness check on trip_id mean anything."""
    assert make_trip_id(SAMPLE) == make_trip_id(dict(SAMPLE))


def test_old_three_field_hash_would_collide_on_distinct_trips():
    """I learned: hashing only vendor|pickup|fare collides on genuinely
    different trips (same vendor, same second, flat fare, different
    destination). Hashing every column keeps them distinct."""
    other_trip = {**SAMPLE, "dolocationid": "132", "total_amount": "20.1"}
    old_key = lambda r: (r["vendorid"], r["tpep_pickup_datetime"], r["fare_amount"])  # noqa: E731
    assert old_key(SAMPLE) == old_key(other_trip)            # old scheme: collision
    assert make_trip_id(SAMPLE) != make_trip_id(other_trip)  # new scheme: distinct


def test_every_source_column_affects_the_hash():
    """I learned: the hash contract is 'all columns in SOURCE_COLUMNS order'.
    Changing any single field must change the id."""
    base = make_trip_id(SAMPLE)
    for col in SOURCE_COLUMNS:
        assert make_trip_id({**SAMPLE, col: "changed"}) != base, col


def test_map_record_parses_decimal_strings_to_numbers():
    """I learned: Socrata sends integer codes as '1.0'. Parse at ingestion so
    quality checks compare numbers, not strings ('10' < '9' as strings)."""
    row = map_record(SAMPLE, run_id="r", ingested_at="t")
    assert row["passenger_count"] == 1 and isinstance(row["passenger_count"], int)
    assert row["pu_location_id"] == 161
    assert row["fare_amount"] == 9.3


def test_unparseable_numbers_become_none_not_crashes():
    row = map_record({**SAMPLE, "fare_amount": "n/a"}, run_id="r", ingested_at="t")
    assert row["fare_amount"] is None


def test_map_record_skips_rows_without_identity():
    """I learned: no vendor or no pickup time -> no meaningful row and no
    partition to put it in; skip and count it rather than load garbage."""
    assert map_record({"fare_amount": "10"}, run_id="r", ingested_at="t") is None


def test_day_query_is_windowed_and_stably_ordered():
    """I learned: offset paging is only safe with a stable $order. Without it
    Socrata may return rows in a different order per request, so pages can
    skip or repeat rows. :id is Socrata's stable internal row id."""
    params = build_day_params(date(2023, 3, 31), limit=10_000, offset=20_000)
    assert params["$order"] == ":id"
    assert "'2023-03-31T00:00:00'" in params["$where"]
    assert "'2023-04-01T00:00:00'" in params["$where"]  # half-open window crosses month
    assert params["$offset"] == 20_000
