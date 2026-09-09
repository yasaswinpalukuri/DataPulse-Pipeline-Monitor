"""Day 1 concept tests: NYC Open Data (Socrata) API ingestion patterns."""

import hashlib

from ingestion.nyc_api_reader import _make_trip_id, _map_record


def test_pagination_is_offset_based_not_cursor_based():
    """
    I learned: the Socrata SODA API paginates via $limit/$offset query
    params, not an opaque cursor/token. This means pagination is not
    airtight against concurrent writes to the underlying dataset --
    a row could theoretically be skipped or duplicated across two page
    requests if the dataset changes mid-pagination. Worth stating this
    limitation explicitly rather than assuming pagination is safe.
    """
    page_1_params = {"$limit": 1000, "$offset": 0}
    page_2_params = {"$limit": 1000, "$offset": 1000}
    assert page_2_params["$offset"] == page_1_params["$offset"] + page_1_params["$limit"]


def test_trip_id_is_deterministic_not_random():
    """
    I learned: the source dataset has no stable trip_id, so I derive
    one via hashing stable fields (vendor_id + pickup_datetime +
    fare_amount). Deterministic IDs mean re-ingesting the same
    underlying row twice (e.g. after a crash/restart) produces the
    SAME id both times -- which is what makes a uniqueness check on
    trip_id meaningful. A random UUID per read would make every
    "duplicate" look unique and the check would never fire.
    """
    id_first_read = _make_trip_id("VTS", "2024-01-01T08:00:00", "12.50")
    id_second_read = _make_trip_id("VTS", "2024-01-01T08:00:00", "12.50")
    assert id_first_read == id_second_read

    different_fare = _make_trip_id("VTS", "2024-01-01T08:00:00", "15.00")
    assert different_fare != id_first_read


def test_trip_id_matches_manual_hash_computation():
    """
    I learned: the trip_id hash is just sha256 of the pipe-joined
    fields, truncated to 32 hex chars. Documenting the exact
    construction so it's not a black box.
    """
    raw = "VTS|2024-01-01T08:00:00|12.50"
    expected = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]
    assert _make_trip_id("VTS", "2024-01-01T08:00:00", "12.50") == expected


def test_map_record_skips_rows_missing_identity_fields():
    """
    I learned: rows without vendor_id or pickup_datetime can't get a
    valid trip_id (both feed the hash), so _map_record returns None
    for them rather than inserting a row with a garbage/incomplete
    identity. The caller counts skips instead of silently losing rows.
    """
    incomplete_record = {"fare_amount": "10.00"}  # missing vendor_id, pickup_datetime
    assert _map_record(incomplete_record, run_id="test-run") is None

    complete_record = {
        "vendor_id": "VTS",
        "pickup_datetime": "2024-01-01T08:00:00",
        "fare_amount": "10.00",
    }
    assert _map_record(complete_record, run_id="test-run") is not None
