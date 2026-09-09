"""
Day 1 concept tests: Snowflake connector behavior.

These document understanding, not just verify code -- each test
encodes one fact I learned and would need to explain in an interview.
"""


def test_default_cursor_returns_tuples_not_dicts():
    """
    I learned: snowflake.connector's default cursor.fetchall() returns
    a list of tuples. Columns are accessed by position (row[0]), which
    silently breaks if a SELECT's column order changes -- there's no
    error, just wrong data landing in the wrong field.
    """
    # Simulates what conn.cursor().fetchall() returns by default
    default_cursor_result = [("run_1", "success", 1000), ("run_2", "failed", 0)]
    assert isinstance(default_cursor_result[0], tuple)
    assert default_cursor_result[0][1] == "success"  # position-dependent access


def test_dict_cursor_returns_dicts_keyed_by_column_name():
    """
    I learned: passing DictCursor to conn.cursor() changes the shape of
    fetchall() results to dicts keyed by column name. This decouples
    calling code from SELECT column ORDER -- reordering columns in a
    query no longer silently corrupts downstream field access.
    """
    # Simulates what conn.cursor(DictCursor).fetchall() returns
    dict_cursor_result = [
        {"run_id": "run_1", "status": "success", "rows_ingested": 1000},
        {"run_id": "run_2", "status": "failed", "rows_ingested": 0},
    ]
    assert isinstance(dict_cursor_result[0], dict)
    assert dict_cursor_result[0]["status"] == "success"  # name-dependent, order-safe


def test_executemany_sends_one_batch_not_n_round_trips():
    """
    I learned: cur.executemany(sql, list_of_param_dicts) sends the
    whole batch as a single request. Looping cur.execute() per row
    would mean one network round-trip per row -- the same N+1 problem
    people associate with ORMs, but it applies to raw DB-API cursors
    just as much.
    """
    rows_to_insert = [{"trip_id": "a"}, {"trip_id": "b"}, {"trip_id": "c"}]
    # executemany takes the whole list in one call -- no per-row loop
    # calling .execute() individually.
    simulated_round_trips_with_executemany = 1
    simulated_round_trips_with_execute_loop = len(rows_to_insert)
    assert simulated_round_trips_with_executemany < simulated_round_trips_with_execute_loop


def test_connection_context_manager_closes_on_exit():
    """
    I learned: wrapping snowflake.connector.connect() in a
    @contextmanager that closes in `finally` guarantees the session
    ends even if the code inside `with get_connection() as conn:`
    raises. This matters because Snowflake bills warehouse compute
    while a session is active -- a leaked connection is a leaked cost,
    not just a resource leak.
    """
    closed = {"value": False}

    class FakeConn:
        def close(self):
            closed["value"] = True

    def fake_get_connection():
        conn = FakeConn()
        try:
            yield conn
        finally:
            conn.close()

    gen = fake_get_connection()
    next(gen)  # advance to the yield, acquiring the fake connection
    try:
        next(gen)
    except StopIteration:
        pass

    assert closed["value"] is True
