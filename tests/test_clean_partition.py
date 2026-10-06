"""Clean-layer SQL: validation, quarantine reasons, deduplication and idempotency (DuckDB)."""
import datetime as dt
import pandas as pd
import pytest
duckdb = pytest.importorskip("duckdb")
from sqlutil import statements

D = dt.date(2015, 6, 1)
T = lambda h: int(dt.datetime(2015, 6, 1, h, 0, tzinfo=dt.timezone.utc).timestamp() * 1000)

def row(ts_ms, v, typ, item, txn=None, ts_override=None):
    ts = ts_override or dt.datetime.fromtimestamp(ts_ms / 1000, dt.timezone.utc).replace(tzinfo=None)
    return dict(event_date=D, event_ts_ms=ts_ms, event_ts=ts, visitor_id=v, event_type=typ, item_id=item,
                transaction_id=txn, _source_file="events.csv", _ingested_at=dt.datetime(2026, 1, 1))

def build_raw():
    good = [row(T(1), 1, "view", 10), row(T(2), 2, "addtocart", 11), row(T(3), 3, "transaction", 12, 700)]
    dup = [row(T(1), 1, "view", 10)]                                                    # exact duplicate of a good row
    bad = [row(T(4), 4, "purchase", 13),                                                # unknown event type
           row(T(5), 5, "view", None),                                                  # null key field
           row(T(6), 6, "transaction", 14, None),                                       # transaction without id
           row(T(7), 7, "view", 15, 999),                                               # non-transaction with id
           row(T(8), 8, "view", 16, ts_override=dt.datetime(1999, 1, 1)),               # implausible timestamp
           row(T(9), 9, "view", 17, ts_override=dt.datetime(2015, 5, 29, 9))]           # outside its partition date
    return pd.DataFrame(good + dup + bad)

@pytest.fixture()
def con():
    c = duckdb.connect()
    df = build_raw(); c.register("df", df)
    c.execute("CREATE TABLE raw_events AS SELECT * FROM df")
    for s in statements("01_schema.sql", raw_glob="unused")[1:]:      # skip the view statement, raw_events is a table here
        c.execute(s)
    return c

def run_partition(c):
    for s in statements("02_clean_partition.sql", d=D.isoformat(), min_ts="2010-01-01"):
        c.execute(s)

def test_rejects_and_dedup(con):
    run_partition(con)
    assert con.execute("SELECT COUNT(*) FROM clean_events").fetchone()[0] == 3            # 3 distinct good rows
    reasons = dict(con.execute("SELECT reject_reason, COUNT(*) FROM quarantine_events GROUP BY 1").fetchall())
    assert reasons == {"invalid_event_type": 1, "null_key_field": 1, "transaction_missing_id": 1,
                       "unexpected_transaction_id": 1, "timestamp_out_of_range": 1, "timestamp_partition_mismatch": 1}

def test_rerun_is_idempotent(con):
    run_partition(con)
    first = con.execute("SELECT COUNT(*), SUM(hash(event_id)) FROM clean_events").fetchone()
    run_partition(con)
    assert con.execute("SELECT COUNT(*), SUM(hash(event_id)) FROM clean_events").fetchone() == first
    assert con.execute("SELECT COUNT(*) FROM quarantine_events").fetchone()[0] == 6

def test_event_id_is_deterministic_and_unique(con):
    run_partition(con)
    n, distinct = con.execute("SELECT COUNT(*), COUNT(DISTINCT event_id) FROM clean_events").fetchone()
    assert n == distinct == 3
